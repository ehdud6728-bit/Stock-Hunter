
# =============================================================
# closing_bet_scanner.py — 종가배팅 타점 스캐너 (v4.3.8 STOCK FEATURE RISK ANALYSIS)
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

# v4.4.9.28: GitHub Actions schedule 실행에서는 workflow_dispatch inputs가 빈 문자열로 들어올 수 있다.
# os.environ.get(name, default)는 환경변수가 존재하지만 값이 ''이면 default를 쓰지 않으므로,
# 숫자형 환경변수는 아래 안전 파서로 통일한다.
def _env_raw(name: str, default=None):
    raw = os.environ.get(name, None)
    if raw is None:
        return default
    raw = str(raw).strip()
    if raw == "":
        return default
    return raw


def _env_int(name: str, default: int = 0) -> int:
    raw = _env_raw(name, default)
    try:
        return int(float(raw))
    except Exception:
        try:
            return int(float(default))
        except Exception:
            return 0


def _env_float(name: str, default: float = 0.0) -> float:
    raw = _env_raw(name, default)
    try:
        return float(raw)
    except Exception:
        try:
            return float(default)
        except Exception:
            return 0.0

CLOSING_BET_SCANNER_VERSION = 'G_MORALES_V4_4_9_43_SJ_THRESHOLD_FAIL_COMMON_AUDIT_20260612'

# v4.3.5 I-CORE strict/supply/main-filter + I-MAIN CORE/ACCEL benchmark options
# 외국인/기관 pykrx 수급은 후보 수가 많으면 느릴 수 있어 옵션화한다.
I_CORE_FETCH_KRX_FLOW = str(os.environ.get('I_CORE_FETCH_KRX_FLOW', '0')).lower() in ('1', 'true', 'yes', 'y')
I_CORE_MIN_SCORE = _env_int('I_CORE_MIN_SCORE', '78')
I_CORE_MIN_AMOUNT_B = _env_float('I_CORE_MIN_AMOUNT_B', '50')
I_CORE_STRICT_PHASE_ONLY = str(os.environ.get('I_CORE_STRICT_PHASE_ONLY', '1')).lower() in ('1', 'true', 'yes', 'y')

# v4.3.3 MAIN FILTER: v4.3.2 결과에서 좋았던 조합을 별도 분해/선택한다.
# 기본은 리포트만 추가하고, I_CORE_MAIN_ONLY=1일 때만 I-CORE 전용 백테스트 결과를 MAIN 후보로 제한한다.
I_CORE_MAIN_ONLY = str(os.environ.get('I_CORE_MAIN_ONLY', '0')).lower() in ('1', 'true', 'yes', 'y')
I_CORE_MAIN_MIN_MATERIAL = _env_int('I_CORE_MAIN_MIN_MATERIAL', '3')
I_CORE_MAIN_LONG_MIN = _env_float('I_CORE_MAIN_LONG_MIN', '-3')
I_CORE_MAIN_LONG_MAX = _env_float('I_CORE_MAIN_LONG_MAX', '18')
I_CORE_MAIN_REQUIRE_OBV_AMOUNT = str(os.environ.get('I_CORE_MAIN_REQUIRE_OBV_AMOUNT', '1')).lower() in ('1', 'true', 'yes', 'y')

# v4.3.5 시장국면/벤치마크 초과수익 검증.
# 상승장 착시를 줄이기 위해 I-CORE 20/40/60일 수익률에서 KOSPI/KOSDAQ 동기간 수익률을 차감한다.
I_CORE_REGIME_VALIDATE = str(os.environ.get('I_CORE_REGIME_VALIDATE', '1')).lower() in ('1', 'true', 'yes', 'y')
I_CORE_COMPARE_KOSDAQ = str(os.environ.get('I_CORE_COMPARE_KOSDAQ', '1')).lower() in ('1', 'true', 'yes', 'y')
I_CORE_MARKET_INDEX = str(os.environ.get('I_CORE_MARKET_INDEX', 'KS11')).strip() or 'KS11'
I_CORE_KOSDAQ_INDEX = str(os.environ.get('I_CORE_KOSDAQ_INDEX', 'KQ11')).strip() or 'KQ11'
I_CORE_MARKET_START = str(os.environ.get('I_CORE_MARKET_START', '2018-01-01')).strip() or '2018-01-01'

# v4.3.8: 차트 외 종목특성별 성공/손절 원인 분석 리포트.
# 시장구분/시총/거래대금/리더십/수급프록시 기준으로 패턴별 손절 다발 구간을 찾는다.
CLOSING_BET_STOCK_FEATURE_REPORT = str(os.environ.get('CLOSING_BET_STOCK_FEATURE_REPORT', '1')).lower() in ('1', 'true', 'yes', 'y')
CLOSING_BET_STOCK_FEATURE_MIN_N = _env_int('CLOSING_BET_STOCK_FEATURE_MIN_N', '5')

# v4.3.9: 텔레그램 운용 요약 압축판.
# 기존 v4.3.8 계산/CSV/HTML 저장은 유지하고, Telegram에는 실전 결론/핵심 성과/중복제거 샘플만 보낸다.
CLOSING_BET_COMPACT_OPERATION_SUMMARY = str(os.environ.get('CLOSING_BET_COMPACT_OPERATION_SUMMARY', '1')).lower() in ('1', 'true', 'yes', 'y', 'on')
CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N = _env_int('CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N', _env_int('CLOSING_BET_BACKTEST_DETAIL_TOP_N', 5))
CLOSING_BET_SHOW_FULL_BACKTEST_REPORT = str(os.environ.get('CLOSING_BET_SHOW_FULL_BACKTEST_REPORT', '0')).lower() in ('1', 'true', 'yes', 'y', 'on')
CLOSING_BET_SHOW_FULL_BROAD_DIAG = str(os.environ.get('CLOSING_BET_SHOW_FULL_BROAD_DIAG', '0')).lower() in ('1', 'true', 'yes', 'y', 'on')



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


# v4.4.9.32: main7_bugfix_2 import 전에 KRX secret 별칭을 맞춘다.
# GitHub Actions에서는 KRX_DATA_ID/KRX_DATA_PW를 쓰고, 일부 기존 코드(main7 계열)는 KRX_ID/KRX_PW를 찾는다.
def _sync_krx_env_aliases():
    try:
        data_id = _env_raw('KRX_DATA_ID', '') or _env_raw('KRX_ID', '')
        data_pw = _env_raw('KRX_DATA_PW', '') or _env_raw('KRX_PW', '')
        if data_id and not str(os.environ.get('KRX_ID', '') or '').strip():
            os.environ['KRX_ID'] = str(data_id)
        if data_pw and not str(os.environ.get('KRX_PW', '') or '').strip():
            os.environ['KRX_PW'] = str(data_pw)
        if data_id and not str(os.environ.get('KRX_DATA_ID', '') or '').strip():
            os.environ['KRX_DATA_ID'] = str(data_id)
        if data_pw and not str(os.environ.get('KRX_DATA_PW', '') or '').strip():
            os.environ['KRX_DATA_PW'] = str(data_pw)
    except Exception:
        pass

_sync_krx_env_aliases()

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
# v4.4.8.1: v4.4.8 기능 유지 + 실전 출력 문구 정리.
# 기본값은 전용방 강제(CLOSING_BET_REQUIRE_DEDICATED_CHAT=1)이며, 전용 CHAT_ID가 없으면 TELEGRAM_CHAT_ID로 fallback하지 않는다.
def _env_first(*names: str) -> tuple[str, str]:
    for name in names:
        v = str(os.environ.get(name, '') or '').strip()
        if v:
            return v, name
    return '', ''

def _bool_env(name: str, default: str = '0') -> bool:
    return str(os.environ.get(name, default)).strip().lower() in ('1', 'true', 'yes', 'y', 'on')

CLOSING_BET_REQUIRE_DEDICATED_CHAT = _bool_env('CLOSING_BET_REQUIRE_DEDICATED_CHAT', '1')
_CLOSING_BET_TOKEN_RAW, CLOSING_BET_TOKEN_SOURCE = _env_first('TELEGRAM_CLOSEBET_TOKEN', 'TELEGRAM_CLOSE_BET_TOKEN', 'CLOSING_BET_TOKEN', 'CLOSE_BET_TOKEN', 'CLOSING_BET_TELEGRAM_TOKEN', 'CLOSE_BET_TELEGRAM_TOKEN')
_DEFAULT_TELEGRAM_TOKEN_RAW, DEFAULT_TELEGRAM_TOKEN_SOURCE = _env_first('TELEGRAM_TOKEN')
_CLOSING_BET_CHAT_RAW, CLOSING_BET_CHAT_SOURCE = _env_first('TELEGRAM_DYUL_CHAT_ID', 'TELEGRAM_CLOSEBET_CHAT_ID', 'TELEGRAM_CLOSE_BET_CHAT_ID', 'TELEGRAM_CLOSEBET_ROOM_ID', 'TELEGRAM_CLOSE_BET_ROOM_ID', 'CLOSING_BET_CHAT_ID', 'CLOSE_BET_CHAT_ID', 'CLOSING_BET_ROOM_ID', 'CLOSE_BET_ROOM_ID', 'CLOSING_BET_TELEGRAM_CHAT_ID', 'CLOSE_BET_TELEGRAM_CHAT_ID')
_DEFAULT_TELEGRAM_CHAT_RAW, DEFAULT_TELEGRAM_CHAT_SOURCE = _env_first('TELEGRAM_CHAT_ID')

TELEGRAM_TOKEN = _CLOSING_BET_TOKEN_RAW or _DEFAULT_TELEGRAM_TOKEN_RAW
TELEGRAM_TOKEN_SOURCE = CLOSING_BET_TOKEN_SOURCE or DEFAULT_TELEGRAM_TOKEN_SOURCE or 'EMPTY'

if _CLOSING_BET_CHAT_RAW:
    _ACTIVE_CHAT_RAW = _CLOSING_BET_CHAT_RAW
    TELEGRAM_CHAT_SOURCE = CLOSING_BET_CHAT_SOURCE
    TELEGRAM_ROUTE_LABEL = 'CLOSING_BET 전용방'
elif CLOSING_BET_REQUIRE_DEDICATED_CHAT:
    _ACTIVE_CHAT_RAW = ''
    TELEGRAM_CHAT_SOURCE = 'EMPTY_FORCED_CLOSING_BET'
    TELEGRAM_ROUTE_LABEL = '전용방 미설정 — 전송차단'
else:
    _ACTIVE_CHAT_RAW = _DEFAULT_TELEGRAM_CHAT_RAW
    TELEGRAM_CHAT_SOURCE = DEFAULT_TELEGRAM_CHAT_SOURCE or 'EMPTY'
    TELEGRAM_ROUTE_LABEL = '기본 TELEGRAM 방 fallback'

CHAT_ID_LIST = [c.strip() for c in str(_ACTIVE_CHAT_RAW or '').split(',') if c.strip()]

def _telegram_route_status() -> str:
    token_state = 'SET' if TELEGRAM_TOKEN else 'EMPTY'
    chat_state = 'SET' if CHAT_ID_LIST else 'EMPTY'
    force_state = 'ON' if CLOSING_BET_REQUIRE_DEDICATED_CHAT else 'OFF'
    return (
        f"route={TELEGRAM_ROUTE_LABEL} | token={token_state}({TELEGRAM_TOKEN_SOURCE}) | "
        f"chat={chat_state}({TELEGRAM_CHAT_SOURCE}) | dedicated_required={force_state}"
    )

def _telegram_route_ready() -> bool:
    if not TELEGRAM_TOKEN:
        try:
            log_error('❌ TELEGRAM_TOKEN/CLOSING_BET_TOKEN 없음 — 텔레그램 전송 차단')
        except Exception:
            pass
        return False
    if not CHAT_ID_LIST:
        try:
            log_error('❌ 종가배팅 전용 CHAT_ID 없음 — 기본방 오발송 방지를 위해 텔레그램 전송 차단')
            log_error('   GitHub Secrets/Variables/Input 중 하나에 TELEGRAM_CLOSEBET_CHAT_ID, CLOSING_BET_CHAT_ID, CLOSE_BET_CHAT_ID, CLOSING_BET_ROOM_ID, CLOSING_BET_TELEGRAM_CHAT_ID를 설정하세요.')
        except Exception:
            pass
        return False
    if CLOSING_BET_REQUIRE_DEDICATED_CHAT and not _CLOSING_BET_CHAT_RAW:
        try:
            log_error('❌ 전용방 강제 모드인데 CLOSING_BET_CHAT_ID가 비어 있음 — 기본방 fallback 금지')
        except Exception:
            pass
        return False
    return True

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '')
CLOSING_BET_DEBATE_TOP_N = _env_int('CLOSING_BET_DEBATE_TOP_N', '7')

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
SCAN_UNIVERSE = _env_raw('CLOSING_BET_SCAN_UNIVERSE', 'hybrid_union')

MAX_WORKERS = _env_int('CLOSING_BET_MAX_WORKERS', '12')
SCAN_FUTURES_TIMEOUT = _env_int('CLOSING_BET_FUTURES_TIMEOUT', '540')

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
GAP_MIN_PCT = _env_float('CLOSING_BET_GAP_MIN_PCT', '2.0')
GAP_MAX_PCT = _env_float('CLOSING_BET_GAP_MAX_PCT', '12.0')
GAP_VOL50_MULT = _env_float('CLOSING_BET_GAP_VOL50_MULT', '1.5')
GAP_LOW_KEEP_PCT = _env_float('CLOSING_BET_GAP_LOW_KEEP_PCT', '0.5')
GAP_CLOSE_OPEN_KEEP = _env_float('CLOSING_BET_GAP_CLOSE_OPEN_KEEP', '0.995')
GAP_BOX_LOOKBACK = _env_int('CLOSING_BET_GAP_BOX_LOOKBACK', '60')
GAP_HIGH_LOOKBACK = _env_int('CLOSING_BET_GAP_HIGH_LOOKBACK', '120')
GAP_DISPARITY20_MAX = _env_float('CLOSING_BET_GAP_DISPARITY20_MAX', '118.0')
GAP_RUNUP20_MAX = _env_float('CLOSING_BET_GAP_RUNUP20_MAX', '35.0')
GAP_UPPER_WICK_MAX = _env_float('CLOSING_BET_GAP_UPPER_WICK_MAX', '0.25')
GAP_LARGE_CAP_MARCAP = _env_float('CLOSING_BET_GAP_LARGE_CAP_MARCAP', '5000000000000')  # 5조
LEADER_GAP_MIN_AMOUNT_B = _env_float('CLOSING_BET_LEADER_GAP_MIN_AMOUNT_B', '3000')  # 3000억 이상
LEADER_GAP_CORE_AMOUNT_B = _env_float('CLOSING_BET_LEADER_GAP_CORE_AMOUNT_B', '5000')  # 5000억 이상 핵심
LEADER_GAP_MIN_PCT = _env_float('CLOSING_BET_LEADER_GAP_MIN_PCT', '1.5')
LEADER_GAP_MAX_PCT = _env_float('CLOSING_BET_LEADER_GAP_MAX_PCT', '12.0')
LEADER_GAP_VOL50_MIN = _env_float('CLOSING_BET_LEADER_GAP_VOL50_MIN', '1.2')
LEADER_GAP_CLOSE_LOC_MIN = _env_float('CLOSING_BET_LEADER_GAP_CLOSE_LOC_MIN', '65.0')

# 전략 S — 고점권 재응축 2차 슈팅 종가매수형
# 핵심 철학: 이미 강하게 오른 주도주가 고점 부근에서 무너지지 않고, 종가가 고점권에서 잠길 때 2차 슈팅 후보로 본다.
HIGH_REACCUM_LOOKBACK = _env_int('CLOSING_BET_HIGH_REACCUM_LOOKBACK', '120')
HIGH_REACCUM_RUNUP_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_RUNUP_MIN', '80.0')
HIGH_REACCUM_NEAR_HIGH_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_NEAR_HIGH_MIN', '85.0')
HIGH_REACCUM_NEAR_HIGH_MAX = _env_float('CLOSING_BET_HIGH_REACCUM_NEAR_HIGH_MAX', '101.5')
HIGH_REACCUM_MAX_PULLBACK = _env_float('CLOSING_BET_HIGH_REACCUM_MAX_PULLBACK', '22.0')
HIGH_REACCUM_RSI_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_RSI_MIN', '45.0')
HIGH_REACCUM_RSI_MAX = _env_float('CLOSING_BET_HIGH_REACCUM_RSI_MAX', '72.0')
HIGH_REACCUM_CLOSE_LOC_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_CLOSE_LOC_MIN', '65.0')
HIGH_REACCUM_UPPER_WICK_RANGE_MAX = _env_float('CLOSING_BET_HIGH_REACCUM_UPPER_WICK_RANGE_MAX', '35.0')
HIGH_REACCUM_CLOSE_OPEN_KEEP = _env_float('CLOSING_BET_HIGH_REACCUM_CLOSE_OPEN_KEEP', '0.99')
HIGH_REACCUM_VMA5_20_MAX = _env_float('CLOSING_BET_HIGH_REACCUM_VMA5_20_MAX', '1.25')
HIGH_REACCUM_TODAY_VOL_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_TODAY_VOL_MIN', '0.50')
HIGH_REACCUM_DISPARITY20_MAX = _env_float('CLOSING_BET_HIGH_REACCUM_DISPARITY20_MAX', '125.0')
HIGH_REACCUM_RUNUP20_MAX = _env_float('CLOSING_BET_HIGH_REACCUM_RUNUP20_MAX', '60.0')
HIGH_REACCUM_SCORE_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_SCORE_MIN', '75.0')
# v2.7: S전략 실전 필터 — 목표 공간/RR/유동성/거래량 상태 보정
HIGH_REACCUM_RR_EXCLUDE_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_RR_EXCLUDE_MIN', '0.30')
HIGH_REACCUM_RR_GOOD_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_RR_GOOD_MIN', '0.70')
HIGH_REACCUM_AMOUNT_GOOD_B = _env_float('CLOSING_BET_HIGH_REACCUM_AMOUNT_GOOD_B', '100.0')
HIGH_REACCUM_TODAY_VOL_GOOD = _env_float('CLOSING_BET_HIGH_REACCUM_TODAY_VOL_GOOD', '1.50')
HIGH_REACCUM_VOLUME_DRY_MAX = _env_float('CLOSING_BET_HIGH_REACCUM_VOLUME_DRY_MAX', '0.85')
HIGH_REACCUM_VOLUME_NORMAL_MAX = _env_float('CLOSING_BET_HIGH_REACCUM_VOLUME_NORMAL_MAX', '1.20')

# v2.8: S전략을 관찰형(S1)과 실행형(S2)으로 분리
HIGH_REACCUM_EXEC_VOL_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_EXEC_VOL_MIN', '1.20')
HIGH_REACCUM_EXEC_AMOUNT_MIN_B = _env_float('CLOSING_BET_HIGH_REACCUM_EXEC_AMOUNT_MIN_B', '100.0')
HIGH_REACCUM_EXEC_CLOSE_LOC_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_EXEC_CLOSE_LOC_MIN', '70.0')
HIGH_REACCUM_EXEC_WICK_MAX = _env_float('CLOSING_BET_HIGH_REACCUM_EXEC_WICK_MAX', '30.0')

# v3.6: S-CORE 중심. 백테스트상 S전략은 거래량 재점화보다 '고점권 거래량 응축' 성과가 좋아 우수응축 기준을 핵심군으로 승격한다.
HIGH_REACCUM_S1_DRY_TODAY_MAX = _env_float('CLOSING_BET_HIGH_REACCUM_S1_DRY_TODAY_MAX', '1.00')
HIGH_REACCUM_S1_DRY_VMA_MAX = _env_float('CLOSING_BET_HIGH_REACCUM_S1_DRY_VMA_MAX', '1.00')
HIGH_REACCUM_S1_GOOD_RR_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_S1_GOOD_RR_MIN', '0.70')
HIGH_REACCUM_S1_GOOD_AMOUNT_MIN_B = _env_float('CLOSING_BET_HIGH_REACCUM_S1_GOOD_AMOUNT_MIN_B', '100.0')
HIGH_REACCUM_S1_GOOD_CLOSE_LOC_MIN = _env_float('CLOSING_BET_HIGH_REACCUM_S1_GOOD_CLOSE_LOC_MIN', '65.0')

# v4.1: S-CORE SAFE/NEUTRAL/RISK 3단계 분리 기준. 12주 손절특이점 분석에서 안정적이었던 조건을 별도 표기한다.
S_CORE_SAFE_RR_MIN = _env_float('CLOSING_BET_S_CORE_SAFE_RR_MIN', '1.00')
S_CORE_SAFE_RR_MAX = _env_float('CLOSING_BET_S_CORE_SAFE_RR_MAX', '1.50')
S_CORE_SAFE_VOL_RATIO_MAX = _env_float('CLOSING_BET_S_CORE_SAFE_VOL_RATIO_MAX', '1.50')
S_CORE_SAFE_CLOSE_LOC_MIN = _env_float('CLOSING_BET_S_CORE_SAFE_CLOSE_LOC_MIN', '70.0')
S_CORE_RISK_RR_LOW = _env_float('CLOSING_BET_S_CORE_RISK_RR_LOW', '0.70')
S_CORE_RISK_RR_HIGH = _env_float('CLOSING_BET_S_CORE_RISK_RR_HIGH', '1.50')
S_CORE_RISK_VOL_RATIO_MIN = _env_float('CLOSING_BET_S_CORE_RISK_VOL_RATIO_MIN', '1.50')
S_CORE_RISK_CLOSE_LOC_MIN = _env_float('CLOSING_BET_S_CORE_RISK_CLOSE_LOC_MIN', '70.0')

# v4.1: 실시간 텔레그램 출력은 SAFE 중심으로 정리한다.
PRACTICAL_SAFE_TOP_N = _env_int('CLOSING_BET_PRACTICAL_SAFE_TOP_N', '5')
PRACTICAL_NEUTRAL_TOP_N = _env_int('CLOSING_BET_PRACTICAL_NEUTRAL_TOP_N', '3')
PRACTICAL_RISK_TOP_N = _env_int('CLOSING_BET_PRACTICAL_RISK_TOP_N', '3')
PRACTICAL_A_TOP_N = _env_int('CLOSING_BET_PRACTICAL_A_TOP_N', '2')
PRACTICAL_G_SAFE_TOP_N = _env_int('CLOSING_BET_PRACTICAL_G_SAFE_TOP_N', '3')
PRACTICAL_G_NEUTRAL_TOP_N = _env_int('CLOSING_BET_PRACTICAL_G_NEUTRAL_TOP_N', '2')
PRACTICAL_G_AGGRESSIVE_TOP_N = _env_int('CLOSING_BET_PRACTICAL_G_AGGRESSIVE_TOP_N', '2')
# v4.2.13: L 대형주 리더갭은 v4.2.12 백테스트 결과를 반영해 실전 보조 후보로 표시한다.
PRACTICAL_L_CORE_TOP_N = _env_int('CLOSING_BET_PRACTICAL_L_CORE_TOP_N', '5')
PRACTICAL_L_WATCH_TOP_N = _env_int('CLOSING_BET_PRACTICAL_L_WATCH_TOP_N', '3')
PRACTICAL_SHOW_L_WATCH = str(os.environ.get('CLOSING_BET_SHOW_L_WATCH', '1')).strip().lower() in ('1', 'true', 'yes', 'y', 'on')
# v4.2.9: H 신고가거자름은 실전 통합 출력에 포함하되, 신규검증 후보로 별도 표시한다.
PRACTICAL_H_TRIANGLE_TOP_N = _env_int('CLOSING_BET_PRACTICAL_H_TRIANGLE_TOP_N', '3')
PRACTICAL_H_CORE_TOP_N = _env_int('CLOSING_BET_PRACTICAL_H_CORE_TOP_N', '5')
PRACTICAL_H_FAST_TOP_N = _env_int('CLOSING_BET_PRACTICAL_H_FAST_TOP_N', '2')
PRACTICAL_SHOW_H_FAST = str(os.environ.get('CLOSING_BET_SHOW_H_FAST', '1')).strip().lower() in ('1', 'true', 'yes', 'y', 'on')
PRACTICAL_SHOW_H_DIAG = str(os.environ.get('CLOSING_BET_SHOW_H_DIAG', '0')).strip().lower() in ('1', 'true', 'yes', 'y', 'on')
# v4.3.6: I-MAIN은 단기 종가배팅이 아니라 20/40/60일 중기 누적관찰 후보로 실시간 별도 표시한다.
PRACTICAL_SHOW_I_MAIN = str(os.environ.get('CLOSING_BET_SHOW_I_MAIN', '1')).strip().lower() in ('1', 'true', 'yes', 'y', 'on')
PRACTICAL_I_MAIN_CORE_TOP_N = _env_int('CLOSING_BET_I_MAIN_CORE_TOP_N', '5')
PRACTICAL_I_MAIN_ACCEL_TOP_N = _env_int('CLOSING_BET_I_MAIN_ACCEL_TOP_N', '5')
PRACTICAL_I_MAIN_WATCH_TOP_N = _env_int('CLOSING_BET_I_MAIN_WATCH_TOP_N', '5')
PRACTICAL_I_MAIN_ADD_TOP_N = _env_int('CLOSING_BET_I_MAIN_ADD_TOP_N', '3')
PRACTICAL_I_MAIN_CONFIRM_TOP_N = _env_int('CLOSING_BET_I_MAIN_CONFIRM_TOP_N', '3')
# v4.4.9.32: SK하이닉스 주도주 사이클 리포트 기반 I-LEADER 라이프사이클 라벨.
# 단기 종가배팅 하드필터가 아니라 I-MAIN/IT 중기 후보의 초입·가속·후반·공세종말 위험 라벨이다.
CLOSING_BET_I_LEADER_LIFECYCLE_LABEL = _bool_env('CLOSING_BET_I_LEADER_LIFECYCLE_LABEL', '1')
# v4.4.1: 기존 검색식은 유지하고 신규 검증 검색식만 별도 추가한다.
# LP=L-PULLBACK 리더갭 눌림재지지, SLOCK=S2-LOCK 상단잠김, IT=I-TRIGGER 중기후보 촉발형.
PRACTICAL_SHOW_NEW_PATTERNS = str(os.environ.get('CLOSING_BET_SHOW_NEW_PATTERNS', '1')).strip().lower() in ('1', 'true', 'yes', 'y', 'on')
PRACTICAL_L_PULLBACK_TOP_N = _env_int('CLOSING_BET_L_PULLBACK_TOP_N', '3')
PRACTICAL_S2_LOCK_TOP_N = _env_int('CLOSING_BET_S2_LOCK_TOP_N', '3')
# v4.4.8: SLOCK은 백테스트 검증에는 남기되 실시간 후보에서는 기본 숨김 처리한다.
PRACTICAL_SHOW_SLOCK_LIVE = str(os.environ.get('CLOSING_BET_SHOW_SLOCK_LIVE', '0')).strip().lower() in ('1', 'true', 'yes', 'y', 'on')
PRACTICAL_I_TRIGGER_TOP_N = _env_int('CLOSING_BET_I_TRIGGER_TOP_N', '3')
# v4.1.6: 역매공파는 종가배팅 후보가 아니라 C-SWING 엄격형 스윙 관심으로 별도 표시한다.
# v4.1.6: 실전 후보가 아니라, 1파 이후 눌림→재지지→재상승 확인형만 별도 검증/추적한다.
PRACTICAL_C_SWING_TOP_N = _env_int('CLOSING_BET_PRACTICAL_C_SWING_TOP_N', '3')
PRACTICAL_C_PULLBACK_TOP_N = _env_int('CLOSING_BET_PRACTICAL_C_PULLBACK_TOP_N', '3')
PRACTICAL_SHOW_C_DIAG = str(os.environ.get('CLOSING_BET_SHOW_C_DIAG', '0')).strip().lower() in ('1', 'true', 'yes', 'y', 'on')
PRACTICAL_SHOW_RISK_DETAILS = str(os.environ.get('CLOSING_BET_SHOW_RISK_DETAILS', '0')).strip().lower() in ('1', 'true', 'yes', 'y', 'on')
PRACTICAL_SHOW_LEGACY_SECTIONS = str(os.environ.get('CLOSING_BET_SHOW_LEGACY_SECTIONS', '0')).strip().lower() in ('1', 'true', 'yes', 'y', 'on')

# v4.4.9.13: 직장인 종가배팅 모드 + 텔레그램 카드 잘림 방지
CLOSING_BET_WORKER_MODE = str(os.environ.get('CLOSING_BET_WORKER_MODE', '1')).strip().lower() in ('1', 'true', 'yes', 'y', 'on')
CLOSING_BET_CARD_SPLIT_GUARD = str(os.environ.get('CLOSING_BET_CARD_SPLIT_GUARD', '1')).strip().lower() in ('1', 'true', 'yes', 'y', 'on')
CLOSING_BET_TELEGRAM_MAX_LEN = _env_int('CLOSING_BET_TELEGRAM_MAX_LEN', '3400')

# v4.4.9.24: 2차 스캔 전용 FINAL KICK COMPACT MODE
# 14:40은 기존처럼 후보 발굴, 15:03은 동시호가 직전 실행/포기만 빠르게 판단한다.
CLOSING_BET_RUN_MODE = str(os.environ.get('CLOSING_BET_RUN_MODE', 'main')).strip().lower() or 'main'
CLOSING_BET_FINAL_KICK_ONLY = _bool_env('CLOSING_BET_FINAL_KICK_ONLY', '1' if CLOSING_BET_RUN_MODE in ('final_kick', 'final-kick', 'kick') else '0')
CLOSING_BET_MAX_FINAL_KICK_CARDS = _env_int('CLOSING_BET_MAX_FINAL_KICK_CARDS', '3')
# v4.4.9.24: 최종킥은 실행/포기만 빠르게 보는 압축 모드가 기본값이다.
CLOSING_BET_FINAL_KICK_COMPACT = _bool_env('CLOSING_BET_FINAL_KICK_COMPACT', '1')

# v4.4.9.16: ST30-RECLAIM은 매수 후보를 즉시 줄이는 하드필터가 아니라
# 백테스트에서 '적용/미적용' 성과를 비교하는 품질 태그로 먼저 검증한다.
ST30_RECLAIM_ENABLE = _bool_env('CLOSING_BET_ST30_RECLAIM_ENABLE', '1')
ST30_OVERSOLD_LEVEL = _env_float('CLOSING_BET_ST30_OVERSOLD_LEVEL', '30')
ST30_OVERSOLD_LOOKBACK = _env_int('CLOSING_BET_ST30_OVERSOLD_LOOKBACK', '7')
ST30_WEEKLY_MACD_CONFIRM = _bool_env('CLOSING_BET_ST30_WEEKLY_MACD_CONFIRM', '1')

# v3.6: A 강한돌파는 S-CORE 보조 후보. 거래량비 3배 이상 + 거래대금 100억 이상으로 좁힌다.
A_STRONG_VOL_RATIO_MIN = _env_float('CLOSING_BET_A_STRONG_VOL_RATIO_MIN', '3.0')
A_STRONG_AMOUNT_MIN_B = _env_float('CLOSING_BET_A_STRONG_AMOUNT_MIN_B', '100.0')

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
# v4.4.9.13 LIVE OPERATION GUARD: 실전에서 실제로 볼 후보만 별도 추적한다.
LIVE_OPERATION_GUARD_CSV = LOG_DIR / "closing_bet_live_operation_guard.csv"
CLOSING_BET_LIVE_OPERATION_GUARD = _bool_env('CLOSING_BET_LIVE_OPERATION_GUARD', '1')
CLOSING_BET_LIVE_TRACKING_LOG = _bool_env('CLOSING_BET_LIVE_TRACKING_LOG', '1')
CLOSING_BET_SHOW_EMPTY_LIVE_SECTIONS = _bool_env('CLOSING_BET_SHOW_EMPTY_LIVE_SECTIONS', '1')
CLOSING_BET_LIVE_PROMOTION_MIN_N = _env_int('CLOSING_BET_LIVE_PROMOTION_MIN_N', '10')
BACKTEST_DETAIL_TOP_N = _env_int('CLOSING_BET_BACKTEST_DETAIL_TOP_N', '8')
BACKTEST_DETAIL_MAX_ROWS = _env_int('CLOSING_BET_BACKTEST_DETAIL_MAX_ROWS', '20000')
JSON_KEY_PATH = str(Path(__file__).resolve().parent / 'stock-key.json')
AI_GSHEET_NAME = '사령부_통합_상황판'
AI_JUDGMENT_TAB_NAME = '종가배팅_AI판정'

# 다음날 성과 평가 가능 시간
EVAL_READY_HOUR = 16
EVAL_READY_MIN = 10

# 전역 지수 소속 맵 / 시총상위 맵 / 시총 맵 / 종목명 맵
INDEX_MAP: dict = {}
TOP_MCAP_SET: set = set()
MARCAP_MAP: dict = {}
STOCK_NAME_MAP: dict = {}

# v4.4.9.32: KRX/FDR/pykrx 유니버스가 모두 막힐 때 쓰는 최소 안전 유니버스.
# 목적은 백테스트/스케줄이 0개로 종료되는 것을 막는 것이며, 실제 종목 데이터 로딩은 기존 FDR/pykrx 일봉 함수가 계속 담당한다.
CLOSING_BET_ALLOW_UNIVERSE_FALLBACK = _bool_env('CLOSING_BET_ALLOW_UNIVERSE_FALLBACK', '1')
CLOSING_BET_STATIC_CORE_MAX = _env_int('CLOSING_BET_STATIC_CORE_MAX', '260')
UNIVERSE_FALLBACK_ACTIVE: bool = False
CURRENT_UNIVERSE_SET: set = set()

STATIC_KOSPI_CORE = [
    ('005930','삼성전자'), ('000660','SK하이닉스'), ('373220','LG에너지솔루션'), ('207940','삼성바이오로직스'),
    ('005380','현대차'), ('000270','기아'), ('005490','POSCO홀딩스'), ('005935','삼성전자우'),
    ('068270','셀트리온'), ('035420','NAVER'), ('105560','KB금융'), ('055550','신한지주'),
    ('000810','삼성화재'), ('032830','삼성생명'), ('012330','현대모비스'), ('028260','삼성물산'),
    ('066570','LG전자'), ('003550','LG'), ('034730','SK'), ('017670','SK텔레콤'),
    ('096770','SK이노베이션'), ('051910','LG화학'), ('009150','삼성전기'), ('011070','LG이노텍'),
    ('010130','고려아연'), ('033780','KT&G'), ('086790','하나금융지주'), ('316140','우리금융지주'),
    ('024110','기업은행'), ('138040','메리츠금융지주'), ('086280','현대글로비스'), ('267260','HD현대일렉트릭'),
    ('329180','HD현대중공업'), ('010140','삼성중공업'), ('042660','한화오션'), ('047810','한국항공우주'),
    ('012450','한화에어로스페이스'), ('272210','한화시스템'), ('047050','포스코인터내셔널'), ('006400','삼성SDI'),
    ('003670','포스코퓨처엠'), ('010950','S-Oil'), ('009830','한화솔루션'), ('018260','삼성에스디에스'),
    ('003490','대한항공'), ('180640','한진칼'), ('028050','삼성엔지니어링'), ('006360','GS건설'),
    ('000720','현대건설'), ('004020','현대제철'), ('011200','HMM'), ('000120','CJ대한통운'),
    ('161390','한국타이어앤테크놀로지'), ('307950','현대오토에버'), ('000150','두산'), ('034020','두산에너빌리티'),
    ('241560','두산밥캣'), ('042700','한미반도체'), ('064350','현대로템'), ('010120','LS ELECTRIC'),
    ('006260','LS'), ('001120','LX인터내셔널'), ('004800','효성'), ('298040','효성중공업'),
    ('001440','대한전선'), ('103590','일진전기'), ('001740','SK네트웍스'), ('005830','DB손해보험'),
    ('039490','키움증권'), ('006800','미래에셋증권'), ('003530','한화투자증권'), ('008770','호텔신라'),
    ('002790','아모레G'), ('090430','아모레퍼시픽'), ('051900','LG생활건강'), ('003230','삼양식품'),
    ('271560','오리온'), ('097950','CJ제일제당'), ('000080','하이트진로'), ('004370','농심'),
    ('000100','유한양행'), ('128940','한미약품'), ('009420','한올바이오파마'), ('008930','한미사이언스'),
    ('326030','SK바이오팜'), ('000155','두산우'), ('000815','삼성화재우'), ('000500','가온전선'),
    ('001340','PKC'), ('002020','코오롱'), ('006060','화승인더'), ('009540','HD한국조선해양'),
    ('010620','현대미포조선'), ('071050','한국금융지주'), ('078930','GS'), ('112610','씨에스윈드'),
    ('137310','에스디바이오센서'), ('139480','이마트'), ('175330','JB금융지주'), ('192820','코스맥스'),
    ('241590','화승엔터프라이즈'), ('251270','넷마블'), ('259960','크래프톤'), ('272450','진에어'),
    ('302440','SK바이오사이언스'), ('323410','카카오뱅크'), ('035720','카카오'), ('036570','엔씨소프트'),
    ('402340','SK스퀘어'), ('064400','LG씨엔에스'), ('092200','디아이씨'), ('005090','SGC에너지'),
]

STATIC_KOSDAQ_CORE = [
    ('086520','에코프로'), ('247540','에코프로비엠'), ('091990','셀트리온헬스케어'), ('196170','알테오젠'),
    ('028300','HLB'), ('068760','셀트리온제약'), ('277810','레인보우로보틱스'), ('108490','로보티즈'),
    ('039030','이오테크닉스'), ('240810','원익IPS'), ('000990','DB하이텍'), ('053610','프로텍'),
    ('084370','유진테크'), ('095340','ISC'), ('036930','주성엔지니어링'), ('067310','하나마이크론'),
    ('064760','티씨케이'), ('078600','대주전자재료'), ('025900','동화기업'), ('121600','나노신소재'),
    ('112040','위메이드'), ('293490','카카오게임즈'), ('263750','펄어비스'), ('041510','에스엠'),
    ('122870','와이지엔터테인먼트'), ('352820','하이브'), ('035900','JYP Ent.'), ('032500','케이엠더블유'),
    ('440110','파두'), ('022100','포스코DX'), ('353200','대덕전자'), ('007660','이수페타시스'),
    ('007810','코리아써키트'), ('003160','디아이'), ('005690','파미셀'), ('014620','성광벤드'),
    ('001270','부국증권'), ('001430','세아베스틸지주'), ('001740','SK네트웍스'), ('006360','GS건설'),
    ('003690','코리안리'), ('003530','한화투자증권'), ('002320','한진'), ('011210','현대위아'),
    ('001120','LX인터내셔널'), ('005090','SGC에너지'), ('000500','가온전선'), ('002020','코오롱'),
]

STATIC_CORE_TICKERS = list(dict.fromkeys([c for c, _ in (STATIC_KOSPI_CORE + STATIC_KOSDAQ_CORE)]))
STATIC_CORE_NAME_MAP = {c: n for c, n in (STATIC_KOSPI_CORE + STATIC_KOSDAQ_CORE)}

STRATEGY_DIAG = {
    'A_try': 0, 'A_hit': 0,
    'B1_try': 0, 'B1_hit': 0,
    'B2_try': 0, 'B2_hit': 0,
    'G_try': 0, 'G_hit': 0,
    'L_try': 0, 'L_hit': 0,
    'S_try': 0, 'S_hit': 0,
    'H_try': 0, 'H_hit': 0,
    'I_try': 0, 'I_hit': 0,
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


def _static_core_listing(limit: int | None = None) -> pd.DataFrame:
    """v4.4.9.32: 외부 유니버스 로딩 실패 시 사용하는 정적 핵심 종목표."""
    try:
        n = int(limit or CLOSING_BET_STATIC_CORE_MAX or len(STATIC_CORE_TICKERS))
    except Exception:
        n = len(STATIC_CORE_TICKERS)
    codes = list(dict.fromkeys(STATIC_CORE_TICKERS))[:max(1, n)]
    return pd.DataFrame({
        'Code': codes,
        'Name': [STATIC_CORE_NAME_MAP.get(c, c) for c in codes],
        # 실제 시총값은 아니지만 _is_universe_allowed가 마켓맵 장애로 전부 탈락시키지 않도록 하는 안전값.
        'Marcap': [1_000_000_000_000 for _ in codes],
        'Market': ['STATIC_FALLBACK' for _ in codes],
    })


def _static_index_members(label: str) -> tuple[list, list]:
    """v4.4.9.32: pykrx 지수 구성 로딩 실패 시 최소 지수맵을 만든다."""
    src = STATIC_KOSPI_CORE if str(label) == '코스피200' else STATIC_KOSDAQ_CORE
    codes = [c for c, _ in src]
    names = [n for _, n in src]
    return codes, names


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
        listing = _get_krx_listing()
        if listing is None or listing.empty:
            log_error("⚠️ KRX listing 비어 있음 — 시총 맵 fallback도 실패")
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
    if is_index_member or is_mcap_or:
        return True

    # v4.4.9.32: KRX/FDR 지수·시총 맵이 모두 비었는데 유니버스 fallback으로 코드를 확보한 경우,
    # 여기서 다시 전부 탈락시키면 백테스트 대상이 0개가 된다. fallback 유니버스 안의 코드만 제한적으로 허용한다.
    if CLOSING_BET_ALLOW_UNIVERSE_FALLBACK and code in CURRENT_UNIVERSE_SET and (not INDEX_MAP and not MARCAP_MAP):
        return True
    if CLOSING_BET_ALLOW_UNIVERSE_FALLBACK and UNIVERSE_FALLBACK_ACTIVE and code in CURRENT_UNIVERSE_SET:
        return True

    return False


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
            raise RuntimeError('empty KRX listing')
        out = listing.copy()
        if 'Code' not in out.columns and 'Symbol' in out.columns:
            out['Code'] = out['Symbol']
        if 'Name' not in out.columns and '종목명' in out.columns:
            out['Name'] = out['종목명']
        if 'Code' not in out.columns:
            raise RuntimeError('Code/Symbol column missing')
        out['Code'] = out['Code'].astype(str).str.zfill(6)
        if 'Name' not in out.columns:
            out['Name'] = out['Code']
        return out
    except Exception as e:
        log_error(f"⚠️ KRX listing 로드 실패: {e}")
        if CLOSING_BET_ALLOW_UNIVERSE_FALLBACK:
            fb = _static_core_listing()
            log_error(f"✅ KRX listing fallback: STATIC_CORE {len(fb)}개 사용")
            return fb
        return pd.DataFrame(columns=['Code','Name'])

def _is_valid_stock_name(name, code: str = '') -> bool:
    """v4.1.1: 종목명확인필요를 줄이기 위한 표시명 유효성 검사."""
    try:
        nm = str(name or '').strip()
        cd = _normalize_code(code) if str(code or '').strip() else ''
        if not nm:
            return False
        if cd and nm == cd:
            return False
        if nm.isdigit() and len(nm) == 6:
            return False
        if nm in ('nan', 'None', '종목명확인필요'):
            return False
        return True
    except Exception:
        return False


def _set_stock_name_map(name_map: dict):
    """v4.1.1: 실시간/백테스트 공통 종목명 fallback 전역 캐시."""
    global STOCK_NAME_MAP
    try:
        cleaned = {}
        for c, n in (name_map or {}).items():
            cd = _normalize_code(c)
            if _is_valid_stock_name(n, cd):
                cleaned[cd] = str(n).strip()
        STOCK_NAME_MAP = cleaned
    except Exception:
        STOCK_NAME_MAP = {}


def _clean_stock_name(code: str, name: str = '', default: str = '종목명확인필요') -> str:
    """v4.1.1: 코드가 이름처럼 들어온 경우 전역 name map으로 한 번 더 보정."""
    cd = _normalize_code(code)
    if _is_valid_stock_name(name, cd):
        return str(name).strip()
    cached = STOCK_NAME_MAP.get(cd, '')
    if _is_valid_stock_name(cached, cd):
        return str(cached).strip()
    return default


def _build_name_map_for_codes(codes: list, base_name_map: dict | None = None) -> dict:
    """v4.1.1: 종목명확인필요 방지용 통합 name map.
    - FDR/KRX listing을 기본으로 전체 코드명을 채운다.
    - 여전히 누락된 코드만 pykrx get_market_ticker_name으로 fallback한다.
    - 기존 v4.1의 codes[:1000] 제한 때문에 뒤쪽 종목명이 누락되던 문제를 제거한다.
    """
    norm_codes = [_normalize_code(c) for c in (codes or []) if str(c).strip()]
    name_map = {}

    for c, n in (base_name_map or {}).items():
        cd = _normalize_code(c)
        if _is_valid_stock_name(n, cd):
            name_map[cd] = str(n).strip()

    try:
        listing = _get_krx_listing()
        if listing is not None and not listing.empty and 'Code' in listing.columns and 'Name' in listing.columns:
            for c, n in zip(listing['Code'], listing['Name']):
                cd = _normalize_code(c)
                if cd in norm_codes and _is_valid_stock_name(n, cd):
                    name_map.setdefault(cd, str(n).strip())
    except Exception as e:
        log_error(f"⚠️ 종목명 FDR fallback 실패: {e}")

    missing = [c for c in norm_codes if not _is_valid_stock_name(name_map.get(c, ''), c)]
    if missing and pykrx_stock is not None:
        max_fallback = _env_int('CLOSING_BET_NAME_FALLBACK_MAX', '3000')
        for c in missing[:max_fallback]:
            try:
                nm = str(pykrx_stock.get_market_ticker_name(c) or '').strip()
                if _is_valid_stock_name(nm, c):
                    name_map[c] = nm
            except Exception:
                pass

    return {c: name_map.get(c, c) for c in norm_codes}


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
        if CLOSING_BET_ALLOW_UNIVERSE_FALLBACK:
            codes, names = _static_index_members(label)
            for c in codes:
                INDEX_MAP[c] = label
            log_error(f"✅ {label} STATIC fallback 사용: {len(codes)}개")
            return codes, names
        return [], []

    try:
        raw = pykrx_stock.get_index_portfolio_deposit_file(idx_code)
        codes = [_normalize_code(c) for c in raw if _normalize_code(c)]
        log_info(f"{label} 구성종목 로드: {len(codes)}개")
        log_info(f"{label} 샘플: {codes[:10]}")
    except Exception as e:
        log_error(f"⚠️ {label} 구성종목 로드 실패: {e}")
        if CLOSING_BET_ALLOW_UNIVERSE_FALLBACK:
            codes, names = _static_index_members(label)
            for c in codes:
                INDEX_MAP[c] = label
            log_error(f"✅ {label} STATIC fallback 사용: {len(codes)}개")
            return codes, names
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
    global INDEX_MAP, UNIVERSE_FALLBACK_ACTIVE, CURRENT_UNIVERSE_SET
    INDEX_MAP = {}
    UNIVERSE_FALLBACK_ACTIVE = False
    CURRENT_UNIVERSE_SET = set()
    universe_name = str(universe_name or '').strip() or 'hybrid_union'

    k200_codes, _ = _load_index_members('코스피200')
    kq150_codes, _ = _load_index_members('코스닥150')
    top_codes, _ = _load_amount_top_universe(TOP_N)

    if universe_name == 'kospi200':
        codes = k200_codes
    elif universe_name == 'kospi200+kosdaq150':
        codes = list(dict.fromkeys(k200_codes + kq150_codes))
    elif universe_name == 'amount_top400':
        codes = top_codes
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

    if not codes and CLOSING_BET_ALLOW_UNIVERSE_FALLBACK:
        log_error(f"⚠️ _load_universe({universe_name}) primary 결과 0개 → fallback 시작")
        try:
            fb_codes, _ = _load_amount_top_universe(TOP_N)
            codes = [_normalize_code(c) for c in fb_codes if str(c).strip()]
        except Exception as e:
            log_error(f"⚠️ amount_top fallback 실패: {e}")
            codes = []
        if not codes:
            try:
                fb = _static_core_listing(TOP_N)
                codes = fb['Code'].astype(str).map(_normalize_code).tolist()
            except Exception as e:
                log_error(f"⚠️ STATIC_CORE fallback 실패: {e}")
                codes = []
        codes = sorted(set(codes))
        UNIVERSE_FALLBACK_ACTIVE = bool(codes)

    CURRENT_UNIVERSE_SET = set(codes)

    if UNIVERSE_FALLBACK_ACTIVE:
        log_error(f"✅ UNIVERSE_FALLBACK_ACTIVE: {len(codes)}개")
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
        df['MA150'] = df['Close'].rolling(150).mean()
        df['MA200'] = df['Close'].rolling(200).mean()
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
    if not str(message or '').strip():
        return
    if not _telegram_route_ready():
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        if not chat_id:
            continue
        try:
            res = requests.post(
                url,
                data={
                    'chat_id': chat_id,
                    'text': message[:4000],
                },
                timeout=8,
            )
            if getattr(res, 'status_code', 200) >= 400:
                log_error(f"텔레그램 전송 실패 HTTP {res.status_code}: {getattr(res, 'text', '')[:300]}")
        except Exception as e:
            log_error(f"텔레그램 전송 실패: {e}")


def _split_telegram_safe(message: str, max_len: int = None) -> list:
    """v4.4.9.13: Telegram 4000자 절단 방지.
    문단/종목 카드 단위로 자르되, 한 카드가 너무 길면 줄 단위로 안전 분할한다.
    """
    max_len = int(max_len or CLOSING_BET_TELEGRAM_MAX_LEN or 3400)
    text = str(message or '').strip()
    if not text:
        return []

    def _push_piece(chunks, current, piece):
        piece = str(piece or '').strip()
        if not piece:
            return current
        sep = '\n\n' if current else ''
        if len(current) + len(sep) + len(piece) <= max_len:
            return current + sep + piece
        if current.strip():
            chunks.append(current.strip())
            current = ''
        if len(piece) <= max_len:
            return piece
        buf = ''
        for line in piece.split('\n'):
            if len(line) > max_len:
                if buf.strip():
                    chunks.append(buf.strip())
                    buf = ''
                step = max(500, max_len - 20)
                for i in range(0, len(line), step):
                    chunks.append(line[i:i + step])
                continue
            add = line if not buf else '\n' + line
            if len(buf) + len(add) > max_len:
                if buf.strip():
                    chunks.append(buf.strip())
                buf = line
            else:
                buf += add
        return buf

    chunks = []
    current = ''
    for para in re.split(r'\n\s*\n', text):
        current = _push_piece(chunks, current, para)
    if current.strip():
        chunks.append(current.strip())
    return [c for c in chunks if str(c).strip()]


def send_telegram_chunks(message: str, max_len: int = 3800):
    chunks = _split_telegram_safe(message, max_len=min(int(max_len or 3800), int(CLOSING_BET_TELEGRAM_MAX_LEN or 3400)))
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
            'close_loc_pct': round(((today_close - today_low) / candle_range * 100.0) if candle_range > 0 else 100.0, 1),
            'gap_unfilled': int(bool(gap_unfilled)),
            'close_support': int(bool(close_support)),
            'close_strength': int(bool(close_strength)),
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

        # v3.6: 최근 백테스트에서 S 거래량비<1.0, 고점권 응축형의 성과가 가장 좋아서 별도 핵심군으로 승격.
        is_s1_dry_good = bool(
            (not is_s2_exec)
            and rr >= HIGH_REACCUM_S1_GOOD_RR_MIN
            and amount_b >= HIGH_REACCUM_S1_GOOD_AMOUNT_MIN_B
            and close_loc_pct >= HIGH_REACCUM_S1_GOOD_CLOSE_LOC_MIN
            and upper_wick_range_pct <= HIGH_REACCUM_UPPER_WICK_RANGE_MAX
            and (today_vol_ratio < HIGH_REACCUM_S1_DRY_TODAY_MAX or vma5_20_ratio <= HIGH_REACCUM_S1_DRY_VMA_MAX)
        )

        if is_s1_dry_good:
            s_type = 'S1'
            s_type_label = 'S1 우수응축형'
            execution_verdict = '실전 우선 후보 — 고점권에서 거래량이 마른 채 종가가 위에서 버티는 응축형'
        elif is_s2_exec:
            s_type = 'S2'
            s_type_label = 'S2 실행형'
            execution_verdict = '종가배팅 실행 후보 — 거래량 재점화와 종가 상단 마감 동시 확인'
        elif today_vol_ratio < 1.0:
            s_type = 'S1'
            s_type_label = 'S1 일반관찰형'
            execution_verdict = '구조는 좋지만 우수응축 기준은 부족 — 다음날 거래량/전고점 재돌파 확인'
        else:
            s_type = 'S1'
            s_type_label = 'S1 일반관찰형'
            execution_verdict = '실행 전 대기 — 거래량/전고점 재돌파 확인 시 S2 또는 우수형으로 승격 가능'

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

        # v3.6: S는 거래량 폭발보다 '고점권 응축 후 버팀'이 더 우수하게 나와 응축형을 우대.
        if is_s1_dry_good:
            score_adjust += 12
        elif today_vol_ratio >= HIGH_REACCUM_TODAY_VOL_GOOD:
            score_adjust += 1
        elif today_vol_ratio >= HIGH_REACCUM_EXEC_VOL_MIN:
            score_adjust += 1
        elif today_vol_ratio < 1.0:
            score_adjust += 3
        else:
            score_adjust -= 2

        if is_s2_exec:
            score_adjust += 6
        # v3.6: S전략은 '무조건 고가 마감'보다 고점권에서 적당히 여지를 남긴 응축 마감도 우대한다.
        # 최근 검증에서 종가위치 85%+보다 65~85% 구간의 재상승 효율이 나쁘지 않았기 때문에 과도한 고가마감 가점은 축소한다.
        if 70 <= close_loc_pct < 85:
            score_adjust += 4
        elif 65 <= close_loc_pct < 70:
            score_adjust += 3
        elif close_loc_pct >= 85:
            score_adjust += 1
        if upper_wick_range_pct <= 15:
            score_adjust += 3
        if near_high120 >= 98.0 and rr < 0.50:
            score_adjust -= 12

        final_score = max(0, min(100, score_raw + score_adjust))

        # v3.6 점수 캡: S1 우수응축형은 실전 핵심군으로 승격하되, S2와는 구분한다.
        if is_s1_dry_good:
            final_score = min(final_score, 94.0)
        elif today_vol_ratio < 1.0:
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
        elif is_s1_dry_good and final_score >= 88:
            grade = '✅A급'
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
            's_quality': 'S1_DRY_GOOD' if is_s1_dry_good else ('S2_EXEC' if is_s2_exec else 'S1_NORMAL'),
            'is_s1_dry_good': int(bool(is_s1_dry_good)),
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
            's_quality': sig.get('s_quality', ''),
            'is_s1_dry_good': sig.get('is_s1_dry_good', 0),
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

        # v4.4.9.8: A-RETEST / A-CONFIRM 실시간 진입가격 코멘트용 일봉 컨텍스트.
        def _a_close_loc_from_info(_info: dict) -> float:
            hi = _safe_float(_info.get('_high', 0), 0.0)
            lo = _safe_float(_info.get('_low', 0), 0.0)
            cl = _safe_float(_info.get('_close', 0), 0.0)
            return max(0.0, min(100.0, ((cl - lo) / (hi - lo) * 100.0))) if hi > lo and cl > 0 else 0.0

        info['close_loc_pct'] = round(_a_close_loc_from_info(info), 1)
        try:
            ma5_now = _safe_float(df['Close'].tail(5).mean(), 0.0)
        except Exception:
            ma5_now = 0.0
        info['ma5'] = round(ma5_now) if ma5_now > 0 else 0

        prev_info = None
        if len(df) >= 2:
            prev_df = df.iloc[:-1].copy()
            prev_row = df.iloc[-2]
            prev_info = _base_info(prev_row, prev_df)
            prev_info['close_loc_pct'] = round(_a_close_loc_from_info(prev_info), 1)
            try:
                prev_ma5 = _safe_float(prev_df['Close'].tail(5).mean(), 0.0)
            except Exception:
                prev_ma5 = 0.0
            # 전일이 A-RETEST CORE②였는지 오늘 다시 계산한다.
            prev_cond = {
                '①전고점85~100%': NEAR_HIGH20_MIN <= _safe_float(prev_info.get('_near20', 0), 0.0) <= NEAR_HIGH20_MAX,
                '②윗꼬리20%이하': _safe_float(prev_info.get('_upper_wick_body', 9), 9.0) <= UPPER_WICK_MAX,
                '③거래량2배폭발': _safe_float(prev_info.get('_vma20', 0), 0.0) > 0 and _safe_float(prev_info.get('_vol', 0), 0.0) >= _safe_float(prev_info.get('_vma20', 0), 0.0) * VOL_MULT,
                '④양봉마감': _safe_float(prev_info.get('_close', 0), 0.0) >= _safe_float(prev_info.get('_open', 0), 0.0),
                '⑤이격도98~112': DISPARITY_MIN <= _safe_float(prev_info.get('_disp', 0), 0.0) <= DISPARITY_MAX,
                '⑥MA20위마감': _safe_float(prev_info.get('_ma20', 0), 0.0) > 0 and _safe_float(prev_info.get('_close', 0), 0.0) >= _safe_float(prev_info.get('_ma20', 0), 0.0),
            }
            prev_score = sum(1 for v in prev_cond.values() if v)
            prev_core2 = (
                prev_score >= 4
                and _safe_float(prev_info.get('amount_b', 0), 0.0) >= 5000.0
                and _safe_float(prev_info.get('close_loc_pct', 0), 0.0) >= 80.0
                and _safe_float(prev_info.get('vol_ratio', 0), 0.0) <= 1.8
                and 0.8 <= _safe_float(prev_info.get('rr', 0), 0.0) <= 1.5
            )
            prev_high = _safe_float(prev_info.get('_high', 0), 0.0)
            prev_close = _safe_float(prev_info.get('_close', 0), 0.0)
            prev_amount = _safe_float(prev_info.get('amount_b', 0), 0.0)
            today_close = _safe_float(info.get('_close', 0), 0.0)
            today_open = _safe_float(info.get('_open', 0), 0.0)
            today_high = _safe_float(info.get('_high', 0), 0.0)
            today_amount = _safe_float(info.get('amount_b', 0), 0.0)
            support_line = max(prev_close * 0.995 if prev_close > 0 else 0.0, ma5_now * 0.995 if ma5_now > 0 else 0.0)
            reclaim_prev_high = bool(prev_high > 0 and today_high >= prev_high)
            close_above_prev_high = bool(prev_high > 0 and today_close >= prev_high)
            bullish_today = bool(today_close >= today_open and today_close > prev_close)
            support_hold = bool(today_close > 0 and (support_line <= 0 or today_close >= support_line))
            amount_hold = bool(prev_amount <= 0 or today_amount >= prev_amount * 0.50)

            # v4.4.9.8: A-CONFIRM 거래량 수축(Volume Contraction) / 가격지지 태그.
            # 하드필터가 아니라 PRIME/CALM 태그로 먼저 검증한다.
            prev_vol = _safe_float(prev_info.get('_vol', 0), 0.0)
            today_vol = _safe_float(info.get('_vol', 0), 0.0)
            today_vr = _safe_float(info.get('vol_ratio', info.get('today_vol_ratio', 0)), 0.0)
            try:
                _v3 = [float(x) for x in df['Volume'].tail(3).tolist() if pd.notna(x)]
            except Exception:
                _v3 = []
            vol_contract_prev = bool(prev_vol > 0 and today_vol > 0 and today_vol <= prev_vol * 0.90)
            vol_contract_3d = bool(len(_v3) >= 3 and _v3[-1] <= max(_v3[-2], _v3[-3]) * 0.95)
            no_bear_expand = bool(not (today_close < today_open and prev_vol > 0 and today_vol >= prev_vol * 1.10))
            vc_safe = bool(prev_core2 and support_hold and amount_hold and no_bear_expand and (vol_contract_prev or vol_contract_3d or (0 < today_vr <= 1.20)))
            vc_watch = bool(prev_core2 and support_hold and no_bear_expand and (vc_safe or vol_contract_prev or vol_contract_3d or (0 < today_vr <= 1.80)))
            # v4.4.9.13: v4.4.9.10 검증 결과, 기존 VC-WATCH가 더 좋은 성과를 보였다.
            # 따라서 실전 라벨은 WATCH를 PRIME으로, SAFE를 CALM으로 재해석한다.
            if vc_watch and not vc_safe:
                vc_reason = 'A-VC-PRIME: 적당히 식었고 가격지지 유지, 매수힘이 남은 확인형'
            elif vc_safe:
                vc_reason = 'A-VC-CALM: 거래량이 강하게 식으며 가격지지 유지, 안정형이지만 힘은 PRIME보다 약할 수 있음'
            elif vc_watch:
                vc_reason = 'A-VC-PRIME: 적당히 식었고 가격지지 유지, 매수힘이 남은 확인형'
            else:
                vc_reason = 'A-VC-확인필요: 거래량 냉각 또는 가격지지 근거가 약함'

            a_confirm_live = bool(prev_core2 and support_hold and amount_hold and (reclaim_prev_high or bullish_today or close_above_prev_high))
            info.update({
                'a_prev_core2': int(prev_core2),
                'a_confirm_live': int(a_confirm_live),
                'a_prev_high': round(prev_high) if prev_high > 0 else 0,
                'a_prev_close': round(prev_close) if prev_close > 0 else 0,
                'a_prev_ma5': round(prev_ma5) if prev_ma5 > 0 else 0,
                'a_prev_amount_b': round(prev_amount, 1),
                'a_reclaim_prev_high': int(reclaim_prev_high),
                'a_close_above_prev_high': int(close_above_prev_high),
                'a_bullish_today': int(bullish_today),
                'a_support_hold': int(support_hold),
                'a_amount_hold': int(amount_hold),
                'a_confirm_entry_price': round(prev_high) if prev_high > 0 else 0,
                'a_vc_safe': int(vc_safe),
                'a_vc_watch': int(vc_watch),
                'a_vc_reason': vc_reason,
                'a_vol_contract_prev': int(vol_contract_prev),
                'a_vol_contract_3d': int(vol_contract_3d),
                'a_no_bear_expand': int(no_bear_expand),
                'a_today_vol_ratio': round(today_vr, 2),
                'a_today_vs_prev_vol_pct': round((today_vol / prev_vol * 100.0), 1) if prev_vol > 0 and today_vol > 0 else 0,
                'a_pullback_low': round(min(x for x in [prev_close, ma5_now] if x > 0)) if any(x > 0 for x in [prev_close, ma5_now]) else 0,
                'a_pullback_high': round(max(x for x in [prev_close, ma5_now] if x > 0)) if any(x > 0 for x in [prev_close, ma5_now]) else 0,
            })
        else:
            info.update({'a_prev_core2': 0, 'a_confirm_live': 0, 'a_prev_high': 0, 'a_prev_close': 0, 'a_prev_ma5': 0, 'a_vc_safe': 0, 'a_vc_watch': 0, 'a_vc_reason': ''})

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



def _check_leader_gap_bet(code: str, name: str) -> dict | None:
    """v4.2.13 실시간용 L 대형주 리더갭.
    v4.2.12 백테스트 결과 반영: 거래대금 5000억+ / 1조+ 대형 주도주 갭은
    G-SAFE 과열 제외가 아니라 별도 실전 보조 후보로 본다.
    """
    try:
        code = _normalize_code(code)
        df = _load_df(code, lookback_days=730)
        if df is None or len(df) < max(130, GAP_HIGH_LOOKBACK + 5):
            return None

        row = df.iloc[-1]
        prev = df.iloc[-2]
        past = df.iloc[:-1]
        info = _base_info(row, df)

        idx_label = INDEX_MAP.get(code, '')
        marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)
        today_open = info['_open']
        today_high = info['_high']
        today_low = info['_low']
        today_close = info['_close']
        today_volume = info['_vol']
        prev_close = _safe_float(prev.get('Close', 0), 0.0)
        amount_b = _safe_float(info.get('amount_b', 0.0), 0.0)

        large_leader = bool(idx_label == '코스피200' or marcap >= GAP_LARGE_CAP_MARCAP or amount_b >= LEADER_GAP_CORE_AMOUNT_B)
        if not large_leader:
            return None
        if today_open <= 0 or today_close <= 0 or prev_close <= 0:
            return None
        if amount_b < LEADER_GAP_MIN_AMOUNT_B:
            return None

        gap_pct = (today_open / prev_close - 1.0) * 100.0
        if not (LEADER_GAP_MIN_PCT <= gap_pct <= LEADER_GAP_MAX_PCT):
            return None

        vol50 = _safe_float(past['Volume'].tail(50).mean(), 0.0)
        vol50_ratio = today_volume / vol50 if vol50 > 0 else 0.0
        if vol50_ratio < LEADER_GAP_VOL50_MIN:
            return None

        candle_range = today_high - today_low
        close_loc_pct = ((today_close - today_low) / candle_range * 100.0) if candle_range > 0 else 100.0
        upper_wick_pct = ((today_high - max(today_open, today_close)) / candle_range * 100.0) if candle_range > 0 else 0.0
        gap_zone_hold = today_low >= prev_close * 0.995
        close_support = today_close >= today_open * 0.990
        close_strength = close_loc_pct >= LEADER_GAP_CLOSE_LOC_MIN
        if not (gap_zone_hold and close_support and close_strength):
            return None

        high_120 = _safe_float(past['High'].tail(GAP_HIGH_LOOKBACK).max(), 0.0)
        high_252 = _safe_float(past['High'].tail(252).max(), 0.0) if len(past) >= 252 else high_120
        box_high_60 = _safe_float(past['High'].tail(GAP_BOX_LOOKBACK).max(), 0.0)
        new_high_120 = bool(high_120 > 0 and today_high >= high_120 * 1.002)
        new_high_52w = bool(high_252 > 0 and today_high >= high_252 * 1.002)
        near_high_120 = bool(high_120 > 0 and today_close >= high_120 * 0.970)
        box_breakout = bool(box_high_60 > 0 and today_close >= box_high_60 * 1.002)
        location_ok = bool(new_high_120 or new_high_52w or near_high_120 or box_breakout)
        if not location_ok:
            return None

        ma20 = _safe_float(row.get('MA20', 0), 0.0)
        disparity20 = today_close / ma20 * 100.0 if ma20 > 0 else 999.0
        close_20ago = _safe_float(df['Close'].iloc[-21], 0.0) if len(df) >= 21 else 0.0
        runup20 = (today_close / close_20ago - 1.0) * 100.0 if close_20ago > 0 else 999.0
        overheat_flag = int(disparity20 > GAP_DISPARITY20_MAX or runup20 > GAP_RUNUP20_MAX or upper_wick_pct > 30.0 or vol50_ratio > 8.0)

        score = 0
        passed = []
        score += 20; passed.append('①대형주/주도주')
        score += 12; passed.append(f'②갭{gap_pct:+.1f}%')
        score += 12; passed.append(f'③Vol50 {vol50_ratio:.1f}배')
        score += 18; passed.append(f'④거래대금 {amount_b:.0f}억')
        if amount_b >= LEADER_GAP_CORE_AMOUNT_B:
            score += 8; passed.append('⑤5000억+ 핵심거래대금')
        if amount_b >= 10000.0:
            score += 4; passed.append('⑤-2 1조+ 메가거래대금')
        if gap_zone_hold:
            score += 8; passed.append('⑥전일종가/갭구간지지')
        if close_strength:
            score += 10; passed.append(f'⑦종가위치 {close_loc_pct:.0f}%')
        if new_high_52w:
            score += 10; passed.append('⑧52주신고가권')
        elif new_high_120:
            score += 8; passed.append('⑧120일신고가권')
        elif near_high_120 or box_breakout:
            score += 6; passed.append('⑧전고점/박스권상단')
        if upper_wick_pct <= 20.0:
            score += 5; passed.append('⑨윗꼬리제한')
        if overheat_flag:
            passed.append('⚠️과열표시:이격/상승률/거래량')
        score = min(int(score), 100)
        if score < 70:
            return None

        grade = '완전체' if score >= 90 else ('✅A급' if score >= 80 else 'B급')
        h = _bt_common_payload(code, name, 'L', '대형주리더갭CORE', grade, score, row, df, idx_label, marcap, passed)
        stoploss = round(min(today_low, prev_close))
        risk = max(today_close - stoploss, 0.0)
        target1 = round(today_close * 1.03)
        h.update({
            'band_type': 'LEADER_GAP_CORE',
            'band_reason': '대형주/섹터대장 갭상승·초대형 거래대금·신고가권 유지 CORE',
            'leader_gap_watch': 1,
            'leader_gap_core': int(amount_b >= LEADER_GAP_CORE_AMOUNT_B),
            'leader_gap_mega': int(amount_b >= 10000.0 or gap_pct >= 6.0),
            'leader_gap_core_safe': int(amount_b >= LEADER_GAP_CORE_AMOUNT_B and close_loc_pct >= 70.0 and upper_wick_pct <= 25.0),
            'leader_gap_tail_absorb': int(amount_b >= LEADER_GAP_CORE_AMOUNT_B and ((25.0 < upper_wick_pct <= 35.0) or (65.0 <= close_loc_pct < 70.0))),
            'leader_gap_weak_watch': int((amount_b < LEADER_GAP_CORE_AMOUNT_B) or close_loc_pct < 65.0 or upper_wick_pct > 35.0),
            'leader_gap_class': (
                'L-MEGA' if (amount_b >= 10000.0 or gap_pct >= 6.0) else
                ('L-CORE SAFE' if (amount_b >= LEADER_GAP_CORE_AMOUNT_B and close_loc_pct >= 70.0 and upper_wick_pct <= 25.0) else
                 ('L-TAIL ABSORB' if (amount_b >= LEADER_GAP_CORE_AMOUNT_B and ((25.0 < upper_wick_pct <= 35.0) or (65.0 <= close_loc_pct < 70.0))) else
                  ('L-WATCH' if amount_b >= LEADER_GAP_MIN_AMOUNT_B else 'L-OTHER')))
            ),
            'gap_pct': round(gap_pct, 2),
            'vol50_ratio': round(vol50_ratio, 2),
            'vol_ratio': round(vol50_ratio, 2),
            'amount_b': round(amount_b, 1),
            'leader_gap_amount_b': round(amount_b, 1),
            'leader_gap_core_amount': int(amount_b >= LEADER_GAP_CORE_AMOUNT_B),
            'leader_gap_large_cap': int(large_leader),
            'leader_gap_new_high_120': int(new_high_120),
            'leader_gap_new_high_52w': int(new_high_52w),
            'leader_gap_near_high_120': int(near_high_120),
            'leader_gap_box_breakout': int(box_breakout),
            'leader_gap_overheat_flag': int(overheat_flag),
            'gap_unfilled': int(bool(gap_zone_hold)),
            'close_support': int(bool(close_support)),
            'close_strength': int(bool(close_strength)),
            'close_loc_pct': round(close_loc_pct, 1),
            'wick_pct': round(upper_wick_pct, 1),
            'upper_wick_pct': round(upper_wick_pct, 1),
            'disparity20': round(disparity20, 1),
            'runup20': round(runup20, 1),
            'prev_close': round(prev_close),
            'gap_low': round(today_low),
            'stoploss': stoploss,
            'target1': target1,
            'rr': round(((target1 - today_close) / risk), 2) if risk > 0 else 0.0,
            'sell_rule': 'L-CORE: 갭 하단/전일종가 지지 유지, +3/+5 우선 익절, 강하면 5일선 추적',
        })
        return h
    except Exception as e:
        log_debug(f"_check_leader_gap_bet 오류 [{code}/{name}]: {e}")
        return None

def _check_high_dryup_bet(code: str, name: str) -> dict | None:
    """v4.2.9 실시간용 H 신고가거자름 STRICT.
    과거 1~10거래일 안의 신고가 장대양봉 돌파봉을 찾고,
    현재봉이 거래량 마른 짧은 타점봉인지 확인한다.
    """
    try:
        code = _normalize_code(code)
        df = _load_df(code, 730)
        if df is None or len(df) < 150:
            return None
        row = df.iloc[-1]
        info = _base_info(row, df)
        idx_label = INDEX_MAP.get(code, '')
        marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)
        is_mcap_or = marcap >= MCAP_OR_MIN
        if not ((idx_label in ('코스피200', '코스닥150')) or is_mcap_or):
            return None
        if info.get('_close', 0) < MIN_PRICE or info.get('amount_b', 0) < (MIN_AMOUNT / 1e8):
            return None

        close = info['_close']; open_p = info['_open']; high = info['_high']; low = info['_low']; vol = info['_vol']
        ma5_now = _safe_float(row.get('MA5', 0), 0.0)
        ma10_now = _safe_float(row.get('MA10', 0), 0.0)
        ma20_now = _safe_float(row.get('MA20', 0), 0.0)
        prev5_vol = _safe_float(df['Volume'].iloc[-6:-1].mean(), 0.0) if len(df) >= 6 else 0.0
        vma20_now = _safe_float(row.get('VMA20', 0), 0.0)

        candle_range = high - low
        body = abs(close - open_p)
        body_pct = (body / close * 100.0) if close > 0 else 999.0
        range_pct = (candle_range / close * 100.0) if close > 0 and candle_range > 0 else 0.0
        close_loc_pct = ((close - low) / candle_range * 100.0) if candle_range > 0 else 100.0
        upper_wick_pct = ((high - max(open_p, close)) / candle_range * 100.0) if candle_range > 0 else 0.0
        short_candle = bool(body_pct <= 3.5 and range_pct <= 7.5)
        short_red_or_small_bull = bool((close < open_p) or (close >= open_p and body_pct <= 2.8))
        volume_dry_prev5 = bool(prev5_vol > 0 and vol <= prev5_vol)
        volume_dry_vma20 = bool(vma20_now > 0 and vol <= vma20_now * 0.85)
        volume_dry = bool(volume_dry_prev5 or volume_dry_vma20)
        ma5_close_hold = bool(ma5_now > 0 and close >= ma5_now)
        ma10_support = bool(ma10_now > 0 and close >= ma10_now * 0.985)
        ma_support = bool(ma5_close_hold or ma10_support)
        entry_close_loc_ok = bool(close_loc_pct >= 60.0)

        breakout_candidates = []
        max_days = min(10, len(df) - 2)
        for d in range(1, max_days + 1):
            pos = len(df) - 1 - d
            if pos <= 60:
                continue
            b = df.iloc[pos]
            prev_b = df.iloc[pos - 1] if pos >= 1 else None
            bopen = _safe_float(b.get('Open', 0), 0.0)
            bclose = _safe_float(b.get('Close', 0), 0.0)
            bhigh = _safe_float(b.get('High', 0), 0.0)
            blow = _safe_float(b.get('Low', 0), 0.0)
            bvol = _safe_float(b.get('Volume', 0), 0.0)
            prev_close = _safe_float(prev_b.get('Close', 0), 0.0) if prev_b is not None else 0.0
            try:
                bdate = pd.Timestamp(b.get('Date')).strftime('%Y-%m-%d') if not pd.isna(b.get('Date')) else ''
            except Exception:
                bdate = ''

            prior252 = df.iloc[max(0, pos-252):pos]
            prior120 = df.iloc[max(0, pos-120):pos]
            high252 = _safe_float(prior252['High'].max(), 0.0) if not prior252.empty else 0.0
            high120 = _safe_float(prior120['High'].max(), 0.0) if not prior120.empty else 0.0
            base_high = high252 if len(prior252) >= 200 and high252 > 0 else high120
            if base_high <= 0 or bopen <= 0 or bclose <= 0 or bhigh <= 0 or blow <= 0:
                continue

            vol60 = _safe_float(df['Volume'].iloc[max(0, pos-60):pos].mean(), 0.0)
            bvol60_ratio = bvol / vol60 if vol60 > 0 else 0.0
            brange = max(0.0, bhigh - blow)
            breakout_day_ret_pct = ((bclose / prev_close - 1.0) * 100.0) if prev_close > 0 else 0.0
            breakout_body_pct = ((bclose - bopen) / bopen * 100.0) if bopen > 0 else 0.0
            breakout_close_loc_pct = ((bclose - blow) / brange * 100.0) if brange > 0 else 100.0
            breakout_upper_wick_pct = ((bhigh - max(bopen, bclose)) / brange * 100.0) if brange > 0 else 0.0
            breakout_body_range_pct = ((bclose - bopen) / brange * 100.0) if brange > 0 else 0.0

            close_new_high = bool(bclose >= base_high * 1.002)
            strong_vol = bool(bvol60_ratio >= 1.5)
            long_bull_body = bool(
                bclose > bopen
                and breakout_day_ret_pct >= 7.0
                and breakout_body_pct >= 5.0
                and breakout_close_loc_pct >= 75.0
                and breakout_upper_wick_pct <= 25.0
            )
            if close_new_high and strong_vol and long_bull_body:
                breakout_candidates.append({
                    'pos': pos,
                    'days': d,
                    'date': bdate,
                    'open': bopen,
                    'close': bclose,
                    'high': bhigh,
                    'low': blow,
                    'volume': bvol,
                    'amount_b': _safe_float(b.get('Amount', bclose * bvol), bclose * bvol) / 1e8,
                    'base_high': base_high,
                    'vol60_ratio': bvol60_ratio,
                    'day_ret_pct': breakout_day_ret_pct,
                    'body_pct': breakout_body_pct,
                    'close_loc_pct': breakout_close_loc_pct,
                    'upper_wick_pct': breakout_upper_wick_pct,
                    'body_range_pct': breakout_body_range_pct,
                    'long_bull': 1,
                    'high_type': '52주신고가' if len(prior252) >= 200 and high252 > 0 else '120일신고가',
                })

        if not breakout_candidates:
            return None

        br = sorted(breakout_candidates, key=lambda x: (x['days'], -x['vol60_ratio']))[0]
        pre_ctx = _evaluate_h_pre_breakout_context(df, br.get('pos', 0))
        post = df.iloc[br['pos']:]
        post_high = _safe_float(post['High'].max(), high) if not post.empty else high
        pullback_pct = ((post_high - close) / post_high * 100.0) if post_high > 0 and close > 0 else 0.0
        hold_breakout_zone = bool(close >= br['base_high'] * 0.99 and close >= br['close'] * 0.92)
        proper_pullback = bool(1.0 <= pullback_pct <= 10.0)
        not_cliff = bool(close >= max(ma20_now, br['base_high'] * 0.94) if ma20_now > 0 else close >= br['base_high'] * 0.94)
        volume_vs_breakout = (vol / br['volume']) if br.get('volume', 0) > 0 else 0.0
        volume_dry_vs_breakout = bool(br.get('volume', 0) > 0 and vol <= br['volume'] * 0.50)
        strict_volume_dry = bool(volume_dry and volume_dry_vs_breakout)

        cond = {
            '①신고가장대양봉돌파': True,
            '②돌파거래량1.5배↑': br['vol60_ratio'] >= 1.5,
            '③돌파봉상승7%↑': br['day_ret_pct'] >= 7.0,
            '④돌파봉몸통5%↑': br['body_pct'] >= 5.0,
            '⑤돌파봉상단마감': br['close_loc_pct'] >= 75.0 and br['upper_wick_pct'] <= 25.0,
            '⑥1~7일내타점': 1 <= br['days'] <= 7,
            '⑦거래량마름': strict_volume_dry,
            '⑧짧은음봉/짧은양봉': short_candle and short_red_or_small_bull,
            '⑨5일선위종가': ma5_close_hold,
            '⑩타점봉중상단마감': entry_close_loc_ok,
            '⑪돌파권유지': hold_breakout_zone,
            '⑫눌림1~10%': proper_pullback,
            '⑬급락아님': not_cliff,
        }
        passed = [k for k, v in cond.items() if bool(v)]
        score = 0
        score += 18 if cond['①신고가장대양봉돌파'] else 0
        score += 10 if cond['②돌파거래량1.5배↑'] else 0
        score += 8 if cond['③돌파봉상승7%↑'] else 0
        score += 8 if cond['④돌파봉몸통5%↑'] else 0
        score += 8 if cond['⑤돌파봉상단마감'] else 0
        score += 8 if cond['⑥1~7일내타점'] else 0
        score += 14 if cond['⑦거래량마름'] else 0
        score += 10 if cond['⑧짧은음봉/짧은양봉'] else 0
        score += 8 if cond['⑨5일선위종가'] else 0
        score += 4 if cond['⑩타점봉중상단마감'] else 0
        score += 2 if cond['⑪돌파권유지'] else 0
        score += 1 if cond['⑫눌림1~10%'] else 0
        score += 1 if cond['⑬급락아님'] else 0
        score = min(int(score), 100)
        if score < 70:
            return None

        grade = '완전체' if score >= 90 else ('✅A급' if score >= 80 else 'B급')
        h = _bt_common_payload(code, name, 'H', '신고가거자름STRICT', grade, score, row, df, idx_label, marcap, passed)
        h_stop = ma5_now * 0.985 if ma5_now > 0 else low
        h.update({
            'band_type': 'HIGH_DRYUP_STRICT',
            'band_reason': '신고가 장대양봉 돌파 후 거래량 마른 짧은 타점봉',
            'high_breakout_date': br['date'],
            'high_breakout_type': br['high_type'],
            'days_since_high_breakout': int(br['days']),
            'breakout_vol60_ratio': round(br['vol60_ratio'], 2),
            'breakout_base_high': round(br['base_high']),
            'breakout_long_bull': 1,
            'breakout_day_ret_pct': round(br['day_ret_pct'], 2),
            'breakout_body_pct': round(br['body_pct'], 2),
            'breakout_close_loc_pct': round(br['close_loc_pct'], 1),
            'breakout_upper_wick_pct': round(br['upper_wick_pct'], 1),
            'breakout_body_range_pct': round(br['body_range_pct'], 1),
            'breakout_close': round(br['close']),
            'breakout_volume': round(br['volume']),
            'breakout_amount_b': round(_safe_float(br.get('amount_b', 0.0), 0.0), 1),
            'entry_amount_b': round(_safe_float(info.get('amount_b', 0.0), 0.0), 1),
            'entry_vs_breakout_amount': round((_safe_float(info.get('amount_b', 0.0), 0.0) / _safe_float(br.get('amount_b', 0.0), 1.0)), 2) if _safe_float(br.get('amount_b', 0.0), 0.0) > 0 else 0.0,
            'post_breakout_high': round(post_high),
            'high_dryup_pullback_pct': round(pullback_pct, 2),
            'high_dryup_proper_pullback': int(bool(proper_pullback)),
            'high_dryup_volume_dry': int(bool(strict_volume_dry)),
            'high_dryup_volume_dry_prev5': int(bool(volume_dry_prev5)),
            'high_dryup_volume_dry_vs_breakout': int(bool(volume_dry_vs_breakout)),
            'high_dryup_short_candle': int(bool(short_candle and short_red_or_small_bull)),
            'high_dryup_ma_support': int(bool(ma_support)),
            'high_dryup_ma5_close_hold': int(bool(ma5_close_hold)),
            'high_dryup_zone_hold': int(bool(hold_breakout_zone)),
            'high_dryup_entry_close_loc_ok': int(bool(entry_close_loc_ok)),
            'high_dryup_close_loc_pct': round(close_loc_pct, 1),
            'high_dryup_body_pct': round(body_pct, 2),
            'high_dryup_range_pct': round(range_pct, 2),
            'high_dryup_upper_wick_pct': round(upper_wick_pct, 1),
            'high_dryup_prev5_vol_ratio': round(vol / prev5_vol, 2) if prev5_vol > 0 else 0.0,
            'high_dryup_vol_vs_breakout': round(volume_vs_breakout, 2),
            'vol_ratio': round(vol / vma20_now, 2) if vma20_now > 0 else info.get('vol_ratio', 0),
            'close_loc_pct': round(close_loc_pct, 1),
            'wick_pct': round(upper_wick_pct, 1),
            'stoploss': round(h_stop) if h_stop > 0 else info.get('stoploss', 0),
            'target1': round(close * 1.05),
            'rr': round(((close * 1.05 - close) / (close - h_stop)), 2) if h_stop > 0 and close > h_stop else 0.0,
            **pre_ctx,
            'sell_rule': '종가 5일선 이탈 시 정리',
        })

        # v4.2.9 실시간 운영에서는 일반 H를 노출하지 않는다.
        # 직전 삼각수렴, 거래대금×Vol60 핵심셀, 또는 8배+ 빠른익절형만 실제 후보로 반환한다.
        _bamt = _safe_float(h.get('breakout_amount_b', 0), 0.0)
        _bvol = _safe_float(h.get('breakout_vol60_ratio', 0), 0.0)
        _is_struct = (
            score >= 82
            and 1 <= _safe_int(h.get('days_since_high_breakout', 999), 999) <= 7
            and _safe_int(h.get('high_dryup_volume_dry', 0), 0) == 1
            and _safe_int(h.get('high_dryup_short_candle', 0), 0) == 1
            and _safe_int(h.get('high_dryup_ma5_close_hold', 0), 0) == 1
            and _safe_int(h.get('high_dryup_entry_close_loc_ok', 0), 0) == 1
            and _safe_int(h.get('high_dryup_zone_hold', 0), 0) == 1
            and 1.0 <= _safe_float(h.get('high_dryup_pullback_pct', 999), 999.0) <= 10.0
            and _bamt >= 100.0
        )
        _is_triangle = _safe_int(h.get('h_pre_triangle', 0), 0) == 1
        _is_core_cell = (
            (_is_struct and 500.0 <= _bamt < 1000.0 and 2.0 <= _bvol < 3.0)
            or (_is_struct and 300.0 <= _bamt < 500.0 and 3.0 <= _bvol < 5.0)
            or (_is_struct and 1000.0 <= _bamt < 2000.0 and 2.0 <= _bvol < 3.0)
        )
        _is_fast = _is_struct and _bvol >= 8.0
        if not (_is_triangle or _is_core_cell or _is_fast):
            return None
        return h
    except Exception as e:
        log_error(f"_check_high_dryup_bet 오류 [{code}/{name}]: {e}")
        return None


def _check_i_core_bet(code: str, name: str) -> dict | None:
    """v4.3.6: 실시간 스캔에서 I-MAIN 150/200일 시세분출 후보 1개를 반환한다.
    I-MAIN은 단기 종가배팅이 아니라 20/40/60일 누적관찰·분할매집 후보이므로,
    S/L/G/H 후보와 분리해 별도 출력한다.
    """
    try:
        code = _normalize_code(code)
        hist = _load_df(code, lookback_days=760)
        if hist is None or hist.empty or len(hist) < 260:
            return None
        row = hist.iloc[-1]
        info = _base_info(row, hist)
        idx_label = str(INDEX_MAP.get(code, '') or '')
        marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)
        hits = _build_icore_hits(code, name, hist, row, info, idx_label, marcap)
        if not hits:
            return None

        def _is_main(h):
            return _safe_int(h.get('i_core_main_candidate', 0), 0) == 1

        def _is_anchor(h):
            d = _safe_int(h.get('i_anchor_days', 999), 999)
            return 120 <= d <= 180

        def _is_monthly(h):
            return _safe_int(h.get('i_monthly_vol_rebuild', 0), 0) == 1

        def _rank(h):
            phase = str(h.get('i_phase', '') or '')
            long_dist = _safe_float(h.get('i_long_ma_dist_pct', 999), 999.0)
            main = _is_main(h)
            accel = _safe_int(h.get('i_core_main_accel', 0), 0) == 1
            anchor = _is_anchor(h)
            monthly = _is_monthly(h)
            # v4.3.5 결과: ACCEL > CORE > MAIN I-4 > ADD > CONFIRM 순서
            if main and accel and anchor:
                cls_rank = 0
            elif main and phase == 'I-4' and anchor and monthly:
                cls_rank = 1
            elif main and phase == 'I-4' and anchor:
                cls_rank = 2
            elif main and phase == 'I-4':
                cls_rank = 3
            elif main and phase == 'I-6':
                cls_rank = 4
            elif main and phase == 'I-5':
                cls_rank = 5
            elif main:
                cls_rank = 6
            else:
                cls_rank = 9
            return (
                cls_rank,
                -_safe_float(h.get('score', 0), 0.0),
                -_safe_float(h.get('i_material_proxy_score', 0), 0.0),
                -_safe_float(h.get('amount_b', 0), 0.0),
                abs(long_dist - 12.0),
            )

        main_hits = [h for h in hits if _is_main(h)]
        pool = main_hits or hits
        pool.sort(key=_rank)
        best = pool[0]
        best['i_realtime_output'] = 1
        best['mode'] = 'I'
        best['strategy'] = 'I'
        return best
    except Exception as e:
        log_error(f"_check_i_core_bet 오류 [{code}/{name}]: {e}")
        return None





def _i_leader_lifecycle_context(hist: pd.DataFrame) -> dict:
    """v4.4.9.32: 주봉 5/20/60 정배열·20주 거래량·MACD/RSI·고점 위험으로
    I-MAIN 후보의 주도주 사이클 위치를 라벨링한다.

    참고 프레임: 주봉 정배열 후 1년 이내는 초입/가속, 1년 이후는 신규매수보다
    고점 신호 감시, -20% 내외 조정·4/26주 데드크로스는 공세종말 위험으로 본다.
    """
    base = {
        'i_leader_class': 'WATCH',
        'i_leader_label': '🔎 I-LEADER WATCH',
        'i_leader_desc': '주도주 사이클 조건 확인 필요',
        'i_leader_age_weeks': 0,
        'i_leader_age_days': 0,
        'i_leader_weekly_align': 0,
        'i_leader_wvol_ratio20': 0.0,
        'i_leader_recent_wvol_ratio20_max': 0.0,
        'i_leader_rsi14w': 0.0,
        'i_leader_macd_osc': 0.0,
        'i_leader_drawdown_52w_pct': 0.0,
        'i_leader_risk_tags': '',
    }
    try:
        if not bool(globals().get('CLOSING_BET_I_LEADER_LIFECYCLE_LABEL', True)):
            return base
        if hist is None or len(hist) < 120:
            return base
        h = hist.copy()
        # 날짜 인덱스가 없으면 Date/date 컬럼을 찾아 주봉 변환을 시도한다.
        if not isinstance(h.index, pd.DatetimeIndex):
            date_col = None
            for c in ('Date', 'date', '날짜'):
                if c in h.columns:
                    date_col = c
                    break
            if date_col is None:
                return base
            h[date_col] = pd.to_datetime(h[date_col], errors='coerce')
            h = h.dropna(subset=[date_col]).set_index(date_col)
        if h.empty or 'Close' not in h.columns or 'Volume' not in h.columns:
            return base
        h = h.sort_index()
        agg = {'Close': 'last', 'Volume': 'sum'}
        if 'High' in h.columns:
            agg['High'] = 'max'
        else:
            h['High'] = h['Close']
            agg['High'] = 'max'
        if 'Low' in h.columns:
            agg['Low'] = 'min'
        else:
            h['Low'] = h['Close']
            agg['Low'] = 'min'
        w = h.resample('W-FRI').agg(agg).dropna(subset=['Close'])
        if len(w) < 70:
            return base
        close = pd.to_numeric(w['Close'], errors='coerce')
        vol = pd.to_numeric(w['Volume'], errors='coerce').fillna(0)
        high = pd.to_numeric(w['High'], errors='coerce')
        low = pd.to_numeric(w['Low'], errors='coerce')
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        ma4 = close.rolling(4).mean()
        ma26 = close.rolling(26).mean()
        v20_prev = vol.rolling(20).mean().shift(1)
        wvol_ratio = vol / v20_prev.replace(0, np.nan)
        recent_wvol_max = _safe_float(wvol_ratio.tail(4).max(), 0.0)
        rsi14 = _safe_float(_calc_rsi(close, 14).iloc[-1], 50.0)
        macd = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
        macd_signal = macd.ewm(span=9, adjust=False).mean()
        macd_osc = macd - macd_signal
        macd_osc_now = _safe_float(macd_osc.iloc[-1], 0.0)
        full_align = (ma5 > ma20) & (ma20 > ma60) & (close >= ma20)
        weekly_align = int(bool(full_align.iloc[-1]))
        age_weeks = 0
        if weekly_align:
            vals = full_align.fillna(False).tolist()
            i = len(vals) - 1
            while i >= 0 and vals[i]:
                age_weeks += 1
                i -= 1
        dd52 = 0.0
        peak52 = _safe_float(high.tail(52).max(), 0.0)
        now_close = _safe_float(close.iloc[-1], 0.0)
        if peak52 > 0 and now_close > 0:
            dd52 = (now_close / peak52 - 1.0) * 100.0
        risk_tags = []
        ma4_now = _safe_float(ma4.iloc[-1], 0.0)
        ma26_now = _safe_float(ma26.iloc[-1], 0.0)
        if dd52 <= -20.0:
            risk_tags.append('고점대비20%조정')
        if ma4_now > 0 and ma26_now > 0 and ma4_now < ma26_now:
            risk_tags.append('4/26주DC')
        if not weekly_align and _safe_float(ma5.iloc[-1], 0.0) > 0 and _safe_float(ma20.iloc[-1], 0.0) > 0 and _safe_float(ma5.iloc[-1], 0.0) < _safe_float(ma20.iloc[-1], 0.0):
            risk_tags.append('5/20주약화')
        # 최근 4주 변동폭이 과도하고 고점에서 밀리면 후반 위험으로 태그만 붙인다.
        recent_range = ((high.tail(4).max() / max(_safe_float(low.tail(4).min(), 0.0), 1e-9)) - 1.0) * 100.0
        if recent_range >= 25.0 and dd52 <= -10.0:
            risk_tags.append('변동성확대')

        # 라벨 결정. 하드 매수/매도 신호가 아니라 사이클 위치 라벨이다.
        if risk_tags:
            cls = 'CULMINATION_RISK'
            label = '🧯 I-LEADER 공세종말 위험'
            desc = '신규진입 금지/비중축소 검토: ' + ','.join(risk_tags)
        elif weekly_align and 1 <= age_weeks <= 12 and (recent_wvol_max >= 3.0 or (macd_osc_now > 0 and 50 <= rsi14 <= 65)):
            cls = 'EARLY'
            label = '👑 I-LEADER 초입'
            desc = '주봉 정배열 전환 초기. 첫 5/20주선 눌림 대기/분할관찰'
        elif weekly_align and age_weeks <= 52:
            cls = 'ACCEL'
            label = '🚀 I-LEADER 가속'
            desc = '정배열 1년 이내 주도 상승 진행. 눌림 재지지 시 중기 우대'
        elif weekly_align and age_weeks <= 78:
            cls = 'MATURE'
            label = '🧭 I-LEADER 허리'
            desc = '정배열 1년 전후. 보유/스윙은 가능하되 고점 신호 감시'
        elif weekly_align:
            cls = 'LATE'
            label = '⚠️ I-LEADER 후반'
            desc = '정배열 장기화. 신규매수보다 분할매도/리스크 관리 우선'
        else:
            cls = 'WATCH'
            label = '🔎 I-LEADER 관찰'
            desc = '주봉 5/20/60 완전 정배열 전환 전이거나 에너지 확인 부족'

        base.update({
            'i_leader_class': cls,
            'i_leader_label': label,
            'i_leader_desc': desc,
            'i_leader_age_weeks': int(age_weeks),
            'i_leader_age_days': int(age_weeks * 7),
            'i_leader_weekly_align': int(weekly_align),
            'i_leader_wvol_ratio20': round(_safe_float(wvol_ratio.iloc[-1], 0.0), 2),
            'i_leader_recent_wvol_ratio20_max': round(recent_wvol_max, 2),
            'i_leader_rsi14w': round(rsi14, 1),
            'i_leader_macd_osc': round(macd_osc_now, 2),
            'i_leader_drawdown_52w_pct': round(dd52, 1),
            'i_leader_risk_tags': ' / '.join(risk_tags),
        })
        return base
    except Exception as e:
        base['i_leader_desc'] = f'주도주 사이클 계산 오류: {type(e).__name__}'
        return base


def _i_leader_summary_text(h: dict) -> str:
    """v4.4.9.32: I-MAIN/IT 카드에 붙일 한 줄 요약."""
    try:
        if not bool(globals().get('CLOSING_BET_I_LEADER_LIFECYCLE_LABEL', True)):
            return ''
        label = str(h.get('i_leader_label', '') or '').strip()
        if not label:
            return ''
        age = _safe_int(h.get('i_leader_age_weeks', 0), 0)
        wvol = _safe_float(h.get('i_leader_recent_wvol_ratio20_max', 0), 0.0)
        rsi = _safe_float(h.get('i_leader_rsi14w', 0), 0.0)
        dd = _safe_float(h.get('i_leader_drawdown_52w_pct', 0), 0.0)
        desc = str(h.get('i_leader_desc', '') or '').strip()
        bits = []
        if age > 0:
            bits.append(f'정배열 {age}주')
        if wvol > 0:
            bits.append(f'최근주간거래량 {wvol:.1f}배')
        if rsi > 0:
            bits.append(f'주봉RSI {rsi:.0f}')
        if dd < 0:
            bits.append(f'52주고점대비 {dd:.1f}%')
        mid = ' / '.join(bits) if bits else '주봉 사이클 확인'
        return f'{label} — {mid}. {desc}'
    except Exception:
        return ''


def _i_leader_final_reason(h: dict) -> str:
    try:
        label = str(h.get('i_leader_label', '') or '').replace('I-LEADER ', '').strip()
        age = _safe_int(h.get('i_leader_age_weeks', 0), 0)
        if not label:
            return ''
        return f'I-LEADER {label}({age}주)' if age > 0 else f'I-LEADER {label}'
    except Exception:
        return ''


def _classify_lp_candidate(h: dict) -> dict:
    """v4.4.7: LP 리더갭 눌림재지지 후보를 SAFE/WATCH/RISK로 분류한다.
    기존 LP 검색식을 훼손하지 않고, 운용 라벨만 추가한다.
    """
    try:
        gap_amt = _safe_float(h.get('lp_gap_amount_b', h.get('leader_gap_amount_b', 0)), 0.0)
        gap_pct = _safe_float(h.get('lp_gap_pct', h.get('gap_pct', 0)), 0.0)
        close_loc = _safe_float(h.get('close_loc_pct', 0), 0.0)
        amount_b = _safe_float(h.get('amount_b', 0), 0.0)
        hold_gap = _safe_int(h.get('lp_gap_zone_hold', 0), 0) == 1
        ma_hold = _safe_int(h.get('lp_ma_hold', 0), 0) == 1
        vol_vs_gap = _safe_float(h.get('lp_volume_vs_gap', 0), 0.0)
        pullback = _safe_float(h.get('lp_pullback_pct', 999), 999.0)
        days = _safe_int(h.get('lp_days_since_gap', 999), 999)
        wick = _safe_float(h.get('wick_pct', 0), 0.0)
        score = _safe_float(h.get('score', 0), 0.0)

        risk_reasons = []
        if not hold_gap:
            risk_reasons.append('갭하단/전일종가 미확인')
        if not ma_hold:
            risk_reasons.append('5·10일선 재지지 약함')
        if close_loc < 55.0:
            risk_reasons.append('타점봉 종가위치 약함')
        if vol_vs_gap > 1.05:
            risk_reasons.append('거래량 재증가 과열')
        if pullback > 12.0:
            risk_reasons.append('눌림폭 과대')
        if wick > 45.0:
            risk_reasons.append('윗꼬리 부담')

        safe_core = bool(
            gap_amt >= 5000.0
            and 3.0 <= gap_pct <= 12.0
            and amount_b >= 1000.0
            and close_loc >= 60.0
            and hold_gap
            and ma_hold
            and 0.20 <= vol_vs_gap <= 0.85
            and 0.0 <= pullback <= 12.0
            and 1 <= days <= 5
            and score >= 82.0
        )
        fast_type = bool(safe_core and (close_loc >= 70.0 or 6.0 <= gap_pct <= 12.0) and pullback <= 8.0)

        if risk_reasons:
            cls = 'LP-RISK'
            emoji = '⚠️'
            decision = '실전 제외/관찰: 갭하단·이평 재지지 확인 전까지 매수 금지'
        elif safe_core:
            cls = 'LP-SAFE'
            emoji = '🥇'
            decision = '단기 최우선 후보: L 당일 추격보다 1~5일 눌림재지지 확인형으로 +3/+5 우선 대응'
        else:
            cls = 'LP-WATCH'
            emoji = '🔁'
            decision = '관찰 후보: 핵심 조건 일부 부족, 다음날 양봉·전일고가 회복·거래대금 유지 시 승격'

        reason_bits = []
        if gap_amt >= 5000.0:
            reason_bits.append('갭봉5000억+')
        if 3.0 <= gap_pct <= 12.0:
            reason_bits.append(f'갭{gap_pct:+.1f}%')
        if close_loc >= 60.0:
            reason_bits.append(f'타점종가{close_loc:.0f}%')
        if amount_b >= 1000.0:
            reason_bits.append(f'현거래대금{amount_b:.0f}억')
        if 0.20 <= vol_vs_gap <= 0.85:
            reason_bits.append(f'거래량식힘{vol_vs_gap:.2f}배')
        if fast_type:
            reason_bits.append('FAST +3/+5형')

        timing_bucket, timing_label = _lp_timing_bucket(h)
        return {
            'lp_class': cls,
            'lp_class_label': f'{emoji}{cls}',
            'lp_timing_bucket': timing_bucket,
            'lp_timing_label': timing_label,
            'lp_fast_take_profit': int(bool(fast_type)),
            'lp_decision': decision,
            'lp_class_reason': ' · '.join(reason_bits) if reason_bits else '조건 확인 필요',
            'lp_risk_reasons': ' / '.join(risk_reasons),
        }
    except Exception as e:
        return {
            'lp_class': 'LP-WATCH',
            'lp_class_label': '🔁LP-WATCH',
            'lp_timing_bucket': 'LP-DX',
            'lp_timing_label': '타점시차 확인 필요',
            'lp_fast_take_profit': 0,
            'lp_decision': f'LP 분류 오류: {type(e).__name__}',
            'lp_class_reason': '',
            'lp_risk_reasons': '',
        }


def _lp_class(h: dict) -> str:
    try:
        cls = str(h.get('lp_class', '') or '').strip()
        if cls:
            return cls
        return str(_classify_lp_candidate(h).get('lp_class', 'LP-WATCH'))
    except Exception:
        return 'LP-WATCH'


def _is_lp_safe_hit(h: dict) -> bool:
    return _lp_class(h) == 'LP-SAFE'


def _is_lp_watch_hit(h: dict) -> bool:
    return _lp_class(h) == 'LP-WATCH'


def _is_lp_risk_hit(h: dict) -> bool:
    return _lp_class(h) == 'LP-RISK'


def _lp_explosion_watch_context(h: dict) -> dict:
    """v4.4.9.39: LP 후보의 추천강도와 진입방식을 분리한다.

    A(엄격형): 강한 조건을 만족하는 LP-POWER PRIME.
    B(확장형): 후보 강도는 높지만 종가추격보다 지정가/다음날 재돌파로 보는 LP-POWER PRIME.

    이 라벨은 보조 후보가 아니라, 강력추천 지정가·다음날 갭하단/전일고가/첫 눌림 재돌파 확인 라벨이다.
    """
    try:
        if _lp_class(h) != 'LP-SAFE':
            return {'is_explosion': False, 'grade': '', 'score': 0, 'reason': ''}
        tb, _ = _lp_timing_bucket(h)
        if tb not in ('LP-D23', 'LP-D45'):
            return {'is_explosion': False, 'grade': '', 'score': 0, 'reason': ''}

        gap_amt = _safe_float(h.get('lp_gap_amount_b', h.get('leader_gap_amount_b', 0)), 0.0)
        gap_pct = _safe_float(h.get('lp_gap_pct', h.get('gap_pct', 0)), 0.0)
        amount_b = _safe_float(h.get('amount_b', 0), 0.0)
        close_loc = _safe_float(h.get('close_loc_pct', 0), 0.0)
        volr = _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0)
        lp_vs_gap = _safe_float(h.get('lp_volume_vs_gap', 0), 0.0)
        pullback = _safe_float(h.get('lp_pullback_pct', 999), 999.0)
        hold_gap = _safe_int(h.get('lp_gap_zone_hold', 0), 0) == 1
        ma_hold = _safe_int(h.get('lp_ma_hold', 0), 0) == 1
        days = _safe_int(h.get('lp_days_since_gap', 999), 999)

        text = ' '.join(str(h.get(k, '') or '') for k in (
            'name', 'sector', 'theme', 'tags', 'n_combo', 'reason', 'material_hint', 'news_hint', 'sector_name'
        ))
        high_beta_hint = any(x in text for x in ('반도체', '장비', '소부장', 'AI', '로봇', '전력', '2차전지', '바이오'))

        common_hold = bool(hold_gap and ma_hold and 1 <= days <= 5)
        strict_a = bool(
            common_hold
            and gap_amt >= 5000.0
            and 3.0 <= gap_pct <= 12.0
            and amount_b >= 1000.0
            and 65.0 <= close_loc <= 85.0
            and 0.45 <= lp_vs_gap <= 0.95
            and 1.15 <= volr <= 2.30
            and 0.0 <= pullback <= 12.0
        )
        # LP-POWER PRIME 확장형: 후보 강도는 높지만 막판 종가위치가 밀려 종가추격 대신 지정가/다음날 재돌파로 보는 후보.
        # 추천강도와 진입방식을 분리한다. 강한 후보라도 시장가 추격이 아니라 지정가/확인형이다.
        hpsp_b = bool(
            common_hold
            and gap_amt >= 3000.0
            and 2.5 <= gap_pct <= 15.0
            and amount_b >= 1000.0
            and 68.0 <= close_loc <= 92.0
            and 0.35 <= lp_vs_gap <= 1.10
            and 1.05 <= volr <= 2.80
            and 0.0 <= pullback <= 14.0
        )
        if not (strict_a or hpsp_b):
            return {'is_explosion': False, 'grade': '', 'score': 0, 'reason': ''}

        grade = 'A' if strict_a else 'B'
        score = 0
        if grade == 'A':
            score += 4
        if tb == 'LP-D23':
            score += 3
        elif tb == 'LP-D45':
            score += 2
        if gap_amt >= 5000:
            score += 2
        elif gap_amt >= 3000:
            score += 1
        if amount_b >= 3000:
            score += 2
        elif amount_b >= 1000:
            score += 1
        if 70 <= close_loc <= 85:
            score += 2
        elif close_loc >= 68:
            score += 1
        if 0.55 <= lp_vs_gap <= 0.90:
            score += 2
        elif 0.35 <= lp_vs_gap <= 1.10:
            score += 1
        if 1.30 <= volr <= 2.30:
            score += 2
        elif 1.05 <= volr <= 2.80:
            score += 1
        if high_beta_hint:
            score += 1

        bits = [
            f'POWER-{grade}',
            tb,
            f'갭봉{gap_amt:.0f}억',
            f'현거래대금{amount_b:.0f}억',
            f'종가위치{close_loc:.0f}%',
            f'거래량비{volr:.2f}',
            f'식힘{lp_vs_gap:.2f}',
        ]
        if high_beta_hint:
            bits.append('고베타/재료확산힌트')
        return {
            'is_explosion': True,
            'grade': grade,
            'score': score,
            'reason': ' · '.join(bits),
            'label': '🔥LP-POWER PRIME',
        }
    except Exception:
        return {'is_explosion': False, 'grade': '', 'score': 0, 'reason': ''}


def _is_lp_explosion_watch(h: dict) -> bool:
    return bool(_lp_explosion_watch_context(h).get('is_explosion', False))


def _priority_lp_explosion(h: dict):
    ctx = _lp_explosion_watch_context(h)
    return (-_safe_float(ctx.get('score', 0), 0.0), _priority_practical(h))


def _lp_timing_bucket(h: dict) -> tuple[str, str]:
    """v4.4.7: LP 타점 시차를 D1 / D23 / D45로 구분한다."""
    try:
        d = _safe_int(h.get('lp_days_since_gap', 0), 0)
    except Exception:
        d = 0
    if d <= 1:
        return 'LP-D1', '갭후 1일 빠른 재지지'
    if d <= 3:
        return 'LP-D23', '갭후 2~3일 핵심 눌림재지지'
    if d <= 5:
        return 'LP-D45', '갭후 4~5일 충분한 식힘 후 재상승'
    return 'LP-DX', '갭후 시차 확인 필요'


def _lp_next_day_scenario(h: dict) -> str:
    """v4.4.7: LP-SAFE/LP-WATCH 다음날 대응 시나리오를 텔레그램에 직접 출력한다."""
    close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
    prev_close = _safe_float(h.get('lp_prev_close', 0), 0.0)
    gap_low = _safe_float(h.get('lp_gap_low', 0), 0.0)
    stop = _safe_float(h.get('stoploss', 0), 0.0)
    t3 = close * 1.03 if close > 0 else 0.0
    t5 = close * 1.05 if close > 0 else 0.0
    support_bits = []
    if gap_low > 0:
        support_bits.append(f"갭저가 {int(gap_low):,}원")
    if prev_close > 0:
        support_bits.append(f"갭전 종가 {int(prev_close):,}원")
    if stop > 0:
        support_bits.append(f"무효 {int(stop):,}원")
    support_text = ' / '.join(support_bits) if support_bits else '갭하단·전일종가·5/10일선'
    return (
        "다음날 대응: +2% 이상 갭상승 출발은 추격금지, 5분봉 첫 눌림·전일고가 재돌파 확인. "
        "보합~+1% 출발은 전일고가 회복 시 1차. -1~-3% 눌림은 갭하단/전일종가 지지 확인 후만 접근. "
        f"지지 기준: {support_text}. 익절: +3% {int(t3):,}원 1차 / +5% {int(t5):,}원 2차. "
        "무효: 갭하단·전일종가 재이탈 또는 거래량 증가 장대음봉."
    )


def _check_leader_gap_pullback_bet(code: str, name: str) -> dict | None:
    """v4.4.1 신규검증 LP — L-PULLBACK 리더갭 눌림재지지형.

    기존 L 리더갭 당일 검색식은 그대로 두고, 최근 1~5거래일 안에 강한 L 갭이 나온 뒤
    갭하단/전일종가/5·10일선을 지키며 거래량이 식은 재진입 타점만 별도 표시한다.
    파일 저장/CSV 확인 없이 텔레그램 실시간 후보에서 바로 판단할 수 있게 운용 문구를 포함한다.
    """
    try:
        code = _normalize_code(code)
        df = _load_df(code, lookback_days=730)
        if df is None or len(df) < 150:
            return None
        row = df.iloc[-1]
        info = _base_info(row, df)
        idx_label = INDEX_MAP.get(code, '')
        marcap = _safe_float(MARCAP_MAP.get(code, 0), 0.0)
        is_mcap_or = marcap >= MCAP_OR_MIN
        if not ((idx_label in ('코스피200', '코스닥150')) or is_mcap_or):
            return None
        close = info['_close']; open_p = info['_open']; high = info['_high']; low = info['_low']; vol = info['_vol']
        if close < MIN_PRICE or info.get('amount_b', 0) < 1000.0:
            return None
        ma5 = _safe_float(row.get('MA5', 0), 0.0)
        ma10 = _safe_float(row.get('MA10', 0), 0.0)
        ma20 = _safe_float(row.get('MA20', 0), 0.0)
        vma20 = _safe_float(row.get('VMA20', 0), 0.0)
        cur_range = max(0.0, high - low)
        cur_close_loc = ((close - low) / cur_range * 100.0) if cur_range > 0 else 100.0
        cur_upper_wick = ((high - max(open_p, close)) / cur_range * 100.0) if cur_range > 0 else 0.0
        # 현재봉이 급락/분배봉이면 눌림재지지가 아니라 관찰 제외
        if cur_close_loc < 55.0 or cur_upper_wick > 45.0:
            return None

        gap_candidates = []
        max_days = min(5, len(df) - 2)
        for d in range(1, max_days + 1):
            pos = len(df) - 1 - d
            if pos < 60:
                continue
            g = df.iloc[pos]
            prev = df.iloc[pos - 1]
            g_open = _safe_float(g.get('Open', 0), 0.0)
            g_close = _safe_float(g.get('Close', 0), 0.0)
            g_high = _safe_float(g.get('High', 0), 0.0)
            g_low = _safe_float(g.get('Low', 0), 0.0)
            g_vol = _safe_float(g.get('Volume', 0), 0.0)
            prev_close = _safe_float(prev.get('Close', 0), 0.0)
            if min(g_open, g_close, g_high, g_low, prev_close) <= 0:
                continue
            g_amount_b = _safe_float(g.get('Amount', g_close * g_vol), g_close * g_vol) / 1e8
            gap_pct = (g_open / prev_close - 1.0) * 100.0
            vol50 = _safe_float(df['Volume'].iloc[max(0, pos-50):pos].mean(), 0.0)
            vol50_ratio = g_vol / vol50 if vol50 > 0 else 0.0
            grange = max(0.0, g_high - g_low)
            g_close_loc = ((g_close - g_low) / grange * 100.0) if grange > 0 else 100.0
            g_upper_wick = ((g_high - max(g_open, g_close)) / grange * 100.0) if grange > 0 else 0.0
            # v4.3.9 성과 핵심: 5000억+·갭3~12·종가위치70%+를 기준으로 삼는다.
            if not (3.0 <= gap_pct <= 12.0):
                continue
            if g_amount_b < LEADER_GAP_CORE_AMOUNT_B:
                continue
            if vol50_ratio < LEADER_GAP_VOL50_MIN:
                continue
            if g_close_loc < 70.0 or g_upper_wick > 35.0:
                continue
            gap_candidates.append({
                'days': d, 'pos': pos, 'gap_pct': gap_pct, 'vol50_ratio': vol50_ratio,
                'amount_b': g_amount_b, 'open': g_open, 'close': g_close, 'high': g_high,
                'low': g_low, 'volume': g_vol, 'prev_close': prev_close,
                'close_loc': g_close_loc, 'wick': g_upper_wick,
            })
        if not gap_candidates:
            return None
        br = sorted(gap_candidates, key=lambda x: (x['days'], -x['amount_b'], -x['vol50_ratio']))[0]

        gap_support = max(br['prev_close'] * 0.995, br['low'] * 0.995)
        hold_gap_zone = close >= gap_support
        ma_hold = (ma5 > 0 and close >= ma5 * 0.995) or (ma10 > 0 and close >= ma10 * 0.995)
        volume_vs_gap = vol / br['volume'] if br.get('volume', 0) > 0 else 0.0
        volume_cooling = 0.20 <= volume_vs_gap <= 0.85
        post = df.iloc[br['pos']:]
        post_high = _safe_float(post['High'].max(), br['high']) if post is not None and not post.empty else br['high']
        pullback_pct = ((post_high - close) / post_high * 100.0) if post_high > 0 else 0.0
        proper_pullback = 0.0 <= pullback_pct <= 12.0
        not_cliff = (ma20 <= 0 or close >= ma20 * 0.97) and close >= br['prev_close'] * 0.995
        if not (hold_gap_zone and ma_hold and volume_cooling and proper_pullback and not_cliff):
            return None

        passed = []
        score = 0
        score += 18; passed.append(f"①최근{br['days']}일내 L갭")
        if br['amount_b'] >= LEADER_GAP_CORE_AMOUNT_B:
            score += 18; passed.append(f"②갭봉대금{br['amount_b']:.0f}억")
        if 6.0 <= br['gap_pct'] <= 12.0:
            score += 12; passed.append(f"③갭6~12%({br['gap_pct']:+.1f}%)")
        else:
            score += 8; passed.append(f"③갭{br['gap_pct']:+.1f}%")
        if hold_gap_zone:
            score += 14; passed.append('④갭하단/전일종가 지지')
        if ma_hold:
            score += 10; passed.append('⑤5·10일선 재지지')
        if volume_cooling:
            score += 12; passed.append(f"⑥거래량식힘 {volume_vs_gap:.2f}배")
        if cur_close_loc >= 65.0:
            score += 8; passed.append(f"⑦타점봉종가위치{cur_close_loc:.0f}%")
        if info.get('amount_b', 0) >= 1000.0:
            score += 8; passed.append(f"⑧현거래대금{info.get('amount_b',0):.0f}억")
        score = min(int(score), 100)
        if score < 76:
            return None
        grade = '완전체' if score >= 90 else ('✅A급' if score >= 82 else 'B급')
        h = _bt_common_payload(code, name, 'LP', 'L-PULLBACK 리더갭 눌림재지지', grade, score, row, df, idx_label, marcap, passed)
        stop_candidates = [x for x in [br['low'], br['prev_close'], ma10] if x > 0]
        stoploss = min(stop_candidates) if stop_candidates else low
        h.update({
            'band_type': 'LEADER_GAP_PULLBACK',
            'band_reason': '최근 L 리더갭 이후 갭하단/전일종가/5·10일선 눌림재지지 신규검증형',
            'lp_days_since_gap': int(br['days']),
            'lp_gap_pct': round(br['gap_pct'], 2),
            'lp_gap_amount_b': round(br['amount_b'], 1),
            'lp_gap_vol50_ratio': round(br['vol50_ratio'], 2),
            'lp_gap_close_loc_pct': round(br['close_loc'], 1),
            'lp_gap_wick_pct': round(br['wick'], 1),
            'lp_gap_low': round(br['low']),
            'lp_prev_close': round(br['prev_close']),
            'lp_volume_vs_gap': round(volume_vs_gap, 2),
            'lp_pullback_pct': round(pullback_pct, 2),
            'lp_gap_zone_hold': int(bool(hold_gap_zone)),
            'lp_ma_hold': int(bool(ma_hold)),
            'close_loc_pct': round(cur_close_loc, 1),
            'wick_pct': round(cur_upper_wick, 1),
            'vol_ratio': round(vol / vma20, 2) if vma20 > 0 else h.get('vol_ratio', 0),
            'stoploss': round(stoploss) if stoploss > 0 else h.get('stoploss', 0),
            'target1': round(close * 1.03),
            'rr': round(((close * 1.03 - close) / (close - stoploss)), 2) if stoploss > 0 and close > stoploss else 0.0,
            'new_pattern': 'LP',
            'sell_rule': 'L-PULLBACK: 갭하단/전일종가/5·10일선 재지지 유지 시만 관찰, 이탈 시 무효',
        })
        h.update(_classify_lp_candidate(h))
        return h
    except Exception as e:
        log_debug(f"_check_leader_gap_pullback_bet 오류 [{code}/{name}]: {e}")
        return None


def _make_s2_lock_candidate(s: dict | None) -> dict | None:
    """v4.4.1 신규검증 SLOCK — 기존 S2 후보 중 종가 상단잠김형만 별도 라벨링."""
    try:
        if not s or str(s.get('mode', '')) != 'S' or str(s.get('s_type', '')) != 'S2':
            return None
        close_loc = _safe_float(s.get('close_loc_pct', 0), 0.0)
        wick = _safe_float(s.get('upper_wick_range_pct', s.get('wick_pct', 0)), 0.0)
        amount_b = _safe_float(s.get('amount_b', 0), 0.0)
        volr = _safe_float(s.get('today_vol_ratio', s.get('vol_ratio', 0)), 0.0)
        rr = _safe_float(s.get('rr', 0), 0.0)
        marcap = _safe_float(s.get('marcap', 0), 0.0)
        idx = str(s.get('index_label', '') or '')
        leader_liq = amount_b >= 3000.0 or marcap >= GAP_LARGE_CAP_MARCAP or idx == '코스피200'
        if not (close_loc >= 80.0 and wick <= 20.0 and leader_liq and 0.70 <= volr <= 1.50 and rr >= 0.80):
            return None
        h = dict(s)
        h.update({
            'mode': 'SLOCK',
            'strategy': 'SLOCK',
            'mode_label': 'S2-LOCK 상단잠김',
            'band_type': 'S2_LOCK',
            'band_reason': '기존 S2 실행형 중 종가위치80%+·윗꼬리20% 이하·유동성 우대 상단잠김 신규검증형',
            'score': min(100.0, _safe_float(s.get('score', 0), 0.0) + 2.0),
            'new_pattern': 'SLOCK',
            'passed': list(s.get('passed', [])) + [f'⑫S2-LOCK 종가위치{close_loc:.0f}%', f'⑬윗꼬리{wick:.0f}%'],
        })
        return h
    except Exception:
        return None


def _make_i_trigger_candidate(i: dict | None) -> dict | None:
    """v4.4.1 신규검증 IT — I-MAIN 중기 후보의 단기 촉발형.

    I-MAIN 자체는 단기 종가배팅이 아니므로, ACCEL/CORE 또는 재료·대금 우수 MAIN 후보 중
    종가위치/거래대금/장기선 이격이 좋아 1차 분할매집 타이밍으로 볼 수 있는 날만 별도 표시한다.
    """
    try:
        if not i:
            return None
        phase = str(i.get('i_phase', '') or '')
        main = _safe_int(i.get('i_core_main_candidate', i.get('i_core_main', 0)), 0) == 1
        accel = _safe_int(i.get('i_core_main_accel', 0), 0) == 1
        material = _safe_float(i.get('i_material_proxy_score', 0), 0.0)
        long_dist = _safe_float(i.get('i_long_ma_dist_pct', 999), 999.0)
        anchor_days = _safe_int(i.get('i_anchor_days', 999), 999)
        monthly_rebuild = _safe_int(i.get('i_monthly_vol_rebuild', 0), 0)
        core = (
            _safe_int(i.get('i_core_main_core', i.get('i_main_core', 0)), 0) == 1
            or (phase == 'I-4' and main and 120 <= anchor_days <= 180 and monthly_rebuild == 1)
        )
        close_loc = _safe_float(i.get('close_loc_pct', 0), 0.0)
        amount_b = _safe_float(i.get('amount_b', 0), 0.0)
        score = _safe_float(i.get('score', 0), 0.0)
        if not main:
            return None
        if phase not in ('I-4', 'I-6'):
            return None
        if not (-3.0 <= long_dist <= 18.0):
            return None
        # ACCEL/CORE는 material 3 이상, WATCH는 material 4 + 종가위치 70% 이상일 때만 촉발형으로 표시
        if accel or core:
            class_ok = material >= 3 and close_loc >= 60.0
        else:
            class_ok = material >= 4 and close_loc >= 70.0
        if not class_ok or amount_b < 300.0 or score < 78.0:
            return None
        h = dict(i)
        h.update({
            'mode': 'IT',
            'strategy': 'IT',
            'mode_label': 'I-TRIGGER 중기후보 촉발형',
            'band_type': 'I_TRIGGER',
            'band_reason': 'I-MAIN ACCEL/CORE/WATCH 중 종가위치·거래대금·장기선 이격이 맞은 1차 분할매집 촉발형',
            'score': min(100.0, score + (3.0 if accel else 2.0)),
            'new_pattern': 'IT',
            'i_trigger_class': 'ACCEL' if accel else ('CORE' if core else 'WATCH'),
            'passed': list(i.get('passed', [])) + [f'IT:{phase}', f'재료대금{material:.0f}', f'장기선이격{long_dist:.1f}%'],
        })
        return h
    except Exception:
        return None

def _check_closing_bet(code: str, name: str) -> dict | None:
    """
    G / S / H / I / A / B1 / B2 / C 중 우선순위가 가장 높은 전략 1개 반환.
    v4.2.9: H 신고가거자름 TRIANGLE/CORE 후보는 S/G 다음 신규검증 후보로 실시간 포함한다.
    v4.3.6: I-MAIN 150/200일 시세분출 후보를 중기 누적관찰 후보로 함께 수집한다.
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
    # v4.4.1: 기존 S2 검색식은 그대로 두고, 상단잠김 조건을 만족하면 신규검증 SLOCK 라벨을 추가 후보로 둔다.
    slock_sig = _make_s2_lock_candidate(s)

    # v4.2.13: SK하이닉스형 대형주 리더갭은 G-SAFE 과열 제외와 분리해 별도 L 후보로 검사한다.
    with DIAG_LOCK:
        STRATEGY_DIAG['L_try'] += 1
    l_sig = _check_leader_gap_bet(code, name)
    if l_sig is not None:
        with DIAG_LOCK:
            STRATEGY_DIAG['L_hit'] += 1
    # v4.4.1 신규검증: L 당일을 놓친 뒤 1~5일 눌림재지지 타점만 별도 후보로 추가한다.
    lp_sig = _check_leader_gap_pullback_bet(code, name)

    with DIAG_LOCK:
        STRATEGY_DIAG['H_try'] += 1
    h_sig = _check_high_dryup_bet(code, name)
    if h_sig is not None:
        with DIAG_LOCK:
            STRATEGY_DIAG['H_hit'] += 1

    with DIAG_LOCK:
        STRATEGY_DIAG['I_try'] += 1
    i_sig = _check_i_core_bet(code, name)
    if i_sig is not None:
        with DIAG_LOCK:
            STRATEGY_DIAG['I_hit'] += 1
    # v4.4.1 신규검증: I-MAIN 중기 후보 중 단기 촉발 조건이 붙은 후보만 별도 표시한다.
    it_sig = _make_i_trigger_candidate(i_sig)

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

    candidates = [x for x in [l_sig, lp_sig, slock_sig, s, g, it_sig, h_sig, i_sig, a, b1, b2, c] if x]
    if not candidates:
        return None

    def _priority(h):
        grade = str(h.get('grade', ''))
        mode = str(h.get('mode', ''))
        g_rank = 0 if '완전체' in grade else (1 if 'A급' in grade else 2)
        mode_rank = {'L': 0, 'LP': 1, 'SLOCK': 2, 'S': 3, 'G': 4, 'IT': 5, 'H': 6, 'I': 7, 'A': 8, 'B1': 9, 'B2': 10, 'C': 11}.get(mode, 12)
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



def _live_text_pressure_label(raw: str, positive_label: str, neutral_label: str, negative_label: str) -> str:
    """v4.4.9.13: 시장/섹터 압력 텍스트를 간단한 3단계로 정규화."""
    try:
        txt = str(raw or '').strip().lower()
        if not txt:
            return neutral_label
        neg_words = ['부담', '약세', '하락', '이탈', '붕괴', 'risk', 'bear', 'down', 'negative', 'weak', '동반약세']
        pos_words = ['양호', '강세', '상승', '회복', '돌파', 'risk-on', 'bull', 'up', 'positive', 'strong', '동반강세']
        if any(w in txt for w in neg_words):
            return negative_label
        if any(w in txt for w in pos_words):
            return positive_label
    except Exception:
        pass
    return neutral_label


def _live_market_pressure_label(h: dict | None = None) -> str:
    """실전 카드용 시장압력: env/후보 필드가 있으면 반영, 없으면 중립."""
    h = h or {}
    raw_parts = [
        os.environ.get('CLOSING_BET_MARKET_PRESSURE', ''),
        os.environ.get('CLOSING_BET_MARKET_CONTEXT', ''),
        str(h.get('market_pressure', '') or ''),
        str(h.get('market_regime', '') or ''),
        str(h.get('kospi_regime', '') or ''),
        str(h.get('i_market_regime', '') or ''),
    ]
    raw = ' '.join([p for p in raw_parts if str(p).strip()])
    return _live_text_pressure_label(raw, '양호', '중립', '부담')


def _live_sector_pressure_label(h: dict | None = None) -> str:
    """실전 카드용 섹터압력: 후보의 섹터/테마 텍스트나 env 힌트를 3단계로 정규화."""
    h = h or {}
    raw_parts = [
        os.environ.get('CLOSING_BET_SECTOR_PRESSURE', ''),
        str(h.get('sector_pressure', '') or ''),
        str(h.get('theme_pressure', '') or ''),
        str(h.get('sector_trend', '') or ''),
        str(h.get('sector_name', '') or ''),
        str(h.get('theme_name', '') or ''),
        str(h.get('industry', '') or ''),
        str(h.get('band_reason', '') or ''),
        str(h.get('band_comment', '') or ''),
    ]
    raw = ' '.join([p for p in raw_parts if str(p).strip()])
    return _live_text_pressure_label(raw, '동반강세', '혼조', '동반약세')


def _save_live_operation_guard_rows(rows: list[dict]):
    """v4.4.9.13: 실전 FAST 출력 후보만 따로 누적 저장.
    기존 closing_bet_signals.csv는 전체 후보 검증용이고, 이 파일은 실제 화면에서 본 후보 추적용이다.
    """
    if not rows or not CLOSING_BET_LIVE_TRACKING_LOG:
        return
    try:
        _ensure_log_dir()
        new_df = pd.DataFrame(rows)
        if new_df.empty:
            return
        if LIVE_OPERATION_GUARD_CSV.exists():
            try:
                old_df = pd.read_csv(LIVE_OPERATION_GUARD_CSV, dtype={'code': str, 'key': str}, encoding='utf-8-sig')
            except Exception:
                old_df = pd.DataFrame()
        else:
            old_df = pd.DataFrame()
        merged = new_df if old_df.empty else pd.concat([old_df, new_df], ignore_index=True)
        if 'key' in merged.columns:
            merged = merged.drop_duplicates(subset=['key'], keep='last')
        sort_cols = [c for c in ['scan_date', 'priority', 'code', 'pattern'] if c in merged.columns]
        if sort_cols:
            merged = merged.sort_values(sort_cols).reset_index(drop=True)
        merged.to_csv(LIVE_OPERATION_GUARD_CSV, index=False, encoding='utf-8-sig')
        log_info(f"✅ 실전 운영 추적로그 저장: {len(new_df)}건 -> {LIVE_OPERATION_GUARD_CSV.name}")
    except Exception as e:
        try:
            log_error(f"⚠️ 실전 운영 추적로그 저장 실패: {e}")
        except Exception:
            pass

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
        stage_meta = _classify_ymgp_stage(df, row, info)
        stage_label = stage_meta.get('c_stage_label', 'C1 1파돌파형')
        
        return {
            **info,
            'code': code,
            'name': name,
            'mode': 'C',
            'mode_label': '역매공파',
            'c_type': 'strict',
            'c_type_label': stage_label,
            **stage_meta,
            'index_label': idx_label,
            'recommended_band': band_rec['recommended_band'],
            'support_band': band_rec['support_band'],
            'volatility_type': band_rec['volatility_type'],
            'universe_tag': band_rec['universe_tag'],
            'band_comment': stage_meta.get('c_stage_desc', '장기 매물대 돌파 초입(YMGP)'),
            'is_top_mcap': int(code in TOP_MCAP_SET),
            'marcap': marcap,
            'score': 7,
            'close': info['_close'],
            'grade': '완전체',
            'kki_pattern': '바닥탈출대시세형',
            'kki_habit': stage_meta.get('c_stage_bias', '매집 완료 후 장기 저항 돌파'),
            'kki_comment': stage_meta.get('c_stage_desc', '역매공파 타점 포착. 스윙 관점 유효.')
        }
    except Exception as e:
        return None




def _is_ymgp_pullback_reentry_hit(h: dict) -> bool:
    """v4.1.6: 역매공파 1파 이후 눌림→재지지→재상승 확인형.
    전체 C/엄격형을 바로 쓰지 않고, 진입 타점에 가까운 후보만 따로 비교한다.
    """
    try:
        return int(_safe_float(h.get('ymgp_pullback_reentry', 0), 0.0)) == 1
    except Exception:
        return False


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
    display_name = _clean_stock_name(code, name)
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
        c_stage_label = str(_g('c_stage_label', default='')).strip()
        c_stage_desc = str(_g('c_stage_desc', default='')).strip()
        c_stage_bias = str(_g('c_stage_bias', default='')).strip()
        d_break = _safe_int(_g('days_since_breakout', default=0), 0)
        d_high = _safe_int(_g('days_since_high', default=0), 0)
        pb = _safe_float(_g('ymgp_pullback_pct', default=0), 0.0)
        supp = _safe_float(_g('ymgp_support_level', default=0), 0.0)
        vdry = _safe_int(_g('ymgp_volume_dry', default=0), 0)
        rev = _safe_int(_g('ymgp_reversal_signal', default=0), 0)
        lines.append("   쉬운설명: 역배열 바닥에서 매집 흔적을 만든 뒤 장기 저항을 돌파하려는 스윙형 후보입니다.")
        if c_stage_label:
            lines.append(f"   단계: {c_stage_label} | 돌파후 {d_break}거래일 | 고점후 {d_high}거래일 | 눌림 {pb:.1f}%")
        if supp > 0:
            lines.append(f"   눌림체크: 지지선 {int(supp):,}원 부근 | 거래량감소 {'YES' if vdry else 'NO'} | 재상승확인 {'YES' if rev else 'NO'}")
        if c_stage_desc:
            lines.append(f"   단계해석: {c_stage_desc}")
        if c_stage_bias:
            lines.append(f"   대응관점: {c_stage_bias}")

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
    mode_rank = {'L': 0, 'LP': 1, 'SLOCK': 2, 'S': 3, 'G': 4, 'IT': 5, 'H': 6, 'I': 7, 'A': 8, 'B1': 9, 'B2': 10, 'C': 11}.get(mode, 12)
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
    log_info(f"Telegram route: {_telegram_route_status()}")

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
        s_quality = str(h.get('s_quality', '') or '')
        # v3.6: S 섹션에서도 백테스트 성과가 좋았던 S1 우수응축형을 S2보다 먼저 보여준다.
        if s_quality == 'S1_DRY_GOOD' or int(_safe_float(h.get('is_s1_dry_good', 0), 0.0)) == 1:
            s_rank = 0
        elif s_type == 'S2':
            s_rank = 1
        else:
            s_rank = 2
        return (
            s_rank,
            g_rank,
            -_safe_score(h),
            -_safe_float(h.get("rr", 0), 0.0),
            -_safe_float(h.get("amount_b", 0), 0.0),
            -_safe_float(h.get("today_vol_ratio", h.get("vol_ratio", 0)), 0.0),
        )

    hits_g = [x for x in hits if _pick_strategy(x) == "G"]
    hits_l = [x for x in hits if _pick_strategy(x) == "L"]
    hits_lp = [x for x in hits if _pick_strategy(x) == "LP"]
    hits_slock = [x for x in hits if _pick_strategy(x) == "SLOCK"]
    hits_s = [x for x in hits if _pick_strategy(x) == "S"]
    hits_s2 = [x for x in hits_s if str(x.get('s_type','')) == 'S2']
    hits_s1 = [x for x in hits_s if str(x.get('s_type','')) != 'S2']
    hits_it = [x for x in hits if _pick_strategy(x) == "IT"]
    hits_i = [x for x in hits if _pick_strategy(x) == "I" or _pick_strategy(x) == "IT" or _safe_int(x.get('i_core', 0), 0) == 1]
    hits_a = [x for x in hits if _pick_strategy(x) == "A"]
    hits_b1 = [x for x in hits if _pick_strategy(x) == "B1"]
    hits_b2 = [x for x in hits if _pick_strategy(x) == "B2"]
    hits_c = [x for x in hits if _pick_strategy(x) == "C"]
    # v4.1.6: C는 운영상 전체 C가 아니라 엄격형만 스윙 관심으로 본다.
    hits_c_swing_strict = [x for x in hits_c if str(x.get('c_type', '') or '').lower() == 'strict' or _grade_core(x) == 'COMPLETE']
    # v4.1.6: 1파 이후 눌림→재지지→재상승 확인형만 별도 검증 후보로 분리
    hits_c_pullback_reentry = [x for x in hits_c if _is_ymgp_pullback_reentry_hit(x)]

    hits_g.sort(key=_priority)
    hits_l.sort(key=_priority)
    hits_lp.sort(key=_priority)
    hits_slock.sort(key=_priority)
    hits_s.sort(key=_priority_s)
    hits_it.sort(key=_priority)
    hits_i.sort(key=_priority)
    hits_a.sort(key=_priority)
    hits_b1.sort(key=_priority)
    hits_b2.sort(key=_priority)
    hits_c.sort(key=_priority)
    hits_c_swing_strict.sort(key=_priority)
    hits_c_pullback_reentry.sort(key=_priority)

    complete_hits = [x for x in hits if _grade_core(x) == "COMPLETE"]
    a_grade_hits = [x for x in hits if _grade_core(x) == "A"]
    b_grade_hits = [x for x in hits if _grade_core(x) == "B"]

    c_pullback_output_n = min(len(hits_c_pullback_reentry), PRACTICAL_C_PULLBACK_TOP_N)
    c_swing_main_output_n = min(len(hits_c_swing_strict), PRACTICAL_C_SWING_TOP_N) if PRACTICAL_SHOW_C_DIAG else 0
    c_diag_output_n = min(len(hits_c), 5) if PRACTICAL_SHOW_C_DIAG else 0
    total = (
        min(len(hits_s), 5) + min(len(hits_l), 5) + min(len(hits_g), 5) + min(len(hits_i), 5) + min(len(hits_a), 5) + min(len(hits_b1), 5) +
        min(len(hits_b2), 5) + c_pullback_output_n + c_swing_main_output_n + c_diag_output_n
    )

    
    if 0 < mins_left <= 180:
        time_text = f"마감까지 {mins_left}분"
    elif mins_left <= 0:
        time_text = "마감직전/마감후"
    else:
        time_text = "정규 종가배팅 시간 아님/테스트"
    header = (
        f"📌 종가배팅 실전 운영 후보 ({TODAY_STR})\n"
        f"🧩 버전 {CLOSING_BET_SCANNER_VERSION}\n"
        f"⏰ {time_text}\n"
        f"📮 전송경로: {TELEGRAM_ROUTE_LABEL} | {TELEGRAM_CHAT_SOURCE} | 기본방 fallback {'금지' if CLOSING_BET_REQUIRE_DEDICATED_CHAT else '허용'}\n"
        f"전체 후보 {len(hits)}개 | v4.4.9.43 SJ THRESHOLD + FAIL AUDIT: 14:40 후보발굴 + 15:03 실행/포기 압축 최종킥\n"
        f"전략 후보: L {len(hits_l)}개 | LP {len(hits_lp)}개 | SLOCK {len(hits_slock)}개 | S {len(hits_s)}개(S2 {len(hits_s2)}/S1 {len(hits_s1)}) | G {len(hits_g)}개 | H {len([x for x in hits if _pick_strategy(x) == 'H' or str(x.get('band_type', '') or '') == 'HIGH_DRYUP_STRICT'])}개 | IT {len(hits_it)}개 | I {len(hits_i)}개 | A {len(hits_a)}개 | "
        f"B1 {len(hits_b1)}개 | B2 {len(hits_b2)}개 | C {len(hits_c)}개 | C-눌림재상승 {len(hits_c_pullback_reentry)}개\n"
        f"전체 후보 등급: 완전체 {len(complete_hits)}개 | A급 {len(a_grade_hits)}개 | B급 {len(b_grade_hits)}개"
    )

    def _is_s1_good(h: dict) -> bool:
        # v3.6: S1 우수응축형은 거래량이 마른 채 고점권에서 버티는 후보를 핵심군으로 본다.
        today_vol = _safe_float(h.get('today_vol_ratio', h.get('vol_ratio', 0)), 0.0)
        vma_ratio = _safe_float(h.get('vma5_20_ratio', 9), 9.0)
        return (
            _pick_strategy(h) == "S"
            and str(h.get('s_type', '')) != 'S2'
            and _safe_float(h.get('rr', 0), 0.0) >= HIGH_REACCUM_S1_GOOD_RR_MIN
            and _safe_float(h.get('amount_b', 0), 0.0) >= HIGH_REACCUM_S1_GOOD_AMOUNT_MIN_B
            and _safe_float(h.get('close_loc_pct', 0), 0.0) >= HIGH_REACCUM_S1_GOOD_CLOSE_LOC_MIN
            and (today_vol < HIGH_REACCUM_S1_DRY_TODAY_MAX or vma_ratio <= HIGH_REACCUM_S1_DRY_VMA_MAX or str(h.get('s_quality','')) == 'S1_DRY_GOOD')
        )

    def _is_s_core_hit(h: dict) -> bool:
        return _pick_strategy(h) == 'S' and (str(h.get('s_type', '')) == 'S2' or _is_s1_good(h))

    def _trade_risk_tags(h: dict) -> list:
        """v4.1.6: 공통 단기 위험태그.
        S-CORE RISK 판정과 A 보조돌파 숨김 기준에 함께 사용한다.
        단, C-SWING은 별도 스윙 기준으로 보므로 이 태그를 매수/제외 기준으로 쓰지 않는다.
        """
        tags = []
        volr = _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0)
        rr = _safe_float(h.get('rr', 0), 0.0)
        close_loc = _safe_float(h.get('close_loc_pct', 0), 0.0)
        if volr >= S_CORE_RISK_VOL_RATIO_MIN:
            tags.append('거래량과열')
        if rr < S_CORE_RISK_RR_LOW:
            tags.append('RR낮음')
        elif rr >= S_CORE_RISK_RR_HIGH:
            tags.append('RR과대')
        if close_loc < S_CORE_RISK_CLOSE_LOC_MIN:
            tags.append('종가위치약함')
        return tags

    def _s_core_risk_tags(h: dict) -> list:
        if _pick_strategy(h) != 'S':
            return []
        return _trade_risk_tags(h)

    def _is_s_core_safe_hit(h: dict) -> bool:
        if not _is_s_core_hit(h):
            return False
        volr = _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0)
        rr = _safe_float(h.get('rr', 0), 0.0)
        close_loc = _safe_float(h.get('close_loc_pct', 0), 0.0)
        return (
            rr >= S_CORE_SAFE_RR_MIN
            and rr < S_CORE_SAFE_RR_MAX
            and volr < S_CORE_SAFE_VOL_RATIO_MAX
            and close_loc >= S_CORE_SAFE_CLOSE_LOC_MIN
        )

    def _is_s_core_neutral_hit(h: dict) -> bool:
        """v4.1: S-CORE이지만 SAFE/RISK가 아닌 중립 후보.
        실전에서는 관찰 가능 후보로 두되, SAFE보다 후순위로 정렬한다.
        """
        return _is_s_core_hit(h) and (not _is_s_core_safe_hit(h)) and (len(_s_core_risk_tags(h)) == 0)

    def _is_a_strong(h: dict) -> bool:
        return (
            _pick_strategy(h) == "A"
            and _grade_core(h) in ("COMPLETE", "A")
            and _safe_float(h.get('vol_ratio', h.get('volume_ratio', 0)), 0.0) >= A_STRONG_VOL_RATIO_MIN
            and _safe_float(h.get('amount_b', 0), 0.0) >= A_STRONG_AMOUNT_MIN_B
        )

    def _a_live_close_loc(h: dict) -> float:
        v = _safe_float(h.get('close_loc_pct', 0), 0.0)
        if v > 0:
            return v
        hi = _safe_float(h.get('_high', h.get('high', 0)), 0.0)
        lo = _safe_float(h.get('_low', h.get('low', 0)), 0.0)
        cl = _safe_float(h.get('_close', h.get('close', 0)), 0.0)
        return max(0.0, min(100.0, ((cl - lo) / (hi - lo) * 100.0))) if hi > lo and cl > 0 else 0.0

    def _is_a_retest_core2_live(h: dict) -> bool:
        if _pick_strategy(h) != 'A':
            return False
        rr = _safe_float(h.get('rr', 0), 0.0)
        vol = _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0)
        return (
            _safe_float(h.get('amount_b', 0), 0.0) >= 5000.0
            and _a_live_close_loc(h) >= 80.0
            and vol <= 1.8
            and 0.8 <= rr <= 1.5
            and not _trade_risk_tags({**h, 'close_loc_pct': _a_live_close_loc(h)})
        )

    def _is_a_confirm_live(h: dict) -> bool:
        if _pick_strategy(h) != 'A':
            return False
        return _safe_int(h.get('a_confirm_live', 0), 0) == 1 and _safe_float(h.get('rr', 0), 0.0) <= 1.8

    def _is_a_confirm_vc_safe(h: dict) -> bool:
        return _is_a_confirm_live(h) and _safe_int(h.get('a_vc_safe', 0), 0) == 1

    def _is_a_confirm_vc_watch(h: dict) -> bool:
        return _is_a_confirm_live(h) and (not _is_a_confirm_vc_safe(h)) and _safe_int(h.get('a_vc_watch', 0), 0) == 1

    def _a_vc_badge(h: dict) -> str:
        # v4.4.9.13 라벨 재보정:
        # 기존 VC-WATCH가 성과 최상위였으므로 PRIME, 기존 VC-SAFE는 CALM으로 표시한다.
        if _is_a_confirm_vc_watch(h):
            return 'A-VC-PRIME'
        if _is_a_confirm_vc_safe(h):
            return 'A-VC-CALM'
        if _is_a_confirm_live(h):
            return 'A-VC-확인필요'
        return ''

    def _a_vc_reason_text(h: dict) -> str:
        reason = str(h.get('a_vc_reason', '') or '').strip()
        if not reason:
            reason = '거래량 수축/가격지지 정보 확인 필요'
        pct = _safe_float(h.get('a_today_vs_prev_vol_pct', 0), 0.0)
        vr = _safe_float(h.get('a_today_vol_ratio', h.get('vol_ratio', 0)), 0.0)
        tail = []
        if pct > 0:
            tail.append(f'전일대비 거래량 {pct:.0f}%')
        if vr > 0:
            tail.append(f'거래량비 {vr:.2f}')
        return reason + ((' | ' + ' / '.join(tail)) if tail else '')

    def _a_price(v) -> str:
        x = _safe_float(v, 0.0)
        return f"{int(x):,}원" if x > 0 else '확인필요'

    def _a_entry_price_comment(h: dict, confirm: bool = False) -> str:
        close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
        high = _safe_float(h.get('_high', h.get('high', 0)), 0.0)
        ma5 = _safe_float(h.get('ma5', h.get('_ma5', 0)), 0.0)
        if confirm:
            prev_high = _safe_float(h.get('a_prev_high', h.get('prev_high', 0)), 0.0)
            prev_close = _safe_float(h.get('a_prev_close', h.get('prev_close', 0)), 0.0)
            pull_low = _safe_float(h.get('a_pullback_low', 0), 0.0)
            pull_high = _safe_float(h.get('a_pullback_high', 0), 0.0)
            pull_txt = f"{_a_price(pull_low)}~{_a_price(pull_high)}" if pull_low > 0 and pull_high > 0 else f"전일종가 {_a_price(prev_close)} / 5MA {_a_price(ma5)}"
            return (
                f"1차: 전일고가 {_a_price(prev_high)} 회복/재돌파 시 20~30%. "
                f"눌림: {pull_txt} 지지 확인 후 반등. "
                f"보수형: 종가가 전일고가 위 또는 양봉 유지 시. "
                f"추격금지: 시초 +2% 이상 급등 직후 매수."
            )
        return (
            f"당일 종가 {_a_price(close)} 부근은 소액 관찰만. "
            f"핵심 진입은 다음날 전일고가 {_a_price(high)} 회복가. "
            f"눌림은 종가/5MA {_a_price(ma5)} 지지 확인 후. RR 1.8 초과는 제외."
        )

    def _is_g_hit(h: dict) -> bool:
        return _pick_strategy(h) == 'G'

    def _g_good_grade(h: dict) -> bool:
        gc = _grade_core(h)
        grade_txt = str(h.get('grade', h.get('등급', '')) or '')
        return gc in ('COMPLETE', 'A') or ('완전체' in grade_txt) or ('A급' in grade_txt) or (grade_txt == 'A')

    def _g_metrics(h: dict) -> dict:
        return {
            'gap': _safe_float(h.get('gap_pct', h.get('gap', 0)), 0.0),
            'vol50': _safe_float(h.get('vol50_ratio', h.get('vol_ratio', h.get('today_vol_ratio', 0))), 0.0),
            'close_loc': _safe_float(h.get('close_loc_pct', 0), 0.0),
            'wick': _safe_float(h.get('wick_pct', h.get('upper_wick_pct', 0)), 0.0),
            'disparity20': _safe_float(h.get('disparity20', h.get('disparity', 0)), 0.0),
            'runup20': _safe_float(h.get('runup20', h.get('runup_20', 0)), 0.0),
            'amount': _safe_float(h.get('amount_b', 0), 0.0),
        }

    def _is_g_safe_hit(h: dict) -> bool:
        """v4.1.9: 실시간 출력용 G-SAFE 판정. 백테스트 v4.1.8 기준을 그대로 유지한다."""
        if not _is_g_hit(h):
            return False
        m = _g_metrics(h)
        return (
            _g_good_grade(h)
            and m['gap'] >= GAP_MIN_PCT
            and m['gap'] <= 8.5
            and m['vol50'] >= GAP_VOL50_MULT
            and m['vol50'] <= 6.0
            and m['close_loc'] >= 70.0
            and m['wick'] <= 20.0
            and m['disparity20'] <= 115.0
            and m['runup20'] <= 30.0
            and m['amount'] >= 100.0
        )

    def _g_aggressive_tags(h: dict) -> list:
        """v4.1.9: G-RISK를 실전 제외 딱지가 아니라 고변동/저유동성 AGGRESSIVE로 표시한다."""
        if not _is_g_hit(h):
            return []
        m = _g_metrics(h)
        tags = []
        if m['gap'] > 10.0:
            tags.append('갭과대')
        if m['vol50'] > 8.0:
            tags.append('Vol50과열')
        if m['close_loc'] < 65.0:
            tags.append('종가위치약함')
        if m['wick'] > 25.0:
            tags.append('윗꼬리과다')
        if m['disparity20'] > GAP_DISPARITY20_MAX:
            tags.append('이격과열')
        if m['runup20'] > GAP_RUNUP20_MAX:
            tags.append('20일상승과열')
        if m['amount'] < 50.0:
            tags.append('저유동성')
        return tags

    def _is_g_aggressive_hit(h: dict) -> bool:
        return _is_g_hit(h) and len(_g_aggressive_tags(h)) > 0

    def _is_g_neutral_hit(h: dict) -> bool:
        return _is_g_hit(h) and (not _is_g_safe_hit(h)) and (not _is_g_aggressive_hit(h))

    def _is_l_hit(h: dict) -> bool:
        return _pick_strategy(h) == 'L' or str(h.get('band_type', '') or '') in ('LEADER_GAP_CORE', 'LEADER_GAP_WATCH')

    def _l_metrics(h: dict) -> dict:
        return {
            'gap': _safe_float(h.get('gap_pct', h.get('gap', 0)), 0.0),
            'vol50': _safe_float(h.get('vol50_ratio', h.get('vol_ratio', 0)), 0.0),
            'amount': _safe_float(h.get('leader_gap_amount_b', h.get('amount_b', 0)), 0.0),
            'close_loc': _safe_float(h.get('close_loc_pct', 0), 0.0),
            'wick': _safe_float(h.get('wick_pct', h.get('upper_wick_pct', 0)), 0.0),
            'overheat': _safe_int(h.get('leader_gap_overheat_flag', 0), 0),
        }

    def _is_l_core_hit(h: dict) -> bool:
        return _is_l_hit(h) and _l_metrics(h)['amount'] >= LEADER_GAP_CORE_AMOUNT_B

    def _is_l_mega_hit(h: dict) -> bool:
        m = _l_metrics(h)
        return _is_l_hit(h) and (m['amount'] >= 10000.0 or m['gap'] >= 6.0)

    def _is_l_mega_upper_limit_followup(h: dict) -> bool:
        """v4.4.9.43: L-MEGA 상한가/잠금형 후속관찰.

        상한가 여부 자체는 데이터 소스마다 표시가 다를 수 있으므로
        종가위치 98~100, 윗꼬리 3% 이하, 5000억+ 대금, L-MEGA/CORE급을
        상한가성 잠금형으로 본다. 이 후보는 신규 종가추격 후보가 아니라
        보유자 대응 + 다음날 기준선 지지/첫 눌림 재돌파 확인 후보다.
        """
        try:
            m = _l_metrics(h)
            return bool(
                _is_l_hit(h)
                and m['amount'] >= 5000.0
                and m['close_loc'] >= 98.0
                and m['wick'] <= 3.0
                and (_is_l_mega_hit(h) or _is_l_core_hit(h))
            )
        except Exception:
            return False

    def _l_mega_upper_followup_reason(h: dict) -> str:
        try:
            m = _l_metrics(h)
            return f"상한가성 잠금형: 거래대금 {m['amount']:.0f}억 · 종가위치 {m['close_loc']:.0f}% · 윗꼬리 {m['wick']:.1f}%"
        except Exception:
            return '상한가성 잠금형: 보유자 대응/다음날 확인'

    def _sj6_checklist_context(h: dict) -> dict:
        """v4.4.9.43: 신정재 종가베팅 6요소(신·좁·깔·거·조·재) 최종 품질 체크.

        하드필터가 아니라 실전 카드의 가점/주의 라벨이다. 뉴스·수급은 데이터가 없을 수 있으므로
        섹터압력/거래대금/텍스트 힌트/추정수급을 프록시로 쓰고, 모르면 🟡로 둔다.
        """
        try:
            mode = str(_pick_strategy(h) or '').upper()
            close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
            amount_b = _safe_float(h.get('amount_b', h.get('leader_gap_amount_b', 0)), 0.0)
            leader_amt = _safe_float(h.get('leader_gap_amount_b', amount_b), amount_b)
            gap_amt = _safe_float(h.get('lp_gap_amount_b', leader_amt), leader_amt)
            use_amt = max(amount_b, leader_amt, gap_amt)
            close_loc = _safe_float(h.get('close_loc_pct', 0), 0.0)
            wick = _safe_float(h.get('wick_pct', h.get('upper_wick_pct', 0)), 0.0)
            volr = _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', h.get('vol50_ratio', 0))), 0.0)
            vol50 = _safe_float(h.get('vol50_ratio', volr), volr)
            disparity20 = _safe_float(h.get('disparity20', h.get('disparity', 0)), 0.0)
            rr = _safe_float(h.get('rr', 0), 0.0)
            runup20 = _safe_float(h.get('runup20', h.get('runup_20', 0)), 0.0)
            near_high120 = _safe_float(h.get('near_high120', h.get('near_high_120', 0)), 0.0)
            # 신: 의미 있는 신고가/전고권. 데이터 필드가 부족하면 종가위치와 고점근접으로 대체.
            new_high = bool(
                _safe_int(h.get('leader_gap_new_high_120', h.get('new_high_120', 0)), 0) == 1
                or _safe_int(h.get('new_high_52w', h.get('leader_gap_new_high_52w', 0)), 0) == 1
                or near_high120 >= 98.0
                or (mode in ('L', 'LP', 'S', 'A') and close_loc >= 90.0)
            )
            # 좁: 전고점/기준선과의 부담이 너무 벌어지지 않은 상태. 상한가성 L-MEGA는 신규진입 좁음이 약할 수 있다.
            narrow = bool((100.0 <= disparity20 <= 108.0) or (0.85 <= rr <= 1.45) or (mode == 'LP' and _safe_float(h.get('lp_volume_vs_gap', 0), 0) > 0))
            if mode == 'L' and _is_l_mega_upper_limit_followup(h):
                narrow = False
            # 깔: 종가권 양봉/윗꼬리 억제.
            clean = bool(close_loc >= 85.0 and wick <= 10.0)
            if close_loc >= 95.0 and wick <= 5.0:
                clean = True
            # 거: 거래대금 상위권 + 평소 대비 거래량/대금 확장.
            money = bool(use_amt >= 1000.0 and (volr >= 1.15 or vol50 >= 2.0 or use_amt >= 3000.0))
            # 조: 3개월 조정/응축을 정확히 알 수 없으면 LP 눌림, I 기준봉, 적당한 runup/disparity를 대체 프록시로 둔다.
            days_after_gap = _safe_float(h.get('lp_days_since_gap', h.get('lp_days_after_gap', 0)), 0.0)
            anchor_days = _safe_float(h.get('i_anchor_days', 0), 0.0)
            adjust = bool((mode == 'LP' and 1 <= days_after_gap <= 5) or (45 <= anchor_days <= 120) or (runup20 <= 20.0 and (disparity20 == 0 or disparity20 <= 112.0)))
            # 재: 뉴스/테마/섹터 동반강세/대금 프록시.
            txt = ' '.join(str(h.get(k, '') or '') for k in ['theme', 'theme_name', 'sector', 'sector_name', 'tags', 'n_combo', 'reason', 'material_hint', 'news_hint', 'issue', 'news_title'])
            material_kw = bool(re.search(r'AI|반도체|전력|원전|방산|로봇|바이오|조선|유가|환율|구리|수주|공시|실적|계약|공급|정책|정부|테마|이슈|뉴스|재료', txt, re.I))
            sector_txt = _live_sector_pressure_label(h)
            material = bool(('동반강세' in sector_txt) or _safe_float(h.get('i_material_proxy_score', 0), 0) >= 3 or (material_kw and use_amt >= 1000.0) or use_amt >= 5000.0)
            checks = [('신', new_high), ('좁', narrow), ('깔', clean), ('거', money), ('조', adjust), ('재', material)]
            score = sum(1 for _, ok in checks if ok)
            market_txt = _live_market_pressure_label(h)
            market_ok = not any(x in market_txt for x in ['부담', '약세', '하락', '위험'])
            flow_comment = str(h.get('flow_comment', h.get('i_flow_label', '')) or '')
            fi_amt = _safe_float(h.get('fi_amt_est_b', 0), 0.0)
            supply_ok = ('쌍끌' in flow_comment or '외인' in flow_comment and '기관' in flow_comment and '유입' in flow_comment) or fi_amt > 0
            supply_unknown = not flow_comment and fi_amt == 0
            badge = '🔥SJ-PRIME' if score >= 6 and market_ok else ('✅SJ-SAFE' if score >= 5 and market_ok else ('🟡SJ-WATCH' if score >= 4 else '❌SJ-FAIL'))
            bits = ' '.join([f"{k}{'✅' if ok else '❌'}" for k, ok in checks])
            market_mark = '시장✅' if market_ok else '시장⚠️'
            supply_mark = '수급🟡' if supply_unknown else ('수급✅' if supply_ok else '수급⚠️')
            short = f"{badge} {score}/6 | {bits} | {market_mark} {supply_mark}"
            line = f"📊 신좁깔거조재: {short}"
            return {'score': score, 'badge': badge, 'short': short, 'line': line, 'checks': checks, 'market_ok': market_ok, 'supply_ok': supply_ok, 'supply_unknown': supply_unknown}
        except Exception as e:
            return {'score': 0, 'badge': 'SJ-확인필요', 'short': f'SJ확인필요:{type(e).__name__}', 'line': '📊 신좁깔거조재: 확인필요'}

    def _is_l_core_safe_hit(h: dict) -> bool:
        """v4.2.15: 5000억+·종가위치70%+·윗꼬리25% 이하.
        v4.2.14 백테스트 기준 L-SAFE B가 A보다 효율적이어서 실전 SAFE 기준으로 사용한다.
        """
        m = _l_metrics(h)
        return _is_l_core_hit(h) and m['close_loc'] >= 70.0 and m['wick'] <= 25.0

    def _is_l_tail_absorb_hit(h: dict) -> bool:
        """v4.2.15: 대형 거래대금이 윗꼬리/중간 종가위치를 흡수한 리더갭.
        윗꼬리 25~35% 또는 종가위치 65~70%는 v4.2.14에서 손절선행이 낮아
        CAUTION 대신 L-TAIL ABSORB로 별도 표시한다.
        """
        m = _l_metrics(h)
        return _is_l_core_hit(h) and (m['wick'] <= 35.0) and (
            (25.0 < m['wick'] <= 35.0) or (65.0 <= m['close_loc'] < 70.0)
        )

    def _is_l_weak_watch_hit(h: dict) -> bool:
        """v4.2.15: 3000~5000억 또는 5000억+라도 윗꼬리 35% 초과/종가위치 약함이면 관찰로 강등."""
        m = _l_metrics(h)
        return _is_l_hit(h) and (
            (LEADER_GAP_MIN_AMOUNT_B <= m['amount'] < LEADER_GAP_CORE_AMOUNT_B)
            or (m['amount'] >= LEADER_GAP_CORE_AMOUNT_B and (m['close_loc'] < 65.0 or m['wick'] > 35.0))
        )

    def _is_l_watch_hit(h: dict) -> bool:
        return _is_l_weak_watch_hit(h)

    def _is_h_hit(h: dict) -> bool:
        return _pick_strategy(h) == 'H' or str(h.get('band_type', '') or '') == 'HIGH_DRYUP_STRICT'

    def _h_breakout_amount(h: dict) -> float:
        return _safe_float(h.get('breakout_amount_b', h.get('amount_b', 0)), 0.0)

    def _h_breakout_vol60(h: dict) -> float:
        return _safe_float(h.get('breakout_vol60_ratio', 0), 0.0)

    def _is_h_struct_hit(h: dict) -> bool:
        if not _is_h_hit(h):
            return False
        bvol = _h_breakout_vol60(h)
        return (
            _safe_score(h) >= 82
            and 1 <= _safe_int(h.get('days_since_high_breakout', 999), 999) <= 7
            and _safe_int(h.get('breakout_long_bull', 0), 0) == 1
            and bvol >= 1.5
            and _safe_float(h.get('breakout_day_ret_pct', 0), 0.0) >= 7.0
            and _safe_float(h.get('breakout_body_pct', 0), 0.0) >= 5.0
            and _safe_float(h.get('breakout_close_loc_pct', 0), 0.0) >= 75.0
            and _safe_float(h.get('breakout_upper_wick_pct', 999), 999.0) <= 25.0
            and _safe_int(h.get('high_dryup_volume_dry', 0), 0) == 1
            and _safe_int(h.get('high_dryup_volume_dry_vs_breakout', 0), 0) == 1
            and _safe_int(h.get('high_dryup_short_candle', 0), 0) == 1
            and _safe_int(h.get('high_dryup_ma5_close_hold', 0), 0) == 1
            and _safe_int(h.get('high_dryup_entry_close_loc_ok', 0), 0) == 1
            and _safe_int(h.get('high_dryup_zone_hold', 0), 0) == 1
            and 1.0 <= _safe_float(h.get('high_dryup_pullback_pct', 999), 999.0) <= 10.0
            and _h_breakout_amount(h) >= 100.0
        )

    def _is_h_triangle_hit(h: dict) -> bool:
        return _is_h_hit(h) and _safe_int(h.get('h_pre_triangle', 0), 0) == 1

    def _is_h_core_500_1000_vol23(h: dict) -> bool:
        amt = _h_breakout_amount(h); bvol = _h_breakout_vol60(h)
        return _is_h_struct_hit(h) and 500.0 <= amt < 1000.0 and 2.0 <= bvol < 3.0

    def _is_h_core_300_500_vol35(h: dict) -> bool:
        amt = _h_breakout_amount(h); bvol = _h_breakout_vol60(h)
        return _is_h_struct_hit(h) and 300.0 <= amt < 500.0 and 3.0 <= bvol < 5.0

    def _is_h_core_1000_2000_vol23(h: dict) -> bool:
        amt = _h_breakout_amount(h); bvol = _h_breakout_vol60(h)
        return _is_h_struct_hit(h) and 1000.0 <= amt < 2000.0 and 2.0 <= bvol < 3.0

    def _is_h_core_union_hit(h: dict) -> bool:
        return (
            _is_h_triangle_hit(h)
            or _is_h_core_500_1000_vol23(h)
            or _is_h_core_300_500_vol35(h)
            or _is_h_core_1000_2000_vol23(h)
        )

    def _is_h_fast_hit(h: dict) -> bool:
        return _is_h_struct_hit(h) and _h_breakout_vol60(h) >= 8.0

    def _is_h_overheat_hit(h: dict) -> bool:
        bvol = _h_breakout_vol60(h)
        return _is_h_struct_hit(h) and 5.0 <= bvol < 8.0

    def _h_entry_close_loc(h: dict) -> float:
        return _safe_float(h.get('high_dryup_close_loc_pct', h.get('close_loc_pct', 0)), 0.0)

    def _h_fast_live_class(h: dict) -> dict:
        """v4.4.9.28: H-FAST 8배+를 실시간 운용 라벨로 분리한다.
        백테스트 결론: H-FAST는 보유형이 아니라 +3 초단기 전용이며,
        5~8배 H-OVERHEAT는 실패율이 높아 실시간 기본 숨김/제외로 둔다.
        """
        bvol = _h_breakout_vol60(h)
        days = _safe_int(h.get('days_since_high_breakout', 999), 999)
        pull = _safe_float(h.get('high_dryup_pullback_pct', 999), 999.0)
        close_loc = _h_entry_close_loc(h)
        entry_vol = _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0)
        entry_amount = _safe_float(h.get('entry_amount_b', h.get('amount_b', 0)), 0.0)
        breakout_amount = _h_breakout_amount(h)

        if _is_h_fast_hit(h):
            prime = (
                8.0 <= bvol < 12.0
                and 1 <= days <= 5
                and 0.0 <= pull <= 6.5
                and close_loc >= 70.0
                and (entry_vol <= 1.20 or entry_vol <= 0)
            )
            if prime:
                return {
                    'key': 'H_FAST_PRIME',
                    'label': '🔥H-FAST PRIME',
                    'tag': 'H-FAST PRIME +3초단타',
                    'reason': 'Vol60 8~12배·짧은 눌림·종가위치70+·진입거래량 냉각. +3 초단타 전용',
                }
            if close_loc >= 70.0 and 8.0 <= bvol < 12.0:
                return {
                    'key': 'H_FAST_WATCH',
                    'label': '🟡H-FAST WATCH',
                    'tag': 'H-FAST WATCH 확인형',
                    'reason': 'H-FAST지만 진입거래량/눌림/타이밍 중 일부 확인 필요. 지정가·+3 단타만',
                }
            return {
                'key': 'H_FAST_RISK',
                'label': '⚠️H-FAST RISK',
                'tag': 'H-FAST RISK 추격금지',
                'reason': '8배+라도 Vol60 과다·종가위치 미달·타이밍 지연이면 빠른 +3 가능성이 낮아 추격금지',
            }
        if _is_h_overheat_hit(h):
            return {
                'key': 'H_OVERHEAT_RISK',
                'label': '⚠️H-OVERHEAT RISK',
                'tag': 'H-OVERHEAT 5~8배 제외',
                'reason': '5~8배 H-OVERHEAT는 반복검증에서 손절 우위. 실시간 기본 숨김',
            }
        if _is_h_core_union_hit(h):
            return {
                'key': 'H_CORE_WATCH',
                'label': '🟡H-CORE WATCH',
                'tag': 'H-CORE WATCH 지정가',
                'reason': 'H-CORE는 아직 표본부족. 전일고가/돌파권 재지지 확인 전까지 WATCH',
            }
        return {
            'key': 'H_OTHER_HIDE',
            'label': '⚪H-HIDE',
            'tag': 'H 일반 숨김',
            'reason': '넓은 H는 추격위험이 커서 실시간 기본 숨김',
        }

    def _h_fast_live_key(h: dict) -> str:
        return str(_h_fast_live_class(h).get('key', ''))

    def _h_fast_live_label(h: dict) -> str:
        return str(_h_fast_live_class(h).get('label', '🔥H-FAST'))

    def _priority_practical(h: dict):
        mode = _pick_strategy(h)
        s_type = str(h.get('s_type', '') or '')
        risk_count = len(_s_core_risk_tags(h))
        if mode == 'S' and _is_s1_good(h) and _is_s_core_safe_hit(h):
            group_rank = 0
        elif mode == 'S' and s_type == 'S2' and _is_s_core_safe_hit(h):
            group_rank = 1
        elif mode == 'L' and _is_l_mega_hit(h):
            group_rank = 2
        elif mode == 'L' and _is_l_core_safe_hit(h):
            group_rank = 3
        elif mode == 'L' and _is_l_tail_absorb_hit(h):
            group_rank = 4
        elif mode == 'L' and _is_l_core_hit(h):
            group_rank = 5
        elif mode == 'G' and _is_g_safe_hit(h):
            group_rank = 6
        elif mode == 'H' and _is_h_triangle_hit(h):
            group_rank = 7
        elif mode == 'H' and _is_h_core_union_hit(h):
            group_rank = 8
        elif mode == 'H' and _is_h_fast_hit(h):
            group_rank = 9
        elif mode == 'S' and _is_s1_good(h) and _is_s_core_neutral_hit(h):
            group_rank = 10
        elif mode == 'S' and s_type == 'S2' and _is_s_core_neutral_hit(h):
            group_rank = 11
        elif mode == 'L' and _is_l_watch_hit(h):
            group_rank = 12
        elif mode == 'G' and _is_g_neutral_hit(h):
            group_rank = 13
        elif mode == 'A' and _is_a_strong(h):
            group_rank = 14
        elif mode == 'G' and _is_g_aggressive_hit(h):
            group_rank = 15
        elif mode == 'S' and _is_s1_good(h):
            group_rank = 16
        elif mode == 'S' and s_type == 'S2':
            group_rank = 17
        else:
            group_rank = 18
        return (
            group_rank,
            risk_count,
            -_safe_float(h.get('rule35_pnl', h.get('rr', 0)), 0.0),
            -_safe_score(h),
            -_safe_float(h.get('breakout_amount_b', h.get('amount_b', 0)), 0.0),
            _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0),
        )


    def _fmt_price(v) -> str:
        try:
            x = _safe_float(v, 0.0)
            return f"{int(x):,}원" if x > 0 else "-"
        except Exception:
            return "-"

    def _gap_support_levels(h: dict, mode: str = '') -> dict:
        """v4.4.7: L/LP/G 갭형 후보의 실전 지지선을 따로 표시한다.
        종가배팅에서 너무 먼 stoploss 하나만 보여주면 늦게 대응할 수 있어
        실전손절(전일종가/갭하단)과 구조손절(당일저가/계산손절)을 분리한다.
        """
        mode = str(mode or _pick_strategy(h) or '').upper()
        close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
        if mode == 'LP':
            prev_close = _safe_float(h.get('lp_prev_close', h.get('prev_close', 0)), 0.0)
            gap_low = _safe_float(h.get('lp_gap_low', h.get('gap_low', 0)), 0.0)
        else:
            prev_close = _safe_float(h.get('prev_close', h.get('lp_prev_close', 0)), 0.0)
            gap_low = _safe_float(h.get('gap_low', h.get('lp_gap_low', 0)), 0.0)
        stop = _safe_float(h.get('stoploss', 0), 0.0)
        ma5 = _safe_float(h.get('MA5', h.get('ma5', 0)), 0.0)
        practical_parts = []
        if prev_close > 0:
            practical_parts.append(f"전일/갭전종가 {_fmt_price(prev_close)}")
        if gap_low > 0:
            practical_parts.append(f"갭하단·당일저가 {_fmt_price(gap_low)}")
        if ma5 > 0:
            practical_parts.append(f"5일선 {_fmt_price(ma5)}")
        if not practical_parts:
            practical_parts.append("전일종가·갭하단·5일선")
        struct_parts = []
        if gap_low > 0:
            struct_parts.append(f"당일저가 {_fmt_price(gap_low)}")
        if stop > 0:
            struct_parts.append(f"계산손절 {_fmt_price(stop)}")
        if not struct_parts:
            struct_parts.append("계산손절/핵심지지선")
        return {
            'close': close,
            'prev_close': prev_close,
            'gap_low': gap_low,
            'stop': stop,
            'practical': ' / '.join(practical_parts),
            'structural': ' / '.join(struct_parts),
        }

    def _practical_stop_text(h: dict, mode: str = '') -> str:
        lv = _gap_support_levels(h, mode)
        return f"실전손절: {lv['practical']} 종가이탈 확인 / 구조손절: {lv['structural']} 이탈"

    def _pattern_entry_tip(h: dict, mode: str = '') -> str:
        """v4.4.6: 패턴 옆에 붙일 쉬운 추천타점 한 줄."""
        mode = str(mode or _pick_strategy(h) or '').upper()
        if mode == 'LP':
            b, lbl = _lp_timing_bucket(h)
            if b == 'LP-D1':
                return "공격타점: 갭후 1일 빠른 재지지. 소액만, 갭하단/전일종가 확인 필수."
            if b == 'LP-D23':
                return "핵심타점: 갭후 2~3일 식힘 후 재지지. LP에서 가장 균형 좋은 구간."
            if b == 'LP-D45':
                return "안정타점: 갭후 4~5일 충분히 식힌 뒤 안 깨지고 재상승."
            return "갭후 1~5일 눌림 중 갭하단/전일종가/5·10일선 재지지 확인."
        if mode == 'L':
            if CLOSING_BET_WORKER_MODE:
                return "직장인 모드: 시초 추격 금지. 전일종가 이하 눌림 지정가 또는 갭하단/전일종가 지지 확인형."
            return "당일 추격보다 다음날 갭하단/전일종가 지지 확인 후, 강하면 5분봉 첫 눌림."
        if mode == 'S':
            if str(h.get('s_type','')) == 'S2':
                return "고점권 재응축 후 거래량 재점화. +3% 추격보다 눌림·전고점 지지 확인."
            return "거래량 마른 응축이 유지될 때 20~30% 소액, 2~5일 횡보 감안."
        if mode == 'G':
            return "갭하단/전일고가 지지 확인 후만. 시초 급등 추격 금지."
        if mode == 'IT':
            return "단기 진입이 아니라 I-MAIN 1차 분할매집. 5MA/20MA 재지지 때 추가."
        if mode == 'I':
            return "20/40/60일 중기 관찰. 장기선 위 재지지·5MA 회복 때 분할."
        if mode == 'H':
            return "돌파 후 거래량 마른 눌림봉. 5일선 재지지 확인 후만."
        if mode == 'A':
            if _is_a_confirm_live(h):
                return _a_entry_price_comment(h, confirm=True)
            if _is_a_retest_core2_live(h):
                return _a_entry_price_comment(h, confirm=False)
            return "즉시매수보다 다음날 전일고가 회복+거래대금 유지 확인 후 승격."
        if mode == 'C':
            return "장기선 돌파 후 눌림재지지. 당일 추격보다 재지지 확인."
        return "추격보다 핵심 지지선 재확인 후 소액/분할."

    def _pattern_path_text(h: dict, mode: str = '') -> str:
        """v4.4.6: 패턴별 타점 이후 예상흐름·멘탈기준."""
        mode = str(mode or _pick_strategy(h) or '').upper()
        if mode == 'LP':
            b, _ = _lp_timing_bucket(h)
            if b == 'LP-D1':
                return "빠른 개미털기후상승형. -1~-3%는 가벼운 흔들림, -3~-6%는 LP식 개미털기 가능. 갭하단/전일종가 종가유지가 핵심."
            if b == 'LP-D23':
                return "핵심 개미털기후상승형. 2~3일 식힌 뒤 흔들어도 +3/+5 반등이 빠른 편. -7% 이상은 지지선/거래량 확인 필수."
            if b == 'LP-D45':
                return "충분한 식힘 후 재상승형. 안 깨지면 안정적이나 +5까지 시간이 걸릴 수 있음. 깊은 흔들림은 갭하단 유지 여부로 판단."
            return "리더갭 후 1~5일 식힘→재지지→+3/+5 반등 시도. 위험은 갭하단 종가이탈."
        if mode == 'L':
            if _is_l_mega_upper_limit_followup(h):
                return "상한가성 L-MEGA 후속형. 당일 신규 종가추격보다 보유자 대응과 다음날 전일 상한가/종가 기준선 지지·첫 눌림 재돌파 확인이 핵심."
            return "강한 갭상승 후 변동성 확대형. 다음날 갭하단 테스트가 정상일 수 있음."
        if mode == 'S':
            return "응축 후 짧은 분출 또는 2~5일 횡보. 거래량 증가 음봉이면 실패위험."
        if mode == 'G':
            return "갭지지 후 단기 반등형. 갭하단 이탈 시 바로 약해지는 패턴."
        if mode == 'IT':
            return "중기 촉발형. 단기 흔들림보다 20/40/60일 구조 유지가 핵심."
        if mode == 'I':
            return "중기 시세분출형. -3~-5% 흔들림도 구조가 살아 있으면 정상 범위일 수 있음."
        if mode == 'A':
            if _is_a_confirm_live(h):
                return "전일 A-RETEST CORE가 다음날 살아난 확인형입니다. 전일고가 회복 후 +3/+5 빠른 익절형으로 보되, PRIME이면 적당히 식었지만 힘이 남은 정상 확인형으로 봅니다. 전일종가 이탈은 위험신호입니다."
            if _is_a_retest_core2_live(h):
                return "당일 돌파재지지 후보입니다. 바로 크게 들어가기보다 다음날 전일고가 회복 여부가 핵심입니다."
            return "돌파 확인 전 흔들림이 잦음. 전일고가 재회복 전까지 관찰 우선."
        if mode == 'C':
            return "장기선 돌파 후 횡보/눌림이 길 수 있음. 재지지 확인이 핵심."
        return "패턴별 지지선 유지 여부를 먼저 확인. 거래량 증가 장대음봉은 위험."

    def _candidate_mental_guide(h: dict, mode: str = '') -> str:
        """v4.4.8: 개별 후보 카드에 붙이는 한 줄 멘탈 기준."""
        mode = str(mode or _pick_strategy(h) or '').upper()
        if mode == 'LP':
            b, _ = _lp_timing_bucket(h)
            if b == 'LP-D23':
                return "LP-D23 핵심타점입니다. 이미 2~3일 식힌 뒤 재지지한 자리라 추가 대기보다 지지선 유지 확인이 핵심입니다. 평균적으로 +3 전 흔들림이 작았으니 전일종가·5/10일선 동시 이탈 전에는 과민반응 금지."
            if b == 'LP-D45':
                return "LP-D45 안정타점입니다. 손절선행은 낮지만 +5까지 시간이 걸릴 수 있습니다. 조용한 횡보·거래량 감소는 정상으로 보고, 갭하단 이탈만 엄격히 확인."
            if b == 'LP-D1':
                return "LP-D1 공격타점입니다. 빠른 대신 -3~-6% 흔들림이 나올 수 있어 소액만 적합합니다. 갭하단 이탈 시 빠르게 제외."
            return "LP 눌림재지지 후보입니다. 갭하단·전일종가·5/10일선 유지가 멘탈 기준이고, +3은 1차 익절 우선."
        if mode == 'L':
            if _is_l_mega_upper_limit_followup(h):
                return "L-MEGA 상한가성 잠금형입니다. 강한 후보지만 신규자는 종가 추격보다 다음날 전일 종가/상한가 기준선 지지와 VWAP 위 첫 눌림 재돌파만 봅니다. 보유자는 +3/+5 분할익절과 기준선 이탈 회복 실패를 관리합니다."
            return "L 당일 갭형입니다. 강하지만 LP보다 손절선행이 높습니다. 종가 추격보다 다음날 갭하단/전일종가 지지 확인이 멘탈 기준입니다."
        if mode == 'S':
            if _is_s_core_safe_hit(h):
                return _s_safe_live_class(h).get('guide', "S 응축형입니다. 2~7일 비비는 흐름이 나올 수 있습니다. 거래량 마른 횡보는 정상, 거래량 증가 장대음봉은 위험신호입니다.")
            return "S 응축형입니다. 2~7일 비비는 흐름이 나올 수 있습니다. 거래량 마른 횡보는 정상, 거래량 증가 장대음봉은 위험신호입니다."
        if mode == 'G':
            return "G 갭지지형입니다. 갭하단이 무너지면 장점이 사라집니다. +3/+5 짧은 대응과 갭하단 종가유지를 우선 확인."
        if mode == 'IT':
            return "IT-ACCEL 촉발형은 단기 몰빵 후보가 아닙니다. 20/40/60일 중기 분할 관점으로 5MA/20MA 재지지 때 비중을 조절하세요."
        if mode == 'I':
            return "I-MAIN 중기 후보입니다. 단기 흔들림보다 박스하단·150/200MA 구조 유지가 핵심입니다. 20MA는 흔들림, 50MA는 비중축소 검토선."
        if mode == 'A':
            if _is_a_confirm_live(h):
                return "A-CONFIRM 확인형입니다. 전일고가 회복가가 가장 좋은 실전 타점으로 검증됐고, PRIME/CALM 라벨로 거래량 냉각과 힘의 잔존 여부를 함께 봅니다. +3 1차익절·전일종가 이탈 무효를 우선합니다."
            if _is_a_retest_core2_live(h):
                return "A-RETEST CORE 당일 후보입니다. LP/L/S가 부족한 날의 보조 후보이며, 핵심은 다음날 전일고가 회복 확인입니다."
            return "A/C류 관찰 후보는 바로 사면 흔들림이 큽니다. 다음날 전일고가 회복과 거래대금 유지 전까지는 승격 금지입니다."
        if mode == 'C':
            return "C 장기저항 돌파형은 재지지 확인이 중요합니다. 당일 추격보다 장기선/전고점 지지 확인 후 접근하세요."
        return "이 후보는 패턴별 핵심 지지선 유지 여부가 멘탈 기준입니다. 거래량 증가 장대음봉은 정상 흔들림이 아닙니다."

    def _entry_plan_text(h: dict, mode: str, label_hint: str = '') -> str:
        """v4.2.10: 실전 후보별 1차/2차 진입·추격금지·익절/손절 계획을 자동 생성한다."""
        close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
        stop = _safe_float(h.get('stoploss', 0), 0.0)
        t3 = close * 1.03 if close > 0 else 0
        t5 = close * 1.05 if close > 0 else 0
        t10 = close * 1.10 if close > 0 else 0
        p2 = close * 0.98 if close > 0 else 0
        p4 = close * 0.96 if close > 0 else 0
        stop_txt = f"{int(stop):,}원 이탈" if stop > 0 else "핵심 지지/5일선 종가 이탈"
        practical_stop_txt = _practical_stop_text(h, mode) if mode in ('L', 'LP', 'G') else stop_txt
        # v4.4.9.13: 직장인 모드 — 장중 차트를 계속 못 보는 사용자를 위한 지정가/자동익절 중심 문구
        if CLOSING_BET_WORKER_MODE and mode in ('LP', 'L', 'G', 'S'):
            p1 = close * 0.99 if close > 0 else 0
            no_chase = min(close * 1.02, t3) if close > 0 and t3 > 0 else 0
            if mode == 'LP':
                timing_bucket, timing_label = _lp_timing_bucket(h)
                return (
                    f"직장인 매수계획: {timing_bucket} {timing_label}. 장중 계속 못 보면 시장가 추격 금지. "
                    f"1차 지정가 {int(p1):,}~{int(close):,}원 부근, 안 잡히면 포기. "
                    f"추격금지 {int(no_chase):,}원 이상 출발/급등. 자동익절 +3% {int(t3):,}원 / +5% {int(t5):,}원. 손절/무효: {practical_stop_txt}."
                )
            if mode == 'L':
                if _is_l_mega_upper_limit_followup(h):
                    base = close
                    return (
                        f"직장인 매수계획: L-MEGA 상한가성 후속관찰입니다. 신규 종가추격 금지. 보유자는 +3% {int(t3):,}원 / +5% {int(t5):,}원 분할익절, "
                        f"신규자는 다음날 {int(base):,}원 기준선 지지 확인 후 첫 눌림·VWAP 재돌파 때만 20~30% 이하. "
                        f"+5% 이상 갭상승 출발은 바로 매수 금지, 미체결/기준선 회복 실패는 포기. 손절/무효: {int(base):,}원 회복 실패 또는 {practical_stop_txt}."
                    )
                return (
                    f"직장인 매수계획: L 당일형은 시장확인용 우선. 장중 확인이 어렵다면 종가 추격보다 다음날 지정가만 사용. "
                    f"1차 지정가 {int(p1):,}~{int(close):,}원 부근 또는 갭하단 지지 확인형, 미체결은 포기. "
                    f"추격금지 {int(no_chase):,}원 이상 급등 출발. 자동익절 +3% {int(t3):,}원 / +5% {int(t5):,}원. 손절/무효: {practical_stop_txt}."
                )
            if mode == 'G':
                return (
                    f"직장인 매수계획: 갭하단 지지형입니다. 1차 지정가 {int(p1):,}~{int(close):,}원 부근만, 시초 급등 추격 금지. "
                    f"자동익절 +3% {int(t3):,}원 / +5% {int(t5):,}원. 손절/무효: {practical_stop_txt}."
                )
            if mode == 'S':
                return (
                    f"직장인 매수계획: 1차 종가 부근 {int(p1):,}~{int(close):,}원 지정가 20~30% 이하, 미체결은 포기. "
                    f"추격금지 {int(no_chase):,}원 이상 급등. 자동익절 +3% {int(t3):,}원 / +5% {int(t5):,}원. 손절/무효: {stop_txt}."
                )
        if mode == 'LP':
            timing_bucket, timing_label = _lp_timing_bucket(h)
            return (
                f"매수계획: {timing_bucket} {timing_label}. LP-SAFE는 L 당일 추격이 아니라 눌림재지지 확인형입니다. "
                f"1차는 갭하단/전일종가/5·10일선 지지 확인 후 20~30%, 2차는 전일고가 회복 또는 양봉 재전환 시만. "
                f"익절: +3% {int(t3):,}원 우선 / +5% {int(t5):,}원 추가. 손절/무효: {practical_stop_txt}."
            )
        if mode == 'SLOCK':
            return (
                f"매수계획: S2 상단잠김 신규검증형입니다. 고점권이므로 1차 20% 이하, +3% 근처 추격 금지. "
                f"익절: +3% {int(t3):,}원 1차 / +5% {int(t5):,}원 추가. 손절/무효: {stop_txt} 또는 종가위치 70% 이탈."
            )
        if mode == 'IT':
            return (
                f"매수계획: 단기 종가배팅이 아니라 I-MAIN 1차 분할매집 촉발형입니다. 1차 20~30% 이하, 2차는 5MA/20MA 재지지 확인 후. "
                f"목표: +10% {int(t10):,}원부터 중기 관찰. 손절/무효: 박스하단·150/200MA 구조 이탈, 50MA 이탈 시 비중축소 검토."
            )
        if mode == 'S':
            return (
                f"매수계획: 1차 현재가/종가 부근 20~30% 이하, 2차 {int(p2):,}~{int(p4):,}원 눌림 후 반등 확인 시 추가. "
                f"추격금지: +3% 근처 급등 추격·거래량 증가 장대음봉. 익절: +3% {int(t3):,}원 / +5% {int(t5):,}원. 손절/무효: {stop_txt}."
            )
        if mode == 'G':
            return (
                f"매수계획: 갭 하단·전일고가 지지 확인 후 1차 20~30%, 당일 고가 추격 금지. "
                f"익절: +3% {int(t3):,}원 우선 / +5% {int(t5):,}원 추가. 손절/무효: {stop_txt} 또는 갭 하단 재이탈."
            )
        if mode == 'L':
            m = _l_metrics(h)
            if _is_l_mega_upper_limit_followup(h):
                typ = 'L-MEGA 상한가성 후속관찰: 신규 종가추격 금지. 보유자 대응과 다음날 전일 종가/상한가 기준선 지지, 첫 눌림·VWAP 재돌파 확인형.'
                style = f'보유자는 +3% {int(t3):,}원 1차, +5% {int(t5):,}원 2차. 신규자는 다음날 기준선 지지 후 소액만.'
            elif _is_l_mega_hit(h):
                typ = 'L-MEGA 초대형 주도갭: 1차 종가/다음날 갭하단 지지 확인 20~30%, 2차 전일고가 재돌파 또는 5일선 지지 확인.'
                style = f'+3% {int(t3):,}원 1차, +5% {int(t5):,}원 2차, 강하면 일부만 5일선 추세 관찰.'
            elif _is_l_core_safe_hit(h):
                typ = 'L-CORE SAFE: 5000억+·종가위치70%+·윗꼬리25% 이하. 1차 소액/분할, 2차 갭하단·전일종가 지지 확인 후 추가.'
                style = f'+3% {int(t3):,}원 우선, +5% {int(t5):,}원 추가 익절.'
            elif _is_l_tail_absorb_hit(h):
                typ = 'L-TAIL ABSORB 윗꼬리 흡수형: 5000억+ 거래대금이 장중 매물을 받은 후보. 1차는 더 작게, 2차는 갭하단/전일종가 지지 확인 후.'
                style = f'+3% {int(t3):,}원 우선 회수, +5% {int(t5):,}원은 지지 유지 시만.'
            elif _is_l_core_hit(h):
                typ = 'L-CORE 5000억+ 리더갭: +3/+5 익절형 보조 후보. 다음날 지지 확인을 우선.'
                style = f'+3% {int(t3):,}원 우선, +5% {int(t5):,}원 추가 익절.'
            else:
                typ = 'L-WATCH 리더갭 관찰: 실전 주력보다 다음날 지지 확인 우선.'
                style = f'+3% {int(t3):,}원 중심 빠른 대응, +5% {int(t5):,}원은 강할 때만.'
            return (
                f"매수계획: {typ} 추격금지: 시초 급등 후 추가 +5% 추격·갭하단 이탈·거래량 증가 장대음봉. "
                f"익절: {style} 손절/무효: {practical_stop_txt}."
            )
        if mode == 'A':
            if _is_a_confirm_live(h):
                entry = _a_entry_price_comment(h, confirm=True)
                return (
                    f"매수계획: A-CONFIRM 다음날 확인형입니다. {entry} "
                    f"A-VC체크: {_a_vc_reason_text(h)}. "
                    f"익절: +3% {int(t3):,}원 우선 / +5% {int(t5):,}원 일부 연장. "
                    f"손절/무효: 전일종가·5MA 이탈 또는 거래량 증가 장대음봉."
                )
            if _is_a_retest_core2_live(h):
                entry = _a_entry_price_comment(h, confirm=False)
                return (
                    f"매수계획: A-RETEST CORE② 보조 승격 후보입니다. {entry} "
                    f"당일은 20% 이하 관찰, 다음날 전일고가 회복 시 추가. "
                    f"익절: +3% {int(t3):,}원 우선 / +5% {int(t5):,}원 일부 연장. 손절/무효: {stop_txt}."
                )
        # A/B/C 등 보조 후보
        return (
            f"매수계획: 주력 후보가 아니므로 소액 관찰만. 1차는 돌파 유지 확인 후, 2차는 눌림 재상승 확인 후. "
            f"익절: +3% {int(t3):,}원 / +5% {int(t5):,}원. 손절/무효: {stop_txt}."
        )

    def _h_entry_plan_text(h: dict, label_hint: str = '') -> str:
        """v4.2.10: H 신규검증 후보별 운용계획.
        v4.2.9 결과 반영: 700~1000억×2~3배는 스윙확장, 500~700억×2~3배는 단기익절형,
        500~1000억×3~4배/4~5배는 실전 확장 금지.
        """
        close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
        stop = _safe_float(h.get('stoploss', 0), 0.0)
        amt = _h_breakout_amount(h)
        bvol = _h_breakout_vol60(h)
        t3 = close * 1.03 if close > 0 else 0
        t5 = close * 1.05 if close > 0 else 0
        t10 = close * 1.10 if close > 0 else 0
        p2 = close * 0.98 if close > 0 else 0
        stop_txt = f"{int(stop):,}원 또는 5일선 종가이탈" if stop > 0 else "5일선 종가이탈"
        if _is_h_triangle_hit(h):
            typ = "삼각수렴형 최우선: 1차 타점봉 종가/다음날 초반 20~30%, 2차 눌림 후 5일선 지지·양봉 회복 시 추가."
            style = f"+3% {int(t3):,}원 일부, +5% {int(t5):,}원 추가, 강하면 +10% {int(t10):,}원까지 5일선 추적."
        elif _is_h_core_500_1000_vol23(h) and amt >= 700.0:
            typ = "700~1000억×2~3배 스윙확장형: 1차 20~30%, 2차 5일선 지지 확인 시 추가."
            style = f"+3% {int(t3):,}원 일부만 덜고, +5% {int(t5):,}원 이후 5일선 유지 시 +10% {int(t10):,}원까지 관찰."
        elif _is_h_core_500_1000_vol23(h):
            typ = "500~700억×2~3배 단기익절형: 1차 소액, 2차는 {0:,}원 부근 눌림 반등 확인 시만.".format(int(p2))
            style = f"+3% {int(t3):,}원 / +5% {int(t5):,}원 빠른 익절 우선, 오래 보유하지 않음."
        elif _is_h_core_300_500_vol35(h):
            typ = "300~500억×3~5배 고수익 신규검증형: 표본이 작으므로 소액만, 추격보다 눌림 반등 확인."
            style = f"+3% {int(t3):,}원 일부, +5% {int(t5):,}원 추가. 5일선 이탈 시 즉시 정리."
        elif _is_h_core_1000_2000_vol23(h):
            typ = "1000~2000억×2~3배 대형수급 관찰형: 표본 적어 소액 검증, 5일선 지지 확인 후 추가."
            style = f"+3% {int(t3):,}원 일부, +5% {int(t5):,}원 추가."
        elif _is_h_fast_hit(h):
            typ = "8배+ H-FAST 급등형: 1차 소액만, 추가매수 금지에 가깝게 운용."
            style = f"+3% {int(t3):,}원·+5% {int(t5):,}원 빠른 익절 전용, 보유 금지."
        else:
            typ = "H 일반 후보는 실전 제외/관찰."
            style = f"+3% {int(t3):,}원 / +5% {int(t5):,}원 기준만 참고."
        return f"매수계획: {typ} 추격금지: 장대양봉 재가속·Vol60 5~8배 과열·타점봉 저가 이탈. 익절: {style} 손절/무효: {stop_txt}."

    # v4.4.9.24: S-SAFE live labels. ST30은 하드필터가 아니라 가점/강등 태그이고,
    # 거래대금 3000~5000억 구간은 5000억+와 분리해서 계속 추적한다.
    def _s_live_st30_pass(h: dict) -> bool:
        label = str(h.get('st30_label', '') or '')
        return bool(_safe_int(h.get('st30_reclaim_pass', 0), 0) == 1 or _safe_int(h.get('st30_weekly_confirm', 0), 0) == 1 or '✅' in label)

    def _s_live_st30_wait(h: dict) -> bool:
        label = str(h.get('st30_label', '') or '')
        return bool(_safe_int(h.get('st30_wait', 0), 0) == 1 or 'WAIT' in label or '대기' in label)

    def _s_safe_live_class(h: dict) -> dict:
        """S-SAFE를 S-RECLAIM / S-MOMENTUM / S-LIQUIDITY로 분리한다.
        - ST30 통과: 조정후 재상승 안정형 가점
        - ST30 미통과 + 고유동성: 추세지속형 WATCH/PRIME
        - ST30 미통과 + 저유동성: RISK/EXCLUDE
        """
        amount_b = _safe_float(h.get('amount_b', 0), 0.0)
        close_loc = _safe_float(h.get('close_loc_pct', 0), 0.0)
        volr = _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0)
        rr = _safe_float(h.get('rr', 0), 0.0)
        st30_pass = _s_live_st30_pass(h)
        st30_wait = _s_live_st30_wait(h)
        quality = (close_loc >= 75.0) and (volr <= 1.80 or volr <= 0) and (0.8 <= rr <= 1.8 or rr <= 0)
        calm = (volr <= 1.20 or volr <= 0) and (0.8 <= rr <= 1.5 or rr <= 0)
        if st30_pass:
            # v4.4.9.24: ST30 통과군도 거래대금에 따라 강도/비중을 나눈다.
            # 핵심은 ST30=구조 안정, 거래대금=지속성/비중 판단이다.
            if amount_b >= 5000:
                return {
                    'key': 'S_RECLAIM_MEGA',
                    'label': '💎S-RECLAIM MEGA',
                    'tag': 'S-RECLAIM MEGA ST30+5000억',
                    'reason': 'ST30 통과 + 거래대금 5000억+ 고유동성. 구조 안정과 수급 대표성이 동시에 확인된 최상위 S-RECLAIM 후보',
                    'guide': 'S-RECLAIM MEGA입니다. 종가위치와 거래량 과열이 무너지지 않으면 1차 20~30% 지정가 가능, +3 자동익절과 +5 부분연장을 우선합니다.',
                }
            if 3000 <= amount_b < 5000:
                return {
                    'key': 'S_RECLAIM_3000_5000',
                    'label': '🟢S-RECLAIM 3000~5000',
                    'tag': 'S-RECLAIM 3000~5000 ST30 준대형',
                    'reason': 'ST30 통과 + 거래대금 3000~5000억. 5000억+와 분리 추적하는 준대형 수급 후보',
                    'guide': 'S-RECLAIM 3000~5000입니다. 표본은 계속 추적하되 ST30 통과형이므로 소액 지정가와 다음날 지지 확인을 병행합니다.',
                }
            if amount_b >= 1000:
                return {
                    'key': 'S_RECLAIM_PRIME',
                    'label': '🟢S-RECLAIM PRIME',
                    'tag': 'S-RECLAIM PRIME 조정후재상승',
                    'reason': 'ST30 조정후 재상승 확인 + 거래대금 1000억+. S-SAFE 중 안정형 가점 후보',
                    'guide': 'S-RECLAIM PRIME입니다. ST30 통과형은 안정성이 높았지만 표본은 작으므로 종가 부근 20~30% 소액 지정가와 +3 자동익절을 우선합니다.',
                }
            if amount_b >= 300:
                return {
                    'key': 'S_RECLAIM_WATCH',
                    'label': '🟡S-RECLAIM WATCH',
                    'tag': 'S-RECLAIM WATCH ST30 저유동성',
                    'reason': 'ST30은 통과했지만 거래대금 300~1000억. 구조는 좋지만 체결/흔들림 리스크가 있어 비중을 낮추는 구간',
                    'guide': 'S-RECLAIM WATCH입니다. ST30 통과만 믿고 추격하지 말고 다음날 거래대금 재증가와 전일고가 회복 확인 전까지 소액/관망 우선입니다.',
                }
            return {
                'key': 'S_RECLAIM_LIQUIDITY_RISK',
                'label': '⚠️S-RECLAIM LIQUIDITY RISK',
                'tag': 'S-RECLAIM LIQUIDITY RISK ST30 저유동',
                'reason': 'ST30은 통과했지만 거래대금 300억 미만. 구조보다 유동성 위험이 커서 실전에서는 확인 필요',
                'guide': 'ST30 통과형이라도 300억 미만은 비중을 싣지 않습니다. 당일 신규매수보다 다음날 거래대금 1000억 이상 재증가 확인을 기다립니다.',
            }
        # ST30 미통과는 제외가 아니라 거래대금/대표성으로 분류한다.
        # v4.4.9.35: 최근 지정학 리스크/헤드라인 장세 검증에서는 PRIME보다 거래량이 식은 CALM의 손절이 더 낮았다.
        if amount_b < 300:
            return {
                'key': 'S_LIQUIDITY_EXCLUDE',
                'label': '❌S-LIQUIDITY EXCLUDE',
                'tag': 'S-LIQUIDITY EXCLUDE 저유동성 제외',
                'reason': 'ST30 미통과 + 거래대금 300억 미만. 단기 표본에서 좋아 보여도 누적검증상 저유동성 위험구간이라 원칙 제외',
                'guide': 'S-SAFE처럼 보여도 저유동성 실패형에 가깝습니다. 당일 종가매수는 원칙적으로 제외하고 다음날 거래대금 재증가가 없으면 관망합니다.',
            }
        if amount_b < 1000:
            return {
                'key': 'S_LIQUIDITY_RISK',
                'label': '⚠️S-LIQUIDITY RISK',
                'tag': 'S-LIQUIDITY RISK 저유동성 주의',
                'reason': 'ST30 미통과 + 거래대금 1000억 미만. 종가위치/RR이 좋아도 저유동성 실패군이 많았던 구간',
                'guide': '저유동성 S-SAFE입니다. 바로 사기보다 다음날 양봉·전일고가 회복·거래대금 재증가 확인 전까지 관망 우선입니다.',
            }
        if amount_b >= 1000 and close_loc >= 75.0 and calm:
            tier = '5000억+' if amount_b >= 5000 else ('3000~5000억' if amount_b >= 3000 else '1000~3000억')
            return {
                'key': 'S_MOMENTUM_CALM',
                'label': '🟢S-MOMENTUM CALM',
                'tag': f'S-MOMENTUM CALM {tier} 거래량냉각',
                'reason': f'ST30 미통과지만 거래대금 {tier}·종가75%+·Vol≤1.2·RR0.8~1.5. 최근 리스크 장세 검증에서 PRIME보다 손절이 낮은 저거래 냉각형 우선 구간',
                'guide': '거래량이 과열되지 않은 CALM형입니다. LP/L/A-CONFIRM이 없을 때 S 보조 후보 중 우선하지만 추격보다 지정가·다음날 지지 확인을 우선합니다.',
            }
        if amount_b >= 1000 and quality:
            tier = '5000억+' if amount_b >= 5000 else ('3000~5000억' if amount_b >= 3000 else '1000~3000억')
            return {
                'key': 'S_MOMENTUM_PRIME',
                'label': '🔥S-MOMENTUM PRIME',
                'tag': f'S-MOMENTUM PRIME {tier} 추세지속',
                'reason': f'ST30 미통과지만 거래대금 {tier}·종가75%+·Vol≤1.8·RR정상. 반복검증에서 1000억+ 품질군이 승률/손절 기준을 통과한 추세지속형 PRIME 구간',
                'guide': 'ST30 조정형은 아니지만 1000억+ 유동성과 품질이 살아 있는 추세지속형입니다. CALM보다 거래량이 덜 식은 구간이므로 보조 후보로만 보고, 추격보다 지정가·다음날 지지 확인을 우선합니다.',
            }
        return {
            'key': 'S_SAFE_CHECK',
            'label': '🟡S-SAFE 확인',
            'tag': 'S-SAFE 확인필요',
            'reason': 'S-SAFE이나 ST30/거래대금/품질 조합이 PRIME/WATCH/RISK로 명확히 분류되지 않음',
            'guide': '분류 애매형입니다. 당일 종가매수보다 다음날 전일고가 회복·거래대금 유지 확인이 우선입니다.',
        }

    def _s_safe_live_key(h: dict) -> str:
        return _s_safe_live_class(h).get('key', 'S_SAFE_CHECK')

    def _brief_practical_line(h: dict, idx: int) -> str:
        code = str(h.get('code', '') or '').strip()
        name = _clean_stock_name(code, str(h.get('name', '') or code).strip())
        mode = _pick_strategy(h)
        risk_tags = _s_core_risk_tags(h) if mode == 'S' else (_trade_risk_tags(h) if mode == 'A' else (_g_aggressive_tags(h) if mode == 'G' else []))

        # v4.1.1: SAFE/NEUTRAL 라벨은 S-CORE 전용이다.
        # A/B/C 보조 후보에 🟡NEUTRAL이 잘못 붙지 않도록 분리한다.
        if _is_s_core_hit(h):
            if _is_s_core_safe_hit(h):
                safe_label = _s_safe_live_class(h).get('label', '🟢SAFE')
            else:
                safe_label = ('⚠️RISK:' + ','.join(risk_tags) if risk_tags else '🟡NEUTRAL')
        elif mode == 'A':
            if _is_a_confirm_live(h):
                safe_label = '🔁A-CONFIRM·' + _a_vc_badge(h)
            elif _is_a_retest_core2_live(h):
                safe_label = '🟣A-CORE'
            else:
                safe_label = ('⚠️A-RISK:' + ','.join(risk_tags)) if risk_tags else '🚀A보조'
        elif mode == 'LP':
            safe_label = str(h.get('lp_class_label', '') or ('🥇LP-SAFE' if _is_lp_safe_hit(h) else ('⚠️LP-RISK' if _is_lp_risk_hit(h) else '🔁LP-WATCH')))
        elif mode == 'SLOCK':
            safe_label = '🔒SLOCK'
        elif mode == 'IT':
            safe_label = '⚡IT'
        elif mode == 'G':
            if _is_g_safe_hit(h):
                safe_label = '🟢G-SAFE'
            elif _is_g_aggressive_hit(h):
                safe_label = '🔥G-AGG:' + ','.join(risk_tags)
            else:
                safe_label = '🟡G-관찰'
        elif mode == 'L':
            if _is_l_mega_hit(h):
                safe_label = '💰L-MEGA'
            elif _is_l_core_safe_hit(h):
                safe_label = '👑L-SAFE'
            elif _is_l_tail_absorb_hit(h):
                safe_label = '🌊L-TAIL'
            elif _is_l_core_hit(h):
                safe_label = '👑L-CORE'
            else:
                safe_label = '🟡L-WATCH'
        elif mode in ('B1', 'B2'):
            safe_label = '👀관찰'
        elif mode == 'C':
            safe_label = '🏆스윙참고'
        else:
            safe_label = '보조후보'
        if mode == 'S' and _is_s_core_safe_hit(h):
            _scls = _s_safe_live_class(h)
            tag = _scls.get('tag', 'S1 우수응축형')
            reason = _scls.get('reason', '고점권에서 거래량이 마른 채 버티는 실전 우선 후보')
        elif mode == 'S' and _is_s1_good(h):
            tag = 'S1 우수응축형'
            reason = '고점권에서 거래량이 마른 채 버티는 실전 우선 후보'
        elif mode == 'S' and str(h.get('s_type', '')) == 'S2':
            tag = 'S2 실행형'
            reason = '고점권 재응축 후 거래량 재점화'
            if '거래량과열' in risk_tags:
                reason += ' — 단, 거래량비 1.5+ 과열 재점화는 추격주의'
        elif mode == 'S':
            tag = 'S1 일반관찰'
            reason = '구조는 좋지만 핵심군 기준 부족, 다음날 확인'
        elif mode == 'A':
            if _is_a_confirm_live(h):
                tag = 'A-SAFE 다음날확인형·' + _a_vc_badge(h)
                reason = '전일 A-RETEST CORE 후 오늘 전일고가 회복/양봉·거래대금 유지가 붙은 확인형 후보 — ' + _a_vc_reason_text(h)
            elif _is_a_retest_core2_live(h):
                tag = 'A-RETEST CORE②'
                reason = '5000억+·종가80+·거래량≤1.8·RR0.8~1.5를 통과한 보조 승격 후보'
            else:
                tag = 'A 강한돌파'
                reason = '+3~+5% 익절형 돌파 후보'
        elif mode == 'LP':
            tag = str(h.get('lp_class', 'LP-WATCH') or 'LP-WATCH') + ' 리더갭 눌림재지지'
            reason = str(h.get('lp_decision', '') or '최근 1~5일 내 5000억+ L 리더갭 이후 갭하단/전일종가/5·10일선 지지 확인형')
        elif mode == 'SLOCK':
            tag = 'S2-LOCK 상단잠김'
            reason = 'S2 실행형 중 종가위치80%+·윗꼬리20% 이하·유동성 우대 상단잠김 후보'
        elif mode == 'IT':
            tag = 'I-TRIGGER 촉발형'
            reason = 'I-MAIN 중기 후보 중 1차 분할매집 촉발 조건이 붙은 후보'
        elif mode == 'G' and _is_g_safe_hit(h):
            tag = 'G 모랄레스갭 SAFE'
            reason = '갭 지지·종가상단·유동성 조건을 통과한 SAFE 다음 보조 실전 후보'
        elif mode == 'G' and _is_g_neutral_hit(h):
            tag = 'G 모랄레스갭 관찰'
            reason = '갭 구조는 있으나 G-SAFE 조건이 부족해 다음날 갭 지지 확인 우선'
        elif mode == 'G':
            tag = 'G-AGGRESSIVE'
            reason = '저유동성 또는 고변동 급등형 — 실전에서는 체결/슬리피지 주의'
        elif mode == 'L' and _is_l_mega_hit(h):
            tag = '대형주 리더갭 MEGA'
            reason = '거래대금 1조+ 또는 갭 6~12%의 초대형 주도갭 — +3/+5 우선, 일부 5일선 추적'
        elif mode == 'L' and _is_l_core_safe_hit(h):
            tag = '대형주 리더갭 SAFE'
            reason = '5000억+·종가위치70%+·윗꼬리25% 이하의 정석 리더갭'
        elif mode == 'L' and _is_l_tail_absorb_hit(h):
            tag = '대형주 리더갭 TAIL-ABSORB'
            reason = '5000억+ 거래대금이 윗꼬리/중간 종가위치를 흡수한 형태 — 지지 확인 후 대응'
        elif mode == 'L' and _is_l_core_hit(h):
            tag = '대형주 리더갭 CORE'
            reason = '거래대금 5000억+ 대형 주도주 갭 — +3/+5 익절 우선'
        elif mode == 'L':
            tag = '대형주 리더갭 WATCH'
            reason = '3000~5000억 또는 품질 약한 리더갭 관찰형 — 다음날 갭 지지 확인 우선'
        else:
            tag = mode
            reason = '조건 확인 필요'
        close = _safe_float(h.get('close', 0), 0.0)
        amount_b = _safe_float(h.get('amount_b', 0), 0.0)
        volr = _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0)
        rr = _safe_float(h.get('rr', 0), 0.0)
        close_loc = _safe_float(h.get('close_loc_pct', 0), 0.0)
        score = _safe_float(h.get('score', 0), 0.0)
        stop = _safe_float(h.get('stoploss', 0), 0.0)
        pressure_line = f"   🌐 시장/섹터: 시장압력 {_live_market_pressure_label(h)} | 섹터압력 {_live_sector_pressure_label(h)}\n"
        risk_line = ''
        if risk_tags:
            risk_line = f"\n   주의태그: {' / '.join('⚠️' + t for t in risk_tags)}"
        sj_line = '   ' + _sj6_checklist_context(h).get('line', '📊 신좁깔거조재: 확인필요') + '\n'
        if mode == 'LP':
            t3 = close * 1.03 if close > 0 else 0
            t5 = close * 1.05 if close > 0 else 0
            plan = _entry_plan_text(h, mode)
            timing_bucket, timing_label = _lp_timing_bucket(h)
            scenario = _lp_next_day_scenario(h)
            entry_tip = _pattern_entry_tip(h, mode)
            path_tip = _pattern_path_text(h, mode)
            stop_txt = _practical_stop_text(h, mode)
            mental_tip = _candidate_mental_guide(h, mode)
            return (
                f"{idx}) {safe_label} | {timing_bucket} | {tag} | {name}({code}) | {int(close):,}원 | 점수 {score:.1f} | "
                f"갭후 {int(_safe_float(h.get('lp_days_since_gap',0),0))}일 | 갭 {_safe_float(h.get('lp_gap_pct',0),0):+.1f}% | 갭봉대금 {_safe_float(h.get('lp_gap_amount_b',0),0):.1f}억 | 현거래대금 {amount_b:.1f}억 | 거래량식힘 {_safe_float(h.get('lp_volume_vs_gap',0),0):.2f}배 | 종가위치 {close_loc:.0f}%\n"
                f"   🎯 추천타점: {entry_tip}\n"
                f"   🧭 예상흐름: {path_tip}\n"
                f"   🧠 이 종목 멘탈기준: {mental_tip}\n"
                f"{pressure_line}"
                f"{sj_line}"
                f"   핵심: {reason}{risk_line}\n"
                f"   분류근거: {str(h.get('lp_class_reason',''))} {(' | 주의: ' + str(h.get('lp_risk_reasons',''))) if str(h.get('lp_risk_reasons','')) else ''}\n"
                f"   대응: +3% {int(t3):,}원 1차 / +5% {int(t5):,}원 추가 / {stop_txt}\n"
                f"   📌 {plan}\n"
                f"   🧭 다음날: {scenario}\n   [카드완료]"
            )
        if mode == 'L':
            lm = _l_metrics(h)
            t3 = close * 1.03 if close > 0 else 0
            t5 = close * 1.05 if close > 0 else 0
            stop_txt = _practical_stop_text(h, mode)
            overheat_txt = ' | 과열표시' if lm.get('overheat', 0) else ''
            plan = _entry_plan_text(h, mode)
            entry_tip = _pattern_entry_tip(h, mode)
            path_tip = _pattern_path_text(h, mode)
            mental_tip = _candidate_mental_guide(h, mode)
            return (
                f"{idx}) {safe_label} | {tag} | {name}({code}) | {int(close):,}원 | 점수 {score:.1f} | "
                f"갭 {lm['gap']:.1f}% | Vol50 {lm['vol50']:.1f} | 거래대금 {lm['amount']:.1f}억 | 종가위치 {lm['close_loc']:.0f}% | 윗꼬리 {lm['wick']:.1f}%{overheat_txt}\n"
                f"   🎯 추천타점: {entry_tip}\n"
                f"   🧭 예상흐름: {path_tip}\n"
                f"   🧠 이 종목 멘탈기준: {mental_tip}\n"
                f"{pressure_line}"
                f"{sj_line}"
                f"   핵심: {reason}{risk_line}\n"
                f"   대응: +3% {int(t3):,}원 1차 / +5% {int(t5):,}원 추가 / {stop_txt}\n"
                f"   📌 {plan}\n   [카드완료]"
            )
        if mode == 'G':
            gm = _g_metrics(h)
            t3 = close * 1.03 if close > 0 else 0
            t5 = close * 1.05 if close > 0 else 0
            stop_txt = _practical_stop_text(h, mode)
            plan = _entry_plan_text(h, mode)
            entry_tip = _pattern_entry_tip(h, mode)
            path_tip = _pattern_path_text(h, mode)
            mental_tip = _candidate_mental_guide(h, mode)
            return (
                f"{idx}) {safe_label} | {tag} | {name}({code}) | {int(close):,}원 | 점수 {score:.1f} | "
                f"갭 {gm['gap']:.1f}% | Vol50 {gm['vol50']:.1f} | 거래대금 {amount_b:.1f}억 | 종가위치 {close_loc:.0f}% | 윗꼬리 {gm['wick']:.1f}%\n"
                f"   🎯 추천타점: {entry_tip}\n"
                f"   🧭 예상흐름: {path_tip}\n"
                f"   🧠 이 종목 멘탈기준: {mental_tip}\n"
                f"{pressure_line}"
                f"{sj_line}"
                f"   핵심: {reason}{risk_line}\n"
                f"   대응: +3% {int(t3):,}원 1차 / +5% {int(t5):,}원 추가 / 5거래일 내 힘 없으면 정리 / {stop_txt}\n"
                f"   📌 {plan}\n   [카드완료]"
            )
        plan = _entry_plan_text(h, mode)
        entry_tip = _pattern_entry_tip(h, mode)
        path_tip = _pattern_path_text(h, mode)
        mental_tip = _candidate_mental_guide(h, mode)
        vc_line = ''
        if mode == 'A' and _is_a_confirm_live(h):
            vc_line = f"   🧊 A-VC체크: {_a_vc_reason_text(h)}\n"
        return (
            f"{idx}) {safe_label} | {tag} | {name}({code}) | {int(close):,}원 | 점수 {score:.1f} | "
            f"거래량비 {volr:.2f} | 거래대금 {amount_b:.1f}억 | RR {rr:.2f} | 종가위치 {close_loc:.0f}%\n"
            f"   🎯 추천타점: {entry_tip}\n"
            f"   🧭 예상흐름: {path_tip}\n"
            f"   🧠 이 종목 멘탈기준: {mental_tip}\n"
            f"{pressure_line}"
            f"{vc_line}"
            f"   핵심: {reason}{risk_line}\n"
            f"   대응: +3% 1차익절 / +5% 추가익절 / 손절 {int(stop):,}원 이탈 관리\n"
            f"   📌 {plan}\n   [카드완료]"
        )

    def _brief_c_swing_line(h: dict, idx: int) -> str:
        code = str(h.get('code', '') or '').strip()
        name = _clean_stock_name(code, str(h.get('name', '') or code).strip())
        close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
        amount_b = _safe_float(h.get('amount_b', 0), 0.0)
        volr = _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0)
        score = _safe_float(h.get('score', 0), 0.0)
        c_stage_label = str(h.get('c_stage_label', h.get('c_type_label', '')) or '').strip()
        c_stage_desc = str(h.get('c_stage_desc', h.get('band_comment', '')) or '').strip()
        c_stage_bias = str(h.get('c_stage_bias', '') or '').strip()
        support = _safe_float(h.get('ymgp_support_level', h.get('stoploss', 0)), 0.0)
        d_break = _safe_int(h.get('days_since_breakout', 0), 0)
        pullback = _safe_float(h.get('ymgp_pullback_pct', 0), 0.0)
        t5 = close * 1.05 if close > 0 else 0
        t10 = close * 1.10 if close > 0 else 0
        stop_txt = f"장기선/재지지선 {int(support):,}원 종가이탈" if support > 0 else "장기선/재지지선 종가이탈"
        stage_txt = c_stage_label or 'C 엄격형'
        desc = c_stage_desc or '역배열 바닥→매집→장기선 돌파 구조가 확인된 스윙 관심 후보'
        bias = c_stage_bias or '당일 추격보다 눌림 후 재지지 확인이 핵심'
        return (
            f"{idx}) 🏆C-SWING | {stage_txt} | {name}({code}) | {int(close):,}원 | 점수 {score:.1f} | "
            f"거래량비 {volr:.2f} | 거래대금 {amount_b:.1f}억\n"
            f"   구조: 역배열 바닥 → 매집흔적 → 공구리/장기선 돌파 → 스윙 관심\n"
            f"   위치: 돌파후 {d_break}거래일 | 눌림 {pullback:.1f}% | {desc}\n"
            f"   대응: +5% {int(t5):,}원 1차 / +10% {int(t10):,}원 2차 / 손절 {stop_txt}\n"
            f"   주의: 종가배팅 후보가 아닙니다. {bias}\n   [카드완료]"
        )

    def _brief_h_line(h: dict, idx: int, label: str = '✅ H-CORE') -> str:
        code = str(h.get('code', '') or '').strip()
        name = _clean_stock_name(code, str(h.get('name', '') or code).strip())
        close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
        score = _safe_float(h.get('score', 0), 0.0)
        b_amt = _h_breakout_amount(h)
        e_amt = _safe_float(h.get('entry_amount_b', h.get('amount_b', 0)), 0.0)
        bvol = _h_breakout_vol60(h)
        d = _safe_int(h.get('days_since_high_breakout', 0), 0)
        pull = _safe_float(h.get('high_dryup_pullback_pct', 0), 0.0)
        close_loc = _safe_float(h.get('high_dryup_close_loc_pct', h.get('close_loc_pct', 0)), 0.0)
        pre = str(h.get('h_pre_structure_label', '') or '').strip()
        br_date = str(h.get('high_breakout_date', '') or '').strip()
        stop = _safe_float(h.get('stoploss', 0), 0.0)
        t3 = close * 1.03 if close > 0 else 0
        t5 = close * 1.05 if close > 0 else 0
        t10 = close * 1.10 if close > 0 else 0
        if _is_h_triangle_hit(h):
            operate = f"+3 {int(t3):,}원 / +5 {int(t5):,}원 우선, 강하면 +10 {int(t10):,}원까지. 종가 5일선 이탈 시 정리"
        elif _is_h_fast_hit(h):
            operate = f"8배+ 빠른익절형: +3 {int(t3):,}원·+5 {int(t5):,}원 우선, 장기 보유 금지"
        else:
            operate = f"+3 {int(t3):,}원 1차 / +5 {int(t5):,}원 2차 / 5일선 종가이탈 정리"
        stop_txt = f"손절 {int(stop):,}원 또는 5일선 종가이탈" if stop > 0 else "5일선 종가이탈"
        plan = _h_entry_plan_text(h, label)
        return (
            f"{idx}) {label} | {name}({code}) | {int(close):,}원 | 점수 {score:.1f}\n"
            f"   돌파: {br_date} | 돌파대금 {b_amt:.1f}억 | Vol60 {bvol:.1f}배 | 타점대금 {e_amt:.1f}억 | 돌파후 {d}일 | 눌림 {pull:.1f}% | 종가위치 {close_loc:.0f}%\n"
            f"   구조: {pre or '구조진단 없음'} | 장대양봉 신고가 돌파 후 거래량 마른 짧은 타점봉\n"
            f"   대응: {operate} / {stop_txt}\n"
            f"   🌐 시장/섹터: 시장압력 {_live_market_pressure_label(h)} | 섹터압력 {_live_sector_pressure_label(h)}\n"
            f"   📌 {plan}\n   [카드완료]"
        )

    def _is_i_main_hit(h: dict) -> bool:
        return _pick_strategy(h) == 'I' or _safe_int(h.get('i_core', 0), 0) == 1

    def _i_phase(h: dict) -> str:
        return str(h.get('i_phase', '') or '').strip()

    def _i_anchor_120_180(h: dict) -> bool:
        d = _safe_int(h.get('i_anchor_days', 999), 999)
        return 120 <= d <= 180

    def _i_monthly_rebuild(h: dict) -> bool:
        return _safe_int(h.get('i_monthly_vol_rebuild', 0), 0) == 1

    def _is_i_main_candidate(h: dict) -> bool:
        return _is_i_main_hit(h) and _safe_int(h.get('i_core_main_candidate', 0), 0) == 1

    def _is_i_main_accel_hit(h: dict) -> bool:
        return _is_i_main_candidate(h) and _safe_int(h.get('i_core_main_accel', 0), 0) == 1 and _i_anchor_120_180(h)

    def _is_i_main_core_hit(h: dict) -> bool:
        return _is_i_main_candidate(h) and _i_phase(h) == 'I-4' and _i_anchor_120_180(h) and _i_monthly_rebuild(h)

    def _is_i_main_watch_hit(h: dict) -> bool:
        return _is_i_main_candidate(h) and _i_phase(h) == 'I-4'

    def _is_i_main_confirm_hit(h: dict) -> bool:
        return _is_i_main_candidate(h) and _i_phase(h) == 'I-5'

    def _is_i_main_add_hit(h: dict) -> bool:
        return _is_i_main_candidate(h) and _i_phase(h) == 'I-6'

    def _priority_i_main(h: dict):
        if _is_i_main_accel_hit(h):
            rank = 0
        elif _is_i_main_core_hit(h):
            rank = 1
        elif _is_i_main_watch_hit(h):
            rank = 2
        elif _is_i_main_add_hit(h):
            rank = 3
        elif _is_i_main_confirm_hit(h):
            rank = 4
        else:
            rank = 9
        return (
            rank,
            -_safe_float(h.get('score', 0), 0.0),
            -_safe_float(h.get('i_material_proxy_score', 0), 0.0),
            -_safe_float(h.get('amount_b', 0), 0.0),
            abs(_safe_float(h.get('i_long_ma_dist_pct', 99), 99.0) - 12.0),
        )

    def _i_main_label(h: dict) -> str:
        if _is_i_main_accel_hit(h):
            return '🚀 I-MAIN ACCEL'
        if _is_i_main_core_hit(h):
            return '✅ I-MAIN CORE'
        if _is_i_main_add_hit(h):
            return '➕ I-MAIN ADD'
        if _is_i_main_confirm_hit(h):
            return '🔎 I-MAIN CONFIRM'
        if _is_i_main_watch_hit(h):
            return '🟡 I-MAIN WATCH'
        return '📈 I-MAIN'

    def _i_entry_plan_text(h: dict, label: str) -> str:
        close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
        t10 = close * 1.10 if close > 0 else 0
        t20 = close * 1.20 if close > 0 else 0
        t30 = close * 1.30 if close > 0 else 0
        base = f"목표: +10 {int(t10):,}원 / +20 {int(t20):,}원 / 강하면 +30 {int(t30):,}원."
        invalid = "무효: 박스하단·150/200MA 구조 종가 이탈. 20MA 이탈은 흔들림, 50MA 이탈은 비중축소 검토."
        if 'ACCEL' in label:
            return f"가속형: 이미 장기선 +10~18% 재점화 구간입니다. 추격보다 눌림/5MA 재지지에 소액, 40~60일 시세분출 추적. {base} {invalid}"
        if 'CORE' in label:
            return f"핵심형: I-4 1차 매집 후보입니다. 20~30% 1차 관찰매수, 박스상단/월봉거래량 유지 시 추가. {base} {invalid}"
        if 'ADD' in label:
            return f"추가매수형: 기존 I-4 보유자의 첫 눌림 재지지 후보입니다. 신규비중보다 보유/추가 확인. {base} {invalid}"
        if 'CONFIRM' in label:
            return f"확인형: I-5 돌파는 신규진입보다 I-4 보유자의 보유확인 신호로 우선 해석. {base} {invalid}"
        return f"관찰형: MAIN I-4 전체 후보입니다. 뉴스/월봉/박스 생존을 20/40/60일 누적 추적. {base} {invalid}"

    def _brief_i_main_line(h: dict, idx: int) -> str:
        code = str(h.get('code', '') or '').strip()
        name = _clean_stock_name(code, str(h.get('name', '') or code).strip())
        close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
        score = _safe_float(h.get('score', 0), 0.0)
        phase = _i_phase(h) or 'I-?'
        label = _i_main_label(h)
        anchor_days = _safe_int(h.get('i_anchor_days', 0), 0)
        anchor_amt = _safe_float(h.get('i_anchor_amount_b', 0), 0.0)
        long_dist = _safe_float(h.get('i_long_ma_dist_pct', 0), 0.0)
        material = _safe_float(h.get('i_material_proxy_score', 0), 0.0)
        amount_b = _safe_float(h.get('amount_b', 0), 0.0)
        close_loc = _safe_float(h.get('close_loc_pct', 0), 0.0)
        obv20 = _safe_int(h.get('i_obv20_up', 0), 0)
        amt_rebuild = _safe_int(h.get('i_amount20_rebuild', 0), 0)
        monthly = _i_monthly_rebuild(h)
        tags = []
        if _i_anchor_120_180(h):
            tags.append('기준봉120~180일')
        if monthly:
            tags.append('월봉거래량↑')
        if obv20:
            tags.append('OBV20↑')
        if amt_rebuild:
            tags.append('거래대금재증가')
        if _safe_int(h.get('i_flash_recovery_tag', 0), 0) == 1:
            tags.append('무거래단기급락회복')
        tag_txt = ' / '.join(tags) if tags else '태그없음'
        plan = _i_entry_plan_text(h, label)
        leader_txt = _i_leader_summary_text(h)
        leader_line = f"   👑 주도주 사이클: {leader_txt}\n" if leader_txt else ''
        if _is_i_main_accel_hit(h):
            mental_tip = 'ACCEL 가속형입니다. 단기 몰빵보다 20/40/60일 분할 관점이며, 5MA/20MA 재지지 때만 비중을 늘립니다.'
        elif _is_i_main_core_hit(h):
            mental_tip = 'CORE 안정형입니다. 빠른 단타보다 박스하단·150/200MA 구조 유지 확인이 핵심입니다.'
        else:
            mental_tip = 'WATCH/CONFIRM 관찰형입니다. 신규진입보다 재지지·뉴스·월봉 생존 확인이 먼저입니다.'
        return (
            f"{idx}) {label} | {phase} | {name}({code}) | {int(close):,}원 | 점수 {score:.1f}\n"
            f"   기준봉: {anchor_days}일 전 | 기준봉대금 {anchor_amt:.1f}억 | 장기선이격 {long_dist:.1f}% | 재료/대금 {material:.0f}점 | 현거래대금 {amount_b:.1f}억 | 종가위치 {close_loc:.0f}%\n"
            f"   태그: {tag_txt}\n"
            f"{leader_line}"
            f"   🧠 이 종목 멘탈기준: {mental_tip}\n"
            f"   🌐 시장/섹터: 시장압력 {_live_market_pressure_label(h)} | 섹터압력 {_live_sector_pressure_label(h)}\n"
            f"   📌 {plan}\n   [카드완료]"
        )

    def _build_practical_block():
        # v4.1: 실시간 출력 최종 구조.
        # - SAFE는 최상단 실전 최우선 후보
        # - NEUTRAL은 관찰 후보
        # - RISK는 기본적으로 상세 숨김, 개수/사유만 경고한다
        safe_pool = sorted([h for h in hits if _is_s_core_safe_hit(h)], key=_priority_practical)
        neutral_pool = sorted([h for h in hits if _is_s_core_neutral_hit(h)], key=_priority_practical)
        risk_pool = sorted([h for h in hits if _is_s_core_hit(h) and _s_core_risk_tags(h)], key=_priority_practical)
        # v4.4.9.24: S-SAFE live label pools
        # ST30 통과형도 거래대금별로 MEGA / 3000~5000 / PRIME / WATCH / RISK를 분리한다.
        s_reclaim_mega_pool = [h for h in safe_pool if _s_safe_live_key(h) == 'S_RECLAIM_MEGA']
        s_reclaim_3000_pool = [h for h in safe_pool if _s_safe_live_key(h) == 'S_RECLAIM_3000_5000']
        s_reclaim_prime_pool = [h for h in safe_pool if _s_safe_live_key(h) == 'S_RECLAIM_PRIME']
        s_reclaim_watch_pool = [h for h in safe_pool if _s_safe_live_key(h) == 'S_RECLAIM_WATCH']
        s_reclaim_risk_pool = [h for h in safe_pool if _s_safe_live_key(h) == 'S_RECLAIM_LIQUIDITY_RISK']
        s_reclaim_exec_pool = s_reclaim_mega_pool + s_reclaim_3000_pool + s_reclaim_prime_pool + s_reclaim_watch_pool
        s_momentum_calm_pool = [h for h in safe_pool if _s_safe_live_key(h) == 'S_MOMENTUM_CALM']
        s_momentum_prime_pool = [h for h in safe_pool if _s_safe_live_key(h) == 'S_MOMENTUM_PRIME']
        s_momentum_3000_pool = [h for h in safe_pool if _s_safe_live_key(h) == 'S_MOMENTUM_3000_5000']
        s_momentum_watch_pool = [h for h in safe_pool if _s_safe_live_key(h) == 'S_MOMENTUM_WATCH']
        s_liquidity_risk_pool = [h for h in safe_pool if _s_safe_live_key(h) in ('S_RECLAIM_LIQUIDITY_RISK', 'S_LIQUIDITY_RISK', 'S_LIQUIDITY_EXCLUDE')]

        # v4.1.9: G 모랄레스갭은 S-CORE와 분리한다.
        # G-SAFE는 SAFE 다음 보조 실전 후보, G-NEUTRAL은 관찰, G-AGGRESSIVE는 저유동성/고변동 주의군이다.
        g_all_pool = sorted([h for h in hits if _is_g_hit(h)], key=_priority_practical)
        g_safe_pool = sorted([h for h in g_all_pool if _is_g_safe_hit(h)], key=_priority_practical)
        g_neutral_pool = sorted([h for h in g_all_pool if _is_g_neutral_hit(h)], key=_priority_practical)
        g_aggressive_pool = sorted([h for h in g_all_pool if _is_g_aggressive_hit(h)], key=_priority_practical)

        # v4.2.15: L 대형주 리더갭은 SAFE / TAIL-ABSORB / WATCH로 세분화한다.
        l_all_pool = sorted([h for h in hits if _is_l_hit(h)], key=_priority_practical)
        l_core_pool = sorted([h for h in l_all_pool if _is_l_core_hit(h) and not _is_l_weak_watch_hit(h)], key=_priority_practical)
        l_mega_pool = sorted([h for h in l_all_pool if _is_l_mega_hit(h) and not _is_l_weak_watch_hit(h)], key=_priority_practical)
        l_mega_upper_pool = sorted([h for h in l_all_pool if _is_l_mega_upper_limit_followup(h)], key=_priority_practical)
        l_safe_pool = sorted([h for h in l_all_pool if _is_l_core_safe_hit(h)], key=_priority_practical)
        l_tail_pool = sorted([h for h in l_all_pool if _is_l_tail_absorb_hit(h)], key=_priority_practical)
        l_watch_pool = sorted([h for h in l_all_pool if _is_l_watch_hit(h)], key=_priority_practical)

        # v4.2.9: H 신고가거자름은 기존 S/G와 분리해 신규검증 후보로 표시한다.
        h_all_pool = sorted([h for h in hits if _is_h_hit(h)], key=_priority_practical)
        h_triangle_pool = sorted([h for h in h_all_pool if _is_h_triangle_hit(h)], key=_priority_practical)
        h_core_500_pool = sorted([h for h in h_all_pool if _is_h_core_500_1000_vol23(h)], key=_priority_practical)
        h_core_300_pool = sorted([h for h in h_all_pool if _is_h_core_300_500_vol35(h)], key=_priority_practical)
        h_core_1000_pool = sorted([h for h in h_all_pool if _is_h_core_1000_2000_vol23(h)], key=_priority_practical)
        h_core_union_pool = sorted([h for h in h_all_pool if _is_h_core_union_hit(h)], key=_priority_practical)
        h_fast_pool = sorted([h for h in h_all_pool if _is_h_fast_hit(h)], key=_priority_practical)
        h_fast_prime_pool = sorted([h for h in h_fast_pool if _h_fast_live_key(h) == 'H_FAST_PRIME'], key=_priority_practical)
        h_fast_watch_pool = sorted([h for h in h_fast_pool if _h_fast_live_key(h) in ('H_FAST_WATCH', 'H_FAST_RISK')], key=_priority_practical)
        h_overheat_pool = sorted([h for h in h_all_pool if _is_h_overheat_hit(h)], key=_priority_practical)
        h_other_pool = sorted([h for h in h_all_pool if not (_is_h_core_union_hit(h) or _is_h_fast_hit(h) or _is_h_overheat_hit(h))], key=_priority_practical)

        # v4.3.6: I-MAIN은 중기 누적관찰 후보로 S/L/G/H 단기 후보 뒤에 별도 표시한다.
        i_all_pool = sorted([h for h in hits_i if _is_i_main_hit(h)], key=_priority_i_main)
        i_main_pool = sorted([h for h in i_all_pool if _is_i_main_candidate(h)], key=_priority_i_main)
        i_accel_pool = sorted([h for h in i_main_pool if _is_i_main_accel_hit(h)], key=_priority_i_main)
        i_core_pool = sorted([h for h in i_main_pool if _is_i_main_core_hit(h)], key=_priority_i_main)
        i_watch_pool = sorted([h for h in i_main_pool if _is_i_main_watch_hit(h) and not (_is_i_main_core_hit(h) or _is_i_main_accel_hit(h))], key=_priority_i_main)
        i_add_pool = sorted([h for h in i_main_pool if _is_i_main_add_hit(h)], key=_priority_i_main)
        i_confirm_pool = sorted([h for h in i_main_pool if _is_i_main_confirm_hit(h)], key=_priority_i_main)

        # v4.4.8 신규검증 검색식 — 기존 L/S/I 검색식은 그대로 두고 별도 섹션에만 표시한다.
        lp_pool = sorted([h for h in hits_lp if _pick_strategy(h) == 'LP'], key=_priority_practical)
        lp_safe_pool = sorted([h for h in lp_pool if _is_lp_safe_hit(h)], key=_priority_practical)
        lp_explosion_pool = sorted([h for h in lp_safe_pool if _is_lp_explosion_watch(h)], key=_priority_lp_explosion)
        lp_stable_pool = sorted([h for h in lp_safe_pool if not _is_lp_explosion_watch(h)], key=_priority_practical)
        lp_watch_pool = sorted([h for h in lp_pool if _is_lp_watch_hit(h)], key=_priority_practical)
        lp_risk_pool = sorted([h for h in lp_pool if _is_lp_risk_hit(h)], key=_priority_practical)
        slock_pool = sorted([h for h in hits_slock if _pick_strategy(h) == 'SLOCK'], key=_priority_practical)
        it_pool = sorted([h for h in hits_it if _pick_strategy(h) == 'IT'], key=_priority_i_main)
        it_accel_live_pool = sorted([h for h in it_pool if _is_i_main_accel_hit(h)], key=_priority_i_main)

        # v4.1.1: A 보조돌파는 S-CORE가 아니므로 SAFE/NEUTRAL로 세지 않는다.
        # RISK 조건에 걸린 A 후보는 기본 숨김 처리하고, 요청 시에만 상세 출력한다.
        a_all_pool = sorted([h for h in hits if _is_a_strong(h)], key=_priority_practical)
        a_confirm_pool = [h for h in a_all_pool if _is_a_confirm_live(h) and not _trade_risk_tags({**h, 'close_loc_pct': _a_live_close_loc(h)})]
        a_core_pool = [h for h in a_all_pool if _is_a_retest_core2_live(h) and not _is_a_confirm_live(h)]
        a_pool = [h for h in a_all_pool if (not _trade_risk_tags({**h, 'close_loc_pct': _a_live_close_loc(h)})) and not _is_a_retest_core2_live(h) and not _is_a_confirm_live(h)]
        a_risk_pool = [h for h in a_all_pool if _trade_risk_tags({**h, 'close_loc_pct': _a_live_close_loc(h)}) or _safe_float(h.get('rr', 0), 0.0) > 1.8]
        a_confirm_vc_safe_n = sum(1 for h in a_confirm_pool if _is_a_confirm_vc_safe(h))
        a_confirm_vc_watch_n = sum(1 for h in a_confirm_pool if _is_a_confirm_vc_watch(h))
        a_output_n = min(len(a_pool), PRACTICAL_A_TOP_N)

        c_pullback_pool = sorted(hits_c_pullback_reentry, key=_priority_practical) if PRACTICAL_SHOW_C_DIAG else []
        c_swing_pool = sorted(hits_c_swing_strict, key=_priority_practical) if PRACTICAL_SHOW_C_DIAG else []
        c_pullback_output_n = min(len(c_pullback_pool), PRACTICAL_C_PULLBACK_TOP_N)
        c_swing_output_n = min(len(c_swing_pool), PRACTICAL_C_SWING_TOP_N)

        # v4.4.9.13: 실제 FAST 실전 출력 후보만 별도 CSV로 추적한다.
        try:
            if CLOSING_BET_LIVE_OPERATION_GUARD and CLOSING_BET_LIVE_TRACKING_LOG:
                now_dt = _now_kst()
                scan_date = now_dt.strftime('%Y-%m-%d')
                scan_time = now_dt.strftime('%H:%M:%S')
                live_rows = []
                used_live_codes = set()
                def _add_live_pool(pool, pattern_label: str, priority: int, limit: int = 5):
                    local = 0
                    for hh in pool:
                        code = str(hh.get('code', '') or '').zfill(6)
                        key = f"{scan_date}|{code}|{pattern_label}"
                        if key in used_live_codes:
                            continue
                        used_live_codes.add(key)
                        local += 1
                        close = _safe_float(hh.get('close', hh.get('_close', 0)), 0.0)
                        live_rows.append({
                            'key': key,
                            'scan_date': scan_date,
                            'scan_time': scan_time,
                            'priority': priority,
                            'code': code,
                            'name': hh.get('name', ''),
                            'pattern': pattern_label,
                            'mode': hh.get('mode', ''),
                            'vc_label': _a_vc_badge(hh) if _pick_strategy(hh) == 'A' else '',
                            'entry_basis': _pattern_entry_tip(hh, _pick_strategy(hh)),
                            'close_entry': close,
                            'target_plus3': round(close * 1.03, 2) if close > 0 else np.nan,
                            'target_plus5': round(close * 1.05, 2) if close > 0 else np.nan,
                            'stoploss': _safe_float(hh.get('stoploss', 0), 0.0),
                            'rr': _safe_float(hh.get('rr', 0), 0.0),
                            'amount_b': _safe_float(hh.get('amount_b', 0), 0.0),
                            'vol_ratio': _safe_float(hh.get('vol_ratio', hh.get('today_vol_ratio', 0)), 0.0),
                            'close_loc_pct': _safe_float(hh.get('close_loc_pct', _a_live_close_loc(hh) if _pick_strategy(hh) == 'A' else 0), 0.0),
                            'market_pressure': _live_market_pressure_label(hh),
                            'sector_pressure': _live_sector_pressure_label(hh),
                            'status': 'pending',
                            'next_open_ret': np.nan,
                            'next_close_ret': np.nan,
                            'd3_high_ret': np.nan,
                            'd5_high_ret': np.nan,
                            'hit_plus3_first': np.nan,
                            'hit_plus5_first': np.nan,
                            'stop_first': np.nan,
                        })
                        if local >= limit:
                            break
                _add_live_pool([h for h in lp_stable_pool if _lp_timing_bucket(h)[0] in ('LP-D23', 'LP-D45')], 'LP-D23/D45', 1, PRACTICAL_L_PULLBACK_TOP_N)
                _add_live_pool(lp_explosion_pool, 'LP-POWER PRIME', 2, PRACTICAL_L_PULLBACK_TOP_N)
                _add_live_pool(lp_safe_pool, 'LP-SAFE', 3, PRACTICAL_L_PULLBACK_TOP_N)
                _add_live_pool(l_mega_upper_pool, 'L-MEGA 상한가후속', 3, PRACTICAL_L_CORE_TOP_N)
                _add_live_pool(l_safe_pool + l_mega_pool + l_tail_pool, 'L-5000억+', 3, PRACTICAL_L_CORE_TOP_N)
                _add_live_pool([h for h in a_confirm_pool if _is_a_confirm_vc_watch(h)], 'A-CONFIRM PRIME', 4, PRACTICAL_A_TOP_N)
                _add_live_pool(s_reclaim_exec_pool + s_momentum_calm_pool + s_momentum_prime_pool + s_momentum_3000_pool + s_momentum_watch_pool + neutral_pool, 'S-RECLAIM/MOMENTUM/NEUTRAL', 5, PRACTICAL_SAFE_TOP_N)
                _add_live_pool([h for h in a_confirm_pool if _is_a_confirm_vc_safe(h)], 'A-CONFIRM CALM', 6, PRACTICAL_A_TOP_N)
                _add_live_pool(a_core_pool, 'A-RETEST CORE②', 7, PRACTICAL_A_TOP_N)
                _add_live_pool(h_fast_prime_pool, 'H-FAST PRIME +3전용', 8, PRACTICAL_H_FAST_TOP_N)
                _add_live_pool(it_accel_live_pool + i_accel_pool, 'IT/I-MAIN ACCEL', 9, PRACTICAL_I_MAIN_ACCEL_TOP_N)
                _save_live_operation_guard_rows(live_rows)
        except Exception as _live_log_e:
            try:
                log_error(f"⚠️ v4.4.9.13 실전 추적로그 구성 실패: {_live_log_e}")
            except Exception:
                pass

        # v4.4.9.24: 2차 스캔 전용 — 동시호가 직전 실행/포기 압축 최종킥
        def _final_kick_decision(h: dict, pattern_label: str) -> tuple[str, str, list]:
            mode = _pick_strategy(h)
            close_loc = _safe_float(h.get('close_loc_pct', _a_live_close_loc(h) if mode == 'A' else 0), 0.0)
            volr = _safe_float(h.get('vol_ratio', h.get('today_vol_ratio', 0)), 0.0)
            amount_b = _safe_float(h.get('amount_b', 0), 0.0)
            risk_tags = _s_core_risk_tags(h) if mode == 'S' else (_trade_risk_tags({**h, 'close_loc_pct': close_loc}) if mode == 'A' else [])
            reasons = []
            if pattern_label:
                reasons.append(pattern_label)
            if close_loc > 0:
                reasons.append(f'종가위치 {close_loc:.0f}%')
            if amount_b > 0:
                reasons.append(f'거래대금 {amount_b:.0f}억')
            if volr > 0:
                reasons.append(f'거래량비 {volr:.2f}')
            if mode == 'LP':
                tb, _ = _lp_timing_bucket(h)
                reasons.append(tb)
                lp_vs_gap = _safe_float(h.get('lp_volume_vs_gap', 0), 0.0)
                if lp_vs_gap > 0:
                    reasons.append(f'거래량식힘 {lp_vs_gap:.2f}배')
                exp_ctx = _lp_explosion_watch_context(h)
                if exp_ctx.get('is_explosion'):
                    reasons.append('🔥LP-POWER PRIME')
            if mode == 'A' and _is_a_confirm_live(h):
                reasons.append(_a_vc_badge(h).replace('🔥 ', '').replace('🟢 ', '').replace('🟡 ', ''))
            if mode in ('IT', 'I'):
                lreason = _i_leader_final_reason(h)
                if lreason:
                    reasons.append(lreason)
            if risk_tags:
                reasons.append('위험태그 ' + '/'.join(risk_tags))

            # v4.4.9.43: L-MEGA 상한가성 잠금형은 과열 때문에 종가매수는 금지하지만,
            # 강한 후보 자체는 사라진 것이 아니므로 보유자 대응/다음날 후속관찰로 별도 표시한다.
            if mode == 'L' and _is_l_mega_upper_limit_followup(h):
                reasons.append('💰L-MEGA 상한가후속')
                return '🟡 지정가만 가능', 'L-MEGA 상한가성 잠금형: 신규 종가추격 금지. 보유자 +3/+5 분할익절, 신규자는 다음날 전일 종가 기준선 지지·첫 눌림/VWAP 재돌파만 확인', reasons

            hard_fail = bool(risk_tags) or (close_loc > 0 and close_loc < 55) or volr >= 2.50
            if hard_fail:
                return '❌ 오늘 포기', '지지/종가위치/거래량 조건이 약해 동시호가 직전 신규진입 제외', reasons

            # L/G 당일 갭형은 강해도 직장인 모드에서는 종가 추격보다 다음날 지정가/눌림 우선.
            if mode in ('L', 'G'):
                return '🟡 지정가만 가능', '당일 갭형은 강하지만 추격보다 전일종가·갭하단 지지 확인이 우선', reasons
            if mode == 'LP':
                tb, _ = _lp_timing_bucket(h)
                exp_ctx = _lp_explosion_watch_context(h)
                if exp_ctx.get('is_explosion'):
                    # 폭발 WATCH는 종가 메인 진입보다 다음날 갭하단/전일고가 확인 1순위로 표시한다.
                    return '🟡 지정가만 가능', f"LP-POWER PRIME: 강력추천 후보지만 종가 시장가 추격은 금지. 지정가·다음날 첫 눌림 재돌파 1순위({exp_ctx.get('reason','')})", reasons
                if close_loc >= 70 and volr <= 1.80 and tb in ('LP-D23', 'LP-D45'):
                    return '✅ 종가진입 가능', 'LP 눌림재지지가 유지되고 종가위치가 양호함', reasons
                return '🟡 지정가만 가능', 'LP 구조는 살아있지만 종가 추격보다 눌림 지정가 우선', reasons
            if mode == 'A':
                if _is_a_confirm_live(h) and close_loc >= 70 and volr <= 1.80:
                    return '✅ 종가진입 가능', 'A-CONFIRM 확인형이 유지되어 소액 종가/회복가 대응 가능', reasons
                return '🟡 지정가만 가능', 'A는 확인형만 가능하며 추격보다 전일고가 회복가/눌림 재지지 우선', reasons
            if mode == 'H':
                if _is_h_fast_hit(h):
                    hkey = _h_fast_live_key(h)
                    hreason = _h_fast_live_class(h).get('reason', '')
                    if hkey == 'H_FAST_PRIME':
                        return '🟡 지정가만 가능', f'H-FAST PRIME: {hreason}. 보유 금지, +3 자동익절 우선', reasons
                    return '❌ 오늘 포기', f'{hreason}. FINAL_KICK에서는 PRIME 외 H-FAST 추격 제외', reasons
                if _is_h_core_union_hit(h) or _is_h_triangle_hit(h):
                    return '🟡 지정가만 가능', 'H-CORE는 승격 전 WATCH. 전일고가/돌파권 재지지 확인 후 지정가만 가능', reasons
                return '❌ 오늘 포기', '넓은 H는 과열·추격 위험이 커서 최종킥 신규진입 제외', reasons
            if mode == 'S':
                if _is_s_core_safe_hit(h):
                    skey = _s_safe_live_key(h)
                    sdesc = _s_safe_live_class(h).get('reason', '')
                    if skey in ('S_RECLAIM_MEGA', 'S_RECLAIM_3000_5000', 'S_RECLAIM_PRIME', 'S_RECLAIM_WATCH', 'S_RECLAIM_LIQUIDITY_RISK'):
                        if skey == 'S_RECLAIM_MEGA' and close_loc >= 70 and volr <= 1.50:
                            return '✅ 종가진입 가능', 'S-RECLAIM MEGA: ST30 통과 + 5000억+ 고유동성. 보유 가능 후보지만 +3 자동익절 우선', reasons
                        if skey == 'S_RECLAIM_3000_5000' and close_loc >= 70 and volr <= 1.50:
                            return '🟡 지정가만 가능', 'S-RECLAIM 3000~5000: ST30 통과 준대형 수급. 표본 공백 구간이라 소액 지정가/+3 단타만', reasons
                        if skey == 'S_RECLAIM_PRIME' and close_loc >= 70 and volr <= 1.50:
                            return '🟡 지정가만 가능', 'S-RECLAIM PRIME: ST30 통과 + 1000억+. 종가 추격보다 지정가/+3 단타 우선', reasons
                        if skey == 'S_RECLAIM_WATCH':
                            return '🟡 지정가만 가능', 'S-RECLAIM WATCH: ST30 통과지만 300~1000억. 보유보다 +3 빠른익절 전용', reasons
                        return '❌ 오늘 포기', 'S-RECLAIM이라도 거래대금 300억 미만 또는 품질 미달은 유동성 확인 전 신규진입 보류', reasons
                    if skey in ('S_MOMENTUM_PRIME', 'S_MOMENTUM_3000_5000', 'S_MOMENTUM_WATCH', 'S_MOMENTUM_CALM'):
                        return '🟡 지정가만 가능', f'{sdesc} — ST30 미통과형 PRIME도 추격보다 지정가/다음날 확인 우선', reasons
                    if skey in ('S_RECLAIM_LIQUIDITY_RISK', 'S_LIQUIDITY_RISK', 'S_LIQUIDITY_EXCLUDE'):
                        return '❌ 오늘 포기', f'{sdesc} — 저유동성 S-SAFE는 오늘 신규진입 보류', reasons
                if close_loc >= 70 and volr <= 1.50:
                    return '✅ 종가진입 가능', 'S 응축형이 거래량 과열 없이 상단을 유지함', reasons
                return '🟡 지정가만 가능', 'S 구조는 있으나 종가 추격보다 눌림 지정가 우선', reasons
            if mode in ('IT', 'I'):
                return '🟡 지정가만 가능', '중기 후보는 종가 몰빵보다 20~30% 분할/재지지 확인형', reasons
            return '🟡 지정가만 가능', '패턴은 살아있지만 최종킥에서는 지정가·소액만 허용', reasons

        def _final_kick_entry_line(h: dict) -> tuple[str, str, str, str]:
            close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
            if close <= 0:
                return '확인 필요', '확인 필요', '확인 필요', '확인 필요'
            mode = _pick_strategy(h)
            if mode == 'L' and _is_l_mega_upper_limit_followup(h):
                t3 = close * 1.03
                t5 = close * 1.05
                stop = _safe_float(h.get('stoploss', 0), 0.0)
                stop_text = f'1차 무효 {int(close):,}원 회복 실패 / 구조무효 {int(stop):,}원 이탈' if stop > 0 else f'1차 무효 {int(close):,}원 회복 실패 / 구조무효 갭하단 이탈'
                return (
                    f'오늘 신규매수 없음 — 내일 {int(close):,}원 기준선 지지 후 첫 눌림/VWAP 재돌파만 20~30% 이하',
                    '+5% 이상 갭상승·시초 급등 추격금지',
                    f'+3% {int(t3):,}원 / +5% {int(t5):,}원',
                    stop_text,
                )
            entry_low = close * 0.99
            entry_high = close
            if mode == 'LP':
                # LP는 눌림재지지형이므로 종가 아래 1%~종가까지만. 미체결은 포기.
                entry_low = close * 0.99
                entry_high = close
            elif mode in ('L', 'G'):
                # 당일 갭형은 추격금지. 전일종가/갭하단 확인이 가장 좋지만, 직장인용으로 종가 아래 지정가만 제시.
                entry_low = close * 0.98
                entry_high = close * 0.995
            elif mode == 'S':
                entry_low = close * 0.99
                entry_high = close
            elif mode == 'A':
                pull_low = _safe_float(h.get('a_pullback_low', 0), 0.0)
                pull_high = _safe_float(h.get('a_pullback_high', 0), 0.0)
                if pull_low > 0 and pull_high > 0:
                    entry_low, entry_high = pull_low, min(pull_high, close)
                else:
                    entry_low, entry_high = close * 0.99, close
            elif mode == 'H':
                entry_low, entry_high = close * 0.98, close * 0.995
            chase = close * 1.02
            t3 = close * 1.03
            t5 = close * 1.05
            stop = _safe_float(h.get('stoploss', 0), 0.0)
            if mode == 'LP':
                gap_low = _safe_float(h.get('lp_gap_low', 0), 0.0)
                prev_close = _safe_float(h.get('lp_prev_close', 0), 0.0)
                if gap_low > 0 or prev_close > 0:
                    stop_text = ' / '.join([x for x in [f'갭하단 {int(gap_low):,}원' if gap_low > 0 else '', f'갭전종가 {int(prev_close):,}원' if prev_close > 0 else ''] if x]) + ' 이탈'
                else:
                    stop_text = f'{int(stop):,}원 이탈' if stop > 0 else '전일종가·5/10일선 동시 이탈'
            else:
                stop_text = f'{int(stop):,}원 이탈' if stop > 0 else '전일종가·지지선 이탈'
            return (
                f'{int(entry_low):,}~{int(entry_high):,}원 이하 지정가',
                f'{int(chase):,}원 이상 추격금지',
                f'+3% {int(t3):,}원 / +5% {int(t5):,}원',
                stop_text,
            )

        def _final_kick_execution_style(h: dict, decision: str) -> str:
            """v4.4.9.24: 후보를 보유 가능 / +3 단타 / 제외로 즉시 읽히게 만든다."""
            if decision.startswith('❌'):
                return '❌ 제외: 오늘 신규매수 금지'
            mode = _pick_strategy(h)
            amount_b = _safe_float(h.get('amount_b', 0), 0.0)
            if mode == 'S' and _is_s_core_safe_hit(h):
                skey = _s_safe_live_key(h)
                if skey == 'S_RECLAIM_MEGA':
                    return '📌 보유 가능: ST30 통과+5000억+, +3 익절 후 +5 일부 연장'
                if skey in ('S_RECLAIM_3000_5000', 'S_RECLAIM_PRIME'):
                    return '⚡ +3 단타 우선: ST30 통과지만 표본/대금별 비중 낮춤'
                if skey == 'S_RECLAIM_WATCH':
                    return '⚡ +3 단타 전용: ST30 통과 저유동성, 오래 보유 금지'
                if skey == 'S_RECLAIM_LIQUIDITY_RISK':
                    return '❌ 제외: ST30 통과라도 300억 미만은 유동성 부족'
                if skey == 'S_MOMENTUM_CALM':
                    return '⚡ +3 단타/다음날 확인: ST30 미통과지만 1000억+ 거래량냉각형'
                if skey == 'S_MOMENTUM_PRIME':
                    return '⚡ +3 단타/다음날 확인: ST30 미통과지만 1000억+ 추세지속형'
                if skey in ('S_MOMENTUM_3000_5000', 'S_MOMENTUM_WATCH'):
                    return '⚡ +3 단타 우선: ST30 미통과형은 확인 전 보유 금지'
                if skey in ('S_LIQUIDITY_RISK', 'S_LIQUIDITY_EXCLUDE'):
                    return '❌ 제외: 저유동성 S-SAFE 위험'
            if mode == 'LP':
                tb, _ = _lp_timing_bucket(h)
                if _is_lp_explosion_watch(h):
                    return '🔥 강력추천 지정가: 후보 강도는 높지만 종가 추격 금지, 지정가·다음날 재돌파 확인'
                return '📌 보유 가능' if tb in ('LP-D23', 'LP-D45') and not decision.startswith('🟡') else '⚡ +3 단타 우선'
            if mode == 'L' and _is_l_mega_upper_limit_followup(h):
                return '💰 후속관찰: 오늘 신규매수 없음, 보유자 대응/내일 기준선 확인'
            if mode in ('L', 'G'):
                return '⚡ +3 단타/다음날 확인: 당일 갭 추격 금지'
            if mode == 'A':
                return '⚡ +3 단타/확인형: 전일고가 회복·눌림 재지지 기준'
            if mode == 'H':
                if _is_h_fast_hit(h):
                    if _h_fast_live_key(h) == 'H_FAST_PRIME':
                        return '🔥 +3 초단타 전용: H-FAST PRIME, 보유/추격 금지'
                    return '❌ 제외: H-FAST WATCH/RISK는 PRIME 확인 전 매수 금지'
                return '🟡 H-CORE WATCH: 지정가·다음날 돌파권 지지 확인'
            if mode in ('IT', 'I'):
                return '📌 중기 분할관찰: 종가 몰빵 금지'
            return '⚡ +3 단타 우선' if amount_b >= 1000 else '🟡 소액 관찰'

        def _build_final_kick_block() -> str:
            max_cards = max(1, CLOSING_BET_MAX_FINAL_KICK_CARDS)
            candidates = []
            used = set()
            def _push(pool, pattern_label: str, limit: int = 3):
                local = 0
                for hh in pool:
                    code = str(hh.get('code', '') or '').zfill(6)
                    if not code or code in used:
                        continue
                    used.add(code)
                    candidates.append((hh, pattern_label))
                    local += 1
                    if local >= limit:
                        break

            # v4.4.9.29: 최종킥 원픽은 단기 종가배팅과 중기 분할관찰을 분리한다.
            # 후보 수집 순서는 실전 운용순서를 유지하되, 최종 표시 전 실행판정 우선으로 재정렬한다.
            _push([h for h in lp_stable_pool if _lp_timing_bucket(h)[0] in ('LP-D23', 'LP-D45')], 'LP-D23/D45', PRACTICAL_L_PULLBACK_TOP_N)
            _push(lp_explosion_pool, 'LP-POWER PRIME', PRACTICAL_L_PULLBACK_TOP_N)
            _push(l_mega_upper_pool, 'L-MEGA 상한가후속', PRACTICAL_L_CORE_TOP_N)
            _push(l_safe_pool + l_mega_pool + l_tail_pool, 'L 5000억+', PRACTICAL_L_CORE_TOP_N)
            _push(s_reclaim_exec_pool + s_momentum_calm_pool + s_momentum_prime_pool + s_momentum_3000_pool + s_momentum_watch_pool + neutral_pool, 'S-RECLAIM/MOMENTUM/NEUTRAL', PRACTICAL_SAFE_TOP_N)
            _push([h for h in a_confirm_pool if _is_a_confirm_vc_watch(h)], 'A-CONFIRM PRIME', PRACTICAL_A_TOP_N)
            _push([h for h in a_confirm_pool if _is_a_confirm_vc_safe(h)], 'A-CONFIRM CALM', PRACTICAL_A_TOP_N)
            _push(g_safe_pool, 'G-SAFE', PRACTICAL_G_SAFE_TOP_N)
            _push(a_core_pool, 'A-RETEST CORE②', PRACTICAL_A_TOP_N)
            _push(h_fast_prime_pool, 'H-FAST PRIME +3전용', PRACTICAL_H_FAST_TOP_N)
            _push(it_accel_live_pool + i_accel_pool, 'IT/I-MAIN ACCEL', PRACTICAL_I_MAIN_ACCEL_TOP_N)

            rows = []
            decision_counts = {'✅ 종가진입 가능': 0, '🟡 지정가만 가능': 0, '❌ 오늘 포기': 0}
            evaluated = []
            for seq, (hh, label) in enumerate(candidates):
                decision, why, reasons = _final_kick_decision(hh, label)
                decision_counts[decision] = decision_counts.get(decision, 0) + 1
                evaluated.append((seq, hh, label, decision, why, reasons))

            # v4.4.9.30: LP 메인 후보가 있는 날에는 S 응축형을 같은 ✅ 메인으로 보이지 않게 낮춘다.
            # S는 좋은 후보라도 LP보다 우선순위가 낮으므로 보조 소액/지정가 후보로 정렬·표시한다.
            has_lp_main = any(
                _pick_strategy(row[1]) == 'LP' and str(row[3]).startswith('✅')
                for row in evaluated
            )
            if has_lp_main:
                adjusted = []
                for seq, hh, label, decision, why, reasons in evaluated:
                    if _pick_strategy(hh) == 'S' and str(decision).startswith('✅'):
                        decision = '🟡 지정가만 가능'
                        why = f'{why} / LP 메인 후보가 있어 S는 보조 소액·지정가로 운용'
                        reasons = list(reasons) + ['LP메인일 S보조']
                    adjusted.append((seq, hh, label, decision, why, reasons))
                evaluated = adjusted

            def _final_kick_pick_sort_key(row):
                # v4.4.9.29: 단기/중기 분리 후 각 그룹 안에서 실제 실행판정 1등을 고른다.
                seq, hh, label, decision, why, reasons = row
                mode = _pick_strategy(hh)
                close = _safe_float(hh.get('close', hh.get('_close', 0)), 0.0)
                amount_b = _safe_float(hh.get('amount_b', 0), 0.0)
                close_loc = _safe_float(hh.get('close_loc_pct', _a_live_close_loc(hh) if mode == 'A' else 0), 0.0)
                stop = _safe_float(hh.get('stoploss', 0), 0.0)
                stop_dist = ((close - stop) / close * 100.0) if close > 0 and stop > 0 and stop < close else 999.0

                decision_rank = 0 if str(decision).startswith('✅') else (1 if str(decision).startswith('🟡') else 9)

                pattern_rank = 50
                if mode == 'LP':
                    tb, _ = _lp_timing_bucket(hh)
                    # 안정형 원픽과 폭발형 WATCH를 분리하되, 같은 판정 안에서는 안정형을 먼저 둔다.
                    if _is_lp_explosion_watch(hh):
                        pattern_rank = {'LP-D23': 1, 'LP-D45': 2, 'LP-D1': 3}.get(tb, 4)
                    else:
                        pattern_rank = {'LP-D23': 0, 'LP-D45': 1, 'LP-D1': 2}.get(tb, 3)
                elif mode == 'L':
                    pattern_rank = 4
                elif mode == 'S':
                    skey = _s_safe_live_key(hh) if _is_s_core_safe_hit(hh) else ''
                    s_rank = {
                        'S_RECLAIM_MEGA': 5, 'S_RECLAIM_3000_5000': 6, 'S_RECLAIM_PRIME': 7,
                        'S_MOMENTUM_CALM': 8, 'S_MOMENTUM_PRIME': 9, 'S_RECLAIM_WATCH': 10, 'S_MOMENTUM_3000_5000': 11,
                        'S_MOMENTUM_WATCH': 12,
                        'S_LIQUIDITY_RISK': 90, 'S_LIQUIDITY_EXCLUDE': 95,
                    }
                    pattern_rank = s_rank.get(skey, 13)
                elif mode == 'A':
                    pattern_rank = 14 if _is_a_confirm_live(hh) else 16
                elif mode == 'G':
                    pattern_rank = 15
                elif mode == 'H':
                    pattern_rank = 17 if _is_h_fast_hit(hh) and _h_fast_live_key(hh) == 'H_FAST_PRIME' else 20
                elif mode in ('IT', 'I'):
                    pattern_rank = 18

                close_rank = 0 if close_loc >= 70 else (1 if close_loc >= 60 else 2)
                return (
                    decision_rank,          # ✅ 종가진입 가능이 🟡 지정가보다 항상 우선
                    pattern_rank,           # 같은 판정이면 운용 우선순위
                    close_rank,             # 종가위치 70%+ 우선
                    stop_dist,              # 무효가가 가까운 후보 우선
                    -amount_b,              # 마지막에 거래대금/대표성
                    seq,                    # 완전 동률이면 기존 수집순서
                )

            evaluated.sort(key=_final_kick_pick_sort_key)

            def _is_midterm_final_row(row) -> bool:
                # IT/I-MAIN은 단기 종가배팅 원픽에서 제외하고, 중기 분할관찰 원픽으로만 표시한다.
                try:
                    return _pick_strategy(row[1]) in ('IT', 'I')
                except Exception:
                    return False

            short_evaluated = [row for row in evaluated if not _is_midterm_final_row(row)]
            mid_evaluated = [row for row in evaluated if _is_midterm_final_row(row)]

            picked_short = [(hh, label, decision, why, reasons) for _, hh, label, decision, why, reasons in short_evaluated if decision != '❌ 오늘 포기'][:max_cards]
            picked_mid = [(hh, label, decision, why, reasons) for _, hh, label, decision, why, reasons in mid_evaluated if decision != '❌ 오늘 포기'][:max_cards]
            # 표시 카드는 단기 후보를 먼저 채우고, 남는 자리에만 중기 분할관찰 후보를 보여준다.
            picked = (picked_short + picked_mid)[:max_cards]

            short_decision_counts = {'✅ 종가진입 가능': 0, '🟡 지정가만 가능': 0, '❌ 오늘 포기': 0}
            mid_decision_counts = {'✅ 종가진입 가능': 0, '🟡 지정가만 가능': 0, '❌ 오늘 포기': 0}
            for row in short_evaluated:
                d = row[3]
                short_decision_counts[d] = short_decision_counts.get(d, 0) + 1
            for row in mid_evaluated:
                d = row[3]
                mid_decision_counts[d] = mid_decision_counts.get(d, 0) + 1

            has_lp_entry_short = any(
                _pick_strategy(row[1]) == 'LP' and str(row[3]).startswith('✅')
                for row in short_evaluated
            )
            if not picked_short:
                op = '금지'
            elif has_lp_entry_short:
                op = '적극'
            elif short_decision_counts.get('✅ 종가진입 가능', 0) >= 1:
                op = '보통'
            else:
                op = '지정가만'
            try:
                if picked_short and all((_pick_strategy(r[0]) == 'L' and _is_l_mega_upper_limit_followup(r[0])) for r in picked_short):
                    op = '신규매수 없음/내일 후속관찰'
            except Exception:
                pass

            def _final_kick_display_decision(hh: dict, decision: str) -> str:
                # v4.4.9.39: 추천강도와 진입방식을 분리한다. LP-POWER PRIME은 보조가 아니라 강력추천 지정가다.
                try:
                    if _pick_strategy(hh) == 'LP' and _is_lp_explosion_watch(hh) and str(decision).startswith('🟡'):
                        return '🔥 강력추천 지정가'
                    if _pick_strategy(hh) == 'L' and _is_l_mega_upper_limit_followup(hh) and str(decision).startswith('🟡'):
                        return '💰 후속관찰'
                except Exception:
                    pass
                return decision

            def _final_kick_top_line(picked_rows, none_text='없음'):
                if not picked_rows:
                    return none_text
                top_h, top_label, top_decision, _, _ = picked_rows[0]
                top_code = str(top_h.get('code', '') or '')
                top_name = _clean_stock_name(top_code, str(top_h.get('name', '') or top_code))
                disp_decision = _final_kick_display_decision(top_h, top_decision)
                return f'{top_name} — {disp_decision} | {top_label}'

            # v4.4.9.32: 최종킥 상단은 유지하고 I-LEADER 주도주 라이프사이클 라벨을 중기 후보에 붙인다.
            # 한 종목만 할 때는 단기 메인만 보고, S·L 보조 후보는 소액/지정가로만 읽히게 한다.
            lp_power_prime_picked = [(hh, label, decision, why, reasons) for hh, label, decision, why, reasons in picked_short if _pick_strategy(hh) == 'LP' and _is_lp_explosion_watch(hh)]
            l_mega_followup_picked = [(hh, label, decision, why, reasons) for hh, label, decision, why, reasons in picked_short if _pick_strategy(hh) == 'L' and _is_l_mega_upper_limit_followup(hh)]
            actionable_short_picked = [(hh, label, decision, why, reasons) for hh, label, decision, why, reasons in picked_short if not ((_pick_strategy(hh) == 'LP' and _is_lp_explosion_watch(hh)) or (_pick_strategy(hh) == 'L' and _is_l_mega_upper_limit_followup(hh)))]
            main_short_line = _final_kick_top_line(actionable_short_picked[:1], '없음')
            aux_short_line = _final_kick_top_line(actionable_short_picked[1:], '없음')
            explosion_short_line = _final_kick_top_line(lp_power_prime_picked, '없음')
            followup_short_line = _final_kick_top_line(l_mega_followup_picked, '없음')
            mid_top_line = _final_kick_top_line(picked_mid, '없음')
            s_label_line = (
                f"S라벨: 💎RE-MEGA {len(s_reclaim_mega_pool)} | 🟢RE3000~5000 {len(s_reclaim_3000_pool)} | "
                f"🟢RE-PRIME {len(s_reclaim_prime_pool)} | 🟡RE-WATCH {len(s_reclaim_watch_pool)} | "
                f"🟢CALM1000+ {len(s_momentum_calm_pool)} | 🔥MOM1000+ {len(s_momentum_prime_pool)} | 🟡MOM3000~5000 {len(s_momentum_3000_pool)} | "
                f"🟡MOM-WATCH {len(s_momentum_watch_pool)} | 🟡NEUTRAL {len(neutral_pool)} | ⚠️RISK {len(s_liquidity_risk_pool)}"
            )
            s_exec_count = len(s_reclaim_exec_pool) + len(s_momentum_calm_pool) + len(s_momentum_prime_pool) + len(s_momentum_3000_pool) + len(s_momentum_watch_pool)
            s_watch_count = len(neutral_pool)
            core_line = (
                f"핵심후보: LP {len(lp_safe_pool)} / POWER PRIME {len(lp_explosion_pool)} | L {len(l_safe_pool) + len(l_mega_pool) + len(l_tail_pool)} / 상한후속 {len(l_mega_upper_pool)} | "
                f"S실전 {s_exec_count} / S관찰 {s_watch_count} | "
                f"A확인 {len(a_confirm_pool)} | H-FAST PRIME {len(h_fast_prime_pool)} / WATCH {len(h_fast_watch_pool)} | IT/I {len(it_accel_live_pool) + len(i_accel_pool)}"
            )
            lines = [
                '🚨 [동시호가 최종킥 — v4.4.9.43 SJ THRESHOLD + FAIL AUDIT]',
                f'시간: {TODAY_STR} {_now_kst().strftime("%H:%M")} | FINAL_KICK_ONLY | 카드최대 {max_cards}개',
                f'오늘 단기 운용: {op}',
                f'오늘 단기 메인: {main_short_line}',
                f'후속관찰 메인: {followup_short_line}',
                f'강력추천 지정가: {explosion_short_line}',
                f'보조 단기 후보: {aux_short_line}',
                f'중기 분할관찰: {mid_top_line}',
                core_line,
                s_label_line,
                f"단기판정: ✅ {short_decision_counts.get('✅ 종가진입 가능', 0)} | 🟡 {short_decision_counts.get('🟡 지정가만 가능', 0)} | ❌ {short_decision_counts.get('❌ 오늘 포기', 0) + len(risk_pool) + len(a_risk_pool) + len(lp_risk_pool)}",
                f"중기판정: 🟡 {mid_decision_counts.get('🟡 지정가만 가능', 0)} | ❌ {mid_decision_counts.get('❌ 오늘 포기', 0)}",
                '',
            ]
            if not picked:
                lines += [
                    '최종결론: 오늘은 신규 종가배팅 없음.',
                    '사유: 동시호가 직전 FAST 후보가 종가진입/지정가 기준을 통과하지 못했습니다.',
                    '마지막 한 줄: 오늘은 쉰다. 무리매수 금지.',
                    '[전체완료]',
                ]
                return '\n'.join(lines).rstrip()
            if not picked_short and picked_mid:
                lines += [
                    '최종결론: 오늘 단기 신규 종가배팅 없음.',
                    '사유: 단기 FAST 후보가 종가진입/지정가 기준을 통과하지 못했습니다.',
                    '중기 후보는 단기 원픽이 아니라 20~30% 분할관찰로만 표시합니다.',
                    '',
                ]

            for i, (hh, label, decision, why, reasons) in enumerate(picked, 1):
                code = str(hh.get('code', '') or '').zfill(6)
                name = _clean_stock_name(code, str(hh.get('name', '') or code))
                mode = _pick_strategy(hh)
                close = _safe_float(hh.get('close', hh.get('_close', 0)), 0.0)
                entry, chase, targets, stop_txt = _final_kick_entry_line(hh)
                reason_txt = ' / '.join([r for r in reasons if str(r).strip()])
                mental = '안 잡히면 포기. 종가 추격 금지.' if decision == '🟡 지정가만 가능' else '+3 자동익절 먼저, +5는 절반 이하만 연장.'
                if decision == '❌ 오늘 포기':
                    mental = '오늘은 버린다. 회복하면 내일 다시 검색기에 맡긴다.'
                exec_style = _final_kick_execution_style(hh, decision)
                # v4.4.9.32: LP 메인일 때 S 카드는 보조 후보임을 제목에서 즉시 표시한다.
                card_decision = decision
                if mode == 'LP' and _is_lp_explosion_watch(hh) and decision.startswith('🟡'):
                    card_decision = '🔥 강력추천 지정가'
                    mental = '강한 후보로 인정하되 종가 시장가 추격은 금지. 지정가 미체결은 포기하고, 다음날 갭하단 지지·전일고가 회복·첫 눌림 재돌파를 1순위로 확인.'
                if mode == 'L' and _is_l_mega_upper_limit_followup(hh) and decision.startswith('🟡'):
                    card_decision = '💰 후속관찰'
                    mental = '오늘 신규매수 후보가 아닙니다. 보유자는 +3/+5 분할익절, 신규자는 다음날 기준선 지지와 VWAP 위 첫 눌림 재돌파만 확인.'
                if has_lp_main and mode == 'S' and decision.startswith('🟡'):
                    card_decision = '🟡 보조/소액 지정가만 가능'
                if CLOSING_BET_FINAL_KICK_COMPACT:
                    lines += [
                        f'{i}) {name}({code}) — {card_decision} | {label}',
                        f'현재가 {int(close):,}원 | 이유 {reason_txt}',
                        f"SJ체크: {_sj6_checklist_context(hh).get('short', '확인필요')}",
                        f'성격: {exec_style}',
                        f'계획: {entry} / {chase}',
                        f'익절·무효: {targets} / {stop_txt}',
                        f'한 줄: {why} — {mental}',
                        '[카드완료]',
                        '',
                    ]
                else:
                    lines += [
                        f'{i}) {name}({code}) — {card_decision} | {label}',
                        f'현재가/종가부근: {int(close):,}원 | 유형: {mode}',
                        f'이유: {reason_txt}',
                        f'판정근거: {why}',
                        f'운용성격: {exec_style}',
                        f'진입: {entry}',
                        f'{chase}',
                        f'자동익절: {targets}',
                        f'무효: {stop_txt}',
                        f'마지막 한 줄: {mental}',
                        '[카드완료]',
                        '',
                    ]
            lines += [
                '운용규칙: 15시 최종킥은 새 분석이 아니라 실행/포기 판정입니다. 단기 종가배팅과 중기 분할관찰을 분리하고, 지정가 미체결은 포기합니다. 체결 시 +3/+5 자동익절을 먼저 걸어둡니다.',
                '[전체완료]',
            ]
            return '\n'.join(lines).rstrip()

        if CLOSING_BET_FINAL_KICK_ONLY:
            return _build_final_kick_block()

        block = ['[🎯 실전 운영 후보 — v4.4.9.43 SJ THRESHOLD + FAIL AUDIT]']
        summary = (
            f"요약: 🥇LP-SAFE {len(lp_safe_pool)}개 | 🔥LP-POWER PRIME {len(lp_explosion_pool)}개 | 🔁LP-WATCH {len(lp_watch_pool)}개 | ⚠️LP-RISK {len(lp_risk_pool)}개 | 👑L-SAFE {len(l_safe_pool)}개 | 🔒SLOCK검증/숨김 {len(slock_pool)}개 | ⚡IT-ACCEL {len(it_accel_live_pool)}개 | 🌊L-TAIL {len(l_tail_pool)}개 | 💰L-MEGA {len(l_mega_pool)}개 | 💰L-상한후속 {len(l_mega_upper_pool)}개 | 💎S-RE-MEGA {len(s_reclaim_mega_pool)}개 | 🟢S-RE3000 {len(s_reclaim_3000_pool)}개 | 🟢S-RE-PRIME {len(s_reclaim_prime_pool)}개 | 🟡S-RE-WATCH {len(s_reclaim_watch_pool)}개 | 🟢S-MOMENTUM CALM {len(s_momentum_calm_pool)}개 | 🔥S-MOMENTUM PRIME {len(s_momentum_prime_pool)}개 | 🟡S-3000~5000 {len(s_momentum_3000_pool)}개 | 🟡S-WATCH {len(s_momentum_watch_pool)}개 | ⚠️S-유동성주의 {len(s_liquidity_risk_pool)}개 | 🟡S관찰 {len(neutral_pool)}개 | 🟢G-SAFE {len(g_safe_pool)}개 | "
            f"🟡L관찰 {len(l_watch_pool)}개 | 🟡G관찰 {len(g_neutral_pool)}개 | "
            f"⚠️S-RISK {len(risk_pool)}개 | 🔥G-AGG {len(g_aggressive_pool)}개 | "
            f"🧊H-TRI {len(h_triangle_pool)}개 | ✅H-CORE {len(h_core_union_pool)}개 | 🔥H-FAST PRIME {len(h_fast_prime_pool)}개 | 🟡H-FAST WATCH {len(h_fast_watch_pool)}개 | "
            f"🚀I-ACCEL {len(i_accel_pool)}개 | ✅I-CORE {len(i_core_pool)}개 | 🟡I-WATCH {len(i_watch_pool)}개 | "
            f"A전체 {len(a_all_pool)}개 | 🔁A-CONFIRM {len(a_confirm_pool)}개(PRIME {a_confirm_vc_watch_n}/CALM {a_confirm_vc_safe_n}) | 🟣A-CORE {len(a_core_pool)}개 | A보조관찰 {a_output_n}개"
        )
        if a_risk_pool:
            summary += f" | ⚠️A주의숨김 {len(a_risk_pool)}개"
        if c_pullback_output_n:
            summary += f" | 진단C {c_pullback_output_n}개"
        block.append(summary)
        block.append(f"🌐 시장/섹터 압력: 시장압력 {_live_market_pressure_label({})} | 섹터압력 {_live_sector_pressure_label({})} — 부담이면 모든 후보를 소액·확인형으로 낮춥니다.")
        # v4.4.9.8: A 카운트가 헷갈리지 않도록 전체/CORE/CONFIRM/RISK를 한 줄로 재정리한다.
        try:
            if len(a_all_pool) > 0 and len(a_confirm_pool) == 0 and len(a_core_pool) == 0:
                block.append(f"🔎 A 확인: A 전체 {len(a_all_pool)}개 중 A-CONFIRM/A-CORE 0개 — 5000억+·종가80+·거래량≤1.8·RR0.8~1.5 또는 전일고가 회복 조건 미충족. A-RISK {len(a_risk_pool)}개는 숨김.")
            elif len(a_confirm_pool) > 0 or len(a_core_pool) > 0:
                block.append(f"🔎 A 확인: A 전체 {len(a_all_pool)}개 중 A-CONFIRM {len(a_confirm_pool)}개 / A-CORE {len(a_core_pool)}개만 보조 승격 후보. RR1.8초과·거래량과열 A는 숨김.")
            else:
                block.append("🔎 A 확인: 오늘 A 계열 보조 후보가 없습니다.")
        except Exception:
            pass
        # v4.4.8: LP 유무에 따라 오늘 공격 강도를 한 줄로 정리한다.
        try:
            lp_d23_n = sum(1 for _h in lp_safe_pool if _lp_timing_bucket(_h)[0] == 'LP-D23')
            lp_d45_n = sum(1 for _h in lp_safe_pool if _lp_timing_bucket(_h)[0] == 'LP-D45')
            lp_d1_n = sum(1 for _h in lp_safe_pool if _lp_timing_bucket(_h)[0] == 'LP-D1')
        except Exception:
            lp_d23_n = lp_d45_n = lp_d1_n = 0
        if lp_d23_n > 0:
            op_line = f"🧭 오늘 운용강도: 적극 — LP-D23 {lp_d23_n}개 출현. 단기 최우선 타점이므로 +3 1차 익절 기준으로 대응."
        elif len(lp_safe_pool) > 0:
            op_line = f"🧭 오늘 운용강도: 보통~적극 — LP-SAFE {len(lp_safe_pool)}개(D1 {lp_d1_n}/D45 {lp_d45_n}). D1은 소액, D45는 기다림 필요."
        elif len(l_mega_upper_pool) > 0:
            op_line = "🧭 오늘 운용강도: 보유자/후속관찰 — LP-SAFE는 없지만 L-MEGA 상한가성 후보가 있습니다. 신규 종가추격보다 다음날 기준선 지지·첫 눌림 재돌파만 봅니다."
        elif len(l_core_pool) > 0:
            op_line = "🧭 오늘 운용강도: 보통 이하 — LP-SAFE 부재. L 리더갭은 강하지만 추격보다 다음날 갭하단/전일종가 지지 확인 우선."
        elif len(a_confirm_pool) > 0:
            op_line = f"🧭 오늘 운용강도: 보통 — LP/L 핵심은 없지만 A-CONFIRM {len(a_confirm_pool)}개(PRIME {a_confirm_vc_watch_n}개/CALM {a_confirm_vc_safe_n}개). 전일고가 회복가·눌림 재지지 기준 소액 확인만."
        elif len(a_core_pool) > 0:
            op_line = f"🧭 오늘 운용강도: 관망~보통 — A-RETEST CORE {len(a_core_pool)}개. 당일 추격보다 다음날 전일고가 회복 확인을 우선."
        elif len(safe_pool) > 0 or len(neutral_pool) > 0:
            op_line = "🧭 오늘 운용강도: 관망~소액 — LP/L/A-CONFIRM/IT 핵심 후보 부재. S 응축형만 소액 확인, 무리한 종가매수 금지."
        else:
            op_line = "🧭 오늘 운용강도: 관망 — 단기 핵심 LP/L/A/S 후보가 약함. IT/I-MAIN은 중기 분할 관찰만."
        if not lp_safe_pool:
            op_line += " LP 없는 날은 무리매수보다 L 지지확인·A-CONFIRM 확인·S 소액·IT 중기분할만 봅니다."
        block.append(op_line)
        block.append('[🚦 v4.4.9.43 실시간 운용판정]\nFAST 실전 출력은 LP-D23/D45, L 5000억+, A-CONFIRM PRIME/CALM, S-RECLAIM/S-MOMENTUM/S-NEUTRAL, A-RETEST CORE②, IT/I-MAIN ACCEL만 우선합니다. S-SAFE는 ST30 통과형도 거래대금별 S-RECLAIM MEGA/3000~5000/PRIME/WATCH/RISK로 나누고, ST30 미통과형은 S-MOMENTUM CALM(1000억+ 거래량냉각)/PRIME/RISK로 분리합니다. 넓은 A/C/B/H/SLOCK은 기본 숨김 또는 연구/관찰입니다.\n직장인 모드: 장중 5분봉 확인보다 지정가·추격금지·+3/+5 자동익절·무효가를 먼저 봅니다. 미체결은 포기하는 방식으로 운용합니다.\nA-RETEST CORE②는 당일 보조 후보, A-CONFIRM은 다음날 전일고가 회복·양봉·거래대금 유지 확인형입니다. PRIME은 힘이 남은 우선 확인형, CALM은 안정형 보조 후보입니다.\n시장/섹터 압력이 부담이면 같은 후보라도 소액·확인 후 진입으로 낮추고, 지지선 종가이탈+거래량 증가 음봉은 정상 흔들림이 아니라 위험신호로 봅니다.')
        block.append('')
        block += _v447_mental_summary_lines()

        if PRACTICAL_SHOW_NEW_PATTERNS and lp_safe_pool:
            block.append('')
            block.append('[🥇 LP-SAFE — 리더갭 눌림재지지 단기 최우선]')
            block.append('v4.4.9.43 운용 반영: LP는 1~5일 내 5000억+ 리더갭 이후 갭하단/전일종가/5·10일선 재지지 후보입니다. D1/D23/D45 타점을 구분하고, 다음날 시초가별 대응 시나리오를 함께 봅니다.')
            shown = 0
            used_codes = set()
            for h in lp_safe_pool:
                code = str(h.get('code',''))
                if code in used_codes:
                    continue
                shown += 1
                used_codes.add(code)
                block.append(_brief_practical_line(h, shown))
                block.append('')
                if shown >= PRACTICAL_L_PULLBACK_TOP_N:
                    break
        elif PRACTICAL_SHOW_NEW_PATTERNS:
            block.append('')
            block.append('[🥇 LP-SAFE — 리더갭 눌림재지지 단기 최우선]')
            block.append('해당 종목 없음 — 오늘은 LP-SAFE 조건을 만족한 리더갭 눌림재지지 후보가 없습니다.')


        if PRACTICAL_SHOW_NEW_PATTERNS and lp_explosion_pool:
            block.append('')
            block.append('[🔥 LP-POWER PRIME — 강력추천 지정가/다음날 1순위]')
            block.append('LP-SAFE 중 추천강도는 높지만 종가 시장가 추격은 금지해야 하는 고베타/대금형입니다. 보조 후보가 아니라 강력추천 지정가·다음날 첫 눌림 재돌파 1순위 라벨입니다.')
            for j, h in enumerate(lp_explosion_pool[:PRACTICAL_L_PULLBACK_TOP_N], 1):
                ctx = _lp_explosion_watch_context(h)
                code = str(h.get('code', '') or '').zfill(6)
                name = _clean_stock_name(code, str(h.get('name', '') or code))
                close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
                block.append(f"{j}) 🔥LP-POWER PRIME | {name}({code}) | {int(close):,}원 | {ctx.get('reason','')}")
                block.append('   ↳ 대응: 추천강도는 강함. 종가 시장가 추격 금지, 지정가 미체결은 포기. 다음날 갭하단 지지·전일고가 회복·첫 눌림 재돌파 확인.')
                block.append('[카드완료]')

        if PRACTICAL_SHOW_NEW_PATTERNS and l_mega_upper_pool:
            block.append('')
            block.append('[💰 L-MEGA 상한가 후속관찰 — 보유자 대응/다음날 확인]')
            block.append('상한가성 잠금형은 강하지만 신규 종가추격 후보가 아닙니다. 보유자는 +3/+5 분할익절, 신규자는 다음날 전일 종가/상한가 기준선 지지와 첫 눌림·VWAP 재돌파만 봅니다.')
            for j, h in enumerate(l_mega_upper_pool[:PRACTICAL_L_CORE_TOP_N], 1):
                code = str(h.get('code', '') or '').zfill(6)
                name = _clean_stock_name(code, str(h.get('name', '') or code))
                close = _safe_float(h.get('close', h.get('_close', 0)), 0.0)
                t3 = close * 1.03 if close > 0 else 0
                t5 = close * 1.05 if close > 0 else 0
                block.append(f"{j}) 💰L-MEGA 상한가후속 | {name}({code}) | {int(close):,}원 | {_l_mega_upper_followup_reason(h)}")
                block.append(f"   ↳ 보유자: +3% {int(t3):,}원 일부 / +5% {int(t5):,}원 추가. 신규자: 시초 추격 금지, 다음날 {int(close):,}원 지지·VWAP 위 첫 눌림 재돌파만 소액.")
                block.append('[카드완료]')

        if l_core_pool:
            block.append('')
            block.append('[🥈 단기 2순위 L-CORE/MEGA 리더갭 — 5000억+ 우선]')
            block.append('v4.4.9 기준: L 당일 후보는 단기 2순위입니다. 5000억+·종가70%+는 우대하지만, 가능하면 LP 눌림재지지 타점으로 재진입하는 쪽을 더 우선합니다.')
            used_codes = set()
            shown = 0
            for h in l_core_pool:
                code = str(h.get('code',''))
                if code in used_codes:
                    continue
                shown += 1
                used_codes.add(code)
                block.append(_brief_practical_line(h, shown))
                block.append('')
                if shown >= PRACTICAL_L_CORE_TOP_N:
                    break
        else:
            block.append('')
            block.append('[🥈 단기 2순위 L-CORE/MEGA 리더갭 — 5000억+ 우선]')
            block.append('해당 종목 없음 — 오늘은 거래대금 5000억+ 대형주 리더갭 후보가 없습니다.')

        if PRACTICAL_SHOW_NEW_PATTERNS and lp_watch_pool:
            block.append('')
            block.append('[🔁 LP-WATCH — 리더갭 눌림재지지 관찰/승격대기]')
            block.append('LP 구조는 있으나 SAFE 조건 일부가 부족합니다. 다음날 양봉·전일고가 회복·거래대금 유지 시만 승격합니다.')
            shown = 0
            used_codes = set()
            for h in lp_watch_pool:
                code = str(h.get('code',''))
                if code in used_codes:
                    continue
                shown += 1
                used_codes.add(code)
                block.append(_brief_practical_line(h, shown))
                block.append('')
                if shown >= min(PRACTICAL_L_PULLBACK_TOP_N, 2):
                    break

        if PRACTICAL_SHOW_NEW_PATTERNS and lp_risk_pool and PRACTICAL_SHOW_RISK_DETAILS:
            block.append('')
            block.append('[⚠️ LP-RISK — 기본 제외/상세요청시에만 표시]')
            for i, h in enumerate(lp_risk_pool[:2], 1):
                block.append(_brief_practical_line(h, i))
                block.append('')

        if PRACTICAL_SHOW_NEW_PATTERNS and PRACTICAL_SHOW_SLOCK_LIVE and slock_pool:
            block.append('')
            block.append('[🔒 SLOCK — S2 상단잠김형 검증표시]')
            block.append('SLOCK은 v4.4.3 성과가 약해 실시간 기본 숨김입니다. CLOSING_BET_SHOW_SLOCK_LIVE=1일 때만 참고용으로 표시합니다.')
            for i, h in enumerate(slock_pool[:PRACTICAL_S2_LOCK_TOP_N], 1):
                block.append(_brief_practical_line(h, i))
                block.append('')

        if safe_pool:
            block.append('')
            block.append('[🥈 단기 2순위 S2/S-CORE — 실행·관찰 후보]')
            block.append('v4.4.9.43 기준: S-SAFE는 ST30 통과형을 S-RECLAIM MEGA(5000억+)·3000~5000·PRIME(1000억+)·WATCH(300~1000)·LIQUIDITY RISK(<300)로 나누고, ST30 미통과형은 S-MOMENTUM CALM(1000억+ 거래량냉각)/PRIME/S-LIQUIDITY로 분리합니다. ST30은 하드필터가 아닙니다.')
            for i, h in enumerate(safe_pool[:PRACTICAL_SAFE_TOP_N], 1):
                block.append(_brief_practical_line(h, i))
                block.append('')
        else:
            block.append('')
            block.append('[🥈 단기 2순위 S2/S-CORE — 실행·관찰 후보]')
            block.append('해당 종목 없음 — 오늘은 S-RECLAIM/S-MOMENTUM으로 분류되는 S-CORE SAFE 후보가 없습니다. S2/S-NEUTRAL은 관찰 후보로 확인합니다.')

        if g_safe_pool:
            block.append('')
            block.append('[🟢 G-SAFE 모랄레스갭 — 보조 실전 후보]')
            block.append('S-CORE SAFE 다음 순위입니다. 갭 전략은 +3/+5 익절형으로, 5거래일 안에 힘이 없으면 정리합니다.')
            for i, h in enumerate(g_safe_pool[:PRACTICAL_G_SAFE_TOP_N], 1):
                block.append(_brief_practical_line(h, i))
                block.append('')
        else:
            block.append('')
            block.append('[🟢 G-SAFE 모랄레스갭 — 보조 실전 후보]')
            block.append('해당 종목 없음 — 오늘은 G-SAFE 조건을 만족한 보조 후보가 없습니다.')

        if h_triangle_pool and PRACTICAL_SHOW_H_DIAG:
            block.append('')
            block.append('[🧊 H-TRIANGLE SAFE — 신규검증 1순위]')
            block.append('직전 삼각수렴 → 신고가 장대양봉 → 거래량 마른 타점봉 구조입니다. 백테스트상 H 중 최상위지만 신규검증 후보로 비중은 S/G보다 낮게 봅니다.')
            for i, h in enumerate(h_triangle_pool[:PRACTICAL_H_TRIANGLE_TOP_N], 1):
                block.append(_brief_h_line(h, i, '🧊 H-TRIANGLE SAFE'))
                block.append('')
        else:
            block.append('')
            block.append('[🧊 H-TRIANGLE SAFE — 신규검증 1순위]')
            block.append('해당 종목 없음 — 오늘은 직전 삼각수렴형 H 후보가 없습니다.')

        if h_core_union_pool and PRACTICAL_SHOW_H_DIAG:
            block.append('')
            block.append('[✅ H-CORE UNION — 신규검증 핵심셀]')
            block.append('삼각수렴형 또는 거래대금×Vol60 핵심셀입니다. S/G 후보가 부족할 때만 신규검증 후보로 봅니다.')
            shown = 0
            used_codes = set()
            for h in h_core_union_pool:
                code = str(h.get('code',''))
                if code in used_codes:
                    continue
                label = '✅ H-CORE'
                if _is_h_triangle_hit(h):
                    label = '🧊 H-TRIANGLE'
                elif _is_h_core_500_1000_vol23(h):
                    label = '🟢 H-CORE 500~1000억×2~3배'
                elif _is_h_core_300_500_vol35(h):
                    label = '🟣 H-CORE 300~500억×3~5배'
                elif _is_h_core_1000_2000_vol23(h):
                    label = '🔵 H-CORE 1000~2000억×2~3배'
                shown += 1
                used_codes.add(code)
                block.append(_brief_h_line(h, shown, label))
                block.append('')
                if shown >= PRACTICAL_H_CORE_TOP_N:
                    break
        else:
            block.append('')
            block.append('[✅ H-CORE UNION — 신규검증 핵심셀]')
            block.append('해당 종목 없음 — 오늘은 H 핵심셀 후보가 없습니다.')

        if PRACTICAL_SHOW_H_FAST and h_fast_prime_pool:
            block.append('')
            block.append('[🔥 H-FAST PRIME — +3 초단타 전용/보유금지]')
            block.append('Vol60 8~12배·짧은 눌림·종가위치70+·진입거래량 냉각 조건만 PRIME으로 표시합니다. 지정가만 가능하며 +3 자동익절 후 미련 없이 정리합니다.')
            for i, h in enumerate(h_fast_prime_pool[:PRACTICAL_H_FAST_TOP_N], 1):
                block.append(_brief_h_line(h, i, _h_fast_live_label(h)))
                block.append('')

        if PRACTICAL_SHOW_H_FAST and PRACTICAL_SHOW_H_DIAG and h_fast_watch_pool:
            block.append('')
            block.append('[🟡 H-FAST WATCH/RISK — PRIME 미충족 관찰]')
            block.append('8배+라도 Vol60 과다·종가위치 미달·타점 지연이면 FINAL_KICK 매수 제외입니다. 다음날 재지지만 관찰합니다.')
            for i, h in enumerate(h_fast_watch_pool[:PRACTICAL_H_FAST_TOP_N], 1):
                block.append(_brief_h_line(h, i, _h_fast_live_label(h)))
                block.append('')

        if PRACTICAL_SHOW_NEW_PATTERNS and it_accel_live_pool:
            block.append('')
            block.append('[⚡ IT-ACCEL — I-MAIN 중기후보 촉발형]')
            block.append('v4.4.9.43 기준: IT는 ACCEL만 실시간 촉발 후보로 표시합니다. 단기 종가배팅이 아니라 20/40/60일 중기 1차 분할매집 타이밍이며, I-LEADER 주봉 사이클 라벨을 보조로 확인합니다.')
            used_codes = set()
            shown = 0
            for h in it_accel_live_pool:
                code = str(h.get('code',''))
                if code in used_codes:
                    continue
                shown += 1
                used_codes.add(code)
                block.append(_brief_practical_line(h, shown))
                block.append('')
                if shown >= PRACTICAL_I_TRIGGER_TOP_N:
                    break

        if PRACTICAL_SHOW_I_MAIN and (i_accel_pool or i_core_pool or i_watch_pool or i_add_pool or i_confirm_pool):
            block.append('')
            block.append('[📈 I-MAIN 150/200일 시세분출 — 중기 누적관찰 후보]')
            block.append('v4.4.9.43 기준: I-MAIN은 단기 종가배팅이 아니라 20/40/60거래일 중기 누적관찰입니다. ACCEL/CORE에 더해 I-LEADER 라벨로 주봉 사이클 초입·가속·후반·공세종말 위험을 함께 봅니다.')
            used_codes = set()
            shown_total = 0
            groups = [
                ('🚀 I-MAIN ACCEL — 고수익 가속형', i_accel_pool, PRACTICAL_I_MAIN_ACCEL_TOP_N),
                ('✅ I-MAIN CORE — 안정형 핵심 후보', i_core_pool, PRACTICAL_I_MAIN_CORE_TOP_N),
                ('🟡 I-MAIN WATCH — I-4 관찰 후보', i_watch_pool, PRACTICAL_I_MAIN_WATCH_TOP_N),
                ('➕ I-MAIN ADD — 첫 눌림 추가 후보', i_add_pool, PRACTICAL_I_MAIN_ADD_TOP_N),
                ('🔎 I-MAIN CONFIRM — 돌파확인/보유확인', i_confirm_pool, PRACTICAL_I_MAIN_CONFIRM_TOP_N),
            ]
            for group_title, pool, limit in groups:
                if not pool:
                    continue
                block.append('')
                block.append(f'[{group_title}]')
                local_shown = 0
                for h in pool:
                    code = str(h.get('code',''))
                    if code in used_codes:
                        continue
                    used_codes.add(code)
                    shown_total += 1
                    local_shown += 1
                    block.append(_brief_i_main_line(h, local_shown))
                    block.append('')
                    if local_shown >= limit:
                        break
            if shown_total == 0:
                block.append('중복 제거 후 표시 가능한 I-MAIN 후보가 없습니다.')
        elif PRACTICAL_SHOW_I_MAIN:
            block.append('')
            block.append('[📈 I-MAIN 150/200일 시세분출 — 중기 누적관찰 후보]')
            block.append('해당 종목 없음 — 오늘은 I-MAIN CORE/ACCEL/WATCH 조건을 만족한 중기 후보가 없습니다.')

        if h_overheat_pool or h_other_pool:
            block.append('')
            block.append('[⚠️ H 제외/숨김 — 일반 H·과열·구조부족]')
            block.append(f"H-OVERHEAT 5~8배 {len(h_overheat_pool)}개 | 기타 일반/구조부족 H {len(h_other_pool)}개")
            block.append('일반 H 전체는 손절선행이 높아 기본 매매 제외입니다. 필요하면 CLOSING_BET_SHOW_H_DIAG=1 또는 CLOSING_BET_SHOW_RISK_DETAILS=1로 상세 확인합니다.')
            if PRACTICAL_SHOW_H_DIAG or PRACTICAL_SHOW_RISK_DETAILS:
                diag_pool = h_overheat_pool[:PRACTICAL_H_FAST_TOP_N] + h_other_pool[:PRACTICAL_H_FAST_TOP_N]
                for i, h in enumerate(diag_pool, 1):
                    block.append(_brief_h_line(h, i, '⚠️ H-DIAG'))
                    block.append('')

        if neutral_pool:
            block.append('')
            block.append('[🟡 S2/S-CORE NEUTRAL — 단기 2순위 관찰 후보]')
            block.append('v4.4.9 기준: S2 실행형/S-NEUTRAL은 단기 2순위입니다. 단, 고점권 전략이므로 KOSPI200·5조+·거래대금 3000억+·종가위치70%+를 우대합니다.')
            for i, h in enumerate(neutral_pool[:PRACTICAL_NEUTRAL_TOP_N], 1):
                block.append(_brief_practical_line(h, i))
                block.append('')

        if PRACTICAL_SHOW_L_WATCH and l_watch_pool:
            block.append('')
            block.append('[🟡 L-WATCH 대형주 리더갭 — 3000~5000억/품질약함 관찰]')
            block.append('3000~5000억 또는 윗꼬리35%+/종가위치65% 미만 리더갭은 CORE보다 약해 관찰 후보로만 봅니다. 다음날 갭 지지·전일고가 회복 확인이 우선입니다.')
            for i, h in enumerate(l_watch_pool[:PRACTICAL_L_WATCH_TOP_N], 1):
                block.append(_brief_practical_line(h, i))
                block.append('')

        if g_neutral_pool:
            block.append('')
            block.append('[🟡 G-NEUTRAL 모랄레스갭 — 갭 지지 관찰]')
            block.append('5일 검증상 기대값이 낮아 즉시매수보다 다음날 갭 지지·전일고가 회복 확인이 우선입니다.')
            for i, h in enumerate(g_neutral_pool[:PRACTICAL_G_NEUTRAL_TOP_N], 1):
                block.append(_brief_practical_line(h, i))
                block.append('')

        if a_confirm_pool:
            block.append('')
            block.append('[🔁 A-CONFIRM — 전일 A 후보 다음날 확인형]')
            block.append('v4.4.9.14 기준: 전일 A-RETEST CORE였고 오늘 전일고가 회복/양봉/거래대금 유지가 확인된 후보입니다. PRIME은 힘이 남은 우선 확인형, CALM은 안정형 보조 후보입니다. 시초 +2% 이상 급등 추격은 피하고, 전일고가 회복가 또는 회복 후 눌림 재지지를 우선합니다.')
            for i, h in enumerate(a_confirm_pool[:PRACTICAL_A_TOP_N], 1):
                block.append(_brief_practical_line(h, i))
                block.append('')

        if a_core_pool:
            block.append('')
            block.append('[🟣 A-RETEST CORE — 당일 보조 승격 후보]')
            block.append('조건: 5000억+·종가80+·거래량≤1.8·RR0.8~1.5. LP/L/S 후보가 부족한 날만 보조로 참고하고, 핵심 진입은 다음날 전일고가 회복 확인입니다. 다음날 적당한 거래량 냉각과 가격지지가 붙으면 A-CONFIRM PRIME으로 봅니다. RR 1.8 초과 A는 제외합니다.')
            for i, h in enumerate(a_core_pool[:PRACTICAL_A_TOP_N], 1):
                block.append(_brief_practical_line(h, i))
                block.append('')

        if a_pool:
            block.append('')
            block.append('[👀 A 보조돌파 5000억+ — 관찰/승격대기]')
            block.append(f'A는 즉시매수 후보가 아닙니다. 5000억+ 고거래대금 후보만 참고하고, 다음날 전일고가 회복 + 거래대금 유지 시만 승격합니다. 출력 {a_output_n}개/전체 {len(a_pool)}개입니다.')
            for i, h in enumerate(a_pool[:PRACTICAL_A_TOP_N], 1):
                block.append(_brief_practical_line(h, i))
                block.append('')

        if a_risk_pool:
            block.append('')
            block.append('[⚠️ A 보조돌파 RISK — 기본 숨김/추격주의]')
            a_reason_counts = {}
            for h in a_risk_pool:
                for t in _trade_risk_tags(h):
                    a_reason_counts[t] = a_reason_counts.get(t, 0) + 1
            a_reason_text = ' / '.join([f"{k} {v}개" for k, v in sorted(a_reason_counts.items())]) or '위험태그 확인 필요'
            block.append(f"A-RISK {len(a_risk_pool)}개 | 사유: {a_reason_text}")
            block.append('A 보조 후보라도 거래량과열·RR불량·종가위치약함이면 기본 제외합니다. 필요하면 CLOSING_BET_SHOW_RISK_DETAILS=1 로 상세를 표시할 수 있습니다.')
            if PRACTICAL_SHOW_RISK_DETAILS:
                for i, h in enumerate(a_risk_pool[:PRACTICAL_A_TOP_N], 1):
                    block.append(_brief_practical_line(h, i))
                    block.append('')

        if g_aggressive_pool:
            block.append('')
            block.append('[🔥 G-AGGRESSIVE — 저유동성/고변동 급등형]')
            g_reason_counts = {}
            for h in g_aggressive_pool:
                for t in _g_aggressive_tags(h):
                    g_reason_counts[t] = g_reason_counts.get(t, 0) + 1
            g_reason_text = ' / '.join([f"{k} {v}개" for k, v in sorted(g_reason_counts.items())]) or '고변동 사유 확인 필요'
            block.append(f"G-AGGRESSIVE {len(g_aggressive_pool)}개 | 사유: {g_reason_text}")
            block.append('백테스트 표본은 작고 실전 체결·호가 공백 리스크가 큽니다. 기본은 관찰/제외이며 필요 시 상세 표시만 합니다.')
            if PRACTICAL_SHOW_RISK_DETAILS:
                for i, h in enumerate(g_aggressive_pool[:PRACTICAL_G_AGGRESSIVE_TOP_N], 1):
                    block.append(_brief_practical_line(h, i))
                    block.append('')

        if c_pullback_pool:
            block.append('')
            block.append('[🔎 C-SWING 눌림재상승형 — 진단용]')
            block.append('실전 후보가 아닙니다. C는 손절선행이 높아 차트 복기/진단용으로만 봅니다.')
            for i, h in enumerate(c_pullback_pool[:PRACTICAL_C_PULLBACK_TOP_N], 1):
                block.append(_brief_c_swing_line(h, i))
                block.append('')

        if c_swing_pool:
            block.append('')
            block.append('[🔎 C-SWING 엄격형 진단 — 기본 숨김]')
            block.append('20거래일 검증상 흔들림이 커서 실전 후보가 아니라 진단용입니다.')
            for i, h in enumerate(c_swing_pool[:PRACTICAL_C_SWING_TOP_N], 1):
                block.append(_brief_c_swing_line(h, i))
                block.append('')

        if risk_pool:
            block.append('')
            block.append('[⚠️ S-CORE RISK — 기본 제외/추격주의]')
            reason_counts = {}
            for h in risk_pool:
                for t in _s_core_risk_tags(h):
                    reason_counts[t] = reason_counts.get(t, 0) + 1
            reason_text = ' / '.join([f"{k} {v}개" for k, v in sorted(reason_counts.items())]) or '위험태그 확인 필요'
            block.append(f"RISK {len(risk_pool)}개 | 사유: {reason_text}")
            block.append('기본 출력에서는 상세 후보를 숨깁니다. 필요하면 CLOSING_BET_SHOW_RISK_DETAILS=1 로 상세를 표시할 수 있습니다.')
            if PRACTICAL_SHOW_RISK_DETAILS:
                for i, h in enumerate(risk_pool[:PRACTICAL_RISK_TOP_N], 1):
                    block.append(_brief_practical_line(h, i))
                    block.append('')

        if CLOSING_BET_CARD_SPLIT_GUARD:
            block.append('')
            block.append('[섹션완료]')
        return '\n'.join(block).rstrip()

    sections = [header, _build_practical_block(), '[전체완료]']

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
                if CLOSING_BET_CARD_SPLIT_GUARD and '[카드완료]' not in entry:
                    entry = entry.rstrip() + '\n[카드완료]'
                block.append(entry)
                block.append("")
        return "\n".join(block).rstrip()

    # v4.1.9: 기본 실시간 출력은 위의 운영 블록만 보낸다.
    # 긴 전략별 레거시 TOP5는 필요할 때만 CLOSING_BET_SHOW_LEGACY_SECTIONS=1 로 표시한다.
    if PRACTICAL_SHOW_LEGACY_SECTIONS:
        sections.append(_build_block("🚀 고점재응축(S) TOP5", hits_s, "S"))
        sections.append(_build_block("👑 대형주리더갭(L) TOP5", hits_l, "L"))
        sections.append(_build_block("🟢 모랄레스갭(G) TOP5", hits_g, "G"))
        sections.append(_build_block("돌파형(A) TOP5", hits_a, "A"))
        sections.append(_build_block("👀 ENV엄격형(B1) 관찰 TOP5", hits_b1, "B1"))
        sections.append(_build_block("👀 BB확장형(B2) 관찰 TOP5", hits_b2, "B2"))
        if PRACTICAL_SHOW_C_DIAG:
            sections.append(_build_block("🔎 C-SWING 눌림재상승형 진단 TOP3", hits_c_pullback_reentry[:PRACTICAL_C_PULLBACK_TOP_N], "C"))
            sections.append(_build_block("🔎 역매공파(C) 진단 TOP5", hits_c, "C"))

    full_message = "\n\n".join([str(sec).strip() for sec in sections if str(sec).strip()])
    chunks = _split_telegram_safe(full_message, max_len=CLOSING_BET_TELEGRAM_MAX_LEN)

    for i, chunk in enumerate(chunks, 1):
        if len(chunks) > 1:
            chunk = f"({i}/{len(chunks)})\n" + chunk
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
            base_name_map = {_normalize_code(c): n for c, n in zip(codes, names)}
            source_codes = allowed_codes
            name_map = _build_name_map_for_codes(source_codes, base_name_map)
            _set_stock_name_map(name_map)
            names = [_clean_stock_name(c, name_map.get(c, c)) for c in source_codes]
        else:
            source_codes = codes
            name_map = _build_name_map_for_codes(source_codes, {_normalize_code(c): n for c, n in zip(codes, names)})
            _set_stock_name_map(name_map)
            names = [_clean_stock_name(c, name_map.get(c, c)) for c in source_codes]
    else:
        codes = _load_universe(SCAN_UNIVERSE)
        codes = [_normalize_code(c) for c in codes]
        codes = sorted(set(codes))
        if not codes:
            log_error("⚠️ 유니버스 로드 실패")
            return []

        # v4.1.1: 종목명 맵을 전체 코드 대상으로 생성한다.
        # 기존 codes[:1000] 제한 때문에 유니버스 뒤쪽 종목이 종목명확인필요로 표시되는 문제가 있었다.
        name_map = _build_name_map_for_codes(codes)
        _set_stock_name_map(name_map)

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
        names = [_clean_stock_name(c, name_map.get(c, c)) for c in source_codes]

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
    hits_l = [h for h in hits if h.get('mode') == 'L']
    hits_lp = [h for h in hits if h.get('mode') == 'LP']
    hits_slock = [h for h in hits if h.get('mode') == 'SLOCK']
    hits_s = [h for h in hits if h.get('mode') == 'S']
    hits_h = [h for h in hits if h.get('mode') == 'H']
    hits_it = [h for h in hits if h.get('mode') == 'IT']
    hits_i = [h for h in hits if h.get('mode') == 'I' or h.get('mode') == 'IT' or _safe_int(h.get('i_core', 0), 0) == 1]
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
    _sort_hit_list(hits_l)
    _sort_hit_list(hits_lp)
    _sort_hit_list(hits_slock)
    _sort_hit_list(hits_it)
    hits_s.sort(
        key=lambda x: (
            0 if str(x.get('s_type', '')) == 'S2' else 1,
            -_safe_float(x.get('score', 0), 0.0),
            -_safe_float(x.get('today_vol_ratio', x.get('vol_ratio', 0)), 0.0),
            -_safe_float(x.get('rr', 0), 0.0),
            -_safe_float(x.get('amount_b', 0), 0.0),
        )
    )
    _sort_hit_list(hits_h)
    _sort_hit_list(hits_i)
    _sort_hit_list(hits_a)
    _sort_hit_list(hits_b1)
    _sort_hit_list(hits_b2)
    _sort_hit_list(hits_c)

    # v4.2.9: 실시간 H 신고가거자름 후보가 최종 합산에서 누락되지 않도록 H도 포함
    # 기존 버그 방지: C전략과 신규 G/S/H전략이 최종 합산에서 빠지지 않도록 모두 포함
    hits = hits_l + hits_lp + hits_slock + hits_s + hits_g + hits_it + hits_h + hits_i + hits_a + hits_b1 + hits_b2 + hits_c

    log_info(f"\n종가배팅 후보: {len(hits)}개")
    log_info(
        f"리더갭(L): {len(hits_l)}개 | L-PULLBACK(LP): {len(hits_lp)}개 | S2-LOCK(SLOCK): {len(hits_slock)}개 | 고점재응축(S): {len(hits_s)}개(S2 {sum(1 for h in hits_s if str(h.get('s_type','')) == 'S2')}/S1 {sum(1 for h in hits_s if str(h.get('s_type','')) != 'S2')}) | 모랄레스갭(G): {len(hits_g)}개 | "
        f"I-TRIGGER(IT): {len(hits_it)}개 | 신고가거자름(H): {len(hits_h)}개 | I-MAIN(I): {len(hits_i)}개 | 돌파형(A): {len(hits_a)}개 | ENV엄격형(B1): {len(hits_b1)}개 | BB확장형(B2): {len(hits_b2)}개 | 역매공파(C): {len(hits_c)}개"
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
        f"[전략진단] S: {STRATEGY_DIAG['S_hit']}/{STRATEGY_DIAG['S_try']} | "
        f"L: {STRATEGY_DIAG['L_hit']}/{STRATEGY_DIAG['L_try']} | "
        f"G: {STRATEGY_DIAG['G_hit']}/{STRATEGY_DIAG['G_try']} | "
        f"H: {STRATEGY_DIAG['H_hit']}/{STRATEGY_DIAG['H_try']} | "
        f"I: {STRATEGY_DIAG['I_hit']}/{STRATEGY_DIAG['I_try']} | "
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
        out['MA150'] = out['Close'].rolling(150).mean()
        out['MA200'] = out['Close'].rolling(200).mean()
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


@lru_cache(maxsize=8)
def _load_market_index_df_cached_for_i_core(symbol: str = 'KS11', pykrx_code: str = '1001', label: str = 'KOSPI') -> pd.DataFrame:
    """v4.3.5 I-CORE 시장국면/초과수익 검증용 지수 데이터 로더.

    - KOSPI: FDR KS11 / pykrx 1001
    - KOSDAQ: FDR KQ11 / pykrx 2001
    - 실패해도 백테스트 본체는 중단하지 않고 시장데이터부족으로 표기한다.
    """
    if not I_CORE_REGIME_VALIDATE:
        return pd.DataFrame()
    try:
        start_ymd = I_CORE_MARKET_START
        end_ymd = (datetime.now() + timedelta(days=10)).strftime('%Y-%m-%d')
        try:
            raw = fdr.DataReader(symbol, start_ymd, end_ymd)
            df = _prepare_price_df(raw)
            if df is not None and not df.empty and len(df) >= 240:
                df['market_source'] = f'fdr:{symbol}'
                return df
        except Exception as e:
            log_debug(f"I-CORE {label} FDR 로드 실패: {type(e).__name__}: {e}")
        try:
            if pykrx_stock is not None:
                start_key = pd.Timestamp(start_ymd).strftime('%Y%m%d')
                end_key = pd.Timestamp(end_ymd).strftime('%Y%m%d')
                raw = pykrx_stock.get_index_ohlcv_by_date(start_key, end_key, pykrx_code)
                df = _prepare_price_df(raw)
                if df is not None and not df.empty and len(df) >= 240:
                    df['market_source'] = f'pykrx:{label}({pykrx_code})'
                    return df
        except Exception as e:
            log_debug(f"I-CORE {label} pykrx 로드 실패: {type(e).__name__}: {e}")
    except Exception as e:
        log_debug(f"I-CORE {label} 시장지수 로드 fatal: {type(e).__name__}: {e}")
    return pd.DataFrame()


def _load_market_index_df_for_i_core() -> pd.DataFrame:
    """KOSPI 지수 데이터. 기존 함수명은 호환용으로 유지."""
    return _load_market_index_df_cached_for_i_core(I_CORE_MARKET_INDEX, '1001', 'KOSPI')


def _load_kosdaq_index_df_for_i_core() -> pd.DataFrame:
    """KOSDAQ 지수 데이터. KOSDAQ/중소형 후보의 상대성과 보정용."""
    return _load_market_index_df_cached_for_i_core(I_CORE_KOSDAQ_INDEX, '2001', 'KOSDAQ')


def _calc_index_future_returns_for_i_core(m: pd.DataFrame, signal_date, stock_i_returns: dict | None, prefix: str) -> dict:
    """신호일 이후 20/40/60거래일 지수수익률과 종목 초과수익 계산."""
    out = {
        f'i_{prefix}_ret_close_20d': np.nan,
        f'i_{prefix}_ret_close_40d': np.nan,
        f'i_{prefix}_ret_close_60d': np.nan,
        f'i_{prefix}_excess_close_20d': np.nan,
        f'i_{prefix}_excess_close_40d': np.nan,
        f'i_{prefix}_excess_close_60d': np.nan,
    }
    try:
        if m is None or m.empty or 'Date' not in m.columns or 'Close' not in m.columns:
            return out
        dt = pd.Timestamp(signal_date)
        m = m.sort_values('Date').reset_index(drop=True)
        idxs = m.index[m['Date'] <= dt].tolist()
        if not idxs:
            return out
        pos = int(idxs[-1])
        close0 = _safe_float(m.iloc[pos].get('Close', 0), 0.0)
        if close0 <= 0:
            return out
        for n in (20, 40, 60):
            j = pos + n
            if j >= len(m):
                continue
            c = _safe_float(m.iloc[j].get('Close', 0), 0.0)
            if c <= 0:
                continue
            mret = round((c / close0 - 1.0) * 100.0, 2)
            out[f'i_{prefix}_ret_close_{n}d'] = mret
            if stock_i_returns:
                sret = _safe_float(stock_i_returns.get(f'i_ret_close_{n}d', np.nan), np.nan)
                if not pd.isna(sret):
                    out[f'i_{prefix}_excess_close_{n}d'] = round(sret - mret, 2)
    except Exception as e:
        log_debug(f"I-CORE {prefix} 지수수익률 계산 오류: {type(e).__name__}: {e}")
    return out


def _calc_i_core_market_context(signal_date, stock_i_returns: dict | None = None) -> dict:
    """신호일 기준 KOSPI 시장국면과 이후 20/40/60일 KOSPI 대비 초과수익 계산.

    시장국면은 신호일 이전 KOSPI 20/60/120일 수익률, 200일선 기울기, 최근 120일 고점 대비 낙폭으로 구분한다.
    이 값은 종목 선택 조건이 아니라 백테스트 진단용이다.
    """
    defaults = {
        'i_mkt_regime': '시장데이터부족',
        'i_mkt_ret20_prior': np.nan,
        'i_mkt_ret60_prior': np.nan,
        'i_mkt_ret120_prior': np.nan,
        'i_mkt_ma200_slope20': np.nan,
        'i_mkt_drawdown120': np.nan,
        'i_kospi_ret_close_20d': np.nan,
        'i_kospi_ret_close_40d': np.nan,
        'i_kospi_ret_close_60d': np.nan,
        'i_excess_close_20d': np.nan,
        'i_excess_close_40d': np.nan,
        'i_excess_close_60d': np.nan,
        'i_kosdaq_ret_close_20d': np.nan,
        'i_kosdaq_ret_close_40d': np.nan,
        'i_kosdaq_ret_close_60d': np.nan,
        'i_kosdaq_excess_close_20d': np.nan,
        'i_kosdaq_excess_close_40d': np.nan,
        'i_kosdaq_excess_close_60d': np.nan,
        'i_bench_name': 'KOSPI',
        'i_bench_ret_close_20d': np.nan,
        'i_bench_ret_close_40d': np.nan,
        'i_bench_ret_close_60d': np.nan,
        'i_bench_excess_close_20d': np.nan,
        'i_bench_excess_close_40d': np.nan,
        'i_bench_excess_close_60d': np.nan,
    }
    if not I_CORE_REGIME_VALIDATE:
        defaults['i_mkt_regime'] = '시장검증OFF'
        return defaults
    try:
        m = _load_market_index_df_for_i_core()
        if m is None or m.empty or 'Date' not in m.columns or 'Close' not in m.columns:
            return defaults
        dt = pd.Timestamp(signal_date)
        m = m.sort_values('Date').reset_index(drop=True)
        idxs = m.index[m['Date'] <= dt].tolist()
        if not idxs:
            return defaults
        pos = int(idxs[-1])
        close0 = _safe_float(m.iloc[pos].get('Close', 0), 0.0)
        if close0 <= 0:
            return defaults
        out = dict(defaults)
        def past_ret(days: int):
            j = pos - int(days)
            if j < 0:
                return np.nan
            c = _safe_float(m.iloc[j].get('Close', 0), 0.0)
            return round((close0 / c - 1.0) * 100.0, 2) if c > 0 else np.nan
        out['i_mkt_ret20_prior'] = past_ret(20)
        out['i_mkt_ret60_prior'] = past_ret(60)
        out['i_mkt_ret120_prior'] = past_ret(120)
        ma200 = pd.to_numeric(m.get('MA200', pd.Series(np.nan, index=m.index)), errors='coerce')
        if pos >= 20 and not pd.isna(ma200.iloc[pos]) and not pd.isna(ma200.iloc[pos-20]) and _safe_float(ma200.iloc[pos-20],0)>0:
            out['i_mkt_ma200_slope20'] = round((ma200.iloc[pos] / ma200.iloc[pos-20] - 1.0) * 100.0, 2)
        start_high = max(0, pos - 120)
        recent_high = _safe_float(m.iloc[start_high:pos+1]['Close'].max(), 0.0)
        if recent_high > 0:
            out['i_mkt_drawdown120'] = round((close0 / recent_high - 1.0) * 100.0, 2)

        ret60 = _safe_float(out['i_mkt_ret60_prior'], np.nan)
        ret120 = _safe_float(out['i_mkt_ret120_prior'], np.nan)
        slope = _safe_float(out['i_mkt_ma200_slope20'], np.nan)
        dd = _safe_float(out['i_mkt_drawdown120'], np.nan)
        # 단순하지만 재현 가능한 국면 분류. 향후 KOSDAQ/환율/금리까지 확장 가능.
        if (not pd.isna(ret60) and ret60 >= 5.0) or ((not pd.isna(ret120) and ret120 >= 8.0) and (not pd.isna(slope) and slope > 0) and (pd.isna(dd) or dd > -8.0)):
            regime = '상승장'
        elif (not pd.isna(ret60) and ret60 <= -5.0) or ((not pd.isna(ret120) and ret120 <= -8.0) and (not pd.isna(slope) and slope < 0)) or (not pd.isna(dd) and dd <= -12.0):
            regime = '하락장'
        else:
            regime = '횡보장'
        out['i_mkt_regime'] = regime

        for n in (20, 40, 60):
            j = pos + n
            if j >= len(m):
                continue
            c = _safe_float(m.iloc[j].get('Close', 0), 0.0)
            if c > 0:
                mret = round((c / close0 - 1.0) * 100.0, 2)
                out[f'i_kospi_ret_close_{n}d'] = mret
                if stock_i_returns:
                    sret = _safe_float(stock_i_returns.get(f'i_ret_close_{n}d', np.nan), np.nan)
                    if not pd.isna(sret):
                        out[f'i_excess_close_{n}d'] = round(sret - mret, 2)

        # v4.3.5: KOSDAQ 및 종목 소속시장 자동 벤치마크 보정.
        if I_CORE_COMPARE_KOSDAQ:
            kq = _calc_index_future_returns_for_i_core(_load_kosdaq_index_df_for_i_core(), signal_date, stock_i_returns, 'kosdaq')
            out.update(kq)
        idx_label = ''
        try:
            idx_label = str((stock_i_returns or {}).get('index_label', '')).upper()
        except Exception:
            idx_label = ''
        use_kosdaq = I_CORE_COMPARE_KOSDAQ and ('KOSDAQ' in idx_label or 'KQ' in idx_label)
        out['i_bench_name'] = 'KOSDAQ' if use_kosdaq else 'KOSPI'
        for n in (20, 40, 60):
            if use_kosdaq and not pd.isna(out.get(f'i_kosdaq_ret_close_{n}d', np.nan)):
                out[f'i_bench_ret_close_{n}d'] = out.get(f'i_kosdaq_ret_close_{n}d', np.nan)
                out[f'i_bench_excess_close_{n}d'] = out.get(f'i_kosdaq_excess_close_{n}d', np.nan)
            else:
                out[f'i_bench_ret_close_{n}d'] = out.get(f'i_kospi_ret_close_{n}d', np.nan)
                out[f'i_bench_excess_close_{n}d'] = out.get(f'i_excess_close_{n}d', np.nan)
        return out
    except Exception as e:
        log_debug(f"I-CORE 시장국면 계산 오류: {type(e).__name__}: {e}")
        return defaults


def _bt_grade_from_score(score: int, complete_min: int = 6, a_min: int = 5) -> str:
    if score >= complete_min:
        return '완전체'
    if score >= a_min:
        return '✅A급'
    return 'B급'



# =============================================================
# v4.4.9.16 ST30-RECLAIM / 조정후 재상승 A/B 검증 유틸
# =============================================================
def _calc_stochastic_series(hist: pd.DataFrame, period: int = 14, smooth: int = 3) -> tuple[pd.Series, pd.Series]:
    """일봉 Stochastic %K/%D. 0~100 범위로 반환한다."""
    try:
        h = hist.copy()
        high = pd.to_numeric(h.get('High'), errors='coerce')
        low = pd.to_numeric(h.get('Low'), errors='coerce')
        close = pd.to_numeric(h.get('Close'), errors='coerce')
        ll = low.rolling(int(period), min_periods=int(period)).min()
        hh = high.rolling(int(period), min_periods=int(period)).max()
        rng = (hh - ll).replace(0, np.nan)
        k = ((close - ll) / rng * 100.0).clip(lower=0, upper=100)
        d = k.rolling(int(smooth), min_periods=int(smooth)).mean()
        return k, d
    except Exception:
        idx = hist.index if hist is not None else pd.RangeIndex(0)
        return pd.Series(np.nan, index=idx), pd.Series(np.nan, index=idx)


def _calc_weekly_macd_hist_context(hist: pd.DataFrame) -> dict:
    """신호일 이전 데이터만으로 주봉 MACD 히스토그램 개선 여부를 계산한다."""
    out = {
        'st30_weekly_macd_up': 0,
        'st30_weekly_macd_crossing': 0,
        'st30_weekly_macd_hist': 0.0,
        'st30_weekly_macd_hist_prev': 0.0,
        'st30_weekly_label': '주봉확인불가',
    }
    try:
        if hist is None or len(hist) < 80 or 'Date' not in hist.columns:
            return out
        h = hist[['Date', 'Close']].copy()
        h['Date'] = pd.to_datetime(h['Date'], errors='coerce')
        h['Close'] = pd.to_numeric(h['Close'], errors='coerce')
        h = h.dropna(subset=['Date', 'Close']).set_index('Date').sort_index()
        if len(h) < 80:
            return out
        # 주봉은 해당 주 마지막 거래일 종가 기준. 신호일까지의 데이터만 들어와 미래누수 없음.
        w = h['Close'].resample('W-FRI').last().dropna()
        if len(w) < 35:
            return out
        ema12 = w.ewm(span=12, adjust=False, min_periods=12).mean()
        ema26 = w.ewm(span=26, adjust=False, min_periods=26).mean()
        macd = ema12 - ema26
        sig = macd.ewm(span=9, adjust=False, min_periods=9).mean()
        histv = macd - sig
        if len(histv.dropna()) < 3:
            return out
        last = float(histv.dropna().iloc[-1])
        prev = float(histv.dropna().iloc[-2])
        macd_last = float(macd.dropna().iloc[-1]) if len(macd.dropna()) else 0.0
        sig_last = float(sig.dropna().iloc[-1]) if len(sig.dropna()) else 0.0
        up = last > prev
        crossing = bool(macd_last < sig_last and last > prev)
        out.update({
            'st30_weekly_macd_up': int(bool(up)),
            'st30_weekly_macd_crossing': int(bool(crossing)),
            'st30_weekly_macd_hist': round(last, 4),
            'st30_weekly_macd_hist_prev': round(prev, 4),
            'st30_weekly_label': '주봉MACD개선' if up else '주봉MACD둔화',
        })
        return out
    except Exception as e:
        out['st30_weekly_label'] = f'주봉오류:{type(e).__name__}'
        return out


def _calc_st30_reclaim_context(row: pd.Series, hist: pd.DataFrame) -> dict:
    """상승추세 중 스토캐스틱 30 이하 식힘 후 재상승 전환 여부를 계산한다.

    목적:
    - 후보를 바로 탈락시키는 하드필터가 아니다.
    - 백테스트에서 '통과/미통과/주봉확인' 성과를 비교해 좋은 후보를 놓치는지 확인한다.
    """
    out = {
        'st30_k': 0.0,
        'st30_d': 0.0,
        'st30_recent_min_k': 0.0,
        'st30_recent_oversold': 0,
        'st30_kd_reclaim': 0,
        'st30_trend20_up': 0,
        'st30_above_ma20': 0,
        'st30_short_ma_turn': 0,
        'st30_reclaim_pass': 0,
        'st30_weekly_confirm': 0,
        'st30_wait': 0,
        'st30_fail': 0,
        'st30_label': 'ST30-미계산',
        'st30_reason': '',
    }
    try:
        if not ST30_RECLAIM_ENABLE or hist is None or len(hist) < 40:
            out['st30_reason'] = 'ST30비활성/데이터부족'
            return out
        h = hist.copy().reset_index(drop=True)
        k, d = _calc_stochastic_series(h, 14, 3)
        close_s = pd.to_numeric(h.get('Close'), errors='coerce')
        ma5 = pd.to_numeric(h.get('MA5', close_s.rolling(5).mean()), errors='coerce')
        ma10 = pd.to_numeric(h.get('MA10', close_s.rolling(10).mean()), errors='coerce')
        ma20 = pd.to_numeric(h.get('MA20', close_s.rolling(20).mean()), errors='coerce')
        close = _safe_float(close_s.iloc[-1], 0.0)
        k_now = _safe_float(k.iloc[-1], 0.0)
        d_now = _safe_float(d.iloc[-1], 0.0)
        k_prev = _safe_float(k.iloc[-2], 0.0) if len(k) >= 2 else 0.0
        d_prev = _safe_float(d.iloc[-2], 0.0) if len(d) >= 2 else 0.0
        lb = max(2, int(ST30_OVERSOLD_LOOKBACK or 7))
        recent_k = pd.to_numeric(k.tail(lb), errors='coerce')
        recent_min_k = _safe_float(recent_k.min(), 999.0)
        recent_oversold = recent_min_k <= float(ST30_OVERSOLD_LEVEL)
        # 방금 골든크로스이거나, 이미 K>D로 올라서며 K가 상승 중이면 재상승 전환으로 본다.
        fresh_cross = (k_prev <= d_prev and k_now > d_now)
        reclaiming = (k_now > d_now and k_now > k_prev)
        kd_reclaim = bool(fresh_cross or reclaiming)
        ma20_now = _safe_float(ma20.iloc[-1], 0.0)
        ma20_prev5 = _safe_float(ma20.iloc[-6], 0.0) if len(ma20) >= 6 else 0.0
        above_ma20 = bool(ma20_now > 0 and close >= ma20_now)
        trend20_up = bool(ma20_now > 0 and ma20_prev5 > 0 and ma20_now >= ma20_prev5)
        ma5_now = _safe_float(ma5.iloc[-1], 0.0)
        ma5_prev3 = _safe_float(ma5.iloc[-4], 0.0) if len(ma5) >= 4 else 0.0
        ma10_now = _safe_float(ma10.iloc[-1], 0.0)
        short_ma_turn = bool((ma5_now > 0 and close >= ma5_now and ma5_now >= ma5_prev3) or (ma10_now > 0 and close >= ma10_now))
        weekly = _calc_weekly_macd_hist_context(h)
        reclaim_pass = bool(above_ma20 and trend20_up and recent_oversold and kd_reclaim and short_ma_turn)
        weekly_confirm = bool(ST30_WEEKLY_MACD_CONFIRM and reclaim_pass and (weekly.get('st30_weekly_macd_up', 0) == 1))
        wait = bool(above_ma20 and trend20_up and recent_oversold and not kd_reclaim)
        fail = bool(not reclaim_pass)
        reasons = []
        reasons.append('20일선위' if above_ma20 else '20일선아래')
        reasons.append('20일선상승' if trend20_up else '20일선둔화')
        reasons.append(f'K최근저점{recent_min_k:.1f}')
        reasons.append('K>D재상승' if kd_reclaim else 'K>D미확인')
        reasons.append('단기선재상승' if short_ma_turn else '단기선미확인')
        reasons.append(str(weekly.get('st30_weekly_label', '주봉확인불가')))
        if weekly_confirm:
            label = '✅ ST30-WEEKLY'
        elif reclaim_pass:
            label = '✅ ST30-RECLAIM'
        elif wait:
            label = '🟡 ST30-WAIT'
        else:
            label = '❌ ST30-NO'
        out.update({
            'st30_k': round(k_now, 2),
            'st30_d': round(d_now, 2),
            'st30_recent_min_k': round(recent_min_k if recent_min_k != 999.0 else 0.0, 2),
            'st30_recent_oversold': int(bool(recent_oversold)),
            'st30_kd_reclaim': int(bool(kd_reclaim)),
            'st30_trend20_up': int(bool(trend20_up)),
            'st30_above_ma20': int(bool(above_ma20)),
            'st30_short_ma_turn': int(bool(short_ma_turn)),
            'st30_reclaim_pass': int(bool(reclaim_pass)),
            'st30_weekly_confirm': int(bool(weekly_confirm)),
            'st30_wait': int(bool(wait)),
            'st30_fail': int(bool(fail)),
            'st30_label': label,
            'st30_reason': ' / '.join(reasons),
        })
        out.update(weekly)
        return out
    except Exception as e:
        out['st30_label'] = f'ST30오류:{type(e).__name__}'
        out['st30_reason'] = str(e)[:120]
        return out


def _st30_num_series(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=float)
    if col not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors='coerce').fillna(default)


def _bt_mask_st30_reclaim(df: pd.DataFrame) -> pd.Series:
    return _st30_num_series(df, 'st30_reclaim_pass', 0).astype(int).eq(1)


def _bt_mask_st30_weekly(df: pd.DataFrame) -> pd.Series:
    return _st30_num_series(df, 'st30_weekly_confirm', 0).astype(int).eq(1)


def _bt_mask_st30_wait(df: pd.DataFrame) -> pd.Series:
    return _st30_num_series(df, 'st30_wait', 0).astype(int).eq(1)


def _format_st30_reclaim_abtest_report(df: pd.DataFrame) -> str:
    """ST30 적용/미적용 비교. 좋은 후보를 놓치는지 확인하기 위해 탈락군도 같이 출력한다."""
    try:
        if df is None or df.empty or 'st30_reclaim_pass' not in df.columns:
            return ''
        mode = df['mode'].astype(str) if 'mode' in df.columns else pd.Series('', index=df.index)
        pass_m = _bt_mask_st30_reclaim(df)
        weekly_m = _bt_mask_st30_weekly(df)
        wait_m = _bt_mask_st30_wait(df)
        fail_m = ~pass_m
        lines = []
        lines.append('[🧪 ST30-RECLAIM 적용/미적용 비교 — 필터 손실 점검 v4.4.9.16]')
        lines.append('- 목적: 스토캐스틱 30 이하 식힘 후 재상승 전환을 하드필터로 바로 적용하지 않고, 기존 후보 대비 성과 개선/탈락 손실을 비교합니다.')
        lines.append('- ST30-RECLAIM: 20일선 위·20일선 상승·최근 K≤30 식힘·K>D 재상승·5/10일선 재상승 확인.')
        lines.append('- ST30-WEEKLY: ST30-RECLAIM에 주봉 MACD 히스토그램 개선까지 붙은 더 엄격한 확인형입니다.')
        def add_scope(scope_df: pd.DataFrame, title: str):
            if scope_df is None or scope_df.empty:
                return
            pm = pass_m.loc[scope_df.index]
            wm = weekly_m.loc[scope_df.index]
            wam = wait_m.loc[scope_df.index]
            fm = ~pm
            lines.append('')
            lines.append(f'[{title}]')
            lines.append(_format_backtest_trade_rule_block(scope_df, f'{title} 기존 전체'))
            lines.append(_format_backtest_trade_rule_block(scope_df[pm], f'{title} ✅ ST30-RECLAIM 통과'))
            lines.append(_format_backtest_trade_rule_block(scope_df[wm], f'{title} ✅ ST30-WEEKLY 확인'))
            lines.append(_format_backtest_trade_rule_block(scope_df[wam], f'{title} 🟡 ST30-WAIT 대기'))
            lines.append(_format_backtest_trade_rule_block(scope_df[fm], f'{title} ❌ ST30 미통과/탈락'))
            try:
                lost = int(fm.sum())
                kept = int(pm.sum())
                keep_rate = kept / max(len(scope_df), 1) * 100.0
                lines.append(f'  ↳ 통과율 {keep_rate:.1f}% ({kept}/{len(scope_df)}) | 미통과 {lost}건. 미통과군 성과가 좋으면 하드필터 금지, 라벨/우선순위만 사용합니다.')
            except Exception:
                pass
        add_scope(df, '전체 선택군')
        practical = df[mode.isin(['LP', 'L', 'S', 'A', 'G', 'IT'])]
        add_scope(practical, '단기 실전군(LP/L/S/A/G/IT)')
        lp = df[mode.eq('LP')]
        add_scope(lp, 'LP 눌림재지지')
        s = df[mode.eq('S')]
        add_scope(s, 'S 응축형')
        a = df[mode.eq('A')]
        add_scope(a, 'A 보조돌파')
        lines.append('')
        lines.append('- 판정 기준: ST30 통과군이 기존 전체보다 손절선행을 낮추고 +3/+5 선행을 높이면 가점 태그로 승격합니다. 반대로 미통과군도 성과가 좋으면 필터로 쓰지 않고 카드 보조 설명만 유지합니다.')
        return '\n'.join(lines)
    except Exception as e:
        return f'[ST30-RECLAIM 비교 리포트 오류] {type(e).__name__}: {e}'


def _v44915_st30_compact_lines(df: pd.DataFrame) -> list[str]:
    """텔레그램 압축 요약에 들어갈 ST30 핵심 비교."""
    lines = []
    try:
        if df is None or df.empty or 'st30_reclaim_pass' not in df.columns:
            return lines
        mode = df['mode'].astype(str) if 'mode' in df.columns else pd.Series('', index=df.index)
        pass_m = _bt_mask_st30_reclaim(df)
        weekly_m = _bt_mask_st30_weekly(df) & pass_m
        reclaim_only_m = pass_m & (~weekly_m)
        wait_m = _bt_mask_st30_wait(df)
        practical = df[mode.isin(['LP', 'L', 'S', 'A', 'G', 'IT'])]
        lines.append('[🧪 ST30-RECLAIM A/B 비교]')
        lines.append('- 이번 버전은 ST30을 하드필터로 적용하지 않고, 기존 후보 전체 vs ST30 통과군 vs 미통과군 성과를 비교합니다.')
        lines.append(_v439_short_trade_line(df, '기존 전체'))
        lines.append(_v439_short_trade_line(df[pass_m], '✅ ST30-RECLAIM 통과 전체'))
        lines.append(_v439_short_trade_line(df[reclaim_only_m], '✅ ST30 일봉만 통과'))
        lines.append(_v439_short_trade_line(df[weekly_m], '✅ ST30-WEEKLY 주봉확인'))
        lines.append(_v439_short_trade_line(df[~pass_m], '❌ ST30 미통과'))
        if not practical.empty:
            pi = practical.index
            lines.append(_v439_short_trade_line(practical, '단기 실전군 기존'))
            lines.append(_v439_short_trade_line(practical[pass_m.loc[pi]], '단기 실전군 ST30 통과'))
            lines.append(_v439_short_trade_line(practical[weekly_m.loc[pi]], '단기 실전군 ST30-WEEKLY'))
            lines.append(_v439_short_trade_line(practical[wait_m.loc[pi]], '단기 실전군 ST30 대기'))
        lines.append(f"- 통과율: 전체 {int(pass_m.sum())}/{len(df)}건, 단기군 {int(pass_m.loc[practical.index].sum()) if not practical.empty else 0}/{len(practical)}건. 미통과군 성과가 좋으면 필터가 아니라 보조 라벨로만 씁니다.")
        lines.append('- v4.4.9.16 보정: ST30-WEEKLY는 ST30 통과군 중 주봉 MACD 히스토그램 개선이 확인된 하위군으로 따로 계산합니다.')
    except Exception as e:
        lines.append(f'[ST30 압축요약 오류] {type(e).__name__}: {e}')
    return lines


# =============================================================
# v4.4.9.17 S-SAFE ST30 DRILLDOWN
# =============================================================
def _v44916_bool_mask(df: pd.DataFrame, default: bool = False) -> pd.Series:
    return pd.Series(bool(default), index=df.index if df is not None else [], dtype=bool)


def _v44916_str_series(df: pd.DataFrame, col: str, default: str = '') -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=str)
    if col not in df.columns:
        return pd.Series(str(default), index=df.index, dtype=str)
    return df[col].fillna(default).astype(str)


def _v44916_num_series(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=float)
    if col not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors='coerce').fillna(default)


def _v44916_pattern_masks(df: pd.DataFrame) -> list[tuple[str, pd.Series, str]]:
    """ST30을 패턴별로 교차검증하기 위한 운용 핵심 마스크.

    반환: (라벨, 마스크, stat_type)
    stat_type='short'는 3/5 익절·손절, 'i'는 I-MAIN 20/40/60 지표를 사용한다.
    """
    if df is None or df.empty:
        return []
    idx = df.index
    mode = _v44916_str_series(df, 'mode')
    amount = _v44916_num_series(df, 'leader_gap_amount_b', np.nan).where(
        _v44916_num_series(df, 'leader_gap_amount_b', np.nan).notna(),
        _v44916_num_series(df, 'amount_b', 0)
    ).fillna(_v44916_num_series(df, 'amount_b', 0))
    close_loc = _v449_close_loc_series(df) if callable(globals().get('_v449_close_loc_series')) else _v44916_num_series(df, 'close_loc_pct', 0)
    vol = _v449_vol_ratio_series(df) if callable(globals().get('_v449_vol_ratio_series')) else _v44916_num_series(df, 'vol_ratio', 0)
    rr = _v449_rr_series(df) if callable(globals().get('_v449_rr_series')) else _v44916_num_series(df, 'rr', 0)
    risk_txt = _v449_safe_contains_series(df, ['risk_tags', 'tags', 'reason', 'final_reason', 'comment'], r'RISK|위험|과열|종가위치약함|RR낮음|RR과대') if callable(globals().get('_v449_safe_contains_series')) else _v44916_bool_mask(df, False)

    lp_all = mode.eq('LP')
    lp_class = _v44916_str_series(df, 'lp_class')
    lp_timing = _v44916_str_series(df, 'lp_timing_bucket')
    lp_safe = lp_all & (lp_class.eq('LP-SAFE') | (~lp_class.ne('').any() and lp_all))
    lp_d1 = lp_safe & lp_timing.eq('LP-D1')
    lp_d23 = lp_safe & lp_timing.eq('LP-D23')
    lp_d45 = lp_safe & lp_timing.eq('LP-D45')

    def _safe_existing_mask(fn_name: str, fallback: pd.Series) -> pd.Series:
        try:
            fn = globals().get(fn_name)
            if callable(fn):
                m = fn(df)
                if isinstance(m, pd.Series):
                    return m.reindex(idx).fillna(False).astype(bool)
        except Exception:
            pass
        return fallback.reindex(idx).fillna(False).astype(bool)

    l_all = _safe_existing_mask('_bt_mask_leader_gap_all', mode.eq('L'))
    l_5000 = _safe_existing_mask('_bt_mask_leader_gap_core_amount', mode.eq('L') & amount.ge(5000))
    l_core_cell = l_5000 & close_loc.ge(70)

    s2_fallback = mode.eq('S') & _v44916_str_series(df, 's_type').eq('S2')
    s2 = _safe_existing_mask('_bt_mask_s2', s2_fallback)
    s_safe = _safe_existing_mask('_bt_mask_s_core_safe', mode.eq('S') & close_loc.ge(70) & rr.between(1.0, 1.5))
    s_neutral = _safe_existing_mask('_bt_mask_s_core_neutral', mode.eq('S') & (~s_safe))

    # A-CONFIRM / A-RETEST CORE②는 기존 v4.4.9.13 검증 로직과 같은 프록시를 사용한다.
    try:
        a_retest = _v449_mask_a_retest(df) if callable(globals().get('_v449_mask_a_retest')) else mode.eq('A')
        a_safe = _v449_mask_a_retest_safe(df) if callable(globals().get('_v449_mask_a_retest_safe')) else a_retest
        next_mask, _ = _v4493_next_confirm_proxy(df, a_safe) if callable(globals().get('_v4493_next_confirm_proxy')) else (_v44916_bool_mask(df), '')
        a_core2 = a_retest & amount.ge(5000) & close_loc.ge(80) & vol.le(1.8) & rr.between(0.8, 1.5) & (~risk_txt)
        a_confirm = next_mask | _v44916_num_series(df, 'a_confirm_live', 0).astype(int).eq(1)
    except Exception:
        a_core2 = _v44916_bool_mask(df)
        a_confirm = _v44916_bool_mask(df)

    it_accel = mode.eq('IT') & _v44916_str_series(df, 'i_trigger_class').eq('ACCEL')

    try:
        i_en = _i_main_enriched_df(df) if callable(globals().get('_i_main_enriched_df')) else df.copy()
        i_main_idx = i_en.index
        i_accel = pd.Series(False, index=idx)
        i_core = pd.Series(False, index=idx)
        i_main = pd.Series(False, index=idx)
        if len(i_en) > 0:
            i_accel.loc[i_main_idx] = _v44916_num_series(i_en, 'imain_accel', 0).astype(int).eq(1).values
            i_core.loc[i_main_idx] = _v44916_num_series(i_en, 'imain_core', 0).astype(int).eq(1).values
            i_main.loc[i_main_idx] = _v44916_num_series(i_en, 'imain_is_main', 0).astype(int).eq(1).values
    except Exception:
        i_accel = _v44916_bool_mask(df)
        i_core = _v44916_bool_mask(df)
        i_main = _v44916_bool_mask(df)

    scopes = [
        ('LP-SAFE 전체', lp_safe, 'short'),
        ('LP-D1 공격타점', lp_d1, 'short'),
        ('LP-D23 핵심타점', lp_d23, 'short'),
        ('LP-D45 안정타점', lp_d45, 'short'),
        ('L 리더갭 전체', l_all, 'short'),
        ('L 5000억+ 핵심', l_5000, 'short'),
        ('L 5000억+·종가70+', l_core_cell, 'short'),
        ('S-SAFE', s_safe, 'short'),
        ('S-NEUTRAL', s_neutral, 'short'),
        ('S2 실행형', s2, 'short'),
        ('A-CONFIRM 확인형', a_confirm, 'short'),
        ('A-RETEST CORE②', a_core2, 'short'),
        ('IT-ACCEL', it_accel, 'i'),
        ('I-MAIN 전체', i_main, 'i'),
        ('I-MAIN ACCEL', i_accel, 'i'),
        ('I-MAIN CORE', i_core, 'i'),
    ]
    out = []
    for label, mask, stat_type in scopes:
        try:
            m = mask.reindex(idx).fillna(False).astype(bool)
            if int(m.sum()) > 0:
                out.append((label, m, stat_type))
        except Exception:
            continue
    return out


def _v44916_scope_stat_line(sub: pd.DataFrame, label: str, stat_type: str = 'short') -> str:
    if stat_type == 'i':
        return _v439_i_line(sub, label)
    return _v439_short_trade_line(sub, label)


def _v44916_delta_text(base: pd.DataFrame, passed: pd.DataFrame, failed: pd.DataFrame, stat_type: str = 'short') -> str:
    try:
        if base is None or len(base) <= 0 or passed is None or len(passed) <= 0:
            return ''
        if stat_type == 'i':
            b = _v439_mean(base, 'i_ret_close_60d')
            p = _v439_mean(passed, 'i_ret_close_60d')
            f = _v439_mean(failed, 'i_ret_close_60d') if failed is not None and len(failed) else np.nan
            return f'  ↳ 60d 변화: 통과-기존 {p-b:+.2f}%p' + (f' / 미통과 {f:.2f}%' if not pd.isna(f) else '')
        b_pnl = _v439_mean(base, 'rule35_pnl')
        p_pnl = _v439_mean(passed, 'rule35_pnl')
        b_stop = _v439_rate(base, 'stop_before_3') if 'stop_before_3' in base.columns else _v439_rate(base, 'rule35_stop')
        p_stop = _v439_rate(passed, 'stop_before_3') if 'stop_before_3' in passed.columns else _v439_rate(passed, 'rule35_stop')
        f_pnl = _v439_mean(failed, 'rule35_pnl') if failed is not None and len(failed) else np.nan
        return f'  ↳ 변화: 3/5 {p_pnl-b_pnl:+.2f}%p / 손절 {p_stop-b_stop:+.1f}%p' + (f' / 미통과 3/5 {f_pnl:.2f}%' if not pd.isna(f_pnl) else '')
    except Exception:
        return ''


def _v44916_missed_winner_line(sub: pd.DataFrame, label: str, fail_m: pd.Series, stat_type: str = 'short', sample_n: int = 3) -> str:
    """ST30 미통과인데도 성공한 종목이 얼마나 있는지 보여준다.
    이 숫자가 크면 하드필터 금지 근거가 된다.
    """
    try:
        if sub is None or sub.empty:
            return ''
        fm = fail_m.reindex(sub.index).fillna(False).astype(bool)
        missed = sub[fm].copy()
        if missed.empty:
            return f'  ↳ ST30 미통과 성공누락: 0건'
        if stat_type == 'i':
            success = missed[_v44916_num_series(missed, 'i_ret_close_60d', 0).gt(10)]
            score_col = 'i_ret_close_60d'
            metric = '+10/60d'
        else:
            hit3 = _v44916_num_series(missed, 'hit3_before_stop', 0).astype(float).gt(0)
            pnl_ok = _v44916_num_series(missed, 'rule35_pnl', 0).astype(float).gt(1.2)
            stop_first = _v44916_num_series(missed, 'stop_before_3', 0).astype(float).gt(0)
            success = missed[(hit3 | pnl_ok) & (~stop_first)]
            score_col = 'rule35_pnl'
            metric = '+3/3·5성공'
        if success.empty:
            return f'  ↳ ST30 미통과 성공누락: 0건'
        sample_cols = []
        for c in ['signal_date', 'date', '종목명', 'name', 'code', 'ticker', score_col]:
            if c in success.columns:
                sample_cols.append(c)
        names = []
        try:
            tmp = success.copy()
            if score_col in tmp.columns:
                tmp['_sort_score'] = pd.to_numeric(tmp[score_col], errors='coerce').fillna(0)
                tmp = tmp.sort_values('_sort_score', ascending=False)
            for _, r in tmp.head(sample_n).iterrows():
                nm = str(r.get('name', r.get('종목명', '')) or '').strip()
                cd = str(r.get('code', r.get('ticker', '')) or '').strip()
                dt = str(r.get('signal_date', r.get('date', '')) or '')[:10]
                sc = _safe_float(r.get(score_col, 0), 0.0)
                label_name = (nm or cd or '종목')
                if cd and nm:
                    label_name = f'{nm}({cd})'
                names.append(f'{dt} {label_name} {sc:.2f}%')
        except Exception:
            names = []
        tail = (' | 예: ' + ', '.join(names)) if names else ''
        return f'  ↳ ST30 미통과 성공누락: {len(success)}/{len(sub)}건({metric}) — 하드필터 금지 근거{tail}'
    except Exception as e:
        return f'  ↳ ST30 미통과 성공누락 계산오류 {type(e).__name__}'


def _v44916_pattern_cross_audit_lines(df: pd.DataFrame, compact: bool = True) -> list[str]:
    """패턴별 ST30 통과/미통과 교차검증.

    핵심 목적:
    - ST30이 LP/L/S/A/I 각각에서 실제로 도움이 되는지 확인한다.
    - ST30 미통과 성공 종목을 같이 보여 하드필터로 좋은 종목을 놓치는지 감사한다.
    """
    lines: list[str] = []
    try:
        if df is None or df.empty or 'st30_reclaim_pass' not in df.columns:
            return lines
        pass_m = _bt_mask_st30_reclaim(df)
        weekly_m = _bt_mask_st30_weekly(df) & pass_m
        fail_m = ~pass_m
        wait_m = _bt_mask_st30_wait(df)
        scopes = _v44916_pattern_masks(df)
        if not scopes:
            return lines
        lines.append('[🔬 ST30 패턴별 교차검증 — 하드필터 금지 감사 v4.4.9.16]')
        lines.append('- 목적: ST30을 적용하면 좋은 종목을 놓치는지 LP/L/S/A/I 패턴별로 확인합니다. ST30은 기본적으로 제외 필터가 아니라 가점·비중조절·추격금지 태그입니다.')
        max_scopes = _env_int('CLOSING_BET_ST30_PATTERN_AUDIT_MAX_SCOPES', '10' if compact else '20')
        min_n = _env_int('CLOSING_BET_ST30_PATTERN_AUDIT_MIN_N', '3')
        shown = 0
        for label, mask, stat_type in scopes:
            sub = df[mask].copy()
            if len(sub) < min_n:
                continue
            pi = sub.index
            psub = sub[pass_m.loc[pi]]
            wsub = sub[weekly_m.loc[pi]]
            fsub = sub[fail_m.loc[pi]]
            waitsub = sub[wait_m.loc[pi]]
            lines.append(f'\n[{label}]')
            lines.append(_v44916_scope_stat_line(sub, '기존', stat_type))
            lines.append(_v44916_scope_stat_line(psub, '✅ ST30 통과', stat_type))
            if len(wsub) > 0:
                lines.append(_v44916_scope_stat_line(wsub, '✅ ST30-WEEKLY', stat_type))
            if len(waitsub) > 0 and not compact:
                lines.append(_v44916_scope_stat_line(waitsub, '🟡 ST30-WAIT', stat_type))
            lines.append(_v44916_scope_stat_line(fsub, '❌ ST30 미통과', stat_type))
            delta = _v44916_delta_text(sub, psub, fsub, stat_type)
            if delta:
                lines.append(delta)
            lines.append(_v44916_missed_winner_line(sub, label, fail_m, stat_type, sample_n=2 if compact else 4))
            try:
                keep = int(pass_m.loc[pi].sum())
                lines.append(f'  ↳ 통과율 {keep}/{len(sub)}건({keep/max(len(sub),1)*100:.1f}%)')
            except Exception:
                pass
            shown += 1
            if shown >= max_scopes:
                break
        lines.append('')
        lines.append('- 운용판정: ST30 통과군이 손절을 낮추면 비중/우선순위 가점, 미통과군 성공누락이 있으면 하드필터 금지입니다. LP/L 핵심 후보는 ST30 미통과라도 제외하지 않고 지정가·소액·추격금지로 낮춥니다.')
    except Exception as e:
        lines.append(f'[ST30 패턴별 교차검증 오류] {type(e).__name__}: {e}')
    return lines


# =============================================================
# v4.4.9.17 S-SAFE ST30 PROFIT QUALITY / MISSED WINNER DRILLDOWN
# =============================================================
def _v44917_s_safe_st30_drilldown_lines(df: pd.DataFrame, compact: bool = True) -> list[str]:
    """S-SAFE에서 ST30 통과 4건의 수익이 의미 있는지, 미통과 성공 종목을 놓치는지 감사한다.

    핵심:
    - rule35_pnl은 +3/+5/손절 선행 규칙 결과라 +4.00%가 실제 최고수익률 자체를 뜻하지 않을 수 있다.
    - 따라서 ret_max_high_hd/path_max_high_ret, ret_close_hd, +3/+5 도달일, drawdown을 함께 본다.
    - ST30 미통과 성공 종목을 개별로 보여 하드필터 위험을 확인한다.
    """
    lines: list[str] = []
    try:
        if df is None or df.empty or 'st30_reclaim_pass' not in df.columns:
            return lines
        try:
            s_safe_mask = _bt_mask_s_core_safe(df)
        except Exception:
            mode = _v44916_str_series(df, 'mode') if callable(globals().get('_v44916_str_series')) else df.get('mode', pd.Series('', index=df.index)).astype(str)
            close_loc = _v449_close_loc_series(df) if callable(globals().get('_v449_close_loc_series')) else pd.to_numeric(df.get('close_loc_pct', 0), errors='coerce').fillna(0)
            rr = _v449_rr_series(df) if callable(globals().get('_v449_rr_series')) else pd.to_numeric(df.get('rr', 0), errors='coerce').fillna(0)
            s_safe_mask = mode.eq('S') & close_loc.ge(70) & rr.between(1.0, 1.5)
        s_safe_mask = s_safe_mask.reindex(df.index).fillna(False).astype(bool)
        sub = df[s_safe_mask].copy()
        if sub.empty:
            return lines

        pass_m = _bt_mask_st30_reclaim(df).reindex(sub.index).fillna(False).astype(bool)
        weekly_m = (_bt_mask_st30_weekly(df) & _bt_mask_st30_reclaim(df)).reindex(sub.index).fillna(False).astype(bool)
        fail_m = ~pass_m
        passed = sub[pass_m].copy()
        weekly = sub[weekly_m].copy()
        failed = sub[fail_m].copy()

        def _nser(xdf: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
            if xdf is None or xdf.empty:
                return pd.Series(dtype=float)
            return pd.to_numeric(xdf.get(col, default), errors='coerce').fillna(default)

        def _actual_max_col(xdf: pd.DataFrame) -> str:
            for c in ['path_max_high_ret', 'ret_max_high_hd', 'ret_max_high_3d', 'ret_next_high']:
                if c in xdf.columns:
                    return c
            return 'rule35_pnl'

        def _drawdown_col(xdf: pd.DataFrame) -> str:
            for c in ['path_pre_plus3_min_low_ret', 'path_first3d_min_low_ret', 'path_min_low_ret', 'ret_min_low_hd', 'ret_next_low']:
                if c in xdf.columns:
                    return c
            return 'rule35_pnl'

        def _dist_line(xdf: pd.DataFrame, label: str) -> str:
            cnt = len(xdf) if xdf is not None else 0
            if cnt <= 0:
                return f'- {label}: 0건'
            pnl = _nser(xdf, 'rule35_pnl', 0)
            max_col = _actual_max_col(xdf)
            maxr = _nser(xdf, max_col, 0)
            close_col = 'ret_close_hd' if 'ret_close_hd' in xdf.columns else ('ret_next_close' if 'ret_next_close' in xdf.columns else 'rule35_pnl')
            closer = _nser(xdf, close_col, 0)
            dd_col = _drawdown_col(xdf)
            dd = _nser(xdf, dd_col, 0)
            hit3 = _nser(xdf, 'hit3_before_stop', 0).gt(0).mean() * 100.0 if 'hit3_before_stop' in xdf.columns else 0.0
            hit5 = _nser(xdf, 'hit5_before_stop', 0).gt(0).mean() * 100.0 if 'hit5_before_stop' in xdf.columns else 0.0
            stop = _nser(xdf, 'stop_before_3', 0).gt(0).mean() * 100.0 if 'stop_before_3' in xdf.columns else 0.0
            return (f'- {label}: {cnt}건 | 3/5 평균/중앙/최고 {pnl.mean():.2f}/{pnl.median():.2f}/{pnl.max():.2f}% | '
                    f'실제최대상승({max_col}) 평균/중앙/최고 {maxr.mean():.2f}/{maxr.median():.2f}/{maxr.max():.2f}% | '
                    f'종가수익({close_col}) 평균 {closer.mean():.2f}% | +3 {hit3:.1f}% / +5 {hit5:.1f}% / 손절 {stop:.1f}% | '
                    f'진입후흔들림({dd_col}) 중앙 {dd.median():.2f}%')

        def _fmt_row(r: pd.Series) -> str:
            dt = str(r.get('signal_date', r.get('date', '')) or '')[:10]
            nm = str(r.get('name', r.get('종목명', '')) or '').strip()
            cd = str(r.get('code', r.get('ticker', '')) or '').strip()
            title = nm or cd or '종목'
            if nm and cd:
                title = f'{nm}({cd})'
            pnl = _safe_float(r.get('rule35_pnl', 0), 0.0)
            max_col = _actual_max_col(pd.DataFrame([r]))
            maxr = _safe_float(r.get(max_col, r.get('ret_max_high_hd', 0)), 0.0)
            close_col = 'ret_close_hd' if 'ret_close_hd' in r.index else ('ret_next_close' if 'ret_next_close' in r.index else 'rule35_pnl')
            closer = _safe_float(r.get(close_col, 0), 0.0)
            h3d = int(_safe_float(r.get('path_first_plus3_day', 0), 0)) if 'path_first_plus3_day' in r.index else 0
            h5d = int(_safe_float(r.get('path_first_plus5_day', 0), 0)) if 'path_first_plus5_day' in r.index else 0
            first_event = str(r.get('first_event', r.get('rule35_exit', '')) or '')
            dd_col = _drawdown_col(pd.DataFrame([r]))
            dd = _safe_float(r.get(dd_col, 0), 0.0)
            st30 = str(r.get('st30_label', '')) or ('✅ ST30' if _safe_float(r.get('st30_reclaim_pass', 0), 0) else '❌ ST30')
            return f'- {dt} {title} | {st30} | 3/5 {pnl:.2f}% | 최대 {maxr:.2f}% | 종가 {closer:.2f}% | +3일 {h3d} / +5일 {h5d} | 흔들림 {dd:.2f}% | {first_event}'

        # 미통과 성공/실패 분리
        if not failed.empty:
            hit3 = _nser(failed, 'hit3_before_stop', 0).gt(0) if 'hit3_before_stop' in failed.columns else _nser(failed, 'rule35_pnl', 0).gt(1.2)
            stop_first = _nser(failed, 'stop_before_3', 0).gt(0) if 'stop_before_3' in failed.columns else _nser(failed, 'rule35_pnl', 0).lt(0)
            pnl_ok = _nser(failed, 'rule35_pnl', 0).gt(1.2)
            missed_win = failed[(hit3 | pnl_ok) & (~stop_first)].copy()
            missed_bad = failed[stop_first | _nser(failed, 'rule35_pnl', 0).lt(0)].copy()
        else:
            missed_win = failed.copy()
            missed_bad = failed.copy()

        lines.append('[🧪 S-SAFE × ST30 심층감사 — 수익 질/누락종목 점검 v4.4.9.17]')
        lines.append('- 목적: S-SAFE에서 ST30 통과 4건의 +4.00%가 실제로 의미 있는 수익인지, ST30 미통과 중 괜찮은 종목을 같이 걸러버리는지 개별 종목 단위로 확인합니다.')
        lines.append('- 주의: 3/5 수익률(rule35_pnl)은 +3/+5/손절 선행 실전규칙 결과라 +4.00%가 실제 최고상승률 그 자체는 아닐 수 있습니다. 실제최대상승·종가수익·흔들림을 같이 봅니다.')
        lines.append(_dist_line(sub, 'S-SAFE 전체'))
        lines.append(_dist_line(passed, '✅ ST30 통과'))
        if len(weekly) > 0:
            lines.append(_dist_line(weekly, '✅ ST30-WEEKLY'))
        lines.append(_dist_line(failed, '❌ ST30 미통과'))
        lines.append(_dist_line(missed_win, '❌ 미통과지만 성공한 누락후보'))
        lines.append(_dist_line(missed_bad, '❌ 미통과 실패/손절후보'))
        lines.append(f'- 누락감사: S-SAFE {len(sub)}건 중 ST30 통과 {len(passed)}건, 미통과 {len(failed)}건, 미통과 성공누락 {len(missed_win)}건, 미통과 실패/손절 {len(missed_bad)}건')

        # 표본 적을 때 자동 판정
        if len(passed) < 10:
            lines.append('- 판정주의: ST30 통과군이 10건 미만이면 “승격 확정”이 아니라 반복검증 후보입니다.')
        if len(missed_win) > 0:
            lines.append('- 하드필터 판정: 미통과 성공누락이 존재하므로 S-SAFE에서도 즉시 완전 제외보다는 “비중강등/관망”이 우선입니다.')
        else:
            lines.append('- 하드필터 판정: 이번 표본에서는 미통과 성공누락이 없지만, 표본 누적 전까지는 관찰 태그로 유지합니다.')

        # 개별 샘플
        show_pass_n = _env_int('CLOSING_BET_SSAFE_ST30_PASS_DETAIL_N', '8' if compact else '20')
        show_miss_n = _env_int('CLOSING_BET_SSAFE_ST30_MISSED_DETAIL_N', '8' if compact else '20')
        sort_col = 'ret_max_high_hd' if 'ret_max_high_hd' in sub.columns else ('path_max_high_ret' if 'path_max_high_ret' in sub.columns else 'rule35_pnl')
        if not passed.empty:
            lines.append('  · ST30 통과 개별:')
            tmp = passed.copy()
            tmp['_sort'] = _nser(tmp, sort_col, 0)
            for _, r in tmp.sort_values('_sort', ascending=False).head(show_pass_n).iterrows():
                lines.append('    ' + _fmt_row(r))
        if not missed_win.empty:
            lines.append('  · ST30 미통과 성공누락 개별:')
            tmp = missed_win.copy()
            tmp['_sort'] = _nser(tmp, sort_col, 0)
            for _, r in tmp.sort_values('_sort', ascending=False).head(show_miss_n).iterrows():
                lines.append('    ' + _fmt_row(r))
        if not missed_bad.empty and not compact:
            lines.append('  · ST30 미통과 실패/손절 개별:')
            tmp = missed_bad.copy()
            tmp['_sort'] = _nser(tmp, 'rule35_pnl', 0)
            for _, r in tmp.sort_values('_sort', ascending=True).head(show_miss_n).iterrows():
                lines.append('    ' + _fmt_row(r))
        lines.append('- 운용결론 후보: S-SAFE + ST30 통과는 우선순위 가점, S-SAFE + ST30 미통과는 바로 제외가 아니라 성공누락 여부를 보고 소액/관망/다음날확인으로 나눕니다.')
    except Exception as e:
        lines.append(f'[S-SAFE ST30 심층감사 오류] {type(e).__name__}: {e}')
    return lines


# =============================================================
# v4.4.9.18 S-SAFE ST30 MISS SUCCESS / FAIL AUDIT
# =============================================================
def _v44918_s_safe_miss_success_fail_audit_lines(df: pd.DataFrame, compact: bool = True) -> list[str]:
    """S-SAFE에서 ST30 미통과 성공군과 실패군을 나누는 실전 특징을 비교한다.

    목적:
    - v4.4.9.17에서 S-SAFE 미통과 성공누락이 많아 ST30 하드필터 금지가 확인되었다.
    - 이번 버전은 미통과 성공군 vs 미통과 실패/손절군의 거래대금, 거래량비, 종가위치, RR, 캔들/MA 품질을 비교해
      '미통과라도 살릴 후보'와 '미통과 중 위험 후보'를 나누는 다음 규칙 후보를 찾는다.
    """
    lines: list[str] = []
    try:
        if df is None or df.empty or 'st30_reclaim_pass' not in df.columns:
            return lines
        try:
            s_safe_mask = _bt_mask_s_core_safe(df)
        except Exception:
            mode = _v44916_str_series(df, 'mode') if callable(globals().get('_v44916_str_series')) else df.get('mode', pd.Series('', index=df.index)).astype(str)
            close_loc = _v449_close_loc_series(df) if callable(globals().get('_v449_close_loc_series')) else pd.to_numeric(df.get('close_loc_pct', 0), errors='coerce').fillna(0)
            rr = _v449_rr_series(df) if callable(globals().get('_v449_rr_series')) else pd.to_numeric(df.get('rr', 0), errors='coerce').fillna(0)
            s_safe_mask = mode.eq('S') & close_loc.ge(70) & rr.between(1.0, 1.5)
        s_safe_mask = s_safe_mask.reindex(df.index).fillna(False).astype(bool)
        sub = df[s_safe_mask].copy()
        if sub.empty:
            return lines

        pass_m_all = _bt_mask_st30_reclaim(df).reindex(df.index).fillna(False).astype(bool)
        miss = sub[(~pass_m_all).reindex(sub.index).fillna(False)].copy()
        if miss.empty:
            return lines

        def _nser(xdf: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
            if xdf is None or xdf.empty:
                return pd.Series(dtype=float)
            if col in xdf.columns:
                return pd.to_numeric(xdf[col], errors='coerce').fillna(default)
            return pd.Series(default, index=xdf.index, dtype=float)

        hit3 = _nser(miss, 'hit3_before_stop', 0).gt(0) if 'hit3_before_stop' in miss.columns else _nser(miss, 'rule35_pnl', 0).gt(1.2)
        stop_first = _nser(miss, 'stop_before_3', 0).gt(0) if 'stop_before_3' in miss.columns else _nser(miss, 'rule35_pnl', 0).lt(0)
        pnl_ok = _nser(miss, 'rule35_pnl', 0).gt(1.2)
        miss_win = miss[(hit3 | pnl_ok) & (~stop_first)].copy()
        miss_bad = miss[stop_first | _nser(miss, 'rule35_pnl', 0).lt(0)].copy()
        if miss_win.empty and miss_bad.empty:
            return lines

        # 전체 df 기준으로 계산해야 fallback close_loc/amount 함수가 안정적으로 동작한다.
        def _whole_series(kind: str) -> pd.Series:
            try:
                if kind == 'amount':
                    if callable(globals().get('_v449_base_liquidity_series')):
                        return _v449_base_liquidity_series(df).reindex(df.index).fillna(0)
                    return _nser(df, 'amount_b', 0)
                if kind == 'close_loc':
                    if callable(globals().get('_v449_close_loc_series')):
                        return _v449_close_loc_series(df).reindex(df.index).fillna(0)
                    return _nser(df, 'close_loc_pct', 0)
                if kind == 'vol':
                    if callable(globals().get('_v449_vol_ratio_series')):
                        return _v449_vol_ratio_series(df).reindex(df.index).fillna(0)
                    return _nser(df, 'vol_ratio', 0)
                if kind == 'rr':
                    if callable(globals().get('_v449_rr_series')):
                        return _v449_rr_series(df).reindex(df.index).fillna(0)
                    return _nser(df, 'rr', 0)
            except Exception:
                pass
            return pd.Series(0.0, index=df.index, dtype=float)

        amount_all = _whole_series('amount')
        close_loc_all = _whole_series('close_loc')
        vol_all = _whole_series('vol')
        rr_all = _whole_series('rr')

        def _price_series(xdf: pd.DataFrame, *cols: str) -> pd.Series:
            for c in cols:
                if c in xdf.columns:
                    return pd.to_numeric(xdf[c], errors='coerce')
            return pd.Series(np.nan, index=xdf.index, dtype=float)

        def _wick_series(xdf: pd.DataFrame) -> pd.Series:
            for c in ['upper_wick_pct', 'wick_pct', '_upper_wick_pct']:
                if c in xdf.columns:
                    return pd.to_numeric(xdf[c], errors='coerce').fillna(0)
            op = _price_series(xdf, '_open', 'Open', 'open')
            cl = _price_series(xdf, '_close', 'Close', 'close')
            hi = _price_series(xdf, '_high', 'High', 'high')
            lo = _price_series(xdf, '_low', 'Low', 'low')
            rng = (hi - lo).replace(0, np.nan)
            return ((hi - pd.concat([op, cl], axis=1).max(axis=1)) / rng * 100.0).replace([np.inf, -np.inf], np.nan).fillna(0)

        def _above_ma_series(xdf: pd.DataFrame, ma_cols: list[str]) -> pd.Series | None:
            cl = _price_series(xdf, '_close', 'Close', 'close')
            ma = None
            for c in ma_cols:
                if c in xdf.columns:
                    ma = pd.to_numeric(xdf[c], errors='coerce')
                    break
            if ma is None or ma.notna().sum() == 0 or cl.notna().sum() == 0:
                return None
            return cl.ge(ma).fillna(False)

        def _pct(mask: pd.Series | None) -> float:
            try:
                if mask is None or len(mask) == 0:
                    return 0.0
                return float(mask.mean() * 100.0)
            except Exception:
                return 0.0

        def _feat_line(xdf: pd.DataFrame, label: str) -> str:
            n = len(xdf) if xdf is not None else 0
            if n <= 0:
                return f'- {label}: 0건'
            ix = xdf.index
            amount = amount_all.reindex(ix).fillna(0)
            close_loc = close_loc_all.reindex(ix).fillna(0)
            vol = vol_all.reindex(ix).fillna(0)
            rr = rr_all.reindex(ix).fillna(0)
            op = _price_series(xdf, '_open', 'Open', 'open')
            cl = _price_series(xdf, '_close', 'Close', 'close')
            bullish = cl.ge(op).fillna(False) if cl.notna().any() and op.notna().any() else None
            wick = _wick_series(xdf)
            ma5 = _above_ma_series(xdf, ['MA5', 'ma5', '_ma5'])
            ma10 = _above_ma_series(xdf, ['MA10', 'ma10', '_ma10'])
            ma20 = _above_ma_series(xdf, ['MA20', 'ma20', '_ma20'])
            extra = []
            if bullish is not None:
                extra.append(f'양봉 {_pct(bullish):.1f}%')
            if wick.notna().sum() > 0:
                extra.append(f'윗꼬리중앙 {wick.median():.1f}%/20↓ {_pct(wick.le(20)):.1f}%')
            ma_bits = []
            if ma5 is not None: ma_bits.append(f'5MA위 {_pct(ma5):.1f}%')
            if ma10 is not None: ma_bits.append(f'10MA위 {_pct(ma10):.1f}%')
            if ma20 is not None: ma_bits.append(f'20MA위 {_pct(ma20):.1f}%')
            if ma_bits:
                extra.append('·'.join(ma_bits))
            return (
                f'- {label}: {n}건 | '
                f'대금 중앙 {amount.median():.0f}억/3000+ {_pct(amount.ge(3000)):.1f}%/5000+ {_pct(amount.ge(5000)):.1f}% | '
                f'거래량비 중앙 {vol.median():.2f}/1.2~1.8 {_pct(vol.between(1.2, 1.8)):.1f}%/2.5+ {_pct(vol.ge(2.5)):.1f}% | '
                f'종가위치 중앙 {close_loc.median():.1f}%/80+ {_pct(close_loc.ge(80)):.1f}%/90+ {_pct(close_loc.ge(90)):.1f}% | '
                f'RR 중앙 {rr.median():.2f}/0.8~1.5 {_pct(rr.between(0.8, 1.5)):.1f}%/1.8초과 {_pct(rr.gt(1.8)):.1f}%'
                + (f' | {" | ".join(extra)}' if extra else '')
            )

        def _path_line(xdf: pd.DataFrame, label: str) -> str:
            if xdf is None or xdf.empty:
                return f'- {label}: 0건'
            max_col = 'path_max_high_ret' if 'path_max_high_ret' in xdf.columns else ('ret_max_high_hd' if 'ret_max_high_hd' in xdf.columns else 'rule35_pnl')
            dd_col = 'path_pre_plus3_min_low_ret' if 'path_pre_plus3_min_low_ret' in xdf.columns else ('path_min_low_ret' if 'path_min_low_ret' in xdf.columns else 'rule35_pnl')
            close_col = 'ret_close_hd' if 'ret_close_hd' in xdf.columns else ('ret_next_close' if 'ret_next_close' in xdf.columns else 'rule35_pnl')
            pnl = _nser(xdf, 'rule35_pnl', 0)
            maxr = _nser(xdf, max_col, 0)
            dd = _nser(xdf, dd_col, 0)
            close_ret = _nser(xdf, close_col, 0)
            return f'- {label}: 3/5 평균 {pnl.mean():.2f}% | 최대상승 중앙/평균 {maxr.median():.2f}/{maxr.mean():.2f}% | 종가수익 평균 {close_ret.mean():.2f}% | +3전흔들림 중앙 {dd.median():.2f}%'

        def _fmt_row(r: pd.Series) -> str:
            dt = str(r.get('signal_date', r.get('date', '')) or '')[:10]
            nm = str(r.get('name', r.get('종목명', '')) or '').strip()
            cd = str(r.get('code', r.get('ticker', '')) or '').strip()
            title = f'{nm}({cd})' if nm and cd else (nm or cd or '종목')
            ix = r.name
            amount = float(amount_all.reindex([ix]).fillna(0).iloc[0]) if ix in amount_all.index else 0.0
            close_loc = float(close_loc_all.reindex([ix]).fillna(0).iloc[0]) if ix in close_loc_all.index else 0.0
            vol = float(vol_all.reindex([ix]).fillna(0).iloc[0]) if ix in vol_all.index else 0.0
            rr = float(rr_all.reindex([ix]).fillna(0).iloc[0]) if ix in rr_all.index else 0.0
            pnl = _safe_float(r.get('rule35_pnl', 0), 0.0)
            max_col = 'path_max_high_ret' if 'path_max_high_ret' in r.index else ('ret_max_high_hd' if 'ret_max_high_hd' in r.index else 'rule35_pnl')
            dd_col = 'path_pre_plus3_min_low_ret' if 'path_pre_plus3_min_low_ret' in r.index else ('path_min_low_ret' if 'path_min_low_ret' in r.index else 'rule35_pnl')
            maxr = _safe_float(r.get(max_col, 0), 0.0)
            dd = _safe_float(r.get(dd_col, 0), 0.0)
            st30 = str(r.get('st30_label', '')) or ('✅ ST30' if _safe_float(r.get('st30_reclaim_pass', 0), 0) else '❌ ST30')
            return f'- {dt} {title} | {st30} | 3/5 {pnl:.2f}% | 최대 {maxr:.2f}% | 흔들림 {dd:.2f}% | 대금 {amount:.0f}억 | Vol {vol:.2f} | 종가위치 {close_loc:.0f}% | RR {rr:.2f}'

        lines.append('[🔎 S-SAFE ST30 미통과 성공/실패 비교감사 — v4.4.9.18]')
        lines.append('- 목적: S-SAFE에서 ST30 미통과를 바로 제외하면 성공 종목을 놓치므로, 미통과 성공군과 실패/손절군을 가르는 실전 특징을 비교합니다.')
        lines.append('- 결론 원칙: ST30 미통과는 제외 신호가 아니라 “추세지속형/실패형 분류 필요” 신호입니다.')
        lines.append(f'- 표본: S-SAFE 전체 {len(sub)}건 | ST30 미통과 {len(miss)}건 | 미통과 성공 {len(miss_win)}건 | 미통과 실패/손절 {len(miss_bad)}건')
        lines.append(_path_line(miss_win, '미통과 성공군 성과질'))
        lines.append(_path_line(miss_bad, '미통과 실패/손절군 성과질'))
        lines.append(_feat_line(miss_win, '미통과 성공군 특징'))
        lines.append(_feat_line(miss_bad, '미통과 실패/손절군 특징'))

        # 단순 후보 규칙별 결과. 확정 규칙이 아니라 다음 검증 후보로만 사용한다.
        miss_ix = miss.index
        strong_price = close_loc_all.reindex(miss_ix).fillna(0).ge(80)
        high_liq = amount_all.reindex(miss_ix).fillna(0).ge(3000)
        vol_ok = vol_all.reindex(miss_ix).fillna(0).between(0.01, 1.8)
        rr_ok = rr_all.reindex(miss_ix).fillna(0).between(0.8, 1.8)
        mom_keep = strong_price & high_liq & vol_ok & rr_ok
        weak_risk = close_loc_all.reindex(miss_ix).fillna(0).lt(70) | vol_all.reindex(miss_ix).fillna(0).ge(2.5) | rr_all.reindex(miss_ix).fillna(0).gt(1.8)
        lines.append(_v439_short_trade_line(miss[mom_keep], '후보규칙 S-MOMENTUM WATCH: 미통과·대금3000+·종가80+·Vol≤1.8·RR0.8~1.8'))
        lines.append(_v439_short_trade_line(miss[weak_risk], '후보규칙 S-FAIL RISK: 종가70미만 또는 Vol2.5+ 또는 RR1.8초과'))
        lines.append('- 위 후보규칙은 확정 필터가 아니라 다음 반복검증용입니다. 표본이 10건 미만이면 실시간에서는 설명 라벨만 적용합니다.')

        show_n = _env_int('CLOSING_BET_SSAFE_MISS_AUDIT_DETAIL_N', '8' if compact else '20')
        if not miss_win.empty:
            lines.append('  · ST30 미통과 성공군 상위:')
            tmp = miss_win.copy()
            sort_col = 'path_max_high_ret' if 'path_max_high_ret' in tmp.columns else ('ret_max_high_hd' if 'ret_max_high_hd' in tmp.columns else 'rule35_pnl')
            tmp['_sort'] = _nser(tmp, sort_col, 0)
            for _, r in tmp.sort_values('_sort', ascending=False).head(show_n).iterrows():
                lines.append('    ' + _fmt_row(r))
        if not miss_bad.empty:
            lines.append('  · ST30 미통과 실패/손절군:')
            tmp = miss_bad.copy()
            tmp['_sort'] = _nser(tmp, 'rule35_pnl', 0)
            for _, r in tmp.sort_values('_sort', ascending=True).head(show_n).iterrows():
                lines.append('    ' + _fmt_row(r))
        lines.append('- 운용안: S-SAFE+ST30 통과는 S-RECLAIM PRIME, S-SAFE+ST30 미통과라도 대금1000억+·종가75%+·Vol≤1.8·RR정상이면 S-MOMENTUM PRIME, 그보다 약하면 WATCH/RISK로 분리합니다.')
    except Exception as e:
        lines.append(f'[S-SAFE 미통과 성공/실패 비교감사 오류] {type(e).__name__}: {e}')
    return lines


def _v44919_s_safe_liquidity_rule_repeat_lines(df: pd.DataFrame, compact: bool = True) -> list[str]:
    """v4.4.9.19: S-SAFE/ST30 미통과 후보의 거래대금 반복검증.

    v4.4.9.18에서 ST30 미통과 성공/실패 차이가 주로 거래대금/대표성에서 갈렸으므로,
    S-SAFE 전체와 ST30 미통과군을 거래대금 버킷 및 후보 규칙별로 반복 검증한다.
    ST30은 하드필터가 아니며, 이 함수도 실시간 제외 조건 확정이 아니라 리포트/라벨 검증용이다.
    """
    lines: list[str] = []
    try:
        if df is None or df.empty:
            return lines
        sub = df[_bt_mask_s_core_safe(df)].copy()
        if sub.empty:
            return lines

        def _nser(xdf: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
            if xdf is None or xdf.empty:
                return pd.Series(dtype=float)
            if col in xdf.columns:
                return pd.to_numeric(xdf[col], errors='coerce').fillna(default)
            return pd.Series(default, index=xdf.index, dtype=float)

        def _first_numeric_series(xdf: pd.DataFrame, cols: list[str], default: float = 0.0) -> pd.Series:
            if xdf is None or xdf.empty:
                return pd.Series(dtype=float)
            for c in cols:
                if c in xdf.columns:
                    return pd.to_numeric(xdf[c], errors='coerce').fillna(default)
            return pd.Series(default, index=xdf.index, dtype=float)

        def _close_loc_series(xdf: pd.DataFrame) -> pd.Series:
            for c in ['close_loc_pct', 'close_loc', 'close_location', '종가위치']:
                if c in xdf.columns:
                    return pd.to_numeric(xdf[c], errors='coerce').fillna(0)
            hi = _first_numeric_series(xdf, ['_high', 'High', 'high'], np.nan)
            lo = _first_numeric_series(xdf, ['_low', 'Low', 'low'], np.nan)
            cl = _first_numeric_series(xdf, ['_close', 'Close', 'close'], np.nan)
            rng = (hi - lo).replace(0, np.nan)
            return ((cl - lo) / rng * 100.0).replace([np.inf, -np.inf], np.nan).fillna(0)

        def _pct(mask: pd.Series | None) -> float:
            try:
                if mask is None or len(mask) == 0:
                    return 0.0
                return float(mask.mean() * 100.0)
            except Exception:
                return 0.0

        # 전체 df 기준 feature series를 만들고 sub/miss index로 reindex해서 사용한다.
        amount_all = _first_numeric_series(df, ['amount_b', 'amount', 'amount_100m', '거래대금'], 0)
        close_loc_all = _close_loc_series(df)
        vol_all = _first_numeric_series(df, ['vol_ratio', 'volume_ratio', 'vma_ratio', '거래량비'], 0)
        rr_all = _first_numeric_series(df, ['rr', 'RR', 'risk_reward'], 0)
        marcap_all = _first_numeric_series(df, ['marcap', 'market_cap', '시가총액'], 0)
        is_top = pd.Series(False, index=df.index)
        if 'is_top_mcap' in df.columns:
            is_top = pd.to_numeric(df['is_top_mcap'], errors='coerce').fillna(0).astype(float).gt(0)
        if 'index_label' in df.columns:
            lab = df['index_label'].astype(str)
            is_top = is_top | lab.str.contains('KOSPI200|코스피200|K200', case=False, regex=True, na=False)

        st30_pass = _first_numeric_series(df, ['st30_reclaim_pass'], 0).gt(0)
        if 'st30_label' in df.columns:
            st30_label = df['st30_label'].astype(str)
            st30_pass = st30_pass | st30_label.str.contains('ST30-RECLAIM|ST30-WEEKLY|✅', regex=True, na=False)
            st30_wait = st30_label.str.contains('ST30-WAIT|WAIT|대기', regex=True, na=False)
        else:
            st30_wait = pd.Series(False, index=df.index)

        passed = sub[st30_pass.reindex(sub.index).fillna(False)].copy()
        miss = sub[~st30_pass.reindex(sub.index).fillna(False)].copy()
        wait = sub[st30_wait.reindex(sub.index).fillna(False)].copy()
        no = miss[~st30_wait.reindex(miss.index).fillna(False)].copy() if not miss.empty else miss

        def _bucket_line(source_df: pd.DataFrame, mask: pd.Series, label: str) -> str:
            try:
                return _v439_short_trade_line(source_df[mask.reindex(source_df.index).fillna(False)], label)
            except Exception:
                return f'- {label}: 계산오류'

        def _amount_buckets(source_df: pd.DataFrame, title: str) -> list[str]:
            if source_df is None or source_df.empty:
                return [f'- {title}: 0건']
            ix = source_df.index
            amt = amount_all.reindex(ix).fillna(0)
            return [
                _v439_short_trade_line(source_df[amt.lt(300)], f'{title} <300억'),
                _v439_short_trade_line(source_df[amt.ge(300) & amt.lt(1000)], f'{title} 300~1000억'),
                _v439_short_trade_line(source_df[amt.ge(1000) & amt.lt(3000)], f'{title} 1000~3000억'),
                _v439_short_trade_line(source_df[amt.ge(3000) & amt.lt(5000)], f'{title} 3000~5000억'),
                _v439_short_trade_line(source_df[amt.ge(5000)], f'{title} 5000억+'),
            ]

        def _quality_trade_line(source_df: pd.DataFrame, label: str) -> str:
            """v4.4.9.24: 거래대금 버킷별 단기 성과뿐 아니라 실제 최대상승/종가수익/흔들림을 함께 본다."""
            n = len(source_df) if source_df is not None else 0
            if n <= 0:
                return f'- {label}: 0건'
            base = _v439_short_trade_line(source_df, label)
            def _col(cols: list[str], default: str = 'rule35_pnl') -> str:
                for c in cols:
                    if c in source_df.columns:
                        return c
                return default
            max_col = _col(['path_max_high_ret', 'ret_max_high_hd', 'ret_max_high_3d', 'ret_next_high'])
            close_col = _col(['ret_close_hd', 'ret_next_close'], 'rule35_pnl')
            dd_col = _col(['path_pre_plus3_min_low_ret', 'path_first3d_min_low_ret', 'path_min_low_ret', 'ret_min_low_hd', 'ret_next_low'], 'rule35_pnl')
            def _ncol(col: str) -> pd.Series:
                if col in source_df.columns:
                    return pd.to_numeric(source_df[col], errors='coerce').fillna(0)
                return pd.Series(0.0, index=source_df.index)
            maxr = _ncol(max_col)
            closer = _ncol(close_col)
            dd = _ncol(dd_col)
            return f'{base} | 최대평균/중앙 {maxr.mean():.2f}/{maxr.median():.2f}% | 종가평균 {closer.mean():.2f}% | +3전흔들림중앙 {dd.median():.2f}%'

        def _amount_quality_buckets(source_df: pd.DataFrame, title: str) -> list[str]:
            if source_df is None or source_df.empty:
                return [f'- {title}: 0건']
            ix = source_df.index
            amt = amount_all.reindex(ix).fillna(0)
            return [
                _quality_trade_line(source_df[amt.lt(300)], f'{title} <300억'),
                _quality_trade_line(source_df[amt.ge(300) & amt.lt(1000)], f'{title} 300~1000억'),
                _quality_trade_line(source_df[amt.ge(1000) & amt.lt(3000)], f'{title} 1000~3000억'),
                _quality_trade_line(source_df[amt.ge(3000) & amt.lt(5000)], f'{title} 3000~5000억'),
                _quality_trade_line(source_df[amt.ge(5000)], f'{title} 5000억+'),
            ]

        def _feature_summary(source_df: pd.DataFrame, label: str) -> str:
            if source_df is None or source_df.empty:
                return f'- {label}: 0건'
            ix = source_df.index
            amt = amount_all.reindex(ix).fillna(0)
            close_loc = close_loc_all.reindex(ix).fillna(0)
            vol = vol_all.reindex(ix).fillna(0)
            rr = rr_all.reindex(ix).fillna(0)
            top = is_top.reindex(ix).fillna(False)
            mc = marcap_all.reindex(ix).fillna(0)
            mc_txt = ''
            if mc.gt(0).any():
                # marcap 단위가 원/억원/백만원 등 혼재 가능하므로 원 단위로 보이는 경우만 5조+ 판정한다.
                mc5_mask = mc.ge(5_000_000_000_000) if mc.median() > 1_000_000_000 else mc.ge(50_000)
                mc_txt = f' | 5조+ {_pct(mc5_mask):.1f}%'
            return (
                f'- {label}: {len(source_df)}건 | 대금중앙 {amt.median():.0f}억 | '
                f'1000+ {_pct(amt.ge(1000)):.1f}% / 3000+ {_pct(amt.ge(3000)):.1f}% / 5000+ {_pct(amt.ge(5000)):.1f}% | '
                f'종가75+ {_pct(close_loc.ge(75)):.1f}% / 종가80+ {_pct(close_loc.ge(80)):.1f}% | '
                f'Vol≤1.2 {_pct(vol.le(1.2)):.1f}% / Vol≤1.8 {_pct(vol.le(1.8)):.1f}% | '
                f'RR0.8~1.5 {_pct(rr.between(0.8, 1.5)):.1f}% / RR0.8~1.8 {_pct(rr.between(0.8, 1.8)):.1f}% | '
                f'KOSPI200/대표 {_pct(top):.1f}%{mc_txt}'
            )

        lines.append('[🔁 S-SAFE 거래대금 규칙 반복검증 — v4.4.9.24]')
        lines.append('- 목적: ST30 통과군 내부에서도 강한 종목이 거래대금/대표성과 연결되는지 추가로 확인합니다.')
        lines.append('- 원칙: ST30은 구조 안정 태그이고, 거래대금은 비중·지속성·보유시간을 나누는 강도 태그입니다.')
        lines.append(f'- 표본: S-SAFE 전체 {len(sub)}건 | ST30 통과 {len(passed)}건 | ST30 미통과 {len(miss)}건 | ST30-WAIT {len(wait)}건 | ST30-NO {len(no)}건')
        lines.append(_feature_summary(sub, 'S-SAFE 전체 특성'))
        lines.append(_feature_summary(passed, 'S-SAFE ST30 통과 특성'))
        lines.append(_feature_summary(miss, 'S-SAFE ST30 미통과 특성'))
        lines.append('')
        lines.append('거래대금 버킷 — S-SAFE 전체')
        lines += _amount_buckets(sub, 'S-SAFE 전체')
        lines.append('')
        lines.append('거래대금 버킷 — S-SAFE ST30 통과')
        lines += _amount_quality_buckets(passed, 'S-SAFE ST30 통과')
        lines.append('')
        lines.append('거래대금 버킷 — S-SAFE ST30 미통과')
        lines += _amount_buckets(miss, 'S-SAFE 미통과')

        if not passed.empty:
            ixp = passed.index
            p_amt = amount_all.reindex(ixp).fillna(0)
            p_close = close_loc_all.reindex(ixp).fillna(0)
            p_vol = vol_all.reindex(ixp).fillna(0)
            p_rr = rr_all.reindex(ixp).fillna(0)
            p_top = is_top.reindex(ixp).fillna(False)
            p_mc = marcap_all.reindex(ixp).fillna(0)
            p_mc5 = (p_mc.ge(5_000_000_000_000) if p_mc.gt(0).any() and p_mc.median() > 1_000_000_000 else p_mc.ge(50_000)) if p_mc.gt(0).any() else pd.Series(False, index=ixp)
            p_q75 = p_close.ge(75)
            p_q80 = p_close.ge(80)
            p_vol18 = p_vol.le(1.8)
            p_rr18 = p_rr.between(0.8, 1.8)
            lines.append('')
            lines.append('ST30 통과 강도규칙 — S-RECLAIM 후보')
            pass_rules: list[tuple[str, pd.Series]] = [
                ('S-RECLAIM MEGA: 통과·대금5000+·종가75+·Vol≤1.8·RR0.8~1.8', p_amt.ge(5000) & p_q75 & p_vol18 & p_rr18),
                ('S-RECLAIM 3000~5000: 통과·대금3000~5000·종가75+·Vol≤1.8·RR0.8~1.8', p_amt.ge(3000) & p_amt.lt(5000) & p_q75 & p_vol18 & p_rr18),
                ('S-RECLAIM PRIME: 통과·대금1000+·종가75+·Vol≤1.8·RR0.8~1.8', p_amt.ge(1000) & p_q75 & p_vol18 & p_rr18),
                ('S-RECLAIM 대표성: 통과·대금1000+·종가75+·KOSPI200/5조+', p_amt.ge(1000) & p_q75 & (p_top | p_mc5)),
                ('S-RECLAIM WATCH: 통과·대금300~1000억', p_amt.ge(300) & p_amt.lt(1000)),
                ('S-RECLAIM LIQUIDITY RISK: 통과·대금300억 미만', p_amt.lt(300)),
            ]
            for label, mask in pass_rules:
                lines.append(_quality_trade_line(passed[mask.reindex(ixp).fillna(False)], label))

        if not miss.empty:
            ix = miss.index
            amt = amount_all.reindex(ix).fillna(0)
            close_loc = close_loc_all.reindex(ix).fillna(0)
            vol = vol_all.reindex(ix).fillna(0)
            rr = rr_all.reindex(ix).fillna(0)
            top = is_top.reindex(ix).fillna(False)
            mc = marcap_all.reindex(ix).fillna(0)
            mc5_mask = (mc.ge(5_000_000_000_000) if mc.gt(0).any() and mc.median() > 1_000_000_000 else mc.ge(50_000)) if mc.gt(0).any() else pd.Series(False, index=ix)
            q75 = close_loc.ge(75)
            q80 = close_loc.ge(80)
            vol12 = vol.le(1.2)
            vol18 = vol.le(1.8)
            rr15 = rr.between(0.8, 1.5)
            rr18 = rr.between(0.8, 1.8)

            lines.append('')
            lines.append('후보규칙 반복검증 — S-MOMENTUM PRIME/WATCH 후보')
            rules: list[tuple[str, pd.Series]] = [
                ('S-MOMENTUM 1000-A: 미통과·대금1000+·종가75+·Vol≤1.8·RR0.8~1.8', amt.ge(1000) & q75 & vol18 & rr18),
                ('S-MOMENTUM 1000-B: 미통과·대금1000+·종가80+·Vol≤1.8·RR0.8~1.8', amt.ge(1000) & q80 & vol18 & rr18),
                ('S-MOMENTUM 3000-A: 미통과·대금3000+·종가75+·Vol≤1.8·RR0.8~1.8', amt.ge(3000) & q75 & vol18 & rr18),
                ('S-MOMENTUM 3000-B: 미통과·대금3000+·종가80+·Vol≤1.8·RR0.8~1.8', amt.ge(3000) & q80 & vol18 & rr18),
                ('S-MOMENTUM 3000~5000-A: 미통과·대금3000~5000억·종가75+·Vol≤1.8·RR0.8~1.8', amt.ge(3000) & amt.lt(5000) & q75 & vol18 & rr18),
                ('S-MOMENTUM 3000~5000-B: 미통과·대금3000~5000억·종가80+·Vol≤1.8·RR0.8~1.8', amt.ge(3000) & amt.lt(5000) & q80 & vol18 & rr18),
                ('S-MOMENTUM 5000-A: 미통과·대금5000+·종가75+·Vol≤1.8·RR0.8~1.8', amt.ge(5000) & q75 & vol18 & rr18),
                ('S-MOMENTUM 5000-B: 미통과·대금5000+·종가80+·Vol≤1.8·RR0.8~1.8', amt.ge(5000) & q80 & vol18 & rr18),
                ('S-MOMENTUM CALM: 미통과·대금1000+·종가75+·Vol≤1.2·RR0.8~1.5', amt.ge(1000) & q75 & vol12 & rr15),
                ('S-MOMENTUM 대표성: 미통과·대금1000+·종가75+·KOSPI200/5조+', amt.ge(1000) & q75 & (top | mc5_mask)),
            ]
            for label, mask in rules:
                lines.append(_v439_short_trade_line(miss[mask.reindex(ix).fillna(False)], label))

            lines.append('')
            lines.append('위험규칙 반복검증 — S-LIQUIDITY RISK 후보')
            risk_rules: list[tuple[str, pd.Series]] = [
                ('S-LIQUIDITY RISK <300억: 미통과·대금<300억', amt.lt(300)),
                ('S-LIQUIDITY RISK <1000억: 미통과·대금<1000억', amt.lt(1000)),
                ('S-LIQUIDITY RISK 저유동+비대표: 미통과·대금<1000억·비KOSPI200/비5조', amt.lt(1000) & ~(top | mc5_mask)),
                ('S-QUALITY RISK: 미통과·종가75미만 또는 Vol>1.8 또는 RR>1.8', close_loc.lt(75) | vol.gt(1.8) | rr.gt(1.8)),
                ('S-FAIL RISK 강형: 미통과·대금<1000억 + (종가75미만 또는 Vol>1.8 또는 RR>1.8)', amt.lt(1000) & (close_loc.lt(75) | vol.gt(1.8) | rr.gt(1.8))),
            ]
            for label, mask in risk_rules:
                lines.append(_v439_short_trade_line(miss[mask.reindex(ix).fillna(False)], label))

        lines.append('- 반복판정 기준: 표본 10건 이상에서 3/5 +1.2%↑, 승률 65%↑, 손절 25%↓이면 실전 라벨 강화 후보입니다. 표본 부족은 설명 라벨만 적용합니다.')
        lines.append('- 운용안: S-SAFE+ST30 통과도 거래대금으로 MEGA(5000억+)·3000~5000·PRIME(1000억+)·WATCH(300~1000)·LIQUIDITY RISK(<300)로 해석합니다. 미통과는 기존처럼 MOMENTUM/RISK로 분리합니다.')
    except Exception as e:
        lines.append(f'[S-SAFE 거래대금 규칙 반복검증 오류] {type(e).__name__}: {e}')
    return lines

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
    st30_rec = _calc_st30_reclaim_context(row, hist)
    return {
        **info,
        **st30_rec,
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



# =============================================================
# I-CORE / 시대중심주 150·200일 시세분출 백테스트 유틸
# =============================================================
def _icore_no_volume_flash_crash(hist: pd.DataFrame, lookback: int = 15) -> dict:
    """
    v4.3.1 I-CORE 참고 태그.
    사용자가 의도한 "거래량 없는 단기 급락 후 빠른 회복"은 후보 제외 조건이 아니다.
    오히려 장기 박스/150·200일선 생존 구조에서 유동성 공백·흔들기 후 회복으로 볼 수 있으므로
    I-CORE 후보에서 제외하지 않고 `무거래단기급락회복` 태그만 붙인다.

    정의:
    - 최근 lookback일 안에 -8% 이상 급락일 존재
    - 해당일 거래량이 VMA20의 0.8배 이하
    - 이후 5거래일 안에 급락 직전 종가의 97% 이상으로 빠르게 회복
    """
    res = {'flag': 0, 'reason': ''}
    try:
        if hist is None or len(hist) < 40:
            return res
        h = hist.copy().reset_index(drop=True)
        if 'VMA20' not in h.columns:
            h['VMA20'] = h['Volume'].rolling(20).mean()
        h['day_ret'] = h['Close'].pct_change() * 100.0
        start = max(1, len(h) - int(lookback))
        for i in range(start, len(h)):
            r = _safe_float(h.loc[i, 'day_ret'], 0.0)
            vol = _safe_float(h.loc[i, 'Volume'], 0.0)
            vma = _safe_float(h.loc[i, 'VMA20'], 0.0)
            prev_close = _safe_float(h.loc[i - 1, 'Close'], 0.0) if i > 0 else 0.0
            if r <= -8.0 and vma > 0 and vol <= vma * 0.8 and prev_close > 0:
                future = h.iloc[i + 1:min(len(h), i + 6)]
                if not future.empty and _safe_float(future['Close'].max(), 0.0) >= prev_close * 0.97:
                    res['flag'] = 1
                    d = h.loc[i, 'Date'] if 'Date' in h.columns else ''
                    dstr = pd.Timestamp(d).strftime('%Y-%m-%d') if not pd.isna(d) and str(d) else ''
                    res['reason'] = f'거래량없는급락후빠른회복({dstr}, {r:.1f}%, Vol/VMA20 {vol / vma:.2f})'
                    return res
    except Exception as e:
        res['reason'] = f'급락필터오류:{type(e).__name__}'
    return res


def _icore_monthly_context(hist: pd.DataFrame) -> dict:
    """월봉 거래량/월봉 구조 간단 진단."""
    out = {
        'monthly_ok': 0,
        'monthly_turning': 0,
        'monthly_vol_rebuild': 0,
        'monthly_label': '월봉데이터부족',
        'monthly_vol_ratio': 0.0,
    }
    try:
        if hist is None or len(hist) < 220 or 'Date' not in hist.columns:
            return out
        h = hist[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
        h['Date'] = pd.to_datetime(h['Date'], errors='coerce')
        h = h.dropna(subset=['Date']).set_index('Date')
        if h.empty:
            return out
        m = pd.DataFrame({
            'Open': h['Open'].resample('M').first(),
            'High': h['High'].resample('M').max(),
            'Low': h['Low'].resample('M').min(),
            'Close': h['Close'].resample('M').last(),
            'Volume': h['Volume'].resample('M').sum(),
        }).dropna()
        if len(m) < 8:
            return out
        m['MMA5'] = m['Close'].rolling(5).mean()
        m['MMA10'] = m['Close'].rolling(10).mean()
        recent = m.tail(2)
        last = m.iloc[-1]
        prev = m.iloc[-2]
        vol_base = _safe_float(m['Volume'].tail(6).iloc[:-1].mean(), 0.0) if len(m) >= 7 else 0.0
        vol_ratio = _safe_float(last['Volume'], 0.0) / vol_base if vol_base > 0 else 0.0
        monthly_above_5 = _safe_float(last['MMA5'], 0.0) > 0 and _safe_float(last['Close'], 0.0) >= _safe_float(last['MMA5'], 0.0) * 0.98
        monthly_bull = _safe_float(last['Close'], 0.0) >= _safe_float(last['Open'], 0.0)
        monthly_turn = monthly_bull and _safe_float(last['Close'], 0.0) >= _safe_float(prev['Close'], 0.0) * 0.98
        vol_rebuild = vol_ratio >= 1.15
        out.update({
            'monthly_ok': int(monthly_above_5),
            'monthly_turning': int(monthly_turn),
            'monthly_vol_rebuild': int(vol_rebuild),
            'monthly_vol_ratio': round(vol_ratio, 2),
            'monthly_label': ('월봉5선회복·거래량재증가' if monthly_above_5 and vol_rebuild else
                              '월봉5선회복' if monthly_above_5 else
                              '월봉양봉전환' if monthly_turn else '월봉확인필요'),
        })
    except Exception as e:
        out['monthly_label'] = f'월봉진단오류:{type(e).__name__}'
    return out


def _icore_anchor_context(hist: pd.DataFrame) -> dict:
    """과거 역사적 거래량/거래대금 기준봉과 150~200일 눌림·박스 여부 진단."""
    out = {
        'anchor_ok': 0, 'anchor_days': 0, 'anchor_date': '', 'anchor_vol_ratio': 0.0,
        'anchor_amount_b': 0.0, 'anchor_ret_pct': 0.0, 'anchor_close_loc': 0.0,
        'box_days': 0, 'pullback_from_anchor_high': 0.0, 'box_range_pct': 0.0,
        'near_long_ma': 0, 'long_ma_dist_pct': 0.0, 'ma5_reclaim_long': 0,
        'box_breakout': 0, 'prior_high_breakout': 0,
        'long_ma_survival_pct': 0.0, 'long_ma_break_days': 0,
    }
    try:
        if hist is None or len(hist) < 240:
            return out
        h = hist.copy().reset_index(drop=True)
        for ma in [5, 20, 50, 150, 200]:
            col = f'MA{ma}'
            if col not in h.columns:
                h[col] = h['Close'].rolling(ma).mean()
        if 'VMA60' not in h.columns:
            h['VMA60'] = h['Volume'].rolling(60).mean()
        h['ret_pct'] = h['Close'].pct_change() * 100.0
        h['amount_b'] = h.get('Amount', h['Close'] * h['Volume']) / 1e8
        h['vol60_ratio'] = h['Volume'] / h['VMA60'].replace(0, np.nan)
        today = h.iloc[-1]
        close = _safe_float(today.get('Close', 0), 0.0)
        ma150 = _safe_float(today.get('MA150', 0), 0.0)
        ma200 = _safe_float(today.get('MA200', 0), 0.0)
        long_ma = max(ma150, ma200)
        if long_ma <= 0 or close <= 0:
            return out
        long_dist = (close / long_ma - 1.0) * 100.0
        out['long_ma_dist_pct'] = round(long_dist, 1)
        out['near_long_ma'] = int(-5.0 <= long_dist <= 18.0)

        # 기준봉 후보: 현재로부터 90~240거래일 전. 너무 최근 기준봉은 I-CORE가 아니라 단기 H/L로 본다.
        start = max(0, len(h) - 260)
        end = max(0, len(h) - 90)
        win = h.iloc[start:end].copy()
        if win.empty:
            return out
        cond = (pd.to_numeric(win['vol60_ratio'], errors='coerce').fillna(0) >= 2.0) | (pd.to_numeric(win['amount_b'], errors='coerce').fillna(0) >= 1000.0)
        cond &= pd.to_numeric(win['ret_pct'], errors='coerce').fillna(0) >= 5.0
        if not cond.any():
            # 완전한 장대양봉이 아니어도 역사적 거래대금 상위봉은 기준봉 후보로 허용
            q = pd.to_numeric(h['amount_b'].iloc[max(0, len(h)-360):end], errors='coerce').quantile(0.90)
            cond = pd.to_numeric(win['amount_b'], errors='coerce').fillna(0) >= max(q, 500.0)
        if not cond.any():
            return out
        cand = win[cond].copy()
        cand['_rank'] = pd.to_numeric(cand['amount_b'], errors='coerce').fillna(0) * 0.6 + pd.to_numeric(cand['vol60_ratio'], errors='coerce').fillna(0) * 100.0
        anchor_idx = int(cand['_rank'].idxmax())
        anchor = h.loc[anchor_idx]
        anchor_days = int(len(h) - 1 - anchor_idx)
        if anchor_days < 90 or anchor_days > 260:
            return out
        anchor_date = pd.Timestamp(anchor.get('Date')).strftime('%Y-%m-%d') if 'Date' in h.columns and not pd.isna(anchor.get('Date')) else ''
        post = h.iloc[anchor_idx:]
        post_high = _safe_float(post['High'].max(), 0.0)
        pullback = (close / post_high - 1.0) * 100.0 if post_high > 0 else 0.0
        box = h.tail(min(anchor_days, 200))
        box_high = _safe_float(box['High'].max(), 0.0)
        box_low = _safe_float(box['Low'].min(), 0.0)
        box_range = (box_high / box_low - 1.0) * 100.0 if box_low > 0 else 0.0
        # 150/200일선 생존율: 장기 박스 구간에서 장기선 근처를 얼마나 지켰는지 본다.
        surv = box.copy()
        long_series = pd.concat([surv.get('MA150', pd.Series(index=surv.index, dtype=float)), surv.get('MA200', pd.Series(index=surv.index, dtype=float))], axis=1).max(axis=1)
        valid_long = long_series > 0
        if valid_long.any():
            survival_mask = pd.to_numeric(surv.loc[valid_long, 'Close'], errors='coerce') >= long_series.loc[valid_long] * 0.94
            survival_pct = float(survival_mask.mean() * 100.0)
            long_break_days = int((~survival_mask).sum())
        else:
            survival_pct = 0.0
            long_break_days = 999
        high120_prev = _safe_float(h['High'].iloc[:-1].tail(120).max(), 0.0)
        box_high60_prev = _safe_float(h['High'].iloc[:-1].tail(60).max(), 0.0)
        ma5 = _safe_float(today.get('MA5', 0), 0.0)
        prev_ma5 = _safe_float(h.iloc[-2].get('MA5', 0), 0.0)
        prev_long = max(_safe_float(h.iloc[-2].get('MA150', 0), 0.0), _safe_float(h.iloc[-2].get('MA200', 0), 0.0))
        ma5_reclaim = bool(ma5 > long_ma and (prev_ma5 <= prev_long * 1.005 or close >= long_ma * 1.005) and long_dist <= 18.0)
        box_breakout = bool(box_high60_prev > 0 and close >= box_high60_prev * 1.005)
        prior_high_breakout = bool(high120_prev > 0 and close >= high120_prev * 1.005)
        rng = _safe_float(today.get('High', 0), 0.0) - _safe_float(today.get('Low', 0), 0.0)
        close_loc = ((_safe_float(today.get('Close', 0), 0.0) - _safe_float(today.get('Low', 0), 0.0)) / rng * 100.0) if rng > 0 else 100.0
        out.update({
            'anchor_ok': 1,
            'anchor_days': anchor_days,
            'anchor_date': anchor_date,
            'anchor_vol_ratio': round(_safe_float(anchor.get('vol60_ratio', 0), 0.0), 2),
            'anchor_amount_b': round(_safe_float(anchor.get('amount_b', 0), 0.0), 1),
            'anchor_ret_pct': round(_safe_float(anchor.get('ret_pct', 0), 0.0), 1),
            'anchor_close_loc': round(close_loc, 1),
            'box_days': min(anchor_days, 200),
            'pullback_from_anchor_high': round(pullback, 1),
            'box_range_pct': round(box_range, 1),
            'ma5_reclaim_long': int(ma5_reclaim),
            'box_breakout': int(box_breakout),
            'prior_high_breakout': int(prior_high_breakout),
            'long_ma_survival_pct': round(survival_pct, 1),
            'long_ma_break_days': long_break_days,
        })
    except Exception:
        pass
    return out



def _icore_supply_context(code: str, hist: pd.DataFrame) -> dict:
    """I-CORE용 수급 진단.
    - 기본은 OHLCV 기반 OBV/거래대금 프록시를 사용한다.
    - I_CORE_FETCH_KRX_FLOW=1 이면 pykrx 외국인/기관 20거래일 순매수도 시도한다.
    """
    out = {
        'supply_score': 0,
        'obv20_up': 0,
        'obv60_up': 0,
        'obv20_chg_pct': 0.0,
        'obv60_chg_pct': 0.0,
        'amount20_rebuild': 0,
        'flow_fetched': 0,
        'frgn_20d_b': 0.0,
        'inst_20d_b': 0.0,
        'fi_20d_b': 0.0,
        'flow_label': '수급프록시(OBV/거래대금)',
    }
    try:
        if hist is None or len(hist) < 80:
            return out
        h = hist.copy().reset_index(drop=True)
        close = pd.to_numeric(h.get('Close'), errors='coerce').fillna(method='ffill')
        vol = pd.to_numeric(h.get('Volume'), errors='coerce').fillna(0)
        direction = np.sign(close.diff().fillna(0))
        obv = (direction * vol).cumsum()
        def _chg(n):
            if len(obv) <= n:
                return 0.0
            base = float(abs(obv.iloc[-n]) or 1.0)
            return float((obv.iloc[-1] - obv.iloc[-n]) / base * 100.0)
        obv20 = _chg(20)
        obv60 = _chg(60)
        out['obv20_chg_pct'] = round(obv20, 2)
        out['obv60_chg_pct'] = round(obv60, 2)
        out['obv20_up'] = int(obv20 > 0)
        out['obv60_up'] = int(obv60 > 0)
        amount = pd.to_numeric(h.get('Amount', h.get('Close') * h.get('Volume')), errors='coerce').fillna(0) / 1e8
        recent20 = float(amount.tail(20).mean() or 0.0)
        prev60 = float(amount.iloc[:-20].tail(60).mean() or 0.0) if len(amount) > 80 else 0.0
        out['amount20_rebuild'] = int(prev60 > 0 and recent20 >= prev60 * 1.15)
        score = 0
        if out['obv20_up']: score += 1
        if out['obv60_up']: score += 1
        if out['amount20_rebuild']: score += 1
        # 선택 옵션: pykrx 외국인/기관 수급
        if I_CORE_FETCH_KRX_FLOW and pykrx_stock is not None and 'Date' in h.columns:
            try:
                d = pd.Timestamp(h.iloc[-1].get('Date'))
                end = d.strftime('%Y%m%d')
                start = (d - timedelta(days=45)).strftime('%Y%m%d')
                flow_df = pykrx_stock.get_market_trading_value_by_date(start, end, code)
                if flow_df is not None and not flow_df.empty:
                    cols = {str(c): c for c in flow_df.columns}
                    fr_col = next((cols[c] for c in cols if '외국' in c), None)
                    in_col = next((cols[c] for c in cols if '기관' in c), None)
                    fr_b = float(pd.to_numeric(flow_df[fr_col], errors='coerce').fillna(0).tail(20).sum() / 1e8) if fr_col is not None else 0.0
                    in_b = float(pd.to_numeric(flow_df[in_col], errors='coerce').fillna(0).tail(20).sum() / 1e8) if in_col is not None else 0.0
                    out['flow_fetched'] = 1
                    out['frgn_20d_b'] = round(fr_b, 1)
                    out['inst_20d_b'] = round(in_b, 1)
                    out['fi_20d_b'] = round(fr_b + in_b, 1)
                    if fr_b > 0: score += 1
                    if in_b > 0: score += 1
                    if fr_b > 0 and in_b > 0: score += 1
                    out['flow_label'] = ('쌍끌이' if fr_b > 0 and in_b > 0 else '외인우위' if fr_b > 0 else '기관우위' if in_b > 0 else '외인기관중립/매도')
            except Exception as e:
                out['flow_label'] = f'pykrx수급오류:{type(e).__name__}'
        out['supply_score'] = int(score)
    except Exception as e:
        out['flow_label'] = f'수급진단오류:{type(e).__name__}'
    return out

def _build_icore_hits(code: str, name: str, hist: pd.DataFrame, row: pd.Series, info: dict, idx_label: str, marcap: float) -> list:
    """I-CORE 타점별 후보 생성. I-3/I-4/I-5/I-6을 각각 별도 신호로 남긴다."""
    hits = []
    try:
        if hist is None or len(hist) < 260:
            return hits
        if info.get('_close', 0) < MIN_PRICE:
            return hits
        # v4.3.1: 거래량 없는 단기 급락 후 빠른 회복은 제외하지 않고 태그만 붙인다.
        # 장기 박스/150·200일선 생존 구조에서는 유동성 공백·흔들기 후 회복일 수 있기 때문이다.
        flash = _icore_no_volume_flash_crash(hist)
        flash_flag = int(flash.get('flag', 0))
        flash_reason = str(flash.get('reason', '') or '')
        ctx = _icore_anchor_context(hist)
        if int(ctx.get('anchor_ok', 0)) != 1:
            return hits
        mon = _icore_monthly_context(hist)
        supply = _icore_supply_context(code, hist)
        leader_cycle = _i_leader_lifecycle_context(hist)
        h = hist.copy().reset_index(drop=True)
        today = h.iloc[-1]
        prev = h.iloc[-2]
        close = _safe_float(today.get('Close', 0), 0.0)
        high = _safe_float(today.get('High', 0), 0.0)
        low = _safe_float(today.get('Low', 0), 0.0)
        open_p = _safe_float(today.get('Open', close), close)
        vol = _safe_float(today.get('Volume', 0), 0.0)
        amount_b = _safe_float(info.get('amount_b', 0.0), 0.0)
        ma5 = _safe_float(today.get('MA5', 0), 0.0)
        ma20 = _safe_float(today.get('MA20', 0), 0.0)
        ma50 = _safe_float(today.get('MA50', 0), 0.0)
        ma150 = _safe_float(today.get('MA150', 0), 0.0)
        ma200 = _safe_float(today.get('MA200', 0), 0.0)
        long_ma = max(ma150, ma200)
        if close <= 0 or long_ma <= 0:
            return hits
        vma20 = _safe_float(h['Volume'].tail(20).mean(), 0.0)
        vma60 = _safe_float(h['Volume'].tail(60).mean(), 0.0)
        vol_ratio20 = vol / vma20 if vma20 > 0 else 0.0
        vol_ratio60 = vol / vma60 if vma60 > 0 else 0.0
        rng = high - low
        close_loc = ((close - low) / rng * 100.0) if rng > 0 else 100.0
        body_pct = abs(close - open_p) / open_p * 100.0 if open_p > 0 else 0.0
        day_ret = (close / _safe_float(prev.get('Close', 0), close) - 1.0) * 100.0 if _safe_float(prev.get('Close', 0), 0) > 0 else 0.0
        # 현재 신호일 자체가 장대음봉/붕괴이면 I-CORE 타점이 아니므로 제외한다.
        # 단, 과거의 '거래량 없는 급락 후 빠른 회복'은 제외하지 않는다(태그만 표시).
        crash_today = bool(day_ret <= -7.0 and close_loc < 35.0)
        if crash_today:
            return hits
        anchor_days = int(ctx.get('anchor_days', 0))
        long_dist = _safe_float(ctx.get('long_ma_dist_pct', 999), 999)
        pullback = _safe_float(ctx.get('pullback_from_anchor_high', 0), 0)
        box_range = _safe_float(ctx.get('box_range_pct', 0), 0)
        material_proxy = 0
        # 뉴스/재료 누적은 아직 외부 뉴스 저장소 없이 프록시로만 둔다: 거래대금 재등장+월봉 거래량 재증가+시장 대표성.
        if amount_b >= 300.0: material_proxy += 1
        if amount_b >= 1000.0: material_proxy += 1
        if int(mon.get('monthly_vol_rebuild', 0)) == 1: material_proxy += 1
        if idx_label in ('코스피200', '코스닥150') or marcap >= MCAP_OR_MIN: material_proxy += 1
        if int(supply.get('supply_score', 0)) >= 2: material_proxy += 1
        # 공통 점수
        base_score = 40
        if 120 <= anchor_days <= 220: base_score += 15
        if -35.0 <= pullback <= -3.0: base_score += 10
        if abs(long_dist) <= 12.0: base_score += 10
        if int(mon.get('monthly_ok', 0)) == 1: base_score += 10
        if material_proxy >= 3: base_score += 10
        if amount_b >= 100.0: base_score += 5
        if _safe_float(ctx.get('long_ma_survival_pct', 0), 0) >= 80.0: base_score += 10
        if int(supply.get('supply_score', 0)) >= 3: base_score += 5
        base_score = min(base_score, 100)

        # v4.3.2 strict context: 진짜 I-CORE 후보 수를 줄이기 위한 공통 필터
        anchor_quality = (int(ctx.get('anchor_days', 0)) >= 110 and int(ctx.get('anchor_days', 0)) <= 260 and (
            _safe_float(ctx.get('anchor_vol_ratio', 0), 0) >= 2.0 or _safe_float(ctx.get('anchor_amount_b', 0), 0) >= 300.0
        ))
        long_survival_ok = _safe_float(ctx.get('long_ma_survival_pct', 0), 0) >= 70.0
        long_dist_ok = -7.0 <= long_dist <= 25.0
        liquidity_ok = amount_b >= I_CORE_MIN_AMOUNT_B or _safe_float(ctx.get('anchor_amount_b', 0), 0) >= 500.0
        context_ok = anchor_quality and long_survival_ok and long_dist_ok and liquidity_ok and base_score >= 65
        if not context_ok:
            return hits
        # I-3: 장기선 근처 생존/관찰
        dry_volume = bool(vol_ratio20 <= 1.2 or vol_ratio60 <= 1.1)
        near_long = bool(-5.0 <= long_dist <= 10.0)
        i3 = near_long and dry_volume and close >= long_ma * 0.98 and close >= ma50 * 0.97 and close_loc >= 45.0 and base_score >= I_CORE_MIN_SCORE - 5 and material_proxy >= 2
        # I-4: 5일선이 150/200일선 위로 올라타는 확인 타점. 과도한 이격은 제외.
        i4 = bool(int(ctx.get('ma5_reclaim_long', 0)) == 1 and -2.0 <= long_dist <= 12.0 and close >= long_ma and close_loc >= 55.0 and base_score >= I_CORE_MIN_SCORE and material_proxy >= 2)
        # I-5: 박스/전고점 돌파. 거래량 재증가와 상단마감 필요.
        i5 = bool((int(ctx.get('box_breakout', 0)) == 1 or int(ctx.get('prior_high_breakout', 0)) == 1) and vol_ratio20 >= 1.2 and close_loc >= 65.0 and long_dist <= 20.0 and base_score >= I_CORE_MIN_SCORE and material_proxy >= 2)
        # I-6: 최근 1~15일 내 I-5형 돌파 후 첫 눌림/재지지.
        i6 = False
        breakout_days = 0
        breakout_ref = 0.0
        try:
            for k in range(1, min(16, len(h)-1)):
                past_hist = h.iloc[:len(h)-k]
                b = past_hist.iloc[-1]
                c = _safe_float(b.get('Close', 0), 0.0)
                prev_box = _safe_float(past_hist.iloc[:-1]['High'].tail(60).max(), 0.0)
                v20 = _safe_float(past_hist['Volume'].tail(20).mean(), 0.0)
                vr = _safe_float(b.get('Volume', 0), 0.0) / v20 if v20 > 0 else 0.0
                rrng = _safe_float(b.get('High', 0), 0.0) - _safe_float(b.get('Low', 0), 0.0)
                cloc = ((c - _safe_float(b.get('Low', 0), 0.0)) / rrng * 100.0) if rrng > 0 else 100.0
                if prev_box > 0 and c >= prev_box * 1.005 and vr >= 1.1 and cloc >= 60.0:
                    breakout_days = k
                    breakout_ref = prev_box
                    break
            if breakout_days > 0 and breakout_ref > 0:
                pull_from_recent_high = (close / _safe_float(h.iloc[-breakout_days-1:]['High'].max(), high) - 1.0) * 100.0
                i6 = bool(-10.0 <= pull_from_recent_high <= -2.0 and close >= max(ma20, breakout_ref * 0.99) and vol_ratio20 <= 1.15 and close_loc >= 50.0 and base_score >= I_CORE_MIN_SCORE)
        except Exception:
            pass

        phase_defs = []
        flash_tag = '무거래단기급락회복' if flash_flag == 1 else ''
        if i3:
            tags = ['150/200일선근처', '거래량감소', mon.get('monthly_label', '')]
            if flash_tag: tags.append(flash_tag)
            phase_defs.append(('I-3', 'I-3 장기선근처 관찰', '관찰', base_score - 5, tags))
        if i4:
            tags = ['5일선 150/200일선 회복', f'장기선이격 {long_dist:.1f}%', mon.get('monthly_label', '')]
            if flash_tag: tags.append(flash_tag)
            phase_defs.append(('I-4', 'I-4 5MA 장기선 회복', '1차20%', base_score + 5, tags))
        if i5:
            tags = ['박스/120일고점 돌파', f'Vol20 {vol_ratio20:.1f}배', f'종가위치 {close_loc:.0f}%']
            if flash_tag: tags.append(flash_tag)
            phase_defs.append(('I-5', 'I-5 박스/전고점 돌파', '2차30%', base_score + 10, tags))
        if i6:
            tags = [f'돌파후 {breakout_days}일 눌림', '20일선/박스상단 재지지', '거래량감소눌림']
            if flash_tag: tags.append(flash_tag)
            phase_defs.append(('I-6', 'I-6 돌파후 첫 눌림', '3차30%', base_score + 8, tags))

        for phase, label, action, score, passed in phase_defs:
            score = int(max(0, min(100, score)))
            grade = '완전체' if score >= 88 else ('✅A급' if score >= 75 else 'B급')
            rec = _bt_common_payload(code, name, 'I', 'I-CORE 시대중심주 150/200', grade, score, row, hist, idx_label, marcap, passed)
            box_stop = long_ma * 0.97
            stoploss = round(min(box_stop, ma50 * 0.98 if ma50 > 0 else box_stop))
            rec.update({
                'band_type': 'I_CORE_150_200',
                'band_reason': '역사적 거래량 이후 150~200일 박스/눌림·월봉 확인·5MA 장기선 회복/돌파 타점',
                'i_core': 1,
                'i_phase': phase,
                'i_phase_label': label,
                'i_action': action,
                'i_anchor_date': ctx.get('anchor_date', ''),
                'i_anchor_days': ctx.get('anchor_days', 0),
                'i_anchor_amount_b': ctx.get('anchor_amount_b', 0),
                'i_anchor_vol_ratio': ctx.get('anchor_vol_ratio', 0),
                'i_anchor_ret_pct': ctx.get('anchor_ret_pct', 0),
                'i_pullback_from_anchor_high': ctx.get('pullback_from_anchor_high', 0),
                'i_box_range_pct': ctx.get('box_range_pct', 0),
                'i_long_ma_dist_pct': round(long_dist, 1),
                'i_ma5_reclaim_long': int(i4),
                'i_box_breakout': int(i5),
                'i_monthly_ok': mon.get('monthly_ok', 0),
                'i_monthly_vol_rebuild': mon.get('monthly_vol_rebuild', 0),
                'i_monthly_label': mon.get('monthly_label', ''),
                'i_monthly_vol_ratio': mon.get('monthly_vol_ratio', 0),
                'i_no_volume_flash_crash': flash_flag,
                'i_flash_recovery_tag': flash_reason,
                'i_material_proxy_score': material_proxy,
                'i_core_main_candidate': int(
                    phase in ('I-4', 'I-5', 'I-6')
                    and material_proxy >= I_CORE_MAIN_MIN_MATERIAL
                    and I_CORE_MAIN_LONG_MIN <= long_dist <= I_CORE_MAIN_LONG_MAX
                    and ((not I_CORE_MAIN_REQUIRE_OBV_AMOUNT) or (int(supply.get('amount20_rebuild', 0)) == 1 and int(supply.get('obv20_up', 0)) == 1))
                ),
                'i_core_main_accel': int(
                    phase in ('I-4', 'I-5', 'I-6')
                    and material_proxy >= I_CORE_MAIN_MIN_MATERIAL
                    and 10.0 < long_dist <= 18.0
                    and ((not I_CORE_MAIN_REQUIRE_OBV_AMOUNT) or (int(supply.get('amount20_rebuild', 0)) == 1 and int(supply.get('obv20_up', 0)) == 1))
                ),
                'i_long_ma_survival_pct': ctx.get('long_ma_survival_pct', 0),
                'i_long_ma_break_days': ctx.get('long_ma_break_days', 0),
                'i_supply_score': supply.get('supply_score', 0),
                'i_obv20_up': supply.get('obv20_up', 0),
                'i_obv60_up': supply.get('obv60_up', 0),
                'i_amount20_rebuild': supply.get('amount20_rebuild', 0),
                'i_flow_fetched': supply.get('flow_fetched', 0),
                'i_frgn_20d_b': supply.get('frgn_20d_b', 0.0),
                'i_inst_20d_b': supply.get('inst_20d_b', 0.0),
                'i_fi_20d_b': supply.get('fi_20d_b', 0.0),
                'i_flow_label': supply.get('flow_label', ''),
                'i_leader_class': leader_cycle.get('i_leader_class', 'WATCH'),
                'i_leader_label': leader_cycle.get('i_leader_label', '🔎 I-LEADER WATCH'),
                'i_leader_desc': leader_cycle.get('i_leader_desc', ''),
                'i_leader_age_weeks': leader_cycle.get('i_leader_age_weeks', 0),
                'i_leader_age_days': leader_cycle.get('i_leader_age_days', 0),
                'i_leader_weekly_align': leader_cycle.get('i_leader_weekly_align', 0),
                'i_leader_wvol_ratio20': leader_cycle.get('i_leader_wvol_ratio20', 0.0),
                'i_leader_recent_wvol_ratio20_max': leader_cycle.get('i_leader_recent_wvol_ratio20_max', 0.0),
                'i_leader_rsi14w': leader_cycle.get('i_leader_rsi14w', 0.0),
                'i_leader_macd_osc': leader_cycle.get('i_leader_macd_osc', 0.0),
                'i_leader_drawdown_52w_pct': leader_cycle.get('i_leader_drawdown_52w_pct', 0.0),
                'i_leader_risk_tags': leader_cycle.get('i_leader_risk_tags', ''),
                'vol_ratio': round(vol_ratio20, 2),
                'vol20_ratio': round(vol_ratio20, 2),
                'vol60_ratio': round(vol_ratio60, 2),
                'close_loc_pct': round(close_loc, 1),
                'stoploss': stoploss,
                'target1': round(close * 1.10),
                'sell_rule': 'I-CORE: 20/40/60일 평가. 박스/150·200선 재이탈, 20/50일선 종가이탈. 무거래 단기급락 후 빠른 회복은 제외하지 않고 태그로만 표시.',
                'rr': round(((close * 1.20 - close) / max(close - stoploss, 1)), 2) if close > stoploss else 0.0,
            })
            hits.append(rec)
    except Exception as e:
        log_debug(f"I-CORE 백테스트 오류 [{code}/{name}]: {e}")
    return hits



# =============================================================
# YMGP / 역매공파 단계 분류 유틸
# =============================================================
def _classify_ymgp_stage(hist: pd.DataFrame, row: pd.Series | None = None, info: dict | None = None) -> dict:
    """
    v4.1.3 역매공파(C) 단계 재분류.

    기존 v4.1.2에서는 C3 눌림완성형이 너무 넓게 잡혀 대부분의 C 후보가 C3로 몰리고,
    C1 1파돌파형은 0건으로 죽는 문제가 있었다. v4.1.3은 아래처럼 더 엄격히 나눈다.

    C1 1파돌파형
      - 최근 0~5거래일 이내 MA112/MA224/BB40 중단 등 장기 저항을 돌파하거나 장기선 위에 막 올라선 구간
      - 고점 대비 눌림이 아직 충분하지 않음
      - 실전 매수보다 관심등록/눌림 대기

    C2 눌림진행형
      - 1파 돌파 이후 2~18거래일 경과
      - 고점 대비 약 2.5~14% 눌림
      - 지지권 접근 또는 거래량 감소는 보이나, 재상승 확인이 부족
      - 관찰/알림 후보

    C3 눌림완성형
      - 1파 돌파 이후 눌림이 진행됨
      - 거래량 감소, 장기선/20·40선/BB40 부근 재지지
      - 양봉/상단마감, 전일고가 회복, 5일선 회복 중 하나 이상으로 2파 재상승 확인
      - 스윙 실전 후보로 별도 성과 검증
    """
    try:
        if hist is None or len(hist) < 80:
            return {
                'c_stage': 'C0',
                'c_stage_label': 'C0 분류불가',
                'c_stage_desc': '데이터 부족으로 역매공파 단계를 분류하지 못했습니다.',
                'c_stage_bias': '관찰',
                'c_stage_rank': 9,
            }

        h = hist.copy()
        if row is None:
            row = h.iloc[-1]
        if info is None:
            info = _base_info(row, h)

        close = _safe_float(row.get('Close', info.get('_close', 0)), 0.0)
        open_p = _safe_float(row.get('Open', info.get('_open', 0)), 0.0)
        high = _safe_float(row.get('High', info.get('_high', 0)), 0.0)
        low = _safe_float(row.get('Low', info.get('_low', 0)), 0.0)
        vol = _safe_float(row.get('Volume', info.get('_vol', 0)), 0.0)
        vma20_now = _safe_float(row.get('VMA20', info.get('_vma20', 0)), 0.0)

        ma5_now = _safe_float(row.get('MA5', 0), 0.0)
        ma20_now = _safe_float(row.get('MA20', 0), 0.0)
        ma40_now = _safe_float(row.get('MA40', 0), 0.0)
        ma112_now = _safe_float(row.get('MA112', 0), 0.0)
        ma224_now = _safe_float(row.get('MA224', 0), 0.0)

        ma5 = pd.to_numeric(h.get('MA5', pd.Series(index=h.index, dtype=float)), errors='coerce')
        ma20 = pd.to_numeric(h.get('MA20', pd.Series(index=h.index, dtype=float)), errors='coerce')
        ma40 = pd.to_numeric(h.get('MA40', pd.Series(index=h.index, dtype=float)), errors='coerce')
        ma112 = pd.to_numeric(h.get('MA112', pd.Series(index=h.index, dtype=float)), errors='coerce')
        ma224 = pd.to_numeric(h.get('MA224', pd.Series(index=h.index, dtype=float)), errors='coerce')
        close_s = pd.to_numeric(h.get('Close', pd.Series(index=h.index, dtype=float)), errors='coerce')
        high_s = pd.to_numeric(h.get('High', pd.Series(index=h.index, dtype=float)), errors='coerce')
        low_s = pd.to_numeric(h.get('Low', pd.Series(index=h.index, dtype=float)), errors='coerce')
        vol_s = pd.to_numeric(h.get('Volume', pd.Series(index=h.index, dtype=float)), errors='coerce')
        vma20_s = pd.to_numeric(h.get('VMA20', pd.Series(index=h.index, dtype=float)), errors='coerce')

        # BB40 중단도 장기 저항/재지지 후보로 사용한다.
        try:
            bb40 = _calc_bollinger(h, 40, 2.0)
            bb40_mid_s = pd.to_numeric(bb40.get('mid', pd.Series(index=h.index, dtype=float)), errors='coerce')
            bb40_mid = _safe_float(bb40_mid_s.iloc[-1], 0.0) if len(bb40_mid_s) else 0.0
        except Exception:
            bb40_mid_s = pd.Series(index=h.index, dtype=float)
            bb40_mid = 0.0

        # 장기 기준선: MA112/MA224/BB40 중단 중 사용 가능한 선.
        long_lines_now = [x for x in [ma112_now, ma224_now, bb40_mid] if x and x > 0]
        long_res_now = max(long_lines_now) if long_lines_now else max([x for x in [ma112_now, ma224_now] if x > 0] or [0])
        long_near_now = min(long_lines_now, key=lambda x: abs(close - x)) if long_lines_now and close > 0 else long_res_now

        # 1파 돌파 위치 탐색: 아래에서 위로 장기 기준선을 돌파한 최근 지점.
        cross_masks = []
        if ma112.notna().any():
            cross_masks.append((ma112 > 0) & (close_s >= ma112) & (close_s.shift(1) < ma112.shift(1)))
        if ma224.notna().any():
            cross_masks.append((ma224 > 0) & (close_s >= ma224) & (close_s.shift(1) < ma224.shift(1)))
        if bb40_mid_s.notna().any():
            cross_masks.append((bb40_mid_s > 0) & (close_s >= bb40_mid_s) & (close_s.shift(1) < bb40_mid_s.shift(1)))

        if cross_masks:
            breakout_mask = cross_masks[0]
            for m in cross_masks[1:]:
                breakout_mask = breakout_mask | m
            breakout_mask = breakout_mask.fillna(False)
        else:
            breakout_mask = pd.Series(False, index=h.index)

        lookback_break = min(60, len(h))
        recent_breaks = [
            int(i) for i, v in enumerate(breakout_mask.iloc[-lookback_break:].tolist(), start=len(h) - lookback_break)
            if bool(v)
        ]

        if recent_breaks:
            breakout_pos = recent_breaks[-1]
            breakout_found = True
        else:
            # 명확한 cross가 없더라도 최근 장기선 위로 안착한 첫 구간을 보조 돌파일로 잡는다.
            above_masks = []
            if ma112.notna().any():
                above_masks.append((ma112 > 0) & (close_s >= ma112))
            if ma224.notna().any():
                above_masks.append((ma224 > 0) & (close_s >= ma224))
            if bb40_mid_s.notna().any():
                above_masks.append((bb40_mid_s > 0) & (close_s >= bb40_mid_s))
            if above_masks:
                above_long = above_masks[0]
                for m in above_masks[1:]:
                    above_long = above_long | m
                above_long = above_long.fillna(False)
            else:
                above_long = pd.Series(False, index=h.index)

            lookback_above = min(25, len(h))
            above_recent = [
                int(i) for i, v in enumerate(above_long.iloc[-lookback_above:].tolist(), start=len(h) - lookback_above)
                if bool(v)
            ]
            breakout_pos = above_recent[0] if above_recent else len(h) - 1
            breakout_found = bool(above_recent)

        breakout_pos = max(0, min(int(breakout_pos), len(h) - 1))
        days_since_breakout = max(0, len(h) - 1 - breakout_pos)

        post = h.iloc[breakout_pos:].copy()
        if post.empty:
            post = h.tail(1).copy()
        post_high = _safe_float(pd.to_numeric(post['High'], errors='coerce').max(), high)
        pullback_pct = round((post_high - close) / post_high * 100.0, 2) if post_high > 0 and close > 0 else 0.0
        try:
            high_idx = pd.to_numeric(post['High'], errors='coerce').idxmax()
            high_pos = int(h.index.get_loc(high_idx))
        except Exception:
            try:
                high_pos = breakout_pos + int(pd.to_numeric(post['High'], errors='coerce').values.argmax())
            except Exception:
                high_pos = breakout_pos
        days_since_high = max(0, len(h) - 1 - int(high_pos))

        # 거래량 감소: 눌림 구간에서는 최근 거래량이 직전 거래량보다 줄거나 VMA20 1.2배 이하여야 한다.
        recent3_vol = _safe_float(vol_s.tail(3).mean(), 0.0)
        prior10_vol = _safe_float(vol_s.iloc[-13:-3].mean(), 0.0) if len(vol_s) >= 13 else _safe_float(vol_s.tail(10).mean(), 0.0)
        recent5_vol = _safe_float(vol_s.tail(5).mean(), 0.0)
        prior20_vol = _safe_float(vol_s.iloc[-25:-5].mean(), 0.0) if len(vol_s) >= 25 else _safe_float(vol_s.tail(20).mean(), 0.0)
        vol_ratio_now = round(vol / vma20_now, 2) if vma20_now > 0 else _safe_float(info.get('vol_ratio', 0), 0.0)
        volume_dry = bool(
            (prior10_vol > 0 and recent3_vol <= prior10_vol * 0.90) or
            (prior20_vol > 0 and recent5_vol <= prior20_vol * 0.88) or
            (vma20_now > 0 and vol <= vma20_now * 1.20)
        )

        # 지지선: MA20/40/112/224/BB40 중 현재가 아래에 있거나 근접한 선 중 가장 가까운 선.
        support_candidates = [x for x in [ma20_now, ma40_now, ma112_now, ma224_now, bb40_mid] if x and x > 0 and x <= close * 1.08]
        support_level = min(support_candidates, key=lambda x: abs(close - x)) if support_candidates and close > 0 else 0.0
        if not support_level:
            support_level = max([x for x in [ma20_now, ma40_now, ma112_now, ma224_now, bb40_mid] if x > 0] or [0])
        support_gap_pct = round((close - support_level) / support_level * 100.0, 2) if support_level > 0 and close > 0 else 999.0
        near_support = bool(support_level > 0 and -2.5 <= support_gap_pct <= 8.0)
        support_hold = bool(support_level > 0 and low >= support_level * 0.965 and close >= support_level * 0.99)

        close_loc_pct = round((close - low) / max(high - low, 1) * 100.0, 1) if high > low else 0.0
        prev_high = _safe_float(h.iloc[-2].get('High', 0), 0.0) if len(h) >= 2 else 0.0
        prev_close = _safe_float(h.iloc[-2].get('Close', 0), 0.0) if len(h) >= 2 else 0.0
        prev_ma5 = _safe_float(h.iloc[-2].get('MA5', 0), 0.0) if len(h) >= 2 else 0.0

        bullish_candle = bool(close >= open_p and close_loc_pct >= 68)
        reclaim_prev_high = bool(prev_high > 0 and close >= prev_high)
        reclaim_prev_close = bool(prev_close > 0 and close >= prev_close * 1.012)
        reclaim_ma5 = bool(ma5_now > 0 and close >= ma5_now and (prev_close <= prev_ma5 if prev_ma5 > 0 else True))
        reversal_signal = bool(bullish_candle or reclaim_prev_high or reclaim_ma5 or reclaim_prev_close)

        # v4.1.6: 진짜 눌림목은 '눌림 중'이 아니라 재상승 확인까지 포함한다.
        # 1파 돌파 후 충분히 식었고, 지지선에서 멀지 않으며, 당일 재상승 확인이 있어야 한다.
        try:
            ma5_prev = _safe_float(ma5.iloc[-2], 0.0) if len(ma5) >= 2 else 0.0
            ma5_slope_up = bool(ma5_now > 0 and ma5_prev > 0 and ma5_now >= ma5_prev * 0.995)
        except Exception:
            ma5_slope_up = False
        try:
            close_prev3_max = _safe_float(close_s.iloc[-4:-1].max(), 0.0) if len(close_s) >= 4 else 0.0
        except Exception:
            close_prev3_max = 0.0
        range_pct = round((high - low) / close * 100.0, 2) if close > 0 and high > low else 0.0
        strong_reentry_signal = bool(
            (reclaim_ma5 and ma5_slope_up) or
            reclaim_prev_high or
            (close_prev3_max > 0 and close >= close_prev3_max * 1.003) or
            (bullish_candle and close_loc_pct >= 72)
        )

        # 눌림 자체 조건. 너무 얕은 눌림은 C1, 너무 깊은 하락은 실패/완화에 가깝게 본다.
        pullback_valid = bool(days_since_breakout >= 2 and days_since_high >= 1 and 2.5 <= pullback_pct <= 14.0)
        pullback_deep_but_alive = bool(days_since_breakout >= 2 and days_since_high >= 1 and 14.0 < pullback_pct <= 20.0 and support_hold)
        fresh_breakout = bool(days_since_breakout <= 5 and (pullback_pct < 2.5 or days_since_high == 0))

        # C3는 v4.1.2보다 강하게 좁힌다. 과열 재돌파나 지지선에서 너무 먼 종목은 제외.
        c3_ready = bool(
            pullback_valid and
            support_hold and
            near_support and
            volume_dry and
            reversal_signal and
            close_loc_pct >= 65 and
            vol_ratio_now <= 1.50 and
            days_since_breakout <= 25
        )

        # v4.1.6 신규 핵심: C-눌림재상승형.
        # 기존 C3가 '완성'처럼 보였지만 실제로는 흔들림이 커서, 재상승 확인을 더 좁게 요구한다.
        pullback_reentry = bool(
            pullback_valid and
            3 <= days_since_breakout <= 28 and
            1 <= days_since_high <= 12 and
            3.0 <= pullback_pct <= 13.0 and
            support_hold and
            near_support and
            support_gap_pct <= 6.0 and
            volume_dry and
            strong_reentry_signal and
            close_loc_pct >= 70 and
            vol_ratio_now <= 1.35 and
            range_pct <= 18.0 and
            close >= max([x for x in [ma5_now, ma20_now] if x > 0] or [0])
        )

        # C2는 눌림이 진행됐고 생존은 했지만, C3 확인 조건이 하나 이상 부족한 구간.
        c2_progress = bool(
            (pullback_valid or pullback_deep_but_alive) and
            (support_hold or near_support or volume_dry) and
            days_since_breakout <= 35
        )

        if pullback_reentry:
            stage = 'C3'
            label = 'C-눌림재상승형'
            desc = '1파 돌파 이후 눌림·거래량 감소·재지지 후 5일선/전일고가 회복까지 확인된 좁은 2파 재상승 후보'
            bias = '눌림 후 재지지 확인형, +5/+10 단기 스윙 검증'
            rank = 0
        elif c3_ready:
            stage = 'C3'
            label = 'C3 눌림완성형'
            desc = '1파 돌파 이후 눌림·거래량 감소·재지지는 보이나 재상승 확인 강도는 추가 검증이 필요한 2파 준비형'
            bias = '눌림 후 2파 후보 관찰'
            rank = 1
        elif c2_progress:
            stage = 'C2'
            label = 'C2 눌림진행형'
            desc = '1파 이후 눌림은 진행됐지만 거래량 감소·재지지·재상승 확인 중 일부가 부족한 관찰형'
            bias = '눌림 관찰'
            rank = 1
        else:
            stage = 'C1'
            label = 'C1 1파돌파형'
            if fresh_breakout or days_since_breakout <= 5:
                desc = '장기선 돌파 1파가 막 나온 구간으로 추격보다 눌림 대기가 유리한 관심형'
            elif not breakout_found:
                desc = '명확한 최근 돌파일은 약하지만 장기선 위 재도전 상태로, 아직 눌림 확인이 부족한 관심형'
            else:
                desc = '1파 이후 눌림/재지지 조건이 충분히 확인되지 않아 추격보다 대기가 필요한 관심형'
            bias = '1파 후 눌림대기'
            rank = 2

        return {
            'c_stage': stage,
            'c_stage_label': label,
            'c_stage_desc': desc,
            'c_stage_bias': bias,
            'c_stage_rank': rank,
            'days_since_breakout': int(days_since_breakout),
            'days_since_high': int(days_since_high),
            'ymgp_pullback_pct': float(pullback_pct),
            'ymgp_post_high': round(post_high) if post_high > 0 else 0,
            'ymgp_support_level': round(support_level) if support_level > 0 else 0,
            'ymgp_support_gap_pct': float(support_gap_pct),
            'ymgp_support_hold': int(bool(support_hold)),
            'ymgp_near_support': int(bool(near_support)),
            'ymgp_volume_dry': int(bool(volume_dry)),
            'ymgp_reversal_signal': int(bool(reversal_signal)),
            'ymgp_close_loc_pct': float(close_loc_pct),
            'ymgp_vol_ratio_now': float(vol_ratio_now),
            'ymgp_strong_reentry_signal': int(bool(strong_reentry_signal)),
            'ymgp_reclaim_ma5': int(bool(reclaim_ma5)),
            'ymgp_reclaim_prev_high': int(bool(reclaim_prev_high)),
            'ymgp_bullish_candle': int(bool(bullish_candle)),
            'ymgp_range_pct': float(range_pct),
            'ymgp_pullback_reentry': int(bool(pullback_reentry)),
            'ymgp_fresh_breakout': int(bool(fresh_breakout)),
        }
    except Exception as e:
        return {
            'c_stage': 'C0',
            'c_stage_label': 'C0 분류오류',
            'c_stage_desc': f'역매공파 단계 분류 중 오류: {type(e).__name__}',
            'c_stage_bias': '관찰',
            'c_stage_rank': 9,
        }



def _evaluate_h_pre_breakout_context(hist: pd.DataFrame, breakout_pos: int) -> dict:
    """v4.2.9: H 신고가거자름 돌파 직전 구조/변동성 진단.

    목적:
    - 신고가 장대양봉이 나오기 직전에 삼각수렴/횡보/역매공파성 1파·2파 눌림 구조가 있었는지 본다.
    - ATR/최근 박스폭/일간 변동률이 과도한 고변동 종목을 별도 분리한다.
    - 이 값은 후보 생성 조건을 강제로 막기보다 백테스트 리포트와 후속 SAFE 재정의에 사용한다.
    """
    base = {
        'h_pre_atr_pct': 0.0,
        'h_pre_range20_pct': 0.0,
        'h_pre_range10_pct': 0.0,
        'h_pre_max_daily_chg_pct': 0.0,
        'h_pre_ma_converge_pct': 999.0,
        'h_high_volatility': 0,
        'h_pre_triangle': 0,
        'h_pre_sideways': 0,
        'h_pre_ymgp_base': 0,
        'h_pre_pullback2': 0,
        'h_pre_ma_converge': 0,
        'h_pre_structure_score': 0,
        'h_pre_structure_label': '구조부족',
    }
    try:
        if hist is None or hist.empty or breakout_pos is None or breakout_pos < 25:
            return base
        pre = hist.iloc[max(0, int(breakout_pos) - 35):int(breakout_pos)].copy()
        if len(pre) < 20:
            return base
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            pre[col] = pd.to_numeric(pre[col], errors='coerce')
        pre = pre.dropna(subset=['Open', 'High', 'Low', 'Close'])
        if len(pre) < 20:
            return base
        close_s = pre['Close']
        high_s = pre['High']
        low_s = pre['Low']
        vol_s = pre['Volume']
        pre_close = _safe_float(close_s.iloc[-1], 0.0)
        if pre_close <= 0:
            return base

        high20 = _safe_float(high_s.tail(20).max(), 0.0)
        low20 = _safe_float(low_s.tail(20).min(), 0.0)
        high10 = _safe_float(high_s.tail(10).max(), 0.0)
        low10 = _safe_float(low_s.tail(10).min(), 0.0)
        range20_pct = (high20 - low20) / pre_close * 100.0 if high20 > low20 else 0.0
        range10_pct = (high10 - low10) / pre_close * 100.0 if high10 > low10 else 0.0
        daily_chg = close_s.pct_change().abs().tail(20) * 100.0
        max_daily_chg = _safe_float(daily_chg.max(), 0.0)

        # ATR20 percent in the 20 bars before breakout.
        tr1 = high_s - low_s
        tr2 = (high_s - close_s.shift(1)).abs()
        tr3 = (low_s - close_s.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr20 = _safe_float(tr.tail(20).mean(), 0.0)
        atr_pct = atr20 / pre_close * 100.0 if pre_close > 0 else 0.0

        ma5 = _safe_float(close_s.tail(5).mean(), 0.0)
        ma20 = _safe_float(close_s.tail(20).mean(), 0.0)
        ma50 = _safe_float(pre['MA50'].iloc[-1], 0.0) if 'MA50' in pre.columns else _safe_float(close_s.tail(min(35, len(close_s))).mean(), 0.0)
        ma112 = _safe_float(pre['MA112'].iloc[-1], 0.0) if 'MA112' in pre.columns else 0.0
        ma_vals = [x for x in [ma5, ma20, ma50, ma112] if x and x > 0]
        if len(ma_vals) >= 3:
            ma_conv_pct = (max(ma_vals) - min(ma_vals)) / pre_close * 100.0
        else:
            ma_conv_pct = 999.0
        ma_converge = bool(ma_conv_pct <= 10.0)

        # Triangle: lower highs + higher lows + range contraction.
        hh = high_s.tail(20).reset_index(drop=True)
        ll = low_s.tail(20).reset_index(drop=True)
        x = np.arange(len(hh), dtype=float)
        high_slope_pct = 0.0
        low_slope_pct = 0.0
        try:
            high_slope_pct = float(np.polyfit(x, hh.values.astype(float), 1)[0] / pre_close * 100.0)
            low_slope_pct = float(np.polyfit(x, ll.values.astype(float), 1)[0] / pre_close * 100.0)
        except Exception:
            pass
        range_contract = bool(range20_pct > 0 and range10_pct <= range20_pct * 0.82)
        triangle = bool(high_slope_pct <= 0.03 and low_slope_pct >= -0.03 and range_contract and range20_pct <= 28.0)

        # Sideways / accumulation base: box is not too wide and MA20 slope is mild.
        ma20_prev = _safe_float(close_s.iloc[-20:-10].mean(), ma20) if len(close_s) >= 20 else ma20
        ma20_slope_pct = (ma20 / ma20_prev - 1.0) * 100.0 if ma20_prev > 0 else 0.0
        sideways = bool(range20_pct <= 18.0 and abs(ma20_slope_pct) <= 5.0 and atr_pct <= 5.5)

        # YMGP-like base: long MA/MA50 barrier has been approached or reclaimed, with short MA recovery.
        near_long = False
        if ma112 > 0:
            near_long = bool(pre_close >= ma112 * 0.94 and pre_close <= ma112 * 1.12)
        else:
            near_long = bool(ma50 > 0 and pre_close >= ma50 * 0.96 and pre_close <= ma50 * 1.12)
        short_recover = bool(ma5 >= ma20 * 0.985 if ma20 > 0 else False)
        vol_dry_base = False
        try:
            vol5 = _safe_float(vol_s.tail(5).mean(), 0.0)
            vol20 = _safe_float(vol_s.tail(20).mean(), 0.0)
            vol_dry_base = bool(vol20 > 0 and vol5 <= vol20 * 0.90)
        except Exception:
            pass
        ymgp_base = bool(near_long and short_recover and (ma_converge or vol_dry_base))

        # 2-wave pullback before breakout: recent high, controlled pullback, volume dry, MA20/MA50 alive.
        prior20_high = _safe_float(high_s.tail(20).max(), 0.0)
        last5_low = _safe_float(low_s.tail(5).min(), 0.0)
        pullback_from_prior_high = (prior20_high - pre_close) / prior20_high * 100.0 if prior20_high > 0 else 999.0
        close_above_ma20 = bool(ma20 > 0 and pre_close >= ma20 * 0.97)
        close_above_ma50 = bool(ma50 > 0 and pre_close >= ma50 * 0.95)
        pullback2 = bool(2.0 <= pullback_from_prior_high <= 12.0 and vol_dry_base and close_above_ma20 and close_above_ma50)

        # High volatility exclusion candidate.
        high_vol = bool(atr_pct >= 7.0 or range20_pct >= 38.0 or max_daily_chg >= 18.0)

        score = int(triangle) + int(sideways) + int(ymgp_base) + int(pullback2) + int(ma_converge)
        labels = []
        if triangle:
            labels.append('삼각수렴')
        if sideways:
            labels.append('횡보/박스')
        if ymgp_base:
            labels.append('역매공파기반')
        if pullback2:
            labels.append('1/2파눌림')
        if ma_converge and not labels:
            labels.append('이평수렴')
        if high_vol:
            labels.append('고변동주의')
        if not labels:
            labels.append('구조부족')

        base.update({
            'h_pre_atr_pct': round(float(atr_pct), 2),
            'h_pre_range20_pct': round(float(range20_pct), 2),
            'h_pre_range10_pct': round(float(range10_pct), 2),
            'h_pre_max_daily_chg_pct': round(float(max_daily_chg), 2),
            'h_pre_ma_converge_pct': round(float(ma_conv_pct), 2),
            'h_high_volatility': int(high_vol),
            'h_pre_triangle': int(triangle),
            'h_pre_sideways': int(sideways),
            'h_pre_ymgp_base': int(ymgp_base),
            'h_pre_pullback2': int(pullback2),
            'h_pre_ma_converge': int(ma_converge),
            'h_pre_structure_score': int(score),
            'h_pre_structure_label': '+'.join(labels[:4]),
        })
        return base
    except Exception as e:
        base['h_pre_structure_label'] = f'구조진단오류:{type(e).__name__}'
        return base



def _bt_check_leader_gap_pullback_on_hist(code: str, name: str, hist: pd.DataFrame, row: pd.Series, info: dict, idx_label: str, marcap: float) -> dict | None:
    """v4.4.6 백테스트용 LP — L-PULLBACK 리더갭 눌림재지지형.
    실시간 LP와 같은 철학을 과거일(hist 마지막 봉) 기준으로 재현한다.
    기존 L 검색식은 건드리지 않고, 별도 mode='LP' 후보만 추가한다.
    """
    try:
        if hist is None or len(hist) < 150:
            return None
        code = _normalize_code(code)
        is_mcap_or = marcap >= MCAP_OR_MIN
        if not ((idx_label in ('코스피200', '코스닥150')) or is_mcap_or):
            return None
        close = _safe_float(info.get('_close', row.get('Close', 0)), 0.0)
        open_p = _safe_float(info.get('_open', row.get('Open', 0)), 0.0)
        high = _safe_float(info.get('_high', row.get('High', 0)), 0.0)
        low = _safe_float(info.get('_low', row.get('Low', 0)), 0.0)
        vol = _safe_float(info.get('_vol', row.get('Volume', 0)), 0.0)
        amount_b = _safe_float(info.get('amount_b', 0), 0.0)
        if close < MIN_PRICE or amount_b < 1000.0:
            return None
        ma5 = _safe_float(row.get('MA5', 0), 0.0)
        ma10 = _safe_float(row.get('MA10', 0), 0.0)
        ma20 = _safe_float(row.get('MA20', 0), 0.0)
        vma20 = _safe_float(row.get('VMA20', 0), 0.0)
        cur_range = max(0.0, high - low)
        cur_close_loc = ((close - low) / cur_range * 100.0) if cur_range > 0 else 100.0
        cur_upper_wick = ((high - max(open_p, close)) / cur_range * 100.0) if cur_range > 0 else 0.0
        if cur_close_loc < 55.0 or cur_upper_wick > 45.0:
            return None

        gap_candidates = []
        max_days = min(5, len(hist) - 2)
        for d in range(1, max_days + 1):
            pos = len(hist) - 1 - d
            if pos < 60:
                continue
            g = hist.iloc[pos]
            prev = hist.iloc[pos - 1]
            g_open = _safe_float(g.get('Open', 0), 0.0)
            g_close = _safe_float(g.get('Close', 0), 0.0)
            g_high = _safe_float(g.get('High', 0), 0.0)
            g_low = _safe_float(g.get('Low', 0), 0.0)
            g_vol = _safe_float(g.get('Volume', 0), 0.0)
            prev_close = _safe_float(prev.get('Close', 0), 0.0)
            if min(g_open, g_close, g_high, g_low, prev_close) <= 0:
                continue
            g_amount_b = _safe_float(g.get('Amount', g_close * g_vol), g_close * g_vol) / 1e8
            gap_pct = (g_open / prev_close - 1.0) * 100.0
            vol50 = _safe_float(hist['Volume'].iloc[max(0, pos-50):pos].mean(), 0.0)
            vol50_ratio = g_vol / vol50 if vol50 > 0 else 0.0
            grange = max(0.0, g_high - g_low)
            g_close_loc = ((g_close - g_low) / grange * 100.0) if grange > 0 else 100.0
            g_upper_wick = ((g_high - max(g_open, g_close)) / grange * 100.0) if grange > 0 else 0.0
            if not (3.0 <= gap_pct <= 12.0):
                continue
            if g_amount_b < LEADER_GAP_CORE_AMOUNT_B:
                continue
            if vol50_ratio < LEADER_GAP_VOL50_MIN:
                continue
            if g_close_loc < 70.0 or g_upper_wick > 35.0:
                continue
            gap_candidates.append({
                'days': d, 'pos': pos, 'gap_pct': gap_pct, 'vol50_ratio': vol50_ratio,
                'amount_b': g_amount_b, 'open': g_open, 'close': g_close, 'high': g_high,
                'low': g_low, 'volume': g_vol, 'prev_close': prev_close,
                'close_loc': g_close_loc, 'wick': g_upper_wick,
            })
        if not gap_candidates:
            return None
        br = sorted(gap_candidates, key=lambda x: (x['days'], -x['amount_b'], -x['vol50_ratio']))[0]
        gap_support = max(br['prev_close'] * 0.995, br['low'] * 0.995)
        hold_gap_zone = close >= gap_support
        ma_hold = (ma5 > 0 and close >= ma5 * 0.995) or (ma10 > 0 and close >= ma10 * 0.995)
        volume_vs_gap = vol / br['volume'] if br.get('volume', 0) > 0 else 0.0
        volume_cooling = 0.20 <= volume_vs_gap <= 0.85
        post = hist.iloc[br['pos']:]
        post_high = _safe_float(post['High'].max(), br['high']) if post is not None and not post.empty else br['high']
        pullback_pct = ((post_high - close) / post_high * 100.0) if post_high > 0 else 0.0
        proper_pullback = 0.0 <= pullback_pct <= 12.0
        not_cliff = (ma20 <= 0 or close >= ma20 * 0.97) and close >= br['prev_close'] * 0.995
        if not (hold_gap_zone and ma_hold and volume_cooling and proper_pullback and not_cliff):
            return None

        passed = []
        score = 0
        score += 18; passed.append(f"①최근{br['days']}일내 L갭")
        if br['amount_b'] >= LEADER_GAP_CORE_AMOUNT_B:
            score += 18; passed.append(f"②갭봉대금{br['amount_b']:.0f}억")
        if 6.0 <= br['gap_pct'] <= 12.0:
            score += 12; passed.append(f"③갭6~12%({br['gap_pct']:+.1f}%)")
        else:
            score += 8; passed.append(f"③갭{br['gap_pct']:+.1f}%")
        if hold_gap_zone:
            score += 14; passed.append('④갭하단/전일종가 지지')
        if ma_hold:
            score += 10; passed.append('⑤5·10일선 재지지')
        if volume_cooling:
            score += 12; passed.append(f"⑥거래량식힘 {volume_vs_gap:.2f}배")
        if cur_close_loc >= 65.0:
            score += 8; passed.append(f"⑦타점봉종가위치{cur_close_loc:.0f}%")
        if amount_b >= 1000.0:
            score += 8; passed.append(f"⑧현거래대금{amount_b:.0f}억")
        score = min(int(score), 100)
        if score < 76:
            return None
        grade = '완전체' if score >= 90 else ('✅A급' if score >= 82 else 'B급')
        h = _bt_common_payload(code, name, 'LP', 'L-PULLBACK 리더갭 눌림재지지', grade, score, row, hist, idx_label, marcap, passed)
        stop_candidates = [x for x in [br['low'], br['prev_close'], ma10] if x > 0]
        stoploss = min(stop_candidates) if stop_candidates else low
        h.update({
            'band_type': 'LEADER_GAP_PULLBACK',
            'band_reason': '최근 L 리더갭 이후 갭하단/전일종가/5·10일선 눌림재지지 신규검증형',
            'lp_days_since_gap': int(br['days']),
            'lp_gap_pct': round(br['gap_pct'], 2),
            'lp_gap_amount_b': round(br['amount_b'], 1),
            'lp_gap_vol50_ratio': round(br['vol50_ratio'], 2),
            'lp_gap_close_loc_pct': round(br['close_loc'], 1),
            'lp_gap_wick_pct': round(br['wick'], 1),
            'lp_gap_low': round(br['low']),
            'lp_prev_close': round(br['prev_close']),
            'lp_volume_vs_gap': round(volume_vs_gap, 2),
            'lp_pullback_pct': round(pullback_pct, 2),
            'lp_gap_zone_hold': int(bool(hold_gap_zone)),
            'lp_ma_hold': int(bool(ma_hold)),
            'close_loc_pct': round(cur_close_loc, 1),
            'wick_pct': round(cur_upper_wick, 1),
            'vol_ratio': round(vol / vma20, 2) if vma20 > 0 else h.get('vol_ratio', 0),
            'stoploss': round(stoploss) if stoploss > 0 else h.get('stoploss', 0),
            'target1': round(close * 1.03),
            'rr': round(((close * 1.03 - close) / (close - stoploss)), 2) if stoploss > 0 and close > stoploss else 0.0,
            'new_pattern': 'LP',
            'sell_rule': 'L-PULLBACK: 갭하단/전일종가/5·10일선 재지지 유지 시만 관찰, 이탈 시 무효',
        })
        h.update(_classify_lp_candidate(h))
        return h
    except Exception as e:
        log_debug(f"LP 백테스트 오류 [{code}/{name}]: {e}")
        return None

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

        # I — 시대중심주 150/200일 시세분출 타점별 백테스트
        # v4.4.6: 기존 I 후보는 그대로 두고, IT(I-TRIGGER) 신규검색식 후보를 별도 mode로 추가해 텔레그램에서 성과검증한다.
        try:
            _i_hits = _build_icore_hits(code, name, hist, row, info, idx_label, marcap)
            hits.extend(_i_hits)
            for _ih in _i_hits:
                _it = _make_i_trigger_candidate(_ih)
                if _it is not None:
                    hits.append(_it)
        except Exception as e:
            log_debug(f"I-CORE/IT 신호 계산 오류 [{code}/{name}]: {e}")

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
                                'close_loc_pct': round(((today_close - today_low) / candle_range * 100.0) if candle_range > 0 else 100.0, 1),
                                'gap_unfilled': int(bool(gap_unfilled)),
                                'close_support': int(bool(close_support)),
                                'close_strength': int(bool(close_strength)),
                                'trail_ma': trail.get('trail_ma', ''),
                                'sell_rule': trail.get('sell_rule', ''),
                                'stoploss': stoploss,
                                'target1': target1,
                                'rr': round(((target1 - today_close) / risk), 2) if risk > 0 else 0.0,
                            })
                            hits.append(h)
            except Exception as e:
                log_debug(f"G 백테스트 오류 [{code}/{name}]: {e}")

        # LG — 대형주 리더갭 WATCH / SK하이닉스 복기형
        # v4.2.12: 일반 G-SAFE가 이격·20일상승률 과열로 제외하는 초대형 주도주 갭을
        # 실전 매수 확정이 아니라 별도 WATCH로 검증한다.
        try:
            if len(hist) >= max(130, GAP_HIGH_LOOKBACK + 5):
                prev = hist.iloc[-2]
                past = hist.iloc[:-1]
                today_open = info['_open']
                today_high = info['_high']
                today_low = info['_low']
                today_close = info['_close']
                today_volume = info['_vol']
                prev_close = _safe_float(prev.get('Close', 0), 0.0)
                amount_b = _safe_float(info.get('amount_b', 0.0), 0.0)
                large_leader = bool(idx_label == '코스피200' or marcap >= GAP_LARGE_CAP_MARCAP or amount_b >= LEADER_GAP_CORE_AMOUNT_B)
                if large_leader and today_open > 0 and today_close > 0 and prev_close > 0 and amount_b >= LEADER_GAP_MIN_AMOUNT_B:
                    gap_pct = (today_open / prev_close - 1.0) * 100.0
                    vol50 = _safe_float(past['Volume'].tail(50).mean(), 0.0)
                    vol50_ratio = today_volume / vol50 if vol50 > 0 else 0.0
                    candle_range = today_high - today_low
                    close_loc_pct = ((today_close - today_low) / candle_range * 100.0) if candle_range > 0 else 100.0
                    upper_wick_pct = ((today_high - max(today_open, today_close)) / candle_range * 100.0) if candle_range > 0 else 0.0
                    gap_ok = LEADER_GAP_MIN_PCT <= gap_pct <= LEADER_GAP_MAX_PCT
                    volume_ok = vol50_ratio >= LEADER_GAP_VOL50_MIN
                    gap_zone_hold = today_low >= prev_close * 0.995
                    close_support = today_close >= today_open * 0.990
                    close_strength = close_loc_pct >= LEADER_GAP_CLOSE_LOC_MIN
                    high_120 = _safe_float(past['High'].tail(GAP_HIGH_LOOKBACK).max(), 0.0)
                    high_252 = _safe_float(past['High'].tail(252).max(), 0.0) if len(past) >= 252 else high_120
                    box_high_60 = _safe_float(past['High'].tail(GAP_BOX_LOOKBACK).max(), 0.0)
                    new_high_120 = bool(high_120 > 0 and today_high >= high_120 * 1.002)
                    new_high_52w = bool(high_252 > 0 and today_high >= high_252 * 1.002)
                    near_high_120 = bool(high_120 > 0 and today_close >= high_120 * 0.970)
                    box_breakout = bool(box_high_60 > 0 and today_close >= box_high_60 * 1.002)
                    location_ok = bool(new_high_120 or new_high_52w or near_high_120 or box_breakout)
                    ma20 = _safe_float(row.get('MA20', 0), 0.0)
                    disparity20 = today_close / ma20 * 100.0 if ma20 > 0 else 999.0
                    close_20ago = _safe_float(hist['Close'].iloc[-21], 0.0) if len(hist) >= 21 else 0.0
                    runup20 = (today_close / close_20ago - 1.0) * 100.0 if close_20ago > 0 else 999.0
                    overheat_flag = int(disparity20 > GAP_DISPARITY20_MAX or runup20 > GAP_RUNUP20_MAX or upper_wick_pct > 30.0 or vol50_ratio > 8.0)

                    if gap_ok and volume_ok and gap_zone_hold and close_support and close_strength and location_ok:
                        score = 0
                        passed = []
                        score += 20; passed.append('①대형주/주도주')
                        score += 12; passed.append(f'②갭{gap_pct:+.1f}%')
                        score += 12; passed.append(f'③Vol50 {vol50_ratio:.1f}배')
                        score += 18; passed.append(f'④거래대금 {amount_b:.0f}억')
                        if amount_b >= LEADER_GAP_CORE_AMOUNT_B:
                            score += 8; passed.append('⑤초대형거래대금')
                        if gap_zone_hold:
                            score += 8; passed.append('⑥전일종가/갭구간지지')
                        if close_strength:
                            score += 10; passed.append(f'⑦종가위치 {close_loc_pct:.0f}%')
                        if new_high_52w:
                            score += 10; passed.append('⑧52주신고가권')
                        elif new_high_120:
                            score += 8; passed.append('⑧120일신고가권')
                        elif near_high_120 or box_breakout:
                            score += 6; passed.append('⑧전고점/박스권상단')
                        if upper_wick_pct <= 20.0:
                            score += 5; passed.append('⑨윗꼬리제한')
                        if overheat_flag:
                            passed.append('⚠️과열표시:이격/상승률/거래량')
                        score = min(int(score), 100)
                        if score >= 70:
                            grade = '완전체' if score >= 90 else ('✅A급' if score >= 80 else 'B급')
                            h = _bt_common_payload(code, name, 'L', '대형주리더갭WATCH', grade, score, row, hist, idx_label, marcap, passed)
                            stoploss = round(min(today_low, prev_close))
                            risk = max(today_close - stoploss, 0.0)
                            target1 = round(today_close * 1.03)
                            h.update({
                                'band_type': 'LEADER_GAP_WATCH',
                                'band_reason': '대형주/섹터대장 갭상승·초대형 거래대금·신고가권 유지 WATCH',
                                'leader_gap_watch': 1,
                                'gap_pct': round(gap_pct, 2),
                                'vol50_ratio': round(vol50_ratio, 2),
                                'vol_ratio': round(vol50_ratio, 2),
                                'amount_b': round(amount_b, 1),
                                'leader_gap_amount_b': round(amount_b, 1),
                                'leader_gap_core_amount': int(amount_b >= LEADER_GAP_CORE_AMOUNT_B),
                                'leader_gap_large_cap': int(large_leader),
                                'leader_gap_new_high_120': int(new_high_120),
                                'leader_gap_new_high_52w': int(new_high_52w),
                                'leader_gap_near_high_120': int(near_high_120),
                                'leader_gap_box_breakout': int(box_breakout),
                                'leader_gap_overheat_flag': int(overheat_flag),
                                'gap_unfilled': int(bool(gap_zone_hold)),
                                'close_support': int(bool(close_support)),
                                'close_strength': int(bool(close_strength)),
                                'close_loc_pct': round(close_loc_pct, 1),
                                'wick_pct': round(upper_wick_pct, 1),
                                'disparity20': round(disparity20, 1),
                                'runup20': round(runup20, 1),
                                'stoploss': stoploss,
                                'target1': target1,
                                'rr': round(((target1 - today_close) / risk), 2) if risk > 0 else 0.0,
                                'sell_rule': 'WATCH: 다음날 갭상단 유지/전일고가 돌파 확인. 추격 금지, 전일종가/갭하단 이탈 시 제외',
                            })
                            hits.append(h)
        except Exception as e:
            log_debug(f"대형주 리더갭 WATCH 백테스트 오류 [{code}/{name}]: {e}")

        # LP — v4.4.6 신규검색식 백테스트: 기존 L 리더갭 이후 1~5일 눌림재지지
        try:
            lp = _bt_check_leader_gap_pullback_on_hist(code, name, hist, row, info, idx_label, marcap)
            if lp is not None:
                hits.append(lp)
        except Exception as e:
            log_debug(f"LP 리더갭 눌림재지지 백테스트 오류 [{code}/{name}]: {e}")


        # H — 신고가 거자름 STRICT / 장대양봉 돌파봉 + 거래량 마른 타점봉 분리
        # v4.2.9 실험형: 돌파 당일을 매수일로 보지 않는다.
        # ① 과거 1~10거래일 안에 52주/120일 신고가를 장대양봉+강한 거래량으로 돌파한 기준봉을 찾고,
        # ② 현재봉이 거래량 마른 짧은 음봉/짧은 양봉으로 5일선 위에서 버티는지 확인한다.
        try:
            if len(hist) >= 150 and info['amount_b'] >= MIN_AMOUNT / 1e8:
                close = info['_close']; open_p = info['_open']; high = info['_high']; low = info['_low']; vol = info['_vol']
                ma5_now = _safe_float(row.get('MA5', 0), 0.0)
                ma10_now = _safe_float(row.get('MA10', 0), 0.0)
                ma20_now = _safe_float(row.get('MA20', 0), 0.0)
                prev5_vol = _safe_float(hist['Volume'].iloc[-6:-1].mean(), 0.0) if len(hist) >= 6 else 0.0
                vma20_now = _safe_float(row.get('VMA20', 0), 0.0)

                # 현재봉 = 실제 매수 타점 후보. 장대양봉이 아니라 거래량 마른 짧은 캔들이어야 한다.
                candle_range = high - low
                body = abs(close - open_p)
                body_pct = (body / close * 100.0) if close > 0 else 999.0
                range_pct = (candle_range / close * 100.0) if close > 0 and candle_range > 0 else 0.0
                close_loc_pct = ((close - low) / candle_range * 100.0) if candle_range > 0 else 100.0
                upper_wick_pct = ((high - max(open_p, close)) / candle_range * 100.0) if candle_range > 0 else 0.0
                short_candle = bool(body_pct <= 3.5 and range_pct <= 7.5)
                short_red_or_small_bull = bool((close < open_p) or (close >= open_p and body_pct <= 2.8))
                volume_dry_prev5 = bool(prev5_vol > 0 and vol <= prev5_vol)
                volume_dry_vma20 = bool(vma20_now > 0 and vol <= vma20_now * 0.85)
                volume_dry = bool(volume_dry_prev5 or volume_dry_vma20)
                ma5_close_hold = bool(ma5_now > 0 and close >= ma5_now)
                ma10_support = bool(ma10_now > 0 and close >= ma10_now * 0.985)
                ma_support = bool(ma5_close_hold or ma10_support)
                entry_close_loc_ok = bool(close_loc_pct >= 60.0)

                breakout_candidates = []
                # 최근 1~10거래일 전의 '신고가 장대양봉 돌파봉' 탐색. 현재봉은 거자름 타점 후보이므로 제외한다.
                max_days = min(10, len(hist) - 2)
                for d in range(1, max_days + 1):
                    pos = len(hist) - 1 - d
                    if pos <= 60:
                        continue
                    b = hist.iloc[pos]
                    prev_b = hist.iloc[pos - 1] if pos >= 1 else None
                    bopen = _safe_float(b.get('Open', 0), 0.0)
                    bclose = _safe_float(b.get('Close', 0), 0.0)
                    bhigh = _safe_float(b.get('High', 0), 0.0)
                    blow = _safe_float(b.get('Low', 0), 0.0)
                    bvol = _safe_float(b.get('Volume', 0), 0.0)
                    prev_close = _safe_float(prev_b.get('Close', 0), 0.0) if prev_b is not None else 0.0
                    bdate = pd.Timestamp(b.get('Date')).strftime('%Y-%m-%d') if not pd.isna(b.get('Date')) else ''

                    # 52주 고점이 가능하면 우선 사용, 부족하면 120일 고점으로 보조한다.
                    prior252 = hist.iloc[max(0, pos-252):pos]
                    prior120 = hist.iloc[max(0, pos-120):pos]
                    high252 = _safe_float(prior252['High'].max(), 0.0) if not prior252.empty else 0.0
                    high120 = _safe_float(prior120['High'].max(), 0.0) if not prior120.empty else 0.0
                    base_high = high252 if len(prior252) >= 200 and high252 > 0 else high120
                    if base_high <= 0 or bopen <= 0 or bclose <= 0 or bhigh <= 0 or blow <= 0:
                        continue

                    vol60 = _safe_float(hist['Volume'].iloc[max(0, pos-60):pos].mean(), 0.0)
                    bvol60_ratio = bvol / vol60 if vol60 > 0 else 0.0
                    brange = max(0.0, bhigh - blow)
                    breakout_day_ret_pct = ((bclose / prev_close - 1.0) * 100.0) if prev_close > 0 else 0.0
                    breakout_body_pct = ((bclose - bopen) / bopen * 100.0) if bopen > 0 else 0.0
                    breakout_close_loc_pct = ((bclose - blow) / brange * 100.0) if brange > 0 else 100.0
                    breakout_upper_wick_pct = ((bhigh - max(bopen, bclose)) / brange * 100.0) if brange > 0 else 0.0
                    breakout_body_range_pct = ((bclose - bopen) / brange * 100.0) if brange > 0 else 0.0

                    # v4.2.9 핵심: 신고가를 '장대양봉'으로 종가 돌파한 기준봉만 인정한다.
                    close_new_high = bool(bclose >= base_high * 1.002)
                    strong_vol = bool(bvol60_ratio >= 1.5)
                    long_bull_body = bool(
                        bclose > bopen
                        and breakout_day_ret_pct >= 7.0
                        and breakout_body_pct >= 5.0
                        and breakout_close_loc_pct >= 75.0
                        and breakout_upper_wick_pct <= 25.0
                    )
                    if close_new_high and strong_vol and long_bull_body:
                        breakout_candidates.append({
                            'pos': pos,
                            'days': d,
                            'date': bdate,
                            'open': bopen,
                            'close': bclose,
                            'high': bhigh,
                            'low': blow,
                            'volume': bvol,
                            'amount_b': _safe_float(b.get('Amount', bclose * bvol), bclose * bvol) / 1e8,
                            'base_high': base_high,
                            'vol60_ratio': bvol60_ratio,
                            'day_ret_pct': breakout_day_ret_pct,
                            'body_pct': breakout_body_pct,
                            'close_loc_pct': breakout_close_loc_pct,
                            'upper_wick_pct': breakout_upper_wick_pct,
                            'body_range_pct': breakout_body_range_pct,
                            'long_bull': 1,
                            'high_type': '52주신고가' if len(prior252) >= 200 and high252 > 0 else '120일신고가',
                        })

                if breakout_candidates:
                    br = sorted(breakout_candidates, key=lambda x: (x['days'], -x['vol60_ratio']))[0]
                    pre_ctx = _evaluate_h_pre_breakout_context(hist, br.get('pos', 0))
                    post = hist.iloc[br['pos']:]
                    post_high = _safe_float(post['High'].max(), high) if not post.empty else high
                    pullback_pct = ((post_high - close) / post_high * 100.0) if post_high > 0 and close > 0 else 0.0
                    hold_breakout_zone = bool(close >= br['base_high'] * 0.99 and close >= br['close'] * 0.92)
                    proper_pullback = bool(1.0 <= pullback_pct <= 10.0)
                    not_over_pullback = bool(0.0 <= pullback_pct <= 12.0)
                    not_cliff = bool(close >= max(ma20_now, br['base_high'] * 0.94) if ma20_now > 0 else close >= br['base_high'] * 0.94)
                    volume_vs_breakout = (vol / br['volume']) if br.get('volume', 0) > 0 else 0.0
                    volume_dry_vs_breakout = bool(br.get('volume', 0) > 0 and vol <= br['volume'] * 0.50)
                    strict_volume_dry = bool(volume_dry and volume_dry_vs_breakout)

                    cond = {
                        '①신고가장대양봉돌파': True,
                        '②돌파거래량1.5배↑': br['vol60_ratio'] >= 1.5,
                        '③돌파봉상승7%↑': br['day_ret_pct'] >= 7.0,
                        '④돌파봉몸통5%↑': br['body_pct'] >= 5.0,
                        '⑤돌파봉상단마감': br['close_loc_pct'] >= 75.0 and br['upper_wick_pct'] <= 25.0,
                        '⑥1~7일내타점': 1 <= br['days'] <= 7,
                        '⑦거래량마름': strict_volume_dry,
                        '⑧짧은음봉/짧은양봉': short_candle and short_red_or_small_bull,
                        '⑨5일선위종가': ma5_close_hold,
                        '⑩타점봉중상단마감': entry_close_loc_ok,
                        '⑪돌파권유지': hold_breakout_zone,
                        '⑫눌림1~10%': proper_pullback,
                        '⑬급락아님': not_cliff,
                    }
                    passed = [k for k, v in cond.items() if bool(v)]
                    score = 0
                    score += 18 if cond['①신고가장대양봉돌파'] else 0
                    score += 10 if cond['②돌파거래량1.5배↑'] else 0
                    score += 8 if cond['③돌파봉상승7%↑'] else 0
                    score += 8 if cond['④돌파봉몸통5%↑'] else 0
                    score += 8 if cond['⑤돌파봉상단마감'] else 0
                    score += 8 if cond['⑥1~7일내타점'] else 0
                    score += 14 if cond['⑦거래량마름'] else 0
                    score += 10 if cond['⑧짧은음봉/짧은양봉'] else 0
                    score += 8 if cond['⑨5일선위종가'] else 0
                    score += 4 if cond['⑩타점봉중상단마감'] else 0
                    score += 2 if cond['⑪돌파권유지'] else 0
                    score += 1 if cond['⑫눌림1~10%'] else 0
                    score += 1 if cond['⑬급락아님'] else 0
                    score = min(int(score), 100)

                    if score >= 70:
                        grade = '완전체' if score >= 90 else ('✅A급' if score >= 80 else 'B급')
                        h = _bt_common_payload(code, name, 'H', '신고가거자름STRICT', grade, score, row, hist, idx_label, marcap, passed)
                        h_stop = ma5_now * 0.985 if ma5_now > 0 else low
                        h.update({
                            'band_type': 'HIGH_DRYUP_STRICT',
                            'band_reason': '신고가 장대양봉 돌파 후 거래량 마른 짧은 타점봉',
                            'high_breakout_date': br['date'],
                            'high_breakout_type': br['high_type'],
                            'days_since_high_breakout': int(br['days']),
                            'breakout_vol60_ratio': round(br['vol60_ratio'], 2),
                            'breakout_base_high': round(br['base_high']),
                            'breakout_long_bull': 1,
                            'breakout_day_ret_pct': round(br['day_ret_pct'], 2),
                            'breakout_body_pct': round(br['body_pct'], 2),
                            'breakout_close_loc_pct': round(br['close_loc_pct'], 1),
                            'breakout_upper_wick_pct': round(br['upper_wick_pct'], 1),
                            'breakout_body_range_pct': round(br['body_range_pct'], 1),
                            'breakout_close': round(br['close']),
                            'breakout_volume': round(br['volume']),
                            'breakout_amount_b': round(_safe_float(br.get('amount_b', 0.0), 0.0), 1),
                            'entry_amount_b': round(_safe_float(info.get('amount_b', 0.0), 0.0), 1),
                            'entry_vs_breakout_amount': round((_safe_float(info.get('amount_b', 0.0), 0.0) / _safe_float(br.get('amount_b', 0.0), 1.0)), 2) if _safe_float(br.get('amount_b', 0.0), 0.0) > 0 else 0.0,
                            'post_breakout_high': round(post_high),
                            'high_dryup_pullback_pct': round(pullback_pct, 2),
                            'high_dryup_proper_pullback': int(bool(proper_pullback)),
                            'high_dryup_volume_dry': int(bool(strict_volume_dry)),
                            'high_dryup_volume_dry_prev5': int(bool(volume_dry_prev5)),
                            'high_dryup_volume_dry_vs_breakout': int(bool(volume_dry_vs_breakout)),
                            'high_dryup_short_candle': int(bool(short_candle and short_red_or_small_bull)),
                            'high_dryup_ma_support': int(bool(ma_support)),
                            'high_dryup_ma5_close_hold': int(bool(ma5_close_hold)),
                            'high_dryup_zone_hold': int(bool(hold_breakout_zone)),
                            'high_dryup_entry_close_loc_ok': int(bool(entry_close_loc_ok)),
                            'high_dryup_close_loc_pct': round(close_loc_pct, 1),
                            'high_dryup_body_pct': round(body_pct, 2),
                            'high_dryup_range_pct': round(range_pct, 2),
                            'high_dryup_upper_wick_pct': round(upper_wick_pct, 1),
                            'high_dryup_prev5_vol_ratio': round(vol / prev5_vol, 2) if prev5_vol > 0 else 0.0,
                            'high_dryup_vol_vs_breakout': round(volume_vs_breakout, 2),
                            'vol_ratio': round(vol / vma20_now, 2) if vma20_now > 0 else info.get('vol_ratio', 0),
                            'close_loc_pct': round(close_loc_pct, 1),
                            'wick_pct': round(upper_wick_pct, 1),
                            'stoploss': round(h_stop) if h_stop > 0 else info.get('stoploss', 0),
                            'target1': round(close * 1.05),
                            'rr': round(((close * 1.05 - close) / (close - h_stop)), 2) if h_stop > 0 and close > h_stop else 0.0,
                            **pre_ctx,
                            'sell_rule': '종가 5일선 이탈 시 정리',
                        })
                        hits.append(h)
        except Exception as e:
            log_debug(f"H 신고가거자름 STRICT 백테스트 오류 [{code}/{name}]: {e}")


        # HW — H 눌림반등 WATCH / 신고가 장대양봉 돌파 후 2~8일 눌림 관전형
        # v4.2.11: SK네트웍스 복기형. 실전 후보가 아니라 "내일 반등 관전" 후보를 검증한다.
        # ① 최근 2~8거래일 안에 신고가/전고점 장대양봉 돌파봉이 있었고,
        # ② 이후 3~15% 눌림, 5/10일선 근처, 거래량 감소, OBV 훼손 제한이면 WATCH로 기록한다.
        try:
            if len(hist) >= 150 and info['amount_b'] >= MIN_AMOUNT / 1e8:
                close = info['_close']; open_p = info['_open']; high = info['_high']; low = info['_low']; vol = info['_vol']
                ma5_now = _safe_float(row.get('MA5', 0), 0.0)
                ma10_now = _safe_float(row.get('MA10', 0), 0.0)
                ma20_now = _safe_float(row.get('MA20', 0), 0.0)
                prev5_vol = _safe_float(hist['Volume'].iloc[-6:-1].mean(), 0.0) if len(hist) >= 6 else 0.0
                vma20_now = _safe_float(row.get('VMA20', 0), 0.0)
                candle_range = high - low
                body = abs(close - open_p)
                body_pct = (body / close * 100.0) if close > 0 else 999.0
                range_pct = (candle_range / close * 100.0) if close > 0 and candle_range > 0 else 0.0
                close_loc_pct = ((close - low) / candle_range * 100.0) if candle_range > 0 else 100.0
                upper_wick_pct = ((high - max(open_p, close)) / candle_range * 100.0) if candle_range > 0 else 0.0
                near_ma5 = bool(ma5_now > 0 and close >= ma5_now * 0.955)
                near_ma10 = bool(ma10_now > 0 and close >= ma10_now * 0.970)
                ma20_alive = bool(ma20_now > 0 and close >= ma20_now * 0.985)
                ma_support_watch = bool(near_ma5 or near_ma10 or ma20_alive)
                volume_dry_prev5 = bool(prev5_vol > 0 and vol <= prev5_vol * 1.15)
                volume_dry_vma20 = bool(vma20_now > 0 and vol <= vma20_now * 1.05)
                try:
                    obv_now = _safe_float(row.get('OBV', 0), 0.0)
                    obv_min20 = _safe_float(hist['OBV'].tail(20).min(), 0.0)
                    obv_ma5 = _safe_float(hist['OBV'].tail(5).mean(), 0.0)
                    obv_ma20 = _safe_float(hist['OBV'].tail(20).mean(), 0.0)
                    obv_alive = bool(obv_now >= obv_min20 and (obv_ma20 == 0 or obv_ma5 >= obv_ma20 * 0.90))
                except Exception:
                    obv_alive = False

                breakout_candidates = []
                max_days = min(8, len(hist) - 2)
                for d in range(2, max_days + 1):
                    pos = len(hist) - 1 - d
                    if pos <= 60:
                        continue
                    b = hist.iloc[pos]
                    prev_b = hist.iloc[pos - 1] if pos >= 1 else None
                    bopen = _safe_float(b.get('Open', 0), 0.0)
                    bclose = _safe_float(b.get('Close', 0), 0.0)
                    bhigh = _safe_float(b.get('High', 0), 0.0)
                    blow = _safe_float(b.get('Low', 0), 0.0)
                    bvol = _safe_float(b.get('Volume', 0), 0.0)
                    prev_close = _safe_float(prev_b.get('Close', 0), 0.0) if prev_b is not None else 0.0
                    bdate = pd.Timestamp(b.get('Date')).strftime('%Y-%m-%d') if not pd.isna(b.get('Date')) else ''
                    prior252 = hist.iloc[max(0, pos-252):pos]
                    prior120 = hist.iloc[max(0, pos-120):pos]
                    high252 = _safe_float(prior252['High'].max(), 0.0) if not prior252.empty else 0.0
                    high120 = _safe_float(prior120['High'].max(), 0.0) if not prior120.empty else 0.0
                    base_high = high252 if len(prior252) >= 200 and high252 > 0 else high120
                    if base_high <= 0 or bopen <= 0 or bclose <= 0 or bhigh <= 0 or blow <= 0:
                        continue
                    vol60 = _safe_float(hist['Volume'].iloc[max(0, pos-60):pos].mean(), 0.0)
                    bvol60_ratio = bvol / vol60 if vol60 > 0 else 0.0
                    brange = max(0.0, bhigh - blow)
                    breakout_day_ret_pct = ((bclose / prev_close - 1.0) * 100.0) if prev_close > 0 else 0.0
                    breakout_body_pct = ((bclose - bopen) / bopen * 100.0) if bopen > 0 else 0.0
                    breakout_close_loc_pct = ((bclose - blow) / brange * 100.0) if brange > 0 else 100.0
                    breakout_upper_wick_pct = ((bhigh - max(bopen, bclose)) / brange * 100.0) if brange > 0 else 0.0
                    # WATCH는 H-STRICT보다 조금 넓다: 종가 신고가 또는 고가 신고가 재돌파를 허용한다.
                    close_new_high = bool(bclose >= base_high * 1.002)
                    high_new_high = bool(bhigh >= base_high * 1.006 and bclose >= base_high * 0.985)
                    strong_breakout = bool(
                        (close_new_high or high_new_high)
                        and bclose > bopen
                        and bvol60_ratio >= 1.5
                        and breakout_day_ret_pct >= 5.0
                        and breakout_body_pct >= 3.5
                        and breakout_close_loc_pct >= 65.0
                        and breakout_upper_wick_pct <= 35.0
                    )
                    if strong_breakout:
                        breakout_candidates.append({
                            'pos': pos, 'days': d, 'date': bdate,
                            'open': bopen, 'close': bclose, 'high': bhigh, 'low': blow,
                            'volume': bvol,
                            'amount_b': _safe_float(b.get('Amount', bclose * bvol), bclose * bvol) / 1e8,
                            'base_high': base_high,
                            'vol60_ratio': bvol60_ratio,
                            'day_ret_pct': breakout_day_ret_pct,
                            'body_pct': breakout_body_pct,
                            'close_loc_pct': breakout_close_loc_pct,
                            'upper_wick_pct': breakout_upper_wick_pct,
                            'long_bull': 1,
                            'high_type': '52주신고가' if len(prior252) >= 200 and high252 > 0 else '120일신고가',
                        })
                if breakout_candidates:
                    br = sorted(breakout_candidates, key=lambda x: (abs(x['days'] - 4), -x['amount_b'], -x['vol60_ratio']))[0]
                    pre_ctx = _evaluate_h_pre_breakout_context(hist, br.get('pos', 0))
                    post = hist.iloc[br['pos']:]
                    post_high = _safe_float(post['High'].max(), high) if not post.empty else high
                    pullback_pct = ((post_high - close) / post_high * 100.0) if post_high > 0 and close > 0 else 0.0
                    volume_vs_breakout = (vol / br['volume']) if br.get('volume', 0) > 0 else 0.0
                    volume_dry_vs_breakout = bool(br.get('volume', 0) > 0 and vol <= br['volume'] * 0.70)
                    volume_cool = bool(volume_dry_vs_breakout or volume_dry_prev5 or volume_dry_vma20)
                    pullback_watch = bool(3.0 <= pullback_pct <= 15.0)
                    deep_but_alive = bool(15.0 < pullback_pct <= 20.0 and close >= ma20_now if ma20_now > 0 else False)
                    zone_alive = bool(close >= br['base_high'] * 0.95 and close >= br['close'] * 0.88)
                    entry_loc_watch = bool(close_loc_pct >= 30.0)
                    not_crash = bool(range_pct <= 10.0 and upper_wick_pct <= 55.0 and close >= low * 1.01 if low > 0 else True)
                    cond = {
                        '①최근신고가돌파봉': True,
                        '②돌파Vol60_1.5배↑': br['vol60_ratio'] >= 1.5,
                        '③돌파후2~8일눌림': 2 <= br['days'] <= 8,
                        '④눌림3~15%': pullback_watch,
                        '⑤5/10/20선근처생존': ma_support_watch,
                        '⑥거래량식음': volume_cool,
                        '⑦돌파권생존': zone_alive,
                        '⑧OBV훼손제한': obv_alive,
                        '⑨종가위치30%이상': entry_loc_watch,
                        '⑩급락봉아님': not_crash,
                    }
                    passed = [k for k, v in cond.items() if bool(v)]
                    score = 0
                    score += 16 if cond['①최근신고가돌파봉'] else 0
                    score += 10 if cond['②돌파Vol60_1.5배↑'] else 0
                    score += 12 if cond['③돌파후2~8일눌림'] else 0
                    score += 14 if cond['④눌림3~15%'] else (6 if deep_but_alive else 0)
                    score += 12 if cond['⑤5/10/20선근처생존'] else 0
                    score += 12 if cond['⑥거래량식음'] else 0
                    score += 8 if cond['⑦돌파권생존'] else 0
                    score += 8 if cond['⑧OBV훼손제한'] else 0
                    score += 4 if cond['⑨종가위치30%이상'] else 0
                    score += 4 if cond['⑩급락봉아님'] else 0
                    score = min(int(score), 100)
                    if score >= 72:
                        grade = '완전체' if score >= 90 else ('✅A급' if score >= 82 else 'B급')
                        h = _bt_common_payload(code, name, 'H', 'H눌림반등WATCH', grade, score, row, hist, idx_label, marcap, passed)
                        h_stop = ma10_now * 0.985 if ma10_now > 0 else (ma20_now * 0.985 if ma20_now > 0 else low)
                        h.update({
                            'band_type': 'HIGH_PULLBACK_WATCH',
                            'band_reason': '신고가/전고점 장대양봉 돌파 후 2~8일 눌림반등 관전형',
                            'high_breakout_date': br['date'],
                            'high_breakout_type': br['high_type'],
                            'days_since_high_breakout': int(br['days']),
                            'breakout_vol60_ratio': round(br['vol60_ratio'], 2),
                            'breakout_base_high': round(br['base_high']),
                            'breakout_long_bull': 1,
                            'breakout_day_ret_pct': round(br['day_ret_pct'], 2),
                            'breakout_body_pct': round(br['body_pct'], 2),
                            'breakout_close_loc_pct': round(br['close_loc_pct'], 1),
                            'breakout_upper_wick_pct': round(br['upper_wick_pct'], 1),
                            'breakout_close': round(br['close']),
                            'breakout_volume': round(br['volume']),
                            'breakout_amount_b': round(_safe_float(br.get('amount_b', 0.0), 0.0), 1),
                            'entry_amount_b': round(_safe_float(info.get('amount_b', 0.0), 0.0), 1),
                            'entry_vs_breakout_amount': round((_safe_float(info.get('amount_b', 0.0), 0.0) / _safe_float(br.get('amount_b', 0.0), 1.0)), 2) if _safe_float(br.get('amount_b', 0.0), 0.0) > 0 else 0.0,
                            'post_breakout_high': round(post_high),
                            'high_pullback_watch': 1,
                            'high_pullback_watch_score': score,
                            'high_pullback_pullback_pct': round(pullback_pct, 2),
                            'high_pullback_volume_cool': int(bool(volume_cool)),
                            'high_pullback_volume_vs_breakout': round(volume_vs_breakout, 2),
                            'high_pullback_near_ma5': int(bool(near_ma5)),
                            'high_pullback_near_ma10': int(bool(near_ma10)),
                            'high_pullback_ma20_alive': int(bool(ma20_alive)),
                            'high_pullback_zone_alive': int(bool(zone_alive)),
                            'high_pullback_obv_alive': int(bool(obv_alive)),
                            'high_pullback_entry_loc_watch': int(bool(entry_loc_watch)),
                            'high_dryup_pullback_pct': round(pullback_pct, 2),
                            'high_dryup_volume_dry': int(bool(volume_cool)),
                            'high_dryup_volume_dry_vs_breakout': int(bool(volume_dry_vs_breakout)),
                            'high_dryup_short_candle': int(bool(body_pct <= 5.5 and range_pct <= 10.0)),
                            'high_dryup_ma_support': int(bool(ma_support_watch)),
                            'high_dryup_ma5_close_hold': int(bool(close >= ma5_now)) if ma5_now > 0 else 0,
                            'high_dryup_zone_hold': int(bool(zone_alive)),
                            'high_dryup_entry_close_loc_ok': int(bool(entry_loc_watch)),
                            'high_dryup_close_loc_pct': round(close_loc_pct, 1),
                            'high_dryup_body_pct': round(body_pct, 2),
                            'high_dryup_range_pct': round(range_pct, 2),
                            'vol_ratio': round(vol / vma20_now, 2) if vma20_now > 0 else info.get('vol_ratio', 0),
                            'close_loc_pct': round(close_loc_pct, 1),
                            'wick_pct': round(upper_wick_pct, 1),
                            'stoploss': round(h_stop) if h_stop > 0 else info.get('stoploss', 0),
                            'target1': round(close * 1.05),
                            'rr': round(((close * 1.05 - close) / (close - h_stop)), 2) if h_stop > 0 and close > h_stop else 0.0,
                            **pre_ctx,
                            'sell_rule': '반등 확인 전 관전형: 전일고가/5일선 회복 실패 또는 10/20일선 이탈 시 제외',
                        })
                        hits.append(h)
        except Exception as e:
            log_debug(f"H 눌림반등 WATCH 백테스트 오류 [{code}/{name}]: {e}")


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
                        's_quality': sig.get('s_quality', ''),
                        'is_s1_dry_good': sig.get('is_s1_dry_good', 0),
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
                    slock_h = _make_s2_lock_candidate(h)
                    if slock_h is not None:
                        hits.append(slock_h)
                    hits.append(h)
        except Exception as e:
            log_debug(f"S/SLOCK 백테스트 오류 [{code}/{name}]: {e}")

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

                # v3.6: 실시간 C전략이 나오는 데 백테스트에서 0건만 나오는 문제를 점검하기 위한 완화형 연결.
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
                    stage_meta = _classify_ymgp_stage(hist, row, info)
                    stage_label = stage_meta.get('c_stage_label', 'C1 1파돌파형')
                    c_grade = '완전체' if is_yma_strict else '✅A급'
                    c_score = 7 if is_yma_strict else 5
                    c_passed = ['역배열바닥', '매집흔적', '공구리', '장기선돌파', stage_label] if is_yma_strict else ['과거역배열', 'MA5/20회복', '장기선재도전', '매집/OBV흔적', stage_label]
                    h = _bt_common_payload(code, name, 'C', '역매공파', c_grade, c_score, row, hist, idx_label, marcap, c_passed)
                    h.update({
                        'band_type': 'YMGP',
                        'c_type': 'strict' if is_yma_strict else 'relaxed',
                        'c_type_label': f"{stage_label}·엄격형" if is_yma_strict else f"{stage_label}·완화형",
                        **stage_meta,
                        'kki_pattern': '바닥탈출대시세형',
                        'kki_habit': stage_meta.get('c_stage_bias', '매집 완료 후 장기 저항 돌파') if is_yma_strict else '과거 역배열 이후 장기선 재도전',
                        'kki_comment': stage_meta.get('c_stage_desc', '역매공파 타점 포착. 스윙 관점 유효.') if is_yma_strict else '완화형 C 백테스트 후보. 실전 후보가 아니라 C 조건 점검/비교용입니다.',
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
    기존 고가/종가 성과에 더해 v3.1부터 실전형 3/5 익절·손절 시뮬레이션을 추가하고, v3.6부터 튜닝 리포트/실전추천 조합을 함께 계산한다.

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

    def _calc_c_swing_metrics():
        """
        v4.1.4 C-SWING 전용 평가.
        기존 3/5 종가배팅 룰은 S-CORE에 맞고, 역매공파는 흔들림 후 10~20거래일에
        뒤늦게 올라가는 경우가 많아 별도 스윙 룰로도 평가한다.

        - 진입: 신호일 종가
        - 평가: 다음 hold_days 거래일
        - 익절 관찰: +5%, +10%, +15% 도달률
        - 손절 관찰: 장기선/재지지선 종가 이탈. 일중 저가가 아니라 종가 이탈만 손절로 본다.
        - 손절선: ymgp_support_level 우선, 없으면 기존 stoploss, 그것도 없으면 진입가 -8%
        """
        defaults = {
            'c_swing_stop_level': np.nan,
            'c_swing_stop_by_close': np.nan,
            'c_swing_stop_date': '',
            'c_swing_hit5_before_stop': np.nan,
            'c_swing_hit10_before_stop': np.nan,
            'c_swing_hit15_before_stop': np.nan,
            'c_swing_first_event': '',
            'c_swing_first_event_date': '',
            'c_swing_exit_ret': np.nan,
            'c_swing_max_high_ret': np.nan,
            'c_swing_min_low_ret': np.nan,
            'c_swing_close_ret': np.nan,
        }
        if str(hit.get('mode', '')) != 'C' or future.empty:
            return defaults

        support_level = _safe_float(hit.get('ymgp_support_level', 0), 0.0)
        base_stop = support_level if support_level > 0 else _safe_float(hit.get('stoploss', 0), 0.0)
        if base_stop <= 0:
            base_stop = entry * 0.92
        # 장기선 종가 이탈 기준이므로 약간의 여유를 둔다.
        swing_stop = base_stop * 0.985 if support_level > 0 else base_stop

        hit5 = hit10 = hit15 = 0
        stop_by_close = 0
        first_event = '기간종료'
        first_event_date = ''
        stop_date = ''
        exit_ret = np.nan
        active_highs = []
        active_lows = []

        for _, bar in future.iterrows():
            bdate = pd.Timestamp(bar.get('Date')).strftime('%Y-%m-%d') if not pd.isna(bar.get('Date')) else ''
            bclose = _safe_float(bar.get('Close', 0), 0.0)
            bhigh = _safe_float(bar.get('High', 0), 0.0)
            blow = _safe_float(bar.get('Low', 0), 0.0)
            if bhigh > 0:
                active_highs.append(bhigh)
            if blow > 0:
                active_lows.append(blow)

            # 장기선 종가 이탈 손절은 일중 저가가 아니라 종가로만 판단한다.
            if swing_stop > 0 and bclose > 0 and bclose < swing_stop:
                stop_by_close = 1
                stop_date = bdate
                if first_event == '기간종료':
                    first_event = '장기선종가이탈'
                    first_event_date = bdate
                exit_ret = _ret(bclose)
                break

            if bhigh >= entry * 1.15:
                hit5 = hit10 = hit15 = 1
                if first_event == '기간종료':
                    first_event = '+15선행'
                    first_event_date = bdate
                continue
            if bhigh >= entry * 1.10:
                hit5 = hit10 = 1
                if first_event == '기간종료':
                    first_event = '+10선행'
                    first_event_date = bdate
                continue
            if bhigh >= entry * 1.05:
                hit5 = 1
                if first_event == '기간종료':
                    first_event = '+5선행'
                    first_event_date = bdate

        if pd.isna(exit_ret):
            exit_ret = _ret(_safe_float(last_bar.get('Close', 0)))

        max_h = max(active_highs) if active_highs else max_high
        min_l = min(active_lows) if active_lows else min_low
        return {
            'c_swing_stop_level': round(swing_stop) if swing_stop > 0 else np.nan,
            'c_swing_stop_by_close': int(stop_by_close),
            'c_swing_stop_date': stop_date,
            'c_swing_hit5_before_stop': int(hit5),
            'c_swing_hit10_before_stop': int(hit10),
            'c_swing_hit15_before_stop': int(hit15),
            'c_swing_first_event': first_event,
            'c_swing_first_event_date': first_event_date,
            'c_swing_exit_ret': round(exit_ret, 2) if not pd.isna(exit_ret) else np.nan,
            'c_swing_max_high_ret': _ret(max_h),
            'c_swing_min_low_ret': _ret(min_l),
            'c_swing_close_ret': _ret(_safe_float(last_bar.get('Close', 0))),
        }



    def _calc_h_dryup_metrics():
        """v4.2.0 H-신고가거자름 전용 평가: 5일선 종가 이탈 손절과 +5/+10 선행을 별도 확인한다."""
        defaults = {
            'h_ma5_exit_by_close': np.nan,
            'h_ma5_exit_date': '',
            'h_hit5_before_ma5_exit': np.nan,
            'h_hit10_before_ma5_exit': np.nan,
            'h_first_event': '',
            'h_first_event_date': '',
            'h_exit_ret': np.nan,
            'h_max_high_ret': np.nan,
            'h_close_ret': np.nan,
        }
        if str(hit.get('mode', '')) != 'H' or future.empty:
            return defaults
        hit5 = hit10 = 0
        ma5_exit = 0
        first_event = '기간종료'
        first_date = ''
        exit_ret = np.nan
        for _, bar in future.iterrows():
            bdate = pd.Timestamp(bar.get('Date')).strftime('%Y-%m-%d') if not pd.isna(bar.get('Date')) else ''
            bclose = _safe_float(bar.get('Close', 0), 0.0)
            bhigh = _safe_float(bar.get('High', 0), 0.0)
            bma5 = _safe_float(bar.get('MA5', 0), 0.0)
            if bhigh >= entry * 1.10:
                hit5 = hit10 = 1
                if first_event == '기간종료':
                    first_event = '+10선행'
                    first_date = bdate
            elif bhigh >= entry * 1.05:
                hit5 = 1
                if first_event == '기간종료':
                    first_event = '+5선행'
                    first_date = bdate
            if bma5 > 0 and bclose > 0 and bclose < bma5:
                ma5_exit = 1
                if first_event == '기간종료':
                    first_event = '5일선종가이탈'
                    first_date = bdate
                exit_ret = _ret(bclose)
                break
        if pd.isna(exit_ret):
            exit_ret = _ret(_safe_float(last_bar.get('Close', 0)))
        return {
            'h_ma5_exit_by_close': int(ma5_exit),
            'h_ma5_exit_date': first_date if ma5_exit else '',
            'h_hit5_before_ma5_exit': int(hit5),
            'h_hit10_before_ma5_exit': int(hit10),
            'h_first_event': first_event,
            'h_first_event_date': first_date,
            'h_exit_ret': round(exit_ret, 2) if not pd.isna(exit_ret) else np.nan,
            'h_max_high_ret': _ret(max_high),
            'h_close_ret': _ret(_safe_float(last_bar.get('Close', 0))),
        }


    def _calc_i_core_metrics():
        """v4.3.1 I-CORE 전용 20/40/60일·+10/+20/+30/+50·MA20/50/박스 실패 평가."""
        defaults = {
            'i_ret_close_20d': np.nan, 'i_ret_close_40d': np.nan, 'i_ret_close_60d': np.nan,
            'i_ret_max_high_20d': np.nan, 'i_ret_max_high_40d': np.nan, 'i_ret_max_high_60d': np.nan,
            'i_hit10_60d': np.nan, 'i_hit20_60d': np.nan, 'i_hit30_60d': np.nan, 'i_hit50_60d': np.nan,
            'i_ma20_break_close': np.nan, 'i_ma50_break_close': np.nan, 'i_box_fail_close': np.nan,
            'i_first_event': '', 'i_first_event_date': '', 'i_exit_ret': np.nan,
            'i_mkt_regime': '시장데이터부족',
            'i_mkt_ret20_prior': np.nan, 'i_mkt_ret60_prior': np.nan, 'i_mkt_ret120_prior': np.nan,
            'i_mkt_ma200_slope20': np.nan, 'i_mkt_drawdown120': np.nan,
            'i_kospi_ret_close_20d': np.nan, 'i_kospi_ret_close_40d': np.nan, 'i_kospi_ret_close_60d': np.nan,
            'i_excess_close_20d': np.nan, 'i_excess_close_40d': np.nan, 'i_excess_close_60d': np.nan,
            'i_kosdaq_ret_close_20d': np.nan, 'i_kosdaq_ret_close_40d': np.nan, 'i_kosdaq_ret_close_60d': np.nan,
            'i_kosdaq_excess_close_20d': np.nan, 'i_kosdaq_excess_close_40d': np.nan, 'i_kosdaq_excess_close_60d': np.nan,
            'i_bench_name': 'KOSPI',
            'i_bench_ret_close_20d': np.nan, 'i_bench_ret_close_40d': np.nan, 'i_bench_ret_close_60d': np.nan,
            'i_bench_excess_close_20d': np.nan, 'i_bench_excess_close_40d': np.nan, 'i_bench_excess_close_60d': np.nan,
        }
        if str(hit.get('mode', '')) not in ('I', 'IT'):
            return defaults
        fut60 = df.iloc[signal_idx + 1: signal_idx + 61].copy()
        if fut60.empty:
            return defaults
        def _slice(n):
            return df.iloc[signal_idx + 1: signal_idx + 1 + n].copy()
        out = dict(defaults)
        for n in [20, 40, 60]:
            fs = _slice(n)
            if fs.empty:
                continue
            out[f'i_ret_close_{n}d'] = _ret(_safe_float(fs.iloc[-1].get('Close', 0)))
            out[f'i_ret_max_high_{n}d'] = _ret(_safe_float(fs['High'].max(), 0))
        max60 = _safe_float(fut60['High'].max(), 0)
        out['i_hit10_60d'] = int(max60 >= entry * 1.10)
        out['i_hit20_60d'] = int(max60 >= entry * 1.20)
        out['i_hit30_60d'] = int(max60 >= entry * 1.30)
        out['i_hit50_60d'] = int(max60 >= entry * 1.50)
        # 기준 박스/장기선 실패: 신호 시점 장기선이나 별도 stoploss 중 큰 쪽을 기준으로 너무 낮게 잡히지 않게 한다.
        base_stop = _safe_float(hit.get('stoploss', 0), 0.0)
        first_event = '기간종료'
        first_date = ''
        exit_ret = np.nan
        for _, bar in fut60.iterrows():
            bdate = pd.Timestamp(bar.get('Date')).strftime('%Y-%m-%d') if not pd.isna(bar.get('Date')) else ''
            bclose = _safe_float(bar.get('Close', 0), 0.0)
            bma20 = _safe_float(bar.get('MA20', 0), 0.0)
            bma50 = _safe_float(bar.get('MA50', 0), 0.0)
            if bma20 > 0 and bclose < bma20 * 0.985 and out['i_ma20_break_close'] is np.nan:
                out['i_ma20_break_close'] = 1
            if bma50 > 0 and bclose < bma50 * 0.985:
                out['i_ma50_break_close'] = 1
                if first_event == '기간종료':
                    first_event = '50일선종가이탈'
                    first_date = bdate
                    exit_ret = _ret(bclose)
                break
            if base_stop > 0 and bclose < base_stop:
                out['i_box_fail_close'] = 1
                if first_event == '기간종료':
                    first_event = '박스/장기선재이탈'
                    first_date = bdate
                    exit_ret = _ret(bclose)
                break
        if pd.isna(out.get('i_ma20_break_close')):
            out['i_ma20_break_close'] = 0
        if pd.isna(out.get('i_ma50_break_close')):
            out['i_ma50_break_close'] = 0
        if pd.isna(out.get('i_box_fail_close')):
            out['i_box_fail_close'] = 0
        if pd.isna(exit_ret):
            exit_ret = _ret(_safe_float(fut60.iloc[-1].get('Close', 0)))
        out['i_first_event'] = first_event
        out['i_first_event_date'] = first_date
        out['i_exit_ret'] = round(exit_ret, 2) if not pd.isna(exit_ret) else np.nan
        try:
            sig_dt = hit.get('signal_date', df.iloc[signal_idx].get('Date', None))
            out.update(_calc_i_core_market_context(sig_dt, {**out, 'index_label': hit.get('index_label', '')}))
        except Exception as e:
            log_debug(f"I-CORE 시장국면 업데이트 오류: {type(e).__name__}: {e}")
        return out

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

    # v4.4.6: 타점 이후 실제 흐름 프로파일(직행/눌림/횡보/개미털기/실패)을 계산한다.
    path_first_plus3_day = 0
    path_first_plus5_day = 0
    path_min_low_ret = np.nan
    path_min_low_day = 0
    path_max_high_ret = np.nan
    path_max_high_day = 0
    # v4.4.6: 이름 보정용 세부 흔들림 지표.
    # path_min_low_ret는 평가기간 전체 최대흔들림에 가까웠으므로,
    # 첫 1~3일 흔들림과 +3/+5 도달 전 흔들림을 따로 계산한다.
    path_first3d_min_low_ret = np.nan
    path_first3d_min_low_day = 0
    path_pre_plus3_min_low_ret = np.nan
    path_pre_plus3_min_low_day = 0
    path_pre_plus5_min_low_ret = np.nan
    path_pre_plus5_min_low_day = 0
    try:
        lows = []
        highs = []
        for _i, (_, bar) in enumerate(future.iterrows(), 1):
            low_ret = _ret(_safe_float(bar.get('Low', 0), 0.0))
            high_ret = _ret(_safe_float(bar.get('High', 0), 0.0))
            lows.append((low_ret, _i))
            highs.append((high_ret, _i))
            if not path_first_plus3_day and not pd.isna(high_ret) and high_ret >= 3.0:
                path_first_plus3_day = int(_i)
            if not path_first_plus5_day and not pd.isna(high_ret) and high_ret >= 5.0:
                path_first_plus5_day = int(_i)
        valid_lows = [(v, d) for v, d in lows if not pd.isna(v)]
        valid_highs = [(v, d) for v, d in highs if not pd.isna(v)]
        if valid_lows:
            path_min_low_ret, path_min_low_day = min(valid_lows, key=lambda x: x[0])
        if valid_highs:
            path_max_high_ret, path_max_high_day = max(valid_highs, key=lambda x: x[0])

        # v4.4.6: 첫흔들림 / 목표도달 전 흔들림 분리.
        first3_lows = [(v, d) for v, d in valid_lows if d <= 3]
        if first3_lows:
            path_first3d_min_low_ret, path_first3d_min_low_day = min(first3_lows, key=lambda x: x[0])

        pre3_limit = int(path_first_plus3_day) if path_first_plus3_day else min(5, len(valid_lows) if valid_lows else 5)
        pre3_lows = [(v, d) for v, d in valid_lows if d <= pre3_limit]
        if pre3_lows:
            path_pre_plus3_min_low_ret, path_pre_plus3_min_low_day = min(pre3_lows, key=lambda x: x[0])

        pre5_limit = int(path_first_plus5_day) if path_first_plus5_day else min(10, len(valid_lows) if valid_lows else 10)
        pre5_lows = [(v, d) for v, d in valid_lows if d <= pre5_limit]
        if pre5_lows:
            path_pre_plus5_min_low_ret, path_pre_plus5_min_low_day = min(pre5_lows, key=lambda x: x[0])
    except Exception:
        pass

    # v4.4.6: 대표흐름 분류는 평가기간 전체 최대하락보다 +3 도달 전 흔들림을 우선 사용한다.
    path_pre3_ref = path_pre_plus3_min_low_ret if not pd.isna(path_pre_plus3_min_low_ret) else path_min_low_ret

    if stop_before_3 == 1 or stop_rule == 1:
        path_type = '실패형'
    elif path_first_plus3_day and path_first_plus3_day <= 2 and (pd.isna(path_pre3_ref) or path_pre3_ref > -1.5):
        path_type = '직행형'
    elif path_first_plus3_day and (not pd.isna(path_pre3_ref)) and path_pre3_ref <= -3.0:
        path_type = '개미털기후상승형'
    elif path_first_plus3_day and (not pd.isna(path_pre3_ref)) and path_pre3_ref <= -1.0:
        path_type = '눌림후상승형'
    elif path_first_plus3_day and path_first_plus3_day >= 4:
        path_type = '횡보후상승형'
    elif (not pd.isna(path_max_high_ret)) and path_max_high_ret >= 2.0:
        path_type = '횡보/관찰형'
    else:
        path_type = '무반응/실패대기형'

    eval_result = {
        'eval_start_date': pd.Timestamp(next_bar.get('Date')).strftime('%Y-%m-%d') if not pd.isna(next_bar.get('Date')) else '',
        'eval_end_date': pd.Timestamp(last_bar.get('Date')).strftime('%Y-%m-%d') if not pd.isna(last_bar.get('Date')) else '',
        'hold_days': int(len(future)),
        'path_type': path_type,
        'path_first_plus3_day': int(path_first_plus3_day),
        'path_first_plus5_day': int(path_first_plus5_day),
        'path_min_low_ret': round(path_min_low_ret, 2) if not pd.isna(path_min_low_ret) else np.nan,
        'path_min_low_day': int(path_min_low_day),
        'path_first3d_min_low_ret': round(path_first3d_min_low_ret, 2) if not pd.isna(path_first3d_min_low_ret) else np.nan,
        'path_first3d_min_low_day': int(path_first3d_min_low_day),
        'path_pre_plus3_min_low_ret': round(path_pre_plus3_min_low_ret, 2) if not pd.isna(path_pre_plus3_min_low_ret) else np.nan,
        'path_pre_plus3_min_low_day': int(path_pre_plus3_min_low_day),
        'path_pre_plus5_min_low_ret': round(path_pre_plus5_min_low_ret, 2) if not pd.isna(path_pre_plus5_min_low_ret) else np.nan,
        'path_pre_plus5_min_low_day': int(path_pre_plus5_min_low_day),
        'path_max_high_ret': round(path_max_high_ret, 2) if not pd.isna(path_max_high_ret) else np.nan,
        'path_max_high_day': int(path_max_high_day),
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
    eval_result.update(_calc_c_swing_metrics())
    eval_result.update(_calc_h_dryup_metrics())
    eval_result.update(_calc_i_core_metrics())
    return eval_result

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
    # v4.1.6: C-눌림재상승형은 C 후보 내에서 최우선으로 선별한다.
    if 'ymgp_pullback_reentry' in work.columns:
        work['_c_pullback_rank'] = pd.to_numeric(work['ymgp_pullback_reentry'], errors='coerce').fillna(0).map(lambda x: 0 if int(x) == 1 else 1)
    else:
        work['_c_pullback_rank'] = 1
    # v4.1.3: C3는 좁은 눌림완성형으로 재정의하고, C2/C1을 분리해 C3 과다분류를 방지한다.
    if 'c_type' in work.columns:
        work['_c_type_rank'] = work['c_type'].astype(str).map(lambda x: 0 if x == 'strict' else (1 if x == 'relaxed' else 0))
    else:
        work['_c_type_rank'] = 0
    if 'c_stage' in work.columns:
        stage_rank_map = {'C3': 0, 'C2': 1, 'C1': 2, 'C0': 9}
        work['_c_stage_rank'] = work['c_stage'].astype(str).map(lambda x: stage_rank_map.get(x, 5))
    else:
        work['_c_stage_rank'] = 5
    work = work.sort_values(['signal_date', 'mode', '_c_pullback_rank', '_c_stage_rank', '_c_type_rank', '_grade_rank', '_score_sort', '_amount_sort', '_vol_sort'], ascending=[True, True, True, True, True, True, False, False, False])
    if not all_candidates:
        work = work.groupby(['signal_date', 'mode'], as_index=False, group_keys=False).head(int(top_per_strategy))
    work['selected_rank'] = work.groupby(['signal_date', 'mode']).cumcount() + 1
    return work.drop(columns=[c for c in ['_grade_rank', '_score_sort', '_amount_sort', '_vol_sort', '_c_type_rank', '_c_stage_rank', '_c_pullback_rank'] if c in work.columns])


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


def _format_c_swing_block(sub: pd.DataFrame, label: str) -> str:
    """v4.1.4 C-SWING 보유형 요약."""
    cnt = len(sub) if sub is not None else 0
    if cnt == 0:
        return f"- {label}: 0건"
    if 'c_swing_hit5_before_stop' not in sub.columns:
        return f"- {label}: C-SWING 평가 데이터 없음"
    hit5 = pd.to_numeric(sub.get('c_swing_hit5_before_stop'), errors='coerce').fillna(0)
    hit10 = pd.to_numeric(sub.get('c_swing_hit10_before_stop'), errors='coerce').fillna(0)
    hit15 = pd.to_numeric(sub.get('c_swing_hit15_before_stop'), errors='coerce').fillna(0)
    stop = pd.to_numeric(sub.get('c_swing_stop_by_close'), errors='coerce').fillna(0)
    max_ret = pd.to_numeric(sub.get('c_swing_max_high_ret'), errors='coerce')
    close_ret = pd.to_numeric(sub.get('c_swing_close_ret'), errors='coerce')
    exit_ret = pd.to_numeric(sub.get('c_swing_exit_ret'), errors='coerce')
    min_ret = pd.to_numeric(sub.get('c_swing_min_low_ret'), errors='coerce')
    return (
        f"- {label}: {cnt}건 | +5선행 {hit5.mean()*100:.1f}% | +10선행 {hit10.mean()*100:.1f}% | +15선행 {hit15.mean()*100:.1f}% | "
        f"장기선종가이탈 {stop.mean()*100:.1f}% | 평균최대상승 {max_ret.mean():.2f}% | "
        f"평균종가수익 {close_ret.mean():.2f}% | 평균청산수익 {exit_ret.mean():.2f}% | 평균최대하락 {min_ret.mean():.2f}%"
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
        order = ['G', 'L', 'S', 'A', 'B1', 'B2', 'C']
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
    return (df['mode'].astype(str).eq('S')) & (df.get('s_type', pd.Series('', index=df.index)).astype(str).eq('S2'))


def _bt_mask_s1_good(df: pd.DataFrame):
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    rr = pd.to_numeric(df.get('rr', 0), errors='coerce').fillna(0)
    amount = pd.to_numeric(df.get('amount_b', 0), errors='coerce').fillna(0)
    close_loc = pd.to_numeric(df.get('close_loc_pct', 0), errors='coerce').fillna(0)
    today_vol = pd.to_numeric(df.get('today_vol_ratio', df.get('vol_ratio', 0)), errors='coerce').fillna(0)
    vma_ratio = pd.to_numeric(df.get('vma5_20_ratio', pd.Series(9, index=df.index)), errors='coerce').fillna(9)
    s_quality = df.get('s_quality', pd.Series('', index=df.index)).astype(str)
    dry_ok = (today_vol < HIGH_REACCUM_S1_DRY_TODAY_MAX) | (vma_ratio <= HIGH_REACCUM_S1_DRY_VMA_MAX) | s_quality.eq('S1_DRY_GOOD')
    return (df['mode'].astype(str).eq('S')) & (df.get('s_type', pd.Series('', index=df.index)).astype(str).eq('S1')) & (rr >= HIGH_REACCUM_S1_GOOD_RR_MIN) & (amount >= HIGH_REACCUM_S1_GOOD_AMOUNT_MIN_B) & (close_loc >= HIGH_REACCUM_S1_GOOD_CLOSE_LOC_MIN) & dry_ok


def _bt_mask_s_core(df: pd.DataFrame):
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    return _bt_mask_s2(df) | _bt_mask_s1_good(df)


def _bt_mask_s_core_safe(df: pd.DataFrame):
    """v4.1: 12주 손절특이점에서 안정적이었던 S-CORE SAFE 조건.
    - S-CORE 내부
    - RR 1.0~1.5
    - 거래량비 1.5 미만
    - 종가위치 70% 이상
    """
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    rr = pd.to_numeric(df.get('rr', 0), errors='coerce').fillna(0)
    volr = pd.to_numeric(df.get('vol_ratio', df.get('today_vol_ratio', 0)), errors='coerce').fillna(0)
    close_loc = pd.to_numeric(df.get('close_loc_pct', 0), errors='coerce').fillna(0)
    return (
        _bt_mask_s_core(df)
        & (rr >= S_CORE_SAFE_RR_MIN)
        & (rr < S_CORE_SAFE_RR_MAX)
        & (volr < S_CORE_SAFE_VOL_RATIO_MAX)
        & (close_loc >= S_CORE_SAFE_CLOSE_LOC_MIN)
    )


def _bt_mask_s_core_risk(df: pd.DataFrame):
    """v4.1: S-CORE 내부 위험 플래그 조건. SAFE와 독립적으로 위험 구간을 잡는다."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    rr = pd.to_numeric(df.get('rr', 0), errors='coerce').fillna(0)
    volr = pd.to_numeric(df.get('vol_ratio', df.get('today_vol_ratio', 0)), errors='coerce').fillna(0)
    close_loc = pd.to_numeric(df.get('close_loc_pct', 0), errors='coerce').fillna(0)
    risk = (volr >= S_CORE_RISK_VOL_RATIO_MIN) | (rr < S_CORE_RISK_RR_LOW) | (rr >= S_CORE_RISK_RR_HIGH) | (close_loc < S_CORE_RISK_CLOSE_LOC_MIN)
    return _bt_mask_s_core(df) & risk


def _bt_mask_s_core_neutral(df: pd.DataFrame):
    """v4.1: S-CORE 중 SAFE도 RISK도 아닌 중립 후보.
    실전에서는 SAFE 다음의 관찰 가능 후보로 분리한다.
    """
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    core = _bt_mask_s_core(df)
    safe = _bt_mask_s_core_safe(df)
    risk = _bt_mask_s_core_risk(df)
    return core & (~safe) & (~risk)


def _bt_mask_s2_moderate_reignite(df: pd.DataFrame):
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    volr = pd.to_numeric(df.get('vol_ratio', df.get('today_vol_ratio', 0)), errors='coerce').fillna(0)
    rr = pd.to_numeric(df.get('rr', 0), errors='coerce').fillna(0)
    close_loc = pd.to_numeric(df.get('close_loc_pct', 0), errors='coerce').fillna(0)
    return _bt_mask_s2(df) & (volr >= 1.0) & (volr < S_CORE_RISK_VOL_RATIO_MIN) & (rr >= S_CORE_SAFE_RR_MIN) & (close_loc >= S_CORE_SAFE_CLOSE_LOC_MIN)


def _bt_mask_a_strong(df: pd.DataFrame):
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    volr = pd.to_numeric(df.get('vol_ratio', 0), errors='coerce').fillna(0)
    amount = pd.to_numeric(df.get('amount_b', 0), errors='coerce').fillna(0)
    grade = df.get('grade', pd.Series('', index=df.index)).astype(str)
    good_grade = grade.str.contains('완전체|A급|A', regex=True, na=False)
    return df['mode'].astype(str).eq('A') & (volr >= A_STRONG_VOL_RATIO_MIN) & (amount >= A_STRONG_AMOUNT_MIN_B) & good_grade



def _bt_mask_h_all(df: pd.DataFrame):
    """v4.2.11: H 신고가 거자름 STRICT 전체 후보. WATCH는 별도 마스크로 분리한다."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    band = df.get('band_type', pd.Series('', index=df.index)).astype(str)
    return df['mode'].astype(str).eq('H') & (band != 'HIGH_PULLBACK_WATCH')


def _bt_mask_h_watch_all(df: pd.DataFrame):
    """v4.2.11: 신고가/전고점 돌파 후 2~8일 눌림반등 관전형 H-WATCH."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    band = df.get('band_type', pd.Series('', index=df.index)).astype(str)
    return df['mode'].astype(str).eq('H') & (band == 'HIGH_PULLBACK_WATCH')


def _bt_mask_h_watch_ready(df: pd.DataFrame):
    """v4.2.11: H-WATCH 중 조건 점수가 높고 눌림·거래량·OBV가 살아있는 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    score = pd.to_numeric(df.get('high_pullback_watch_score', df.get('score', 0)), errors='coerce').fillna(0)
    pullback = pd.to_numeric(df.get('high_pullback_pullback_pct', df.get('high_dryup_pullback_pct', 999)), errors='coerce').fillna(999)
    cool = pd.to_numeric(df.get('high_pullback_volume_cool', 0), errors='coerce').fillna(0).astype(int)
    zone = pd.to_numeric(df.get('high_pullback_zone_alive', 0), errors='coerce').fillna(0).astype(int)
    obv = pd.to_numeric(df.get('high_pullback_obv_alive', 0), errors='coerce').fillna(0).astype(int)
    return _bt_mask_h_watch_all(df) & (score >= 82) & (pullback >= 3.0) & (pullback <= 15.0) & (cool == 1) & (zone == 1) & (obv == 1)


def _bt_mask_h_watch_ma5_reclaim(df: pd.DataFrame):
    """v4.2.11: H-WATCH 중 5일선 근처/회복형."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    ma5 = pd.to_numeric(df.get('high_pullback_near_ma5', 0), errors='coerce').fillna(0).astype(int)
    ma10 = pd.to_numeric(df.get('high_pullback_near_ma10', 0), errors='coerce').fillna(0).astype(int)
    return _bt_mask_h_watch_all(df) & ((ma5 == 1) | (ma10 == 1))


def _bt_mask_h_struct_safe(df: pd.DataFrame):
    """v4.2.9: H 신고가 거자름 STRUCT-SAFE 후보.
    장대양봉 신고가 돌파봉 + 거래량 마른 짧은 타점봉이라는 구조 조건만 통과한 후보다.
    거래량 구간별 운용분류(H-VOL SAFE/SWING/OVERHEAT/AGGRESSIVE)의 모수로 사용한다.
    """
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    mode_h = _bt_mask_h_all(df)
    score = pd.to_numeric(df.get('score', 0), errors='coerce').fillna(0)
    days = pd.to_numeric(df.get('days_since_high_breakout', 999), errors='coerce').fillna(999)
    bvol = pd.to_numeric(df.get('breakout_vol60_ratio', 0), errors='coerce').fillna(0)
    b_long = pd.to_numeric(df.get('breakout_long_bull', 0), errors='coerce').fillna(0).astype(int)
    b_ret = pd.to_numeric(df.get('breakout_day_ret_pct', 0), errors='coerce').fillna(0)
    b_body = pd.to_numeric(df.get('breakout_body_pct', 0), errors='coerce').fillna(0)
    b_close_loc = pd.to_numeric(df.get('breakout_close_loc_pct', 0), errors='coerce').fillna(0)
    b_wick = pd.to_numeric(df.get('breakout_upper_wick_pct', 999), errors='coerce').fillna(999)
    dry = pd.to_numeric(df.get('high_dryup_volume_dry', 0), errors='coerce').fillna(0).astype(int)
    dry_vs_breakout = pd.to_numeric(df.get('high_dryup_volume_dry_vs_breakout', 0), errors='coerce').fillna(0).astype(int)
    short = pd.to_numeric(df.get('high_dryup_short_candle', 0), errors='coerce').fillna(0).astype(int)
    ma5_hold = pd.to_numeric(df.get('high_dryup_ma5_close_hold', 0), errors='coerce').fillna(0).astype(int)
    zone = pd.to_numeric(df.get('high_dryup_zone_hold', 0), errors='coerce').fillna(0).astype(int)
    entry_loc_ok = pd.to_numeric(df.get('high_dryup_entry_close_loc_ok', 0), errors='coerce').fillna(0).astype(int)
    pullback = pd.to_numeric(df.get('high_dryup_pullback_pct', 999), errors='coerce').fillna(999)
    amount = pd.to_numeric(df.get('breakout_amount_b', df.get('amount_b', 0)), errors='coerce').fillna(0)
    return (
        mode_h
        & (score >= 82)
        & (days >= 1) & (days <= 7)
        & (b_long == 1)
        & (bvol >= 1.5)
        & (b_ret >= 7.0)
        & (b_body >= 5.0)
        & (b_close_loc >= 75.0)
        & (b_wick <= 25.0)
        & (dry == 1)
        & (dry_vs_breakout == 1)
        & (short == 1)
        & (ma5_hold == 1)
        & (entry_loc_ok == 1)
        & (zone == 1)
        & (pullback >= 1.0) & (pullback <= 10.0)
        & (amount >= 100.0)
    )


def _bt_mask_h_vol_safe(df: pd.DataFrame):
    """v4.2.9: H-VOL SAFE. 돌파Vol60 2~3배의 정상 강도 신고가 거자름."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    bvol = pd.to_numeric(df.get('breakout_vol60_ratio', 0), errors='coerce').fillna(0)
    return _bt_mask_h_struct_safe(df) & (bvol >= 2.0) & (bvol < 3.0)


def _bt_mask_h_vol_swing(df: pd.DataFrame):
    """v4.2.9: H-VOL SWING. 돌파Vol60 3~5배, 3/5보다 MA5 기준 스윙 검증 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    bvol = pd.to_numeric(df.get('breakout_vol60_ratio', 0), errors='coerce').fillna(0)
    return _bt_mask_h_struct_safe(df) & (bvol >= 3.0) & (bvol < 5.0)


def _bt_mask_h_vol_overheat(df: pd.DataFrame):
    """v4.2.9: H-VOL OVERHEAT. 돌파Vol60 5~8배, 과열/실패 가능성 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    bvol = pd.to_numeric(df.get('breakout_vol60_ratio', 0), errors='coerce').fillna(0)
    return _bt_mask_h_struct_safe(df) & (bvol >= 5.0) & (bvol < 8.0)


def _bt_mask_h_vol_aggressive(df: pd.DataFrame):
    """v4.2.9: H-VOL AGGRESSIVE. 돌파Vol60 8배 이상, 빠른 +3/+5 익절 전용 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    bvol = pd.to_numeric(df.get('breakout_vol60_ratio', 0), errors='coerce').fillna(0)
    return _bt_mask_h_struct_safe(df) & (bvol >= 8.0)


def _bt_mask_h_safe(df: pd.DataFrame):
    """v4.2.9: 최종 H-SAFE는 돌파Vol60 2~3배 구간으로 제한한다."""
    return _bt_mask_h_vol_safe(df)


def _bt_mask_h_risk(df: pd.DataFrame):
    """v4.2.9: H 신고가 거자름 STRICT 위험 후보.
    장대양봉 돌파봉은 있었지만, 타점봉이 5일선을 지키지 못하거나 거래량 마름/돌파권 유지가 깨진 경우다.
    """
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    mode_h = _bt_mask_h_all(df)
    days = pd.to_numeric(df.get('days_since_high_breakout', 999), errors='coerce').fillna(999)
    dry = pd.to_numeric(df.get('high_dryup_volume_dry', 0), errors='coerce').fillna(0).astype(int)
    short = pd.to_numeric(df.get('high_dryup_short_candle', 0), errors='coerce').fillna(0).astype(int)
    ma5_hold = pd.to_numeric(df.get('high_dryup_ma5_close_hold', 0), errors='coerce').fillna(0).astype(int)
    zone = pd.to_numeric(df.get('high_dryup_zone_hold', 0), errors='coerce').fillna(0).astype(int)
    entry_loc_ok = pd.to_numeric(df.get('high_dryup_entry_close_loc_ok', 0), errors='coerce').fillna(0).astype(int)
    pullback = pd.to_numeric(df.get('high_dryup_pullback_pct', 0), errors='coerce').fillna(0)
    amount = pd.to_numeric(df.get('breakout_amount_b', df.get('amount_b', 0)), errors='coerce').fillna(0)
    return mode_h & ((days > 10) | (dry == 0) | (short == 0) | (ma5_hold == 0) | (zone == 0) | (entry_loc_ok == 0) | (pullback > 12.0) | (amount < 50.0))


def _bt_mask_h_neutral(df: pd.DataFrame):
    """v4.2.9: H 후보 중 최종 SAFE/SWING/OVERHEAT/AGGRESSIVE/RISK 어디에도 속하지 않는 잔여 관찰 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    return (
        _bt_mask_h_all(df)
        & (~_bt_mask_h_safe(df))
        & (~_bt_mask_h_vol_swing(df))
        & (~_bt_mask_h_vol_overheat(df))
        & (~_bt_mask_h_vol_aggressive(df))
        & (~_bt_mask_h_risk(df))
    )




def _bt_mask_h_lowvol(df: pd.DataFrame):
    """v4.2.9: H 후보 중 직전 구조 기준 고변동이 아닌 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    highvol = pd.to_numeric(df.get('h_high_volatility', 0), errors='coerce').fillna(0).astype(int)
    return _bt_mask_h_all(df) & (highvol == 0)


def _bt_mask_h_prior_structure(df: pd.DataFrame):
    """v4.2.9: H 후보 중 돌파 직전 삼각수렴/횡보/역매공파/눌림 구조가 1개 이상 있는 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    score = pd.to_numeric(df.get('h_pre_structure_score', 0), errors='coerce').fillna(0)
    return _bt_mask_h_all(df) & (score >= 1)


def _bt_mask_h_pattern_safe(df: pd.DataFrame):
    """v4.2.9: H-STRUCT + 저변동 + 직전 구조 존재 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    return _bt_mask_h_struct_safe(df) & _bt_mask_h_lowvol(df) & _bt_mask_h_prior_structure(df)


def _bt_mask_h_vol_safe_pattern(df: pd.DataFrame):
    """v4.2.9: H-VOL SAFE 2~3배 + 저변동 + 직전 구조 존재 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    return _bt_mask_h_safe(df) & _bt_mask_h_lowvol(df) & _bt_mask_h_prior_structure(df)




def _bt_h_breakout_amount(df: pd.DataFrame) -> pd.Series:
    """v4.2.9: 신고가 장대양봉 돌파봉 거래대금(억원) 우선 사용."""
    if df is None:
        return pd.Series(dtype=float)
    return pd.to_numeric(df.get('breakout_amount_b', df.get('amount_b', 0)), errors='coerce').fillna(0)


def _bt_h_breakout_vol60(df: pd.DataFrame) -> pd.Series:
    """v4.2.9: 신고가 장대양봉 돌파봉 Vol60 배율."""
    if df is None:
        return pd.Series(dtype=float)
    return pd.to_numeric(df.get('breakout_vol60_ratio', 0), errors='coerce').fillna(0)


def _bt_mask_h_triangle(df: pd.DataFrame):
    """v4.2.9: 돌파 직전 삼각수렴이 있었던 H 전체 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    tri = pd.to_numeric(df.get('h_pre_triangle', 0), errors='coerce').fillna(0).astype(int)
    return _bt_mask_h_all(df) & (tri == 1)


def _bt_mask_h_triangle_struct(df: pd.DataFrame):
    """v4.2.9: H-STRUCT 조건까지 통과한 직전 삼각수렴 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    return _bt_mask_h_struct_safe(df) & _bt_mask_h_triangle(df)


def _bt_mask_h_triangle_lowvol(df: pd.DataFrame):
    """v4.2.9: 직전 삼각수렴 + 고변동 제외 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    return _bt_mask_h_triangle(df) & _bt_mask_h_lowvol(df)


def _bt_mask_h_core_500_1000_vol23(df: pd.DataFrame):
    """v4.2.9: 돌파봉 거래대금 500~1000억 × Vol60 2~3배 핵심 H 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    amt = _bt_h_breakout_amount(df)
    bvol = _bt_h_breakout_vol60(df)
    return _bt_mask_h_struct_safe(df) & (amt >= 500.0) & (amt < 1000.0) & (bvol >= 2.0) & (bvol < 3.0)


def _bt_mask_h_core_300_500_vol35(df: pd.DataFrame):
    """v4.2.9: 돌파봉 거래대금 300~500억 × Vol60 3~5배 핵심 H 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    amt = _bt_h_breakout_amount(df)
    bvol = _bt_h_breakout_vol60(df)
    return _bt_mask_h_struct_safe(df) & (amt >= 300.0) & (amt < 500.0) & (bvol >= 3.0) & (bvol < 5.0)


def _bt_mask_h_core_1000_2000_vol23(df: pd.DataFrame):
    """v4.2.9: 돌파봉 거래대금 1000~2000억 × Vol60 2~3배 보조 핵심 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    amt = _bt_h_breakout_amount(df)
    bvol = _bt_h_breakout_vol60(df)
    return _bt_mask_h_struct_safe(df) & (amt >= 1000.0) & (amt < 2000.0) & (bvol >= 2.0) & (bvol < 3.0)



def _bt_mask_h_core_500_700_vol23(df: pd.DataFrame):
    """v4.2.9: 500~700억 × Vol60 2~3배 세분화 핵심 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    amt = _bt_h_breakout_amount(df)
    bvol = _bt_h_breakout_vol60(df)
    return _bt_mask_h_struct_safe(df) & (amt >= 500.0) & (amt < 700.0) & (bvol >= 2.0) & (bvol < 3.0)


def _bt_mask_h_core_700_1000_vol23(df: pd.DataFrame):
    """v4.2.9: 700~1000억 × Vol60 2~3배 세분화 핵심 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    amt = _bt_h_breakout_amount(df)
    bvol = _bt_h_breakout_vol60(df)
    return _bt_mask_h_struct_safe(df) & (amt >= 700.0) & (amt < 1000.0) & (bvol >= 2.0) & (bvol < 3.0)


def _bt_mask_h_watch_500_700_vol30_40(df: pd.DataFrame):
    """v4.2.9: 500~700억 × Vol60 3~4배 관찰 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    amt = _bt_h_breakout_amount(df)
    bvol = _bt_h_breakout_vol60(df)
    return _bt_mask_h_struct_safe(df) & (amt >= 500.0) & (amt < 700.0) & (bvol >= 3.0) & (bvol < 4.0)


def _bt_mask_h_watch_700_1000_vol30_40(df: pd.DataFrame):
    """v4.2.9: 700~1000억 × Vol60 3~4배 관찰 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    amt = _bt_h_breakout_amount(df)
    bvol = _bt_h_breakout_vol60(df)
    return _bt_mask_h_struct_safe(df) & (amt >= 700.0) & (amt < 1000.0) & (bvol >= 3.0) & (bvol < 4.0)


def _bt_mask_h_watch_500_1000_vol30_40(df: pd.DataFrame):
    """v4.2.9: 500~1000억 × Vol60 3~4배 통합 관찰 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    amt = _bt_h_breakout_amount(df)
    bvol = _bt_h_breakout_vol60(df)
    return _bt_mask_h_struct_safe(df) & (amt >= 500.0) & (amt < 1000.0) & (bvol >= 3.0) & (bvol < 4.0)


def _bt_mask_h_watch_500_1000_vol40_50(df: pd.DataFrame):
    """v4.2.9: 500~1000억 × Vol60 4~5배 과열 경계 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    amt = _bt_h_breakout_amount(df)
    bvol = _bt_h_breakout_vol60(df)
    return _bt_mask_h_struct_safe(df) & (amt >= 500.0) & (amt < 1000.0) & (bvol >= 4.0) & (bvol < 5.0)


def _bt_mask_h_fast_8x(df: pd.DataFrame):
    """v4.2.9: Vol60 8배 이상 빠른 +3/+5 익절형 후보."""
    return _bt_mask_h_vol_aggressive(df)


def _bt_mask_h_v427_core_union(df: pd.DataFrame):
    """v4.2.9: 실험용 H 핵심 후보 통합.
    삼각수렴형과 거래대금×Vol 핵심셀을 OR로 묶어 실전 후보군 가능성을 본다.
    """
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    return (
        _bt_mask_h_triangle(df)
        | _bt_mask_h_core_500_1000_vol23(df)
        | _bt_mask_h_core_300_500_vol35(df)
        | _bt_mask_h_core_1000_2000_vol23(df)
    )


def _format_h_v427_core_report(df: pd.DataFrame) -> str:
    """v4.2.9: H-TRIANGLE / 거래대금×Vol 핵심 운용분류 요약."""
    lines = ["[H v4.2.10 핵심 운용분류 — TRIANGLE / AMOUNT×VOL + ENTRY PLAN]"]
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            lines.append("- 데이터 없음")
            return "\n".join(lines)
        h = df[_bt_mask_h_all(df)].copy()
        if h.empty:
            lines.append("- H 후보 없음")
            return "\n".join(lines)

        def _blk(label, mask):
            try:
                sub = h[mask(h)] if callable(mask) else h[mask]
                return _format_backtest_trade_rule_block(sub, label)
            except Exception as e:
                return f"- {label}: 계산 오류({e})"

        lines.append(_blk('🧊 H-TRIANGLE SAFE 직전삼각', _bt_mask_h_triangle))
        lines.append(_blk('🧊 H-TRIANGLE STRUCT 직전삼각+구조통과', _bt_mask_h_triangle_struct))
        lines.append(_blk('🧊 H-TRIANGLE LOWVOL 직전삼각+저변동', _bt_mask_h_triangle_lowvol))
        lines.append(_blk('🟢 H-CORE 500~1000억×2~3배', _bt_mask_h_core_500_1000_vol23))
        lines.append(_blk('  · 세분 500~700억×2~3배', _bt_mask_h_core_500_700_vol23))
        lines.append(_blk('  · 세분 700~1000억×2~3배', _bt_mask_h_core_700_1000_vol23))
        lines.append(_blk('🟡 H-WATCH 500~1000억×3~4배', _bt_mask_h_watch_500_1000_vol30_40))
        lines.append(_blk('⚠️ H-WATCH 500~1000억×4~5배', _bt_mask_h_watch_500_1000_vol40_50))
        lines.append(_blk('🟣 H-CORE 300~500억×3~5배', _bt_mask_h_core_300_500_vol35))
        lines.append(_blk('🔵 H-CORE 1000~2000억×2~3배', _bt_mask_h_core_1000_2000_vol23))
        lines.append(_blk('🔥 H-FAST 8배+ 빠른익절형', _bt_mask_h_fast_8x))
        lines.append(_blk('✅ H-CORE UNION 삼각/핵심셀 통합', _bt_mask_h_v427_core_union))
        lines.append(_blk('⚠️ H-OVERHEAT 5~8배 제외후보', _bt_mask_h_vol_overheat))
        lines.append("- 해석: v4.2.9은 일반 H를 실전 후보로 보지 않고, 직전 삼각수렴 또는 돌파봉 거래대금×Vol60 핵심셀만 따로 검증합니다.")
    except Exception as e:
        lines.append(f"- H v4.2.9 핵심분류 리포트 오류: {e}")
    return "\n".join(lines)


def _format_leader_gap_watch_report(df: pd.DataFrame) -> str:
    """v4.2.12: 대형주/섹터대장 리더갭 WATCH 성과 리포트."""
    lines = ["[👑 대형주 리더갭 WATCH — SK하이닉스형 갭상승 관전 v4.2.15]"]
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            lines.append("- 데이터 없음")
            return "\n".join(lines)
        lg = df[_bt_mask_leader_gap_all(df)].copy()
        if lg.empty:
            lines.append("- 대형주 리더갭 WATCH 후보 없음")
            return "\n".join(lines)

        def _blk(label, sub):
            return _format_backtest_trade_rule_block(sub, label)

        lines.append(_blk('👑 리더갭 WATCH 전체', lg))
        lines.append(_blk('🟢 리더갭 READY 점수80+·거래대금3000억+·종가위치70+', lg[_bt_mask_leader_gap_ready(lg)]))
        lines.append(_blk('💰 초대형 거래대금 5000억+ 리더갭', lg[_bt_mask_leader_gap_core_amount(lg)]))
        lines.append(_blk('⚠️ 이격/20일상승/거래량 과열표시 리더갭', lg[_bt_mask_leader_gap_overheat(lg)]))
        amount = pd.to_numeric(lg.get('leader_gap_amount_b', lg.get('amount_b', 0)), errors='coerce').fillna(0)
        for lo, hi, label in [(3000, 5000, '거래대금 3000~5000억'), (5000, 10000, '거래대금 5000억~1조'), (10000, 999999999, '거래대금 1조+')]:
            sub = lg[(amount >= lo) & (amount < hi)]
            if not sub.empty:
                lines.append(_blk(label, sub))
        gap = pd.to_numeric(lg.get('gap_pct', 0), errors='coerce').fillna(0)
        for lo, hi, label in [(1.5, 3, '갭 1.5~3%'), (3, 6, '갭 3~6%'), (6, 12.1, '갭 6~12%')]:
            sub = lg[(gap >= lo) & (gap < hi)]
            if not sub.empty:
                lines.append(_blk(label, sub))
        lines.append("")
        lines.append(_format_leader_gap_wick_impact_report(df))
        lines.append("- 해석: 이 섹션은 G-SAFE가 과열로 제외할 수 있는 대형 주도주 갭을 별도 WATCH로 검증합니다. 이격/20일상승률은 제외조건이 아니라 과열표시로만 봅니다. v4.2.15는 윗꼬리/종가위치 영향도를 함께 분해합니다.")
    except Exception as e:
        lines.append(f"- 대형주 리더갭 WATCH 리포트 오류: {e}")
    return "\n".join(lines)


def _format_leader_gap_wick_impact_report(df: pd.DataFrame) -> str:
    """v4.2.15: L 대형주 리더갭에서 윗꼬리/종가위치가 성과에 주는 영향도 리포트.
    목적: LG씨엔에스/HD현대에너지솔루션/LG전자처럼 같은 L-CORE라도
    윗꼬리와 종가위치에 따라 SAFE/CAUTION을 나눌 수 있는지 검증한다.
    """
    lines = ["[🧪 L-CORE 윗꼬리 영향도 — v4.2.15]"]
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            lines.append("- 데이터 없음")
            return "\n".join(lines)

        lg = df[_bt_mask_leader_gap_all(df)].copy()
        if lg.empty:
            lines.append("- 대형주 리더갭 후보 없음")
            return "\n".join(lines)

        def _blk(label, sub):
            return _format_backtest_trade_rule_block(sub, label)

        wick = pd.to_numeric(lg.get('wick_pct', lg.get('upper_wick_pct', 0)), errors='coerce').fillna(0.0)
        close_loc = pd.to_numeric(lg.get('close_loc_pct', 0), errors='coerce').fillna(0.0)
        amount = pd.to_numeric(lg.get('leader_gap_amount_b', lg.get('amount_b', 0)), errors='coerce').fillna(0.0)
        gap = pd.to_numeric(lg.get('gap_pct', 0), errors='coerce').fillna(0.0)
        vol50 = pd.to_numeric(lg.get('vol50_ratio', lg.get('vol_ratio', 0)), errors='coerce').fillna(0.0)

        core = lg[amount >= LEADER_GAP_CORE_AMOUNT_B].copy()
        core_wick = pd.to_numeric(core.get('wick_pct', core.get('upper_wick_pct', 0)), errors='coerce').fillna(0.0) if not core.empty else pd.Series(dtype=float)
        core_close = pd.to_numeric(core.get('close_loc_pct', 0), errors='coerce').fillna(0.0) if not core.empty else pd.Series(dtype=float)
        core_amount = pd.to_numeric(core.get('leader_gap_amount_b', core.get('amount_b', 0)), errors='coerce').fillna(0.0) if not core.empty else pd.Series(dtype=float)
        core_gap = pd.to_numeric(core.get('gap_pct', 0), errors='coerce').fillna(0.0) if not core.empty else pd.Series(dtype=float)

        lines.append(_blk('전체 L 리더갭', lg))
        lines.append(_blk('L-CORE 5000억+', core))
        lines.append("")
        lines.append("[윗꼬리 구간별 — L 전체]")
        for lo, hi, label in [
            (0, 10, '윗꼬리 0~10%'),
            (10, 20, '윗꼬리 10~20%'),
            (20, 25, '윗꼬리 20~25%'),
            (25, 35, '윗꼬리 25~35%'),
            (35, 999, '윗꼬리 35%+')
        ]:
            sub = lg[(wick >= lo) & (wick < hi)]
            if not sub.empty:
                lines.append(_blk(label, sub))

        if not core.empty:
            lines.append("")
            lines.append("[윗꼬리 구간별 — L-CORE 5000억+]")
            for lo, hi, label in [
                (0, 10, 'CORE 윗꼬리 0~10%'),
                (10, 20, 'CORE 윗꼬리 10~20%'),
                (20, 25, 'CORE 윗꼬리 20~25%'),
                (25, 35, 'CORE 윗꼬리 25~35%'),
                (35, 999, 'CORE 윗꼬리 35%+')
            ]:
                sub = core[(core_wick >= lo) & (core_wick < hi)]
                if not sub.empty:
                    lines.append(_blk(label, sub))

        lines.append("")
        lines.append("[종가위치 구간별 — L 전체]")
        for lo, hi, label in [
            (85, 101, '종가위치 85%+'),
            (75, 85, '종가위치 75~85%'),
            (70, 75, '종가위치 70~75%'),
            (65, 70, '종가위치 65~70%'),
            (0, 65, '종가위치 65% 미만')
        ]:
            sub = lg[(close_loc >= lo) & (close_loc < hi)]
            if not sub.empty:
                lines.append(_blk(label, sub))

        lines.append("")
        lines.append("[윗꼬리 × 종가위치 조합]")
        safe_a = lg[(amount >= LEADER_GAP_CORE_AMOUNT_B) & (close_loc >= 75) & (wick <= 20)]
        safe_b = lg[(amount >= LEADER_GAP_CORE_AMOUNT_B) & (close_loc >= 70) & (wick <= 25)]
        caution = lg[(amount >= LEADER_GAP_CORE_AMOUNT_B) & (((close_loc >= 65) & (close_loc < 70)) | ((wick > 25) & (wick <= 35)))]
        weak = lg[(amount >= LEADER_GAP_CORE_AMOUNT_B) & ((close_loc < 65) | (wick > 35))]
        mega_caution = lg[(amount >= 10000) & ((wick > 25) & (wick <= 35))]
        gap_big_good = lg[(amount >= LEADER_GAP_CORE_AMOUNT_B) & (gap >= 6) & (gap <= 12) & (close_loc >= 70)]
        lines.append(_blk('👑 L-SAFE 후보 A: 5000억+·종가위치75%+·윗꼬리20% 이하', safe_a))
        lines.append(_blk('👑 L-SAFE 후보 B: 5000억+·종가위치70%+·윗꼬리25% 이하', safe_b))
        lines.append(_blk('⚠️ L-CAUTION: 5000억+·종가위치65~70 또는 윗꼬리25~35%', caution))
        lines.append(_blk('🟡 L-WEAK/WATCH: 5000억+·종가위치65 미만 또는 윗꼬리35%+', weak))
        lines.append(_blk('💰 1조+인데 윗꼬리25~35%', mega_caution))
        lines.append(_blk('🚀 갭6~12%·5000억+·종가위치70%+', gap_big_good))

        lines.append("")
        lines.append("[실전 해석 가이드]")
        lines.append("- L-SAFE A/B가 전체 L-CORE보다 손절선행이 낮고 +3선행이 높으면, 실시간 L-CORE를 SAFE/CAUTION으로 분리합니다.")
        lines.append("- L-CAUTION이 평균수익은 플러스라도 손절선행이 20~25% 이상이면 +3/+5 빠른 익절형으로 낮춥니다.")
        lines.append("- 윗꼬리35%+ 또는 종가위치65% 미만이 약하면 L-WATCH/제외로 내립니다.")
        lines.append("- 1조+ 거래대금에서 25~35% 윗꼬리가 버티는지 확인하면, 개인 추격매수 과열을 대형 수급이 흡수하는지 판단할 수 있습니다.")
    except Exception as e:
        lines.append(f"- L 윗꼬리 영향도 리포트 오류: {e}")
    return "\n".join(lines)


def _format_h_pullback_watch_report(df: pd.DataFrame) -> str:
    """v4.2.11: H 돌파 후 눌림반등 WATCH 전용 성과 리포트."""
    lines = ["[👀 H 눌림반등 WATCH — 돌파 후 2~8일 눌림 관전형 v4.2.11]"]
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            lines.append("- 데이터 없음")
            return "\n".join(lines)
        h = df[_bt_mask_h_watch_all(df)].copy()
        if h.empty:
            lines.append("- H 눌림반등 WATCH 후보 없음")
            return "\n".join(lines)

        def _blk(label, sub):
            return _format_backtest_trade_rule_block(sub, label)

        lines.append(_blk('H-WATCH 전체', h))
        lines.append(_blk('🟢 H-WATCH READY 점수82+·눌림3~15·거래량식음·OBV생존', h[_bt_mask_h_watch_ready(h)]))
        lines.append(_blk('🟡 H-WATCH 5/10일선 근처', h[_bt_mask_h_watch_ma5_reclaim(h)]))
        # 돌파 후 경과일별
        days = pd.to_numeric(h.get('days_since_high_breakout', 999), errors='coerce').fillna(999)
        for lo, hi, label in [(2,3,'2~3일 눌림'), (4,5,'4~5일 눌림'), (6,8,'6~8일 눌림')]:
            sub = h[(days >= lo) & (days <= hi)]
            if not sub.empty:
                lines.append(_blk(label, sub))
        # 눌림률별
        pb = pd.to_numeric(h.get('high_pullback_pullback_pct', h.get('high_dryup_pullback_pct', np.nan)), errors='coerce')
        for lo, hi, label in [(3,7,'눌림 3~7%'), (7,12,'눌림 7~12%'), (12,15,'눌림 12~15%'), (15,21,'눌림 15%+ 생존')]:
            sub = h[(pb >= lo) & (pb < hi)]
            if not sub.empty:
                lines.append(_blk(label, sub))
        # 거래대금/Vol 조합 간단 분해
        amt = pd.to_numeric(h.get('breakout_amount_b', h.get('amount_b', 0)), errors='coerce').fillna(0)
        bvol = pd.to_numeric(h.get('breakout_vol60_ratio', 0), errors='coerce').fillna(0)
        core_like = h[(amt >= 300.0) & (amt < 1000.0) & (bvol >= 1.5) & (bvol < 5.0)]
        if not core_like.empty:
            lines.append(_blk('거래대금 300~1000억 × Vol60 1.5~5배 WATCH', core_like))
        event_like = h[(pd.to_numeric(h.get('entry_vs_breakout_amount', 0), errors='coerce').fillna(0) <= 0.7) & (pd.to_numeric(h.get('high_pullback_obv_alive', 0), errors='coerce').fillna(0).astype(int) == 1)]
        if not event_like.empty:
            lines.append(_blk('거래량 감소 + OBV 생존형', event_like))
        lines.append("- 해석: 이 섹션은 매수 확정 후보가 아니라 SK네트웍스처럼 돌파 후 며칠 눌린 뒤 재료/실적이 붙을 수 있는 '내일 반등 관전 후보'를 검증합니다.")
    except Exception as e:
        lines.append(f"- H 눌림반등 WATCH 리포트 오류: {e}")
    return "\n".join(lines)


def _format_h_pre_structure_report(df: pd.DataFrame) -> str:
    """v4.2.9: H 돌파 직전 구조/고변동 필터 성과."""
    lines = ["[H 직전 구조·고변동 필터 성과 — v4.2.9]"]
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            lines.append("- 데이터 없음")
            return "\n".join(lines)
        h = df[_bt_mask_h_all(df)].copy()
        if h.empty:
            lines.append("- H 후보 없음")
            return "\n".join(lines)

        def _blk(label, sub):
            return _format_backtest_trade_rule_block(sub, label)

        lines.append(_blk('H 전체', h))
        lines.append(_blk('H-STRUCT', h[_bt_mask_h_struct_safe(h)]))
        lines.append(_blk('🧊 H-TRIANGLE SAFE 직전삼각', h[_bt_mask_h_triangle(h)]))
        lines.append(_blk('🧊 H-TRIANGLE STRUCT 직전삼각+구조통과', h[_bt_mask_h_triangle_struct(h)]))
        lines.append(_blk('H-STRUCT 저변동', h[_bt_mask_h_struct_safe(h) & _bt_mask_h_lowvol(h)]))
        lines.append(_blk('H-STRUCT 직전구조 있음', h[_bt_mask_h_struct_safe(h) & _bt_mask_h_prior_structure(h)]))
        lines.append(_blk('H-STRUCT 저변동+직전구조', h[_bt_mask_h_pattern_safe(h)]))
        lines.append(_blk('🟢 H-VOL SAFE 2~3배', h[_bt_mask_h_safe(h)]))
        lines.append(_blk('🟢 H-VOL SAFE 2~3배+저변동+직전구조', h[_bt_mask_h_vol_safe_pattern(h)]))
        lines.append(_blk('⚠️ H 고변동 후보', h[_bt_mask_h_all(h) & (~_bt_mask_h_lowvol(h))]))
        lines.append("")
        lines.append("- 직전 구조별")
        flag_map = [
            ('삼각수렴 직전', 'h_pre_triangle'),
            ('횡보/박스 직전', 'h_pre_sideways'),
            ('역매공파 기반 직전', 'h_pre_ymgp_base'),
            ('1/2파 눌림 직전', 'h_pre_pullback2'),
            ('이평수렴 직전', 'h_pre_ma_converge'),
        ]
        for label, col in flag_map:
            if col in h.columns:
                flag = pd.to_numeric(h.get(col, 0), errors='coerce').fillna(0).astype(int)
                sub = h[flag == 1]
                if not sub.empty:
                    lines.append(_blk(label, sub))
        no_struct = h[pd.to_numeric(h.get('h_pre_structure_score', 0), errors='coerce').fillna(0) <= 0]
        lines.append(_blk('직전 구조 부족', no_struct))
        try:
            atr = pd.to_numeric(h.get('h_pre_atr_pct', np.nan), errors='coerce')
            rng = pd.to_numeric(h.get('h_pre_range20_pct', np.nan), errors='coerce')
            mx = pd.to_numeric(h.get('h_pre_max_daily_chg_pct', np.nan), errors='coerce')
            lines.append(
                f"- 직전 변동성 분포: ATR20 중앙 {atr.median():.1f}% / 20일박스폭 중앙 {rng.median():.1f}% / 20일최대일변동 중앙 {mx.median():.1f}%"
            )
        except Exception:
            pass
        lines.append("- 해석: 고변동 후보를 제외하고, 돌파 직전에 삼각수렴·횡보·역매공파 기반·1/2파 눌림 구조가 있던 H 후보가 실제로 손절선행을 낮추는지 확인합니다.")
    except Exception as e:
        lines.append(f"- H 직전 구조 리포트 오류: {e}")
    return "\n".join(lines)


def _format_h_amount_vol_matrix_report(df: pd.DataFrame) -> str:
    """v4.2.9: 돌파봉 거래대금 × 돌파Vol60 교차 성과.

    v4.2.5의 amount_b는 타점봉 거래대금에 가까웠다. v4.2.9에서는 breakout_amount_b를 사용해
    신고가 장대양봉이 실제로 어느 정도 자금으로 터졌는지와 Vol60 배율의 조합을 본다.
    """
    lines = ["[H 돌파봉 거래대금 × Vol60 매트릭스 — v4.2.9]"]
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            lines.append("- 데이터 없음")
            return "\n".join(lines)
        h = df[_bt_mask_h_struct_safe(df)].copy()
        if h.empty:
            lines.append("- H-STRUCT 후보 없음")
            return "\n".join(lines)
        amount_col = 'breakout_amount_b' if 'breakout_amount_b' in h.columns else 'amount_b'
        amt = pd.to_numeric(h.get(amount_col, np.nan), errors='coerce')
        bvol = pd.to_numeric(h.get('breakout_vol60_ratio', np.nan), errors='coerce')
        amount_bins = [0, 100, 200, 300, 500, 1000, 2000, 3000, 5000, 10000, 999999999]
        amount_labels = ['<100억', '100~200억', '200~300억', '300~500억', '500~1000억', '1000~2000억', '2000~3000억', '3000~5000억', '5000억~1조', '1조+']
        vol_bins = [0, 2, 3, 5, 8, 999999]
        vol_labels = ['<2배', '2~3배', '3~5배', '5~8배', '8배+']
        h['돌파거래대금구간'] = pd.cut(amt.fillna(-1), bins=amount_bins, labels=amount_labels, right=False, include_lowest=True)
        h['돌파Vol구간'] = pd.cut(bvol.fillna(-1), bins=vol_bins, labels=vol_labels, right=False, include_lowest=True)

        def _one(label: str, sub: pd.DataFrame) -> str:
            cnt = len(sub)
            if cnt == 0:
                return ""
            rule_pnl = pd.to_numeric(sub.get('rule35_pnl', np.nan), errors='coerce')
            win = rule_pnl.gt(0).mean() * 100 if rule_pnl.notna().any() else 0.0
            hit3 = pd.to_numeric(sub.get('hit3_before_stop', 0), errors='coerce').fillna(0).mean() * 100
            hit5 = pd.to_numeric(sub.get('hit5_before_stop', 0), errors='coerce').fillna(0).mean() * 100
            stop_first = pd.to_numeric(sub.get('stop_before_3', 0), errors='coerce').fillna(0).mean() * 100
            maxup = pd.to_numeric(sub.get('ret_max_high_hd', np.nan), errors='coerce').mean()
            h_exit_ret = pd.to_numeric(sub.get('h_exit_ret', np.nan), errors='coerce').mean()
            ma5_exit = pd.to_numeric(sub.get('h_ma5_exit_by_close', 0), errors='coerce').fillna(0).mean() * 100
            med_amt = pd.to_numeric(sub.get(amount_col, np.nan), errors='coerce').median()
            med_bvol = pd.to_numeric(sub.get('breakout_vol60_ratio', np.nan), errors='coerce').median()
            return (
                f"- {label}: {cnt}건 | 돌파대금중앙 {med_amt:.1f}억 | Vol60중앙 {med_bvol:.1f}배 | "
                f"3/5평균 {rule_pnl.mean():.2f}% | 승률 {win:.1f}% | +3선행 {hit3:.1f}% | +5선행 {hit5:.1f}% | 손절선행 {stop_first:.1f}% | "
                f"평균최대상승 {maxup:.2f}% | MA5청산 {h_exit_ret:.2f}% | MA5이탈 {ma5_exit:.1f}%"
            )

        lines.append("- H-STRUCT 기준, 돌파봉 거래대금 기준")
        for a_lab in amount_labels:
            sub_a = h[h['돌파거래대금구간'].astype(str).eq(str(a_lab))]
            if sub_a.empty:
                continue
            lines.append(f"- {a_lab} 구간")
            for v_lab in vol_labels:
                sub = sub_a[sub_a['돌파Vol구간'].astype(str).eq(str(v_lab))]
                # 너무 작은 셀도 보여주되 1~2건은 표본주의 표시
                if len(sub) > 0:
                    suffix = ' ⚠️표본주의' if len(sub) < 5 else ''
                    line = _one(f"  · {a_lab} × {v_lab}{suffix}", sub)
                    if line:
                        lines.append(line)
        # 저변동+직전구조 필터 후 핵심 셀만 압축
        core = h[_bt_mask_h_pattern_safe(h)].copy()
        lines.append("- 저변동+직전구조 필터 후 핵심 셀")
        if core.empty:
            lines.append("  · 후보 없음")
        else:
            core['돌파거래대금구간'] = pd.cut(pd.to_numeric(core.get(amount_col, np.nan), errors='coerce').fillna(-1), bins=amount_bins, labels=amount_labels, right=False, include_lowest=True)
            core['돌파Vol구간'] = pd.cut(pd.to_numeric(core.get('breakout_vol60_ratio', np.nan), errors='coerce').fillna(-1), bins=vol_bins, labels=vol_labels, right=False, include_lowest=True)
            for a_lab in amount_labels:
                for v_lab in vol_labels:
                    sub = core[(core['돌파거래대금구간'].astype(str).eq(str(a_lab))) & (core['돌파Vol구간'].astype(str).eq(str(v_lab)))]
                    if len(sub) > 0:
                        suffix = ' ⚠️표본주의' if len(sub) < 5 else ''
                        line = _one(f"  · CORE {a_lab} × {v_lab}{suffix}", sub)
                        if line:
                            lines.append(line)
        lines.append("- 해석: 거래량 2~3배를 단독 기준으로 보지 않고, 돌파봉 거래대금 구간 안에서 적정 Vol60 배율을 찾기 위한 교차표입니다.")
    except Exception as e:
        lines.append(f"- H 거래대금×Vol 매트릭스 오류: {e}")
    return "\n".join(lines)


def _format_h_500_1000_fine_matrix_report(df: pd.DataFrame) -> str:
    """v4.2.9: 500~1000억 구간을 촘촘하게 재검증하는 전용 리포트.

    목적:
    - 500~1000억 × 2~3배가 정말 500억대와 700~1000억대 모두에서 좋은지 확인한다.
    - 기존 3~5배 통합 구간이 나빴던 이유가 3~3.5배는 괜찮고 4~5배가 망쳤기 때문인지 분해한다.
    - 실전 조건은 바로 넓히지 않고, 3~4배 확장 가능성을 먼저 검증한다.
    """
    lines = ["[H 500~1000억 정밀 매트릭스 — v4.2.9]"]
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            lines.append("- 데이터 없음")
            return "\n".join(lines)
        h = df[_bt_mask_h_struct_safe(df)].copy()
        if h.empty:
            lines.append("- H-STRUCT 후보 없음")
            return "\n".join(lines)
        amount_col = 'breakout_amount_b' if 'breakout_amount_b' in h.columns else 'amount_b'
        amt = pd.to_numeric(h.get(amount_col, np.nan), errors='coerce')
        bvol = pd.to_numeric(h.get('breakout_vol60_ratio', np.nan), errors='coerce')
        fine = h[(amt >= 500.0) & (amt < 1000.0) & (bvol >= 1.5) & (bvol < 5.0)].copy()
        if fine.empty:
            lines.append("- 500~1000억 × Vol60 1.5~5배 H-STRUCT 후보 없음")
            return "\n".join(lines)

        def _one(label: str, sub: pd.DataFrame) -> str:
            cnt = len(sub)
            if cnt == 0:
                return ""
            rule_pnl = pd.to_numeric(sub.get('rule35_pnl', np.nan), errors='coerce')
            win = rule_pnl.gt(0).mean() * 100 if rule_pnl.notna().any() else 0.0
            hit3 = pd.to_numeric(sub.get('hit3_before_stop', 0), errors='coerce').fillna(0).mean() * 100
            hit5 = pd.to_numeric(sub.get('hit5_before_stop', 0), errors='coerce').fillna(0).mean() * 100
            stop_first = pd.to_numeric(sub.get('stop_before_3', 0), errors='coerce').fillna(0).mean() * 100
            maxup = pd.to_numeric(sub.get('ret_max_high_hd', np.nan), errors='coerce').mean()
            close_ret = pd.to_numeric(sub.get('ret_close_hd', np.nan), errors='coerce').mean()
            h_exit_ret = pd.to_numeric(sub.get('h_exit_ret', np.nan), errors='coerce').mean()
            ma5_exit = pd.to_numeric(sub.get('h_ma5_exit_by_close', 0), errors='coerce').fillna(0).mean() * 100
            med_amt = pd.to_numeric(sub.get(amount_col, np.nan), errors='coerce').median()
            med_bvol = pd.to_numeric(sub.get('breakout_vol60_ratio', np.nan), errors='coerce').median()
            sample = ' ⚠️표본주의' if cnt < 5 else ''
            return (
                f"- {label}{sample}: {cnt}건 | 돌파대금중앙 {med_amt:.1f}억 | Vol60중앙 {med_bvol:.2f}배 | "
                f"3/5평균 {rule_pnl.mean():.2f}% | 승률 {win:.1f}% | +3선행 {hit3:.1f}% | +5선행 {hit5:.1f}% | 손절선행 {stop_first:.1f}% | "
                f"평균최대상승 {maxup:.2f}% | 평균종가수익 {close_ret:.2f}% | MA5청산 {h_exit_ret:.2f}% | MA5이탈 {ma5_exit:.1f}%"
            )

        # 통합/요약 먼저 출력
        lines.append("- 요약: 500~1000억 내부 핵심 비교")
        for label, lo_amt, hi_amt, lo_vol, hi_vol in [
            ('500~1000억 × 2~3배 기존 CORE', 500, 1000, 2.0, 3.0),
            ('500~1000억 × 3~4배 확장관찰', 500, 1000, 3.0, 4.0),
            ('500~1000억 × 4~5배 과열경계', 500, 1000, 4.0, 5.0),
            ('500~700억 × 2~3배', 500, 700, 2.0, 3.0),
            ('700~1000억 × 2~3배', 700, 1000, 2.0, 3.0),
            ('500~700억 × 3~4배', 500, 700, 3.0, 4.0),
            ('700~1000억 × 3~4배', 700, 1000, 3.0, 4.0),
        ]:
            sub = h[(pd.to_numeric(h.get(amount_col, np.nan), errors='coerce') >= lo_amt)
                    & (pd.to_numeric(h.get(amount_col, np.nan), errors='coerce') < hi_amt)
                    & (pd.to_numeric(h.get('breakout_vol60_ratio', np.nan), errors='coerce') >= lo_vol)
                    & (pd.to_numeric(h.get('breakout_vol60_ratio', np.nan), errors='coerce') < hi_vol)]
            line = _one(label, sub)
            if line:
                lines.append(line)

        # 촘촘한 그리드
        amount_bins = [500, 600, 700, 850, 1000]
        amount_labels = ['500~600억', '600~700억', '700~850억', '850~1000억']
        vol_bins = [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]
        vol_labels = ['1.5~2배', '2~2.5배', '2.5~3배', '3~3.5배', '3.5~4배', '4~5배']
        fine['세부대금구간'] = pd.cut(pd.to_numeric(fine.get(amount_col, np.nan), errors='coerce'), bins=amount_bins, labels=amount_labels, right=False, include_lowest=True)
        fine['세부Vol구간'] = pd.cut(pd.to_numeric(fine.get('breakout_vol60_ratio', np.nan), errors='coerce'), bins=vol_bins, labels=vol_labels, right=False, include_lowest=True)
        lines.append("- 상세: 500~600 / 600~700 / 700~850 / 850~1000억 × Vol60 1.5~5배")
        for a_lab in amount_labels:
            sub_a = fine[fine['세부대금구간'].astype(str).eq(str(a_lab))]
            if sub_a.empty:
                continue
            lines.append(f"- {a_lab}")
            for v_lab in vol_labels:
                sub = sub_a[sub_a['세부Vol구간'].astype(str).eq(str(v_lab))]
                if len(sub) > 0:
                    line = _one(f"  · {a_lab} × {v_lab}", sub)
                    if line:
                        lines.append(line)
        lines.append("- 해석: 500~1000억에서 2~3배만 살릴지, 3~4배 일부를 관찰 후보로 확장할지 판단하기 위한 전용 리포트입니다. 4~5배가 계속 약하면 실전에서는 과열/제외로 유지합니다.")
    except Exception as e:
        lines.append(f"- H 500~1000억 정밀 리포트 오류: {e}")
    return "\n".join(lines)


def _format_h_ma5_block(sub: pd.DataFrame, label: str) -> str:
    """H 신고가 거자름 전용: 5일선 종가이탈 손절 기준 성과."""
    cnt = len(sub) if sub is not None else 0
    if cnt == 0:
        return f"- {label}: 0건"
    try:
        hit5 = pd.to_numeric(sub.get('h_hit5_before_ma5_exit', np.nan), errors='coerce')
        hit10 = pd.to_numeric(sub.get('h_hit10_before_ma5_exit', np.nan), errors='coerce')
        ma5_exit = pd.to_numeric(sub.get('h_ma5_exit_by_close', np.nan), errors='coerce')
        exit_ret = pd.to_numeric(sub.get('h_exit_ret', np.nan), errors='coerce')
        max_ret = pd.to_numeric(sub.get('h_max_high_ret', np.nan), errors='coerce')
        close_ret = pd.to_numeric(sub.get('h_close_ret', np.nan), errors='coerce')
        return (
            f"- {label}: {cnt}건 | +5선행 {hit5.fillna(0).mean()*100:.1f}% | +10선행 {hit10.fillna(0).mean()*100:.1f}% | "
            f"5일선종가이탈 {ma5_exit.fillna(0).mean()*100:.1f}% | 평균최대상승 {max_ret.mean():.2f}% | "
            f"평균종가수익 {close_ret.mean():.2f}% | 평균청산수익 {exit_ret.mean():.2f}%"
        )
    except Exception as e:
        return f"- {label}: {cnt}건 | H 전용 평가 오류: {e}"


def _format_h_breakout_volume_report(df: pd.DataFrame) -> str:
    """v4.2.9: 신고가 장대양봉 돌파봉의 Vol60 배율별 성과를 분해한다.

    목적:
    - '돌파봉 거래량이 평균 2~3배 정도가 좋은지, 5배/8배 이상이 더 좋은지'를 확인한다.
    - H 후보 전체와 H-SAFE 각각에서 3/5룰, 5일선 이탈 손절, +5/+10 선행을 함께 본다.
    """
    lines = ["[H 돌파봉 거래량 배율별 성과 — v4.2.9]"]
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            lines.append("- 데이터 없음")
            return "\n".join(lines)
        h = df[_bt_mask_h_all(df)].copy()
        if h.empty or 'breakout_vol60_ratio' not in h.columns:
            lines.append("- H 후보 또는 breakout_vol60_ratio 데이터 없음")
            return "\n".join(lines)

        def _num(frame, col, default=np.nan):
            return pd.to_numeric(frame.get(col, default), errors='coerce').fillna(default)

        bvol = _num(h, 'breakout_vol60_ratio')
        bins = [0, 2, 3, 5, 8, 12, 999999]
        labels = ['<2배', '2~3배', '3~5배', '5~8배', '8~12배', '12배+']
        try:
            h['돌파Vol60구간'] = pd.cut(bvol, bins=bins, labels=labels, right=False, include_lowest=True)
        except Exception:
            h['돌파Vol60구간'] = '구간화오류'

        def _one(label: str, sub: pd.DataFrame) -> str:
            cnt = len(sub)
            if cnt == 0:
                return f"- {label}: 0건"
            rule_pnl = pd.to_numeric(sub.get('rule35_pnl', np.nan), errors='coerce')
            win = rule_pnl.gt(0).mean() * 100 if rule_pnl.notna().any() else 0.0
            hit3 = pd.to_numeric(sub.get('hit3_before_stop', 0), errors='coerce').fillna(0).mean() * 100
            hit5 = pd.to_numeric(sub.get('hit5_before_stop', 0), errors='coerce').fillna(0).mean() * 100
            stop_first = pd.to_numeric(sub.get('stop_before_3', 0), errors='coerce').fillna(0).mean() * 100
            maxup = pd.to_numeric(sub.get('ret_max_high_hd', np.nan), errors='coerce').mean()
            close_ret = pd.to_numeric(sub.get('ret_close_hd', np.nan), errors='coerce').mean()
            ma5_exit = pd.to_numeric(sub.get('h_ma5_exit_by_close', 0), errors='coerce').fillna(0).mean() * 100
            h_exit_ret = pd.to_numeric(sub.get('h_exit_ret', np.nan), errors='coerce').mean()
            h_hit5 = pd.to_numeric(sub.get('h_hit5_before_ma5_exit', 0), errors='coerce').fillna(0).mean() * 100
            h_hit10 = pd.to_numeric(sub.get('h_hit10_before_ma5_exit', 0), errors='coerce').fillna(0).mean() * 100
            med_bvol = pd.to_numeric(sub.get('breakout_vol60_ratio', np.nan), errors='coerce').median()
            return (
                f"- {label}: {cnt}건 | 돌파Vol60중앙 {med_bvol:.1f}배 | "
                f"3/5평균 {rule_pnl.mean():.2f}% | 승률 {win:.1f}% | +3선행 {hit3:.1f}% | +5선행 {hit5:.1f}% | 손절선행 {stop_first:.1f}% | "
                f"평균최대상승 {maxup:.2f}% | 평균종가수익 {close_ret:.2f}% | "
                f"MA5청산수익 {h_exit_ret:.2f}% | MA5이탈 {ma5_exit:.1f}% | MA5전+5 {h_hit5:.1f}% | MA5전+10 {h_hit10:.1f}%"
            )

        lines.append("- H 전체 기준")
        for lab in labels:
            sub = h[h['돌파Vol60구간'].astype(str).eq(str(lab))]
            if len(sub) > 0:
                lines.append(_one(f"전체 {lab}", sub))
        safe = h[_bt_mask_h_safe(h)].copy()
        lines.append("- H-SAFE 기준")
        if safe.empty:
            lines.append("  · H-SAFE 후보 없음")
        else:
            sbvol = pd.to_numeric(safe.get('breakout_vol60_ratio', np.nan), errors='coerce')
            try:
                safe['돌파Vol60구간'] = pd.cut(sbvol, bins=bins, labels=labels, right=False, include_lowest=True)
            except Exception:
                safe['돌파Vol60구간'] = '구간화오류'
            for lab in labels:
                sub = safe[safe['돌파Vol60구간'].astype(str).eq(str(lab))]
                if len(sub) > 0:
                    lines.append(_one(f"SAFE {lab}", sub))

        lines.append("- v4.2.10 운용분류: 🟢H-VOL SAFE=2~3배 / 🟣H-VOL SWING=3~5배 / ⚠️H-OVERHEAT=5~8배 / 🔥H-AGGRESSIVE=8배 이상")
        lines.append("- 해석 가이드: 2~3배는 정상 강도, 3~5배는 MA5 스윙 확인, 5~8배는 과열주의, 8배 이상은 빠른 +3/+5 익절 전용으로 봅니다.")
    except Exception as e:
        lines.append(f"- H 돌파 거래량 분석 오류: {e}")
    return "\n".join(lines)


def _format_h_amount_report(df: pd.DataFrame) -> str:
    """v4.2.9: H 신고가거자름의 거래대금 구간별 성과를 분해한다.

    목적:
    - 돌파Vol60 2~3배가 좋더라도 거래대금 규모에 따라 실전성이 달라지는지 확인한다.
    - H 전체/STRUCT/VOL-SAFE/SWING/OVERHEAT/AGGRESSIVE 각각에서 거래대금별 3/5룰과 MA5 청산 성과를 비교한다.
    """
    lines = ["[H 거래대금 세분화 구간별 성과 — v4.2.9]"]
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            lines.append("- 데이터 없음")
            return "\n".join(lines)
        h = df[_bt_mask_h_all(df)].copy()
        if h.empty or 'amount_b' not in h.columns:
            lines.append("- H 후보 또는 amount_b 데이터 없음")
            return "\n".join(lines)

        def _amount_bucket(frame: pd.DataFrame) -> pd.Series:
            amt_col = 'breakout_amount_b' if 'breakout_amount_b' in frame.columns else 'amount_b'
            amt = pd.to_numeric(frame.get(amt_col, np.nan), errors='coerce')
            # v4.2.9: 거래대금 구간 세분화. amount_b 단위는 억원.
            # 1조 = 10,000억. 너무 큰 구간을 줄여 2~3배 돌파봉의 실전 유동성 구간을 더 정확히 본다.
            bins = [0, 30, 50, 100, 200, 300, 500, 1000, 2000, 3000, 5000, 10000, 999999999]
            labels = [
                '<30억', '30~50억', '50~100억', '100~200억', '200~300억',
                '300~500억', '500~1000억', '1000~2000억', '2000~3000억',
                '3000~5000억', '5000억~1조', '1조+'
            ]
            return pd.cut(amt.fillna(-1), bins=bins, labels=labels, right=False, include_lowest=True)

        amount_labels = [
            '<30억', '30~50억', '50~100억', '100~200억', '200~300억',
            '300~500억', '500~1000억', '1000~2000억', '2000~3000억',
            '3000~5000억', '5000억~1조', '1조+'
        ]

        def _one(label: str, sub: pd.DataFrame) -> str:
            cnt = len(sub)
            if cnt == 0:
                return f"- {label}: 0건"
            rule_pnl = pd.to_numeric(sub.get('rule35_pnl', np.nan), errors='coerce')
            win = rule_pnl.gt(0).mean() * 100 if rule_pnl.notna().any() else 0.0
            hit3 = pd.to_numeric(sub.get('hit3_before_stop', 0), errors='coerce').fillna(0).mean() * 100
            hit5 = pd.to_numeric(sub.get('hit5_before_stop', 0), errors='coerce').fillna(0).mean() * 100
            stop_first = pd.to_numeric(sub.get('stop_before_3', 0), errors='coerce').fillna(0).mean() * 100
            maxup = pd.to_numeric(sub.get('ret_max_high_hd', np.nan), errors='coerce').mean()
            close_ret = pd.to_numeric(sub.get('ret_close_hd', np.nan), errors='coerce').mean()
            ma5_exit = pd.to_numeric(sub.get('h_ma5_exit_by_close', 0), errors='coerce').fillna(0).mean() * 100
            h_exit_ret = pd.to_numeric(sub.get('h_exit_ret', np.nan), errors='coerce').mean()
            h_hit5 = pd.to_numeric(sub.get('h_hit5_before_ma5_exit', 0), errors='coerce').fillna(0).mean() * 100
            h_hit10 = pd.to_numeric(sub.get('h_hit10_before_ma5_exit', 0), errors='coerce').fillna(0).mean() * 100
            amt_col2 = 'breakout_amount_b' if 'breakout_amount_b' in sub.columns else 'amount_b'
            med_amt = pd.to_numeric(sub.get(amt_col2, np.nan), errors='coerce').median()
            med_bvol = pd.to_numeric(sub.get('breakout_vol60_ratio', np.nan), errors='coerce').median()
            return (
                f"- {label}: {cnt}건 | 거래대금중앙 {med_amt:.1f}억 | 돌파Vol60중앙 {med_bvol:.1f}배 | "
                f"3/5평균 {rule_pnl.mean():.2f}% | 승률 {win:.1f}% | +3선행 {hit3:.1f}% | +5선행 {hit5:.1f}% | 손절선행 {stop_first:.1f}% | "
                f"평균최대상승 {maxup:.2f}% | 평균종가수익 {close_ret:.2f}% | MA5청산수익 {h_exit_ret:.2f}% | "
                f"MA5이탈 {ma5_exit:.1f}% | MA5전+5 {h_hit5:.1f}% | MA5전+10 {h_hit10:.1f}%"
            )

        scopes = [
            ('H 전체', h),
            ('H-STRUCT', h[_bt_mask_h_struct_safe(h)]),
            ('🟢 H-VOL SAFE 2~3배', h[_bt_mask_h_safe(h)]),
            ('🟣 H-VOL SWING 3~5배', h[_bt_mask_h_vol_swing(h)]),
            ('⚠️ H-OVERHEAT 5~8배', h[_bt_mask_h_vol_overheat(h)]),
            ('🔥 H-AGGRESSIVE 8배+', h[_bt_mask_h_vol_aggressive(h)]),
        ]

        for scope_label, scope_df in scopes:
            lines.append(f"- {scope_label} 기준")
            if scope_df is None or scope_df.empty:
                lines.append("  · 후보 없음")
                continue
            work = scope_df.copy()
            work['거래대금구간'] = _amount_bucket(work)
            for lab in amount_labels:
                sub = work[work['거래대금구간'].astype(str).eq(str(lab))]
                if len(sub) > 0:
                    lines.append(_one(f"{scope_label} {lab}", sub))

        # 실전 판단용: 가장 중요한 2~3배 구간 안에서 거래대금별로 한 번 더 압축 표시
        vol_safe = h[_bt_mask_h_safe(h)].copy()
        lines.append("- 핵심 체크: 🟢H-VOL SAFE 2~3배 내부 거래대금")
        if vol_safe.empty:
            lines.append("  · H-VOL SAFE 2~3배 후보 없음")
        else:
            vol_safe['거래대금구간'] = _amount_bucket(vol_safe)
            for lab in amount_labels:
                sub = vol_safe[vol_safe['거래대금구간'].astype(str).eq(str(lab))]
                if len(sub) > 0:
                    lines.append(_one(f"2~3배×{lab}", sub))

        lines.append("- 해석 가이드: <50억은 호가/체결 리스크, 50~100억은 최소 유동성, 100~300억은 중소형 실전 가능, 300~1000억은 안정 구간, 1000억~3000억은 기관성 수급, 3000억 이상은 대형 수급 구간으로 봅니다.")
        lines.append("- 목적: 돌파Vol60 2~3배가 좋더라도 거래대금이 너무 작거나 너무 큰 구간에서 성과가 달라지는지 확인합니다.")
    except Exception as e:
        lines.append(f"- H 거래대금 분석 오류: {e}")
    return "\n".join(lines)


def _format_h_reason_report(df: pd.DataFrame) -> str:
    """H 신고가 거자름 STRICT SAFE/RISK 탈락 원인 요약."""
    lines = ["[H 신고가 거자름 STRICT VOL 분류/탈락 사유 분석 — v4.2.9]"]
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            lines.append("- 데이터 없음")
            return "\n".join(lines)
        h = df[_bt_mask_h_all(df)].copy()
        if h.empty:
            lines.append("- H 후보 없음")
            return "\n".join(lines)
        safe = h[_bt_mask_h_safe(h)]
        swing = h[_bt_mask_h_vol_swing(h)]
        overheat = h[_bt_mask_h_vol_overheat(h)]
        aggressive = h[_bt_mask_h_vol_aggressive(h)]
        neutral = h[_bt_mask_h_neutral(h)]
        risk = h[_bt_mask_h_risk(h)]
        struct_safe = h[_bt_mask_h_struct_safe(h)]
        lines.append(f"- H 전체 {len(h)}건 | STRUCT {len(struct_safe)}건 | VOL-SAFE {len(safe)}건 | SWING {len(swing)}건 | OVERHEAT {len(overheat)}건 | AGGRESSIVE {len(aggressive)}건 | NEUTRAL {len(neutral)}건 | RISK {len(risk)}건")
        def _num(col, default=0.0):
            return pd.to_numeric(h.get(col, default), errors='coerce').fillna(default)
        flags = {
            '돌파후7일초과': _num('days_since_high_breakout', 999) > 7,
            '돌파거래량1.5미만': _num('breakout_vol60_ratio', 0) < 1.5,
            '돌파Vol60_2미만': _num('breakout_vol60_ratio', 0) < 2.0,
            '돌파Vol60_3이상_SAFE제외': _num('breakout_vol60_ratio', 0) >= 3.0,
            '돌파봉상승7미만': _num('breakout_day_ret_pct', 0) < 7.0,
            '돌파봉몸통5미만': _num('breakout_body_pct', 0) < 5.0,
            '돌파봉종가위치75미만': _num('breakout_close_loc_pct', 0) < 75.0,
            '돌파봉윗꼬리25초과': _num('breakout_upper_wick_pct', 999) > 25.0,
            '거래량마름아님': _num('high_dryup_volume_dry', 0).astype(int) != 1,
            '돌파봉대비거래량50초과': _num('high_dryup_volume_dry_vs_breakout', 0).astype(int) != 1,
            '짧은캔들아님': _num('high_dryup_short_candle', 0).astype(int) != 1,
            '5일선위종가아님': _num('high_dryup_ma5_close_hold', 0).astype(int) != 1,
            '타점봉종가위치60미만': _num('high_dryup_entry_close_loc_ok', 0).astype(int) != 1,
            '돌파권유지아님': _num('high_dryup_zone_hold', 0).astype(int) != 1,
            '눌림10초과': _num('high_dryup_pullback_pct', 0) > 10.0,
            '거래대금100억미만': _num('amount_b', 0) < 100.0,
        }
        rows = []
        for name, ser in flags.items():
            try:
                cnt = int(pd.Series(ser, index=h.index).fillna(False).astype(bool).sum())
            except Exception:
                cnt = 0
            if cnt:
                rows.append((cnt, name))
        rows.sort(reverse=True)
        lines.append("- SAFE 탈락 사유 TOP")
        if rows:
            for cnt, name in rows[:10]:
                lines.append(f"  · {name}: {cnt}건 ({cnt/len(h)*100:.1f}%)")
        else:
            lines.append("  · 해당 사유 없음")
        def _q(col):
            s = _num(col, np.nan).dropna()
            if s.empty:
                return "값없음"
            return f"중앙 {s.median():.1f} / 평균 {s.mean():.1f} / P75 {s.quantile(0.75):.1f} / 최대 {s.max():.1f}"
        lines.append("- H 주요 지표 분포")
        lines.append(f"  · 돌파후경과일: {_q('days_since_high_breakout')}")
        lines.append(f"  · 돌파Vol60: {_q('breakout_vol60_ratio')}")
        lines.append(f"  · 돌파봉상승률: {_q('breakout_day_ret_pct')}")
        lines.append(f"  · 돌파봉몸통%: {_q('breakout_body_pct')}")
        lines.append(f"  · 돌파봉종가위치: {_q('breakout_close_loc_pct')}")
        lines.append(f"  · 돌파봉윗꼬리: {_q('breakout_upper_wick_pct')}")
        lines.append(f"  · 눌림률: {_q('high_dryup_pullback_pct')}")
        lines.append(f"  · 현거래량/직전5일: {_q('high_dryup_prev5_vol_ratio')}")
        lines.append(f"  · 현거래량/돌파봉: {_q('high_dryup_vol_vs_breakout')}")
        lines.append(f"  · 몸통%: {_q('high_dryup_body_pct')}")
        lines.append(f"  · 종가위치: {_q('close_loc_pct')}")
        lines.append(f"  · 거래대금: {_q('amount_b')}")
        lines.append("- 해석: H-STRICT는 장대양봉 신고가 돌파봉을 먼저 찾고, 이후 거래량이 마른 짧은 타점봉을 검증합니다. v4.2.10에서는 돌파Vol60 2~3배를 기본 H-SAFE로 두고, 실전 출력에는 후보별 매수계획을 함께 표시합니다.")
    except Exception as e:
        lines.append(f"- H 사유 분석 오류: {e}")
    return "\n".join(lines)



def _bt_mask_leader_gap_all(df: pd.DataFrame):
    """v4.2.12: 대형주/섹터대장 리더갭 WATCH 전체 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    band = df.get('band_type', pd.Series('', index=df.index)).astype(str)
    return df['mode'].astype(str).eq('L') & (band == 'LEADER_GAP_WATCH')


def _bt_mask_leader_gap_ready(df: pd.DataFrame):
    """v4.2.12: 리더갭 중 종가위치/거래대금/갭지지가 좋은 READY 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    score = pd.to_numeric(df.get('score', 0), errors='coerce').fillna(0)
    amount = pd.to_numeric(df.get('leader_gap_amount_b', df.get('amount_b', 0)), errors='coerce').fillna(0)
    close_loc = pd.to_numeric(df.get('close_loc_pct', 0), errors='coerce').fillna(0)
    gap_hold = pd.to_numeric(df.get('gap_unfilled', 0), errors='coerce').fillna(0).astype(int)
    vol50 = pd.to_numeric(df.get('vol50_ratio', df.get('vol_ratio', 0)), errors='coerce').fillna(0)
    return _bt_mask_leader_gap_all(df) & (score >= 80) & (amount >= LEADER_GAP_MIN_AMOUNT_B) & (close_loc >= 70) & (gap_hold == 1) & (vol50 >= LEADER_GAP_VOL50_MIN)


def _bt_mask_leader_gap_core_amount(df: pd.DataFrame):
    """v4.2.12: 거래대금 5000억+ 초대형 리더갭."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    amount = pd.to_numeric(df.get('leader_gap_amount_b', df.get('amount_b', 0)), errors='coerce').fillna(0)
    return _bt_mask_leader_gap_all(df) & (amount >= LEADER_GAP_CORE_AMOUNT_B)


def _bt_mask_leader_gap_overheat(df: pd.DataFrame):
    """v4.2.12: G-SAFE에서는 제외될 수 있는 이격/상승률/거래량 과열 리더갭."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    flag = pd.to_numeric(df.get('leader_gap_overheat_flag', 0), errors='coerce').fillna(0).astype(int)
    return _bt_mask_leader_gap_all(df) & (flag == 1)

def _bt_mask_g_all(df: pd.DataFrame):
    """v4.1.7: G 모랄레스갭 전체 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    return df['mode'].astype(str).eq('G')


def _bt_mask_g_safe(df: pd.DataFrame):
    """v4.1.7: G 모랄레스갭 SAFE 후보.
    갭 후보는 RR 구조가 S-CORE와 다르므로 S의 RR 1.0~1.5를 그대로 쓰지 않는다.
    대신 갭폭/종가위치/윗꼬리/과열/유동성/거래량 품질을 함께 본다.
    """
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    mode_g = _bt_mask_g_all(df)
    gap = pd.to_numeric(df.get('gap_pct', 0), errors='coerce').fillna(0)
    vol50 = pd.to_numeric(df.get('vol50_ratio', df.get('vol_ratio', 0)), errors='coerce').fillna(0)
    close_loc = pd.to_numeric(df.get('close_loc_pct', 0), errors='coerce').fillna(0)
    wick = pd.to_numeric(df.get('wick_pct', 100), errors='coerce').fillna(100)
    disparity20 = pd.to_numeric(df.get('disparity20', 999), errors='coerce').fillna(999)
    runup20 = pd.to_numeric(df.get('runup20', 999), errors='coerce').fillna(999)
    amount = pd.to_numeric(df.get('amount_b', 0), errors='coerce').fillna(0)
    grade = df.get('grade', pd.Series('', index=df.index)).astype(str)
    good_grade = grade.str.contains('완전체|A급|A', regex=True, na=False)
    return (
        mode_g
        & good_grade
        & (gap >= GAP_MIN_PCT)
        & (gap <= 8.5)
        & (vol50 >= GAP_VOL50_MULT)
        & (vol50 <= 6.0)
        & (close_loc >= 70.0)
        & (wick <= 20.0)
        & (disparity20 <= 115.0)
        & (runup20 <= 30.0)
        & (amount >= 100.0)
    )


def _bt_mask_g_risk(df: pd.DataFrame):
    """v4.1.7: G 모랄레스갭 위험 후보.
    갭만 뜨고 종가가 약하거나, 윗꼬리/과열/유동성 문제가 있는 경우를 분리한다.
    """
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    mode_g = _bt_mask_g_all(df)
    gap = pd.to_numeric(df.get('gap_pct', 0), errors='coerce').fillna(0)
    vol50 = pd.to_numeric(df.get('vol50_ratio', df.get('vol_ratio', 0)), errors='coerce').fillna(0)
    close_loc = pd.to_numeric(df.get('close_loc_pct', 0), errors='coerce').fillna(0)
    wick = pd.to_numeric(df.get('wick_pct', 0), errors='coerce').fillna(0)
    disparity20 = pd.to_numeric(df.get('disparity20', 0), errors='coerce').fillna(0)
    runup20 = pd.to_numeric(df.get('runup20', 0), errors='coerce').fillna(0)
    amount = pd.to_numeric(df.get('amount_b', 0), errors='coerce').fillna(0)
    risk = (
        (gap > 10.0)
        | (vol50 > 8.0)
        | (close_loc < 65.0)
        | (wick > 25.0)
        | (disparity20 > GAP_DISPARITY20_MAX)
        | (runup20 > GAP_RUNUP20_MAX)
        | (amount < 50.0)
    )
    return mode_g & risk


def _bt_mask_g_neutral(df: pd.DataFrame):
    """v4.1.7: G 후보 중 SAFE도 RISK도 아닌 중립 후보."""
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    return _bt_mask_g_all(df) & (~_bt_mask_g_safe(df)) & (~_bt_mask_g_risk(df))




def _bt_g_reason_flags(df: pd.DataFrame) -> pd.DataFrame:
    """v4.1.9: G 모랄레스갭이 SAFE/RISK로 분류된 사유를 조건별 boolean으로 반환한다.
    목적은 기준 튜닝 전, 어떤 컬럼/조건 때문에 G가 전부 RISK로 떨어지는지 확인하는 것이다.
    """
    idx = df.index if df is not None else []
    if df is None or df.empty:
        return pd.DataFrame(index=idx)

    def _num(col, default=0.0):
        if col in df.columns:
            return pd.to_numeric(df[col], errors='coerce').fillna(default)
        return pd.Series(default, index=df.index, dtype='float64')

    def _txt(col, default=''):
        if col in df.columns:
            return df[col].astype(str).fillna(default)
        return pd.Series(default, index=df.index, dtype='object')

    gap = _num('gap_pct', 0.0)
    vol50 = _num('vol50_ratio', 0.0)
    close_loc_missing = 'close_loc_pct' not in df.columns
    close_loc = _num('close_loc_pct', 0.0)
    wick_missing = 'wick_pct' not in df.columns
    wick = _num('wick_pct', 0.0)
    disparity20 = _num('disparity20', 0.0)
    runup20 = _num('runup20', 0.0)
    amount = _num('amount_b', 0.0)
    grade = _txt('grade', '')
    good_grade = grade.str.contains('완전체|A급|A', regex=True, na=False)

    flags = pd.DataFrame(index=df.index)
    # SAFE 탈락 사유: v4.1.7의 SAFE 기준을 그대로 분해한다.
    flags['SAFE탈락_등급미달'] = ~good_grade
    flags['SAFE탈락_갭폭8.5초과'] = gap > 8.5
    flags['SAFE탈락_Vol50_1.5미만'] = vol50 < GAP_VOL50_MULT
    flags['SAFE탈락_Vol50_6초과'] = vol50 > 6.0
    flags['SAFE탈락_종가위치컬럼없음'] = bool(close_loc_missing)
    flags['SAFE탈락_종가위치70미만'] = close_loc < 70.0
    flags['SAFE탈락_윗꼬리컬럼없음'] = bool(wick_missing)
    flags['SAFE탈락_윗꼬리20초과'] = wick > 20.0
    flags['SAFE탈락_이격115초과'] = disparity20 > 115.0
    flags['SAFE탈락_20일상승30초과'] = runup20 > 30.0
    flags['SAFE탈락_거래대금100억미만'] = amount < 100.0

    # RISK 사유: v4.1.7의 RISK 기준을 그대로 분해한다.
    flags['RISK_갭폭10초과'] = gap > 10.0
    flags['RISK_Vol50_8초과'] = vol50 > 8.0
    flags['RISK_종가위치65미만'] = close_loc < 65.0
    flags['RISK_윗꼬리25초과'] = wick > 25.0
    flags['RISK_이격상한초과'] = disparity20 > GAP_DISPARITY20_MAX
    flags['RISK_20일상승상한초과'] = runup20 > GAP_RUNUP20_MAX
    flags['RISK_거래대금50억미만'] = amount < 50.0
    return flags


def _format_g_reason_report(df: pd.DataFrame, max_reasons: int = 12) -> str:
    """v4.1.9: G 후보가 G-SAFE/NEUTRAL/RISK로 나뉘지 않은 원인을 사유별로 보여준다."""
    lines = ["[G 모랄레스갭 AGGRESSIVE/SAFE 탈락 사유 분석 — v4.1.9]"]
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            lines.append("- 데이터 없음")
            return "\n".join(lines)
        g = df[_bt_mask_g_all(df)].copy()
        if g.empty:
            lines.append("- G 후보 없음")
            return "\n".join(lines)
        safe = g[_bt_mask_g_safe(g)]
        neutral = g[_bt_mask_g_neutral(g)]
        risk = g[_bt_mask_g_risk(g)]
        lines.append(f"- G 전체 {len(g)}건 | SAFE {len(safe)}건 | NEUTRAL {len(neutral)}건 | RISK {len(risk)}건")
        flags = _bt_g_reason_flags(g)
        if flags.empty:
            lines.append("- 사유 계산 데이터 없음")
            return "\n".join(lines)

        safe_cols = [c for c in flags.columns if c.startswith('SAFE탈락_')]
        risk_cols = [c for c in flags.columns if c.startswith('RISK_')]

        def _append_counts(title, cols):
            rows = []
            for c in cols:
                try:
                    cnt = int(flags[c].fillna(False).astype(bool).sum())
                except Exception:
                    cnt = 0
                if cnt > 0:
                    rows.append((cnt, c))
            rows.sort(reverse=True)
            lines.append(f"- {title}")
            if not rows:
                lines.append("  · 해당 사유 없음")
                return
            for cnt, c in rows[:max_reasons]:
                name = c.replace('SAFE탈락_', '').replace('RISK_', '')
                pct = cnt / len(g) * 100.0 if len(g) else 0.0
                lines.append(f"  · {name}: {cnt}건 ({pct:.1f}%)")

        _append_counts('SAFE 탈락 사유 TOP', safe_cols)
        _append_counts('AGGRESSIVE 판정 사유 TOP', risk_cols)

        # 주요 수치 분포를 같이 보여줘서 기준 완화 방향을 정한다.
        def _q(col):
            if col not in g.columns:
                return "컬럼없음"
            s = pd.to_numeric(g[col], errors='coerce').dropna()
            if s.empty:
                return "값없음"
            return f"중앙 {s.median():.1f} / 평균 {s.mean():.1f} / P75 {s.quantile(0.75):.1f} / 최대 {s.max():.1f}"
        lines.append("- G 주요 지표 분포")
        lines.append(f"  · 갭폭: {_q('gap_pct')}")
        lines.append(f"  · Vol50: {_q('vol50_ratio')}")
        lines.append(f"  · 종가위치: {_q('close_loc_pct')}")
        lines.append(f"  · 윗꼬리: {_q('wick_pct')}")
        lines.append(f"  · 이격20: {_q('disparity20')}")
        lines.append(f"  · 20일상승: {_q('runup20')}")
        lines.append(f"  · 거래대금: {_q('amount_b')}")
        lines.append("- 해석: 이 리포트는 G 기준을 바로 바꾸기 전, 어떤 조건이 과하게 작동하는지 확인하기 위한 진단입니다.")
    except Exception as e:
        lines.append(f"- G 사유 분석 오류: {e}")
    return "\n".join(lines)

def _format_practical_combo_report(df: pd.DataFrame) -> str:
    """v4.1: 백테스트 기반 실전 추천 조합 요약. S-CORE를 SAFE/NEUTRAL/RISK 3단계로 분리한다."""
    lines = ["[실전 추천 조합 — v4.1 S-CORE 3단계 튜닝 관점]"]
    if df is None or df.empty:
        lines.append("- 데이터 없음")
        return "\n".join(lines)
    try:
        s2 = df[_bt_mask_s2(df)]
        s1_good = df[_bt_mask_s1_good(df)]
        s_core = df[_bt_mask_s_core(df)]
        s_core_safe = df[_bt_mask_s_core_safe(df)]
        s_core_neutral = df[_bt_mask_s_core_neutral(df)]
        s_core_risk = df[_bt_mask_s_core_risk(df)]
        s2_moderate = df[_bt_mask_s2_moderate_reignite(df)]
        a_strong = df[_bt_mask_a_strong(df)]
        core_plus_a = df[_bt_mask_s_core(df) | _bt_mask_a_strong(df)]
        b_observe = df[df['mode'].astype(str).isin(['B1', 'B2'])]
        g = df[_bt_mask_g_all(df)]
        g_safe = df[_bt_mask_g_safe(df)]
        g_neutral = df[_bt_mask_g_neutral(df)]
        g_risk = df[_bt_mask_g_risk(df)]
        c_all = df[df['mode'].astype(str).eq('C')]
        if 'c_type' in c_all.columns:
            c_strict = c_all[c_all['c_type'].astype(str).eq('strict')]
            c_relaxed = c_all[c_all['c_type'].astype(str).eq('relaxed')]
        else:
            c_strict = c_all
            c_relaxed = c_all.iloc[0:0]
        lines.append(_format_backtest_trade_rule_block(s_core, 'S-CORE 전체(S1우수응축+S2)'))
        lines.append(_format_backtest_trade_rule_block(s_core_safe, '🟢 S-CORE SAFE'))
        lines.append(_format_backtest_trade_rule_block(s_core_neutral, '🟡 S-CORE NEUTRAL'))
        lines.append(_format_backtest_trade_rule_block(s_core_risk, '⚠️ S-CORE RISK'))
        lines.append(_format_backtest_trade_rule_block(s1_good, 'S1 우수응축형'))
        lines.append(_format_backtest_trade_rule_block(s2, 'S2 실행형'))
        lines.append(_format_backtest_trade_rule_block(s2_moderate, 'S2 적당재점화(거래량 1.0~1.5)'))
        lines.append(_format_backtest_trade_rule_block(a_strong, 'A 보조돌파'))
        lines.append(_format_backtest_trade_rule_block(core_plus_a, '실전 전체군(S-CORE+A보조)'))
        lines.append(_format_backtest_trade_rule_block(b_observe, 'B1/B2 관찰군'))
        if len(g) > 0:
            lines.append(_format_backtest_trade_rule_block(g, 'G 모랄레스갭 전체'))
            lines.append(_format_backtest_trade_rule_block(g_safe, '🟢 G-SAFE 모랄레스갭'))
            lines.append(_format_backtest_trade_rule_block(g_neutral, '🟡 G-NEUTRAL 모랄레스갭'))
            lines.append(_format_backtest_trade_rule_block(g_risk, '⚠️ G-AGGRESSIVE 모랄레스갭'))
            lines.append(_format_g_reason_report(df))
        if len(c_strict) > 0:
            lines.append(_format_backtest_trade_rule_block(c_strict, 'C 엄격형(스윙참고)'))
        if len(c_relaxed) > 0:
            lines.append(_format_backtest_trade_rule_block(c_relaxed, 'C 완화형(진단용)'))
        lines.append("- SAFE 기준: S-CORE 내부에서 RR 1.0~1.5, 거래량비 1.5 미만, 종가위치 70% 이상인 후보입니다.")
        lines.append("- NEUTRAL 기준: S-CORE이지만 SAFE/RISK가 아닌 중립 후보입니다. 실전에서는 SAFE 다음의 관찰 가능 후보로 봅니다.")
        lines.append("- RISK 기준: 거래량비 1.5+, RR 0.7 미만/1.5 이상, 종가위치 70% 미만 중 하나라도 걸린 후보입니다.")
        lines.append("- G-SAFE 기준: G 후보 중 갭폭 8.5% 이하, Vol50 1.5~6배, 종가위치 70% 이상, 윗꼬리 20% 이하, 과열/유동성 필터를 통과한 후보입니다.")
        lines.append("- G-AGGRESSIVE 기준: 갭과대, Vol50 과열, 종가위치 약함, 윗꼬리 과다, 과열/유동성 문제 중 하나라도 걸린 후보입니다.")
        lines.append("- 해석: 실전 최우선은 S1 우수응축형 중 SAFE 후보입니다. G는 v4.1.9에서 AGGRESSIVE/SAFE 탈락 사유를 먼저 확인한 뒤 기준을 재설계합니다.")
    except Exception as e:
        lines.append(f"- 조합 리포트 생성 오류: {e}")
    return "\n".join(lines)

def _bucket_series(values: pd.Series, bins: list, labels: list) -> pd.Series:
    try:
        return pd.cut(pd.to_numeric(values, errors='coerce'), bins=bins, labels=labels, include_lowest=True)
    except Exception:
        return pd.Series(['확인불가'] * len(values), index=values.index)


def _format_bucket_table(df: pd.DataFrame, bucket_col: str, label: str) -> list:
    """v3.6: 조건 구간별 성과를 건수뿐 아니라 3/5규칙 성과까지 출력한다."""
    lines = []
    if df is None or df.empty or bucket_col not in df.columns:
        lines.append(f"- {label}: 데이터 없음")
        return lines

    def _num_series(sub: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
        try:
            if col in sub.columns:
                return pd.to_numeric(sub[col], errors='coerce').fillna(default)
        except Exception:
            pass
        return pd.Series([default] * len(sub), index=sub.index, dtype='float64')

    try:
        grouped = df.groupby(bucket_col, dropna=False, observed=False)
    except TypeError:
        grouped = df.groupby(bucket_col)

    for bucket, sub in grouped:
        if sub is None or len(sub) == 0:
            continue
        bucket_name = str(bucket)
        pnl = _num_series(sub, 'rule35_pnl', 0.0)
        win = _num_series(sub, 'rule35_win', 0.0)
        p3 = _num_series(sub, 'hit3_before_stop', 0.0)
        p5 = _num_series(sub, 'hit5_before_stop', 0.0)
        sl = _num_series(sub, 'stop_before_3', 0.0)
        max_up = _num_series(sub, 'ret_max_high_hd', 0.0)
        lines.append(
            f"- {label} {bucket_name}: {len(sub)}건 | "
            f"3/5평균 {pnl.mean():.2f}% | 승률 {win.mean()*100:.1f}% | "
            f"+3선행 {p3.mean()*100:.1f}% | +5선행 {p5.mean()*100:.1f}% | "
            f"손절선행 {sl.mean()*100:.1f}% | 평균최대상승 {max_up.mean():.2f}%"
        )
    return lines


def _format_tuning_report(df: pd.DataFrame) -> str:
    """v4.1.7: S/A/B/G 조건별 튜닝 리포트. 과최적화를 막기 위해 조건별 성과만 비교한다."""
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
            lines.append("- S-CORE SAFE/NEUTRAL/RISK 3단계 분리 성과")
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_s_core_safe(df)], '🟢 S-CORE SAFE'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_s_core_neutral(df)], '🟡 S-CORE NEUTRAL'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_s_core_risk(df)], '⚠️ S-CORE RISK'))

        g_df = df[_bt_mask_g_all(df)]
        if len(g_df) > 0:
            lines.append("- G 모랄레스갭: 3단계 분리 성과")
            lines.append(_format_backtest_trade_rule_block(g_df, 'G 전체'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_g_safe(df)], '🟢 G-SAFE'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_g_neutral(df)], '🟡 G-NEUTRAL'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_g_risk(df)], '🔥 G-AGGRESSIVE'))
            lines.append(_format_g_reason_report(df))

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


def _bt_num(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    """v4.1: 백테스트 진단용 안전 숫자 시리즈."""
    try:
        if df is not None and col in df.columns:
            return pd.to_numeric(df[col], errors='coerce').fillna(default)
    except Exception:
        pass
    return pd.Series([default] * (len(df) if df is not None else 0), index=df.index if df is not None else None, dtype='float64')


def _bt_bool(df: pd.DataFrame, col: str) -> pd.Series:
    try:
        return _bt_num(df, col, 0).fillna(0).astype(float).gt(0)
    except Exception:
        return pd.Series(False, index=df.index if df is not None else [])


def _format_stop_bucket_table(df: pd.DataFrame, bucket_col: str, label: str, min_count: int = 3) -> list:
    """v4.1: 손절선행이 어떤 구간에 몰리는지 확인."""
    lines = []
    if df is None or df.empty or bucket_col not in df.columns:
        return lines
    try:
        grouped = df.groupby(bucket_col, dropna=False, observed=False)
    except TypeError:
        grouped = df.groupby(bucket_col)
    for bucket, sub in grouped:
        if sub is None or len(sub) == 0:
            continue
        pnl = _bt_num(sub, 'rule35_pnl', 0.0)
        stop_first = _bt_bool(sub, 'stop_before_3')
        stop_rule = _bt_bool(sub, 'rule35_stop')
        hit3 = _bt_bool(sub, 'hit3_before_stop')
        win = _bt_bool(sub, 'rule35_win')
        warn = " ⚠️" if len(sub) >= min_count and stop_first.mean() >= 0.20 else ""
        lines.append(
            f"- {label} {bucket}: {len(sub)}건 | 3/5평균 {pnl.mean():.2f}% | "
            f"승률 {win.mean()*100:.1f}% | +3선행 {hit3.mean()*100:.1f}% | "
            f"손절선행 {stop_first.mean()*100:.1f}% | 손절발생 {stop_rule.mean()*100:.1f}%{warn}"
        )
    return lines


def _format_stoploss_signature_report(df: pd.DataFrame, scope_label: str = '현재 선택군', include_buckets: bool = True) -> str:
    """v4.1: 계산 범위를 명확히 분리해 손절 종목 특이점을 요약.

    - scope_label: 전체 선택군 / S-CORE 선택군 / SAFE 선택군 등 표시용 라벨
    - include_buckets: 전체 선택군에서는 요약만, S-CORE에서는 구간별 집중표까지 출력 가능
    """
    lines = [f"[🩸 손절 종목 특이점 분석 — {scope_label}]"]
    if df is None or df.empty:
        lines.append("- 데이터 없음")
        return "\n".join(lines)
    try:
        work = df.copy()
        stop_first = _bt_bool(work, 'stop_before_3')
        stop_rule = _bt_bool(work, 'rule35_stop')
        stop_any = stop_first | stop_rule
        good = (~stop_any) & _bt_bool(work, 'rule35_win')
        lines.append(
            f"- 손절선행 {int(stop_first.sum())}건 / 전체 {len(work)}건 "
            f"({stop_first.mean()*100:.1f}%) | 3/5규칙 손절발생 {int(stop_rule.sum())}건 ({stop_rule.mean()*100:.1f}%)"
        )
        if int(stop_any.sum()) == 0:
            lines.append("- 이번 선택군에서는 3/5규칙상 손절발생 종목이 없습니다. 현재 조건은 손절 회피력이 매우 양호합니다.")
        else:
            metrics = [
                ('vol_ratio', '거래량비', '배'),
                ('today_vol_ratio', '당일거래량비', '배'),
                ('vma5_20_ratio', 'VMA5/VMA20', ''),
                ('rr', 'RR', ''),
                ('close_loc_pct', '종가위치', '%'),
                ('upper_wick_pct', '윗꼬리', '%'),
                ('amount_b', '거래대금', '억'),
                ('runup120_pct', '120일상승률', '%'),
                ('high_near_pct', '고점근접', '%'),
                ('ret_max_high_hd', '평가기간 최대상승', '%'),
                ('ret_min_low_hd', '평가기간 최대하락', '%'),
            ]
            lines.append("- 손절발생 vs 성공종목 평균 비교:")
            any_metric = False
            for col, label, suffix in metrics:
                if col not in work.columns:
                    continue
                s_stop = _bt_num(work.loc[stop_any], col, np.nan).dropna()
                s_good = _bt_num(work.loc[good], col, np.nan).dropna()
                if len(s_stop) == 0 or len(s_good) == 0:
                    continue
                any_metric = True
                lines.append(f"  · {label}: 손절 {s_stop.mean():.2f}{suffix} / 성공 {s_good.mean():.2f}{suffix}")
            if not any_metric:
                lines.append("  · 비교 가능한 세부 지표가 부족합니다.")

        s_df = work[work.get('mode', pd.Series('', index=work.index)).astype(str).eq('S')].copy()
        if include_buckets and not s_df.empty:
            s_df['RR구간'] = _bucket_series(s_df.get('rr', 0), [-999, 0.7, 1.0, 1.5, 999], ['RR<0.7', '0.7~1.0', '1.0~1.5', '1.5+'])
            s_df['거래량구간'] = _bucket_series(s_df.get('vol_ratio', 0), [-999, 1.0, 1.2, 1.5, 999], ['<1.0', '1.0~1.2', '1.2~1.5', '1.5+'])
            s_df['종가위치구간'] = _bucket_series(s_df.get('close_loc_pct', 0), [-999, 70, 85, 101], ['<70%', '70~85%', '85%+'])
            lines.append("- S전략 손절 집중 구간:")
            lines += _format_stop_bucket_table(s_df, 'RR구간', 'S RR')
            lines += _format_stop_bucket_table(s_df, '거래량구간', 'S 거래량비')
            lines += _format_stop_bucket_table(s_df, '종가위치구간', 'S 종가위치')

        advice = []
        if 's_df' in locals() and not s_df.empty:
            vol_hi = s_df[_bucket_series(s_df.get('vol_ratio', 0), [-999, 1.5, 999], ['<1.5', '1.5+']).astype(str).eq('1.5+')]
            if len(vol_hi) >= 3 and _bt_bool(vol_hi, 'stop_before_3').mean() >= 0.20:
                advice.append("거래량비 1.5+ 구간은 손절선행이 높으므로 S2라도 추격주의/감점 유지")
            rr_low = s_df[_bt_num(s_df, 'rr', 0).lt(0.7)]
            rr_high = s_df[_bt_num(s_df, 'rr', 0).ge(1.5)]
            if len(rr_low) >= 3 and _bt_bool(rr_low, 'stop_before_3').mean() >= 0.20:
                advice.append("RR 0.7 미만은 목표공간/방어선 품질이 약해 제외 또는 강등")
            if len(rr_high) >= 3 and _bt_bool(rr_high, 'stop_before_3').mean() >= 0.20:
                advice.append("RR 1.5+는 손절선이 멀거나 변동성이 큰 종목일 수 있어 과신 금지")
            close_low = s_df[_bt_num(s_df, 'close_loc_pct', 0).lt(70)]
            if len(close_low) >= 3 and _bt_bool(close_low, 'stop_before_3').mean() >= 0.20:
                advice.append("종가위치 70% 미만은 캔들 상단 마감 실패로 보고 제외/강등")
        if stop_first.mean() <= 0.05:
            advice.append("전체 손절선행이 5% 이하이면 현재 S-CORE 조건은 손절 회피력이 양호")
        if not advice:
            advice.append("특정 손절 집중 구간이 강하지 않습니다. 4주/12주 누적 결과로 재확인하세요.")
        lines.append("- 튜닝 권고:")
        for item in advice[:6]:
            lines.append(f"  · {item}")
    except Exception as e:
        lines.append(f"- 손절 특이점 분석 오류: {e}")
    return "\n".join(lines)




def _stock_feature_bucket_columns(df: pd.DataFrame) -> pd.DataFrame:
    """v4.3.8: 차트가 아니라 종목 자체의 속성을 비교하기 위한 공통 버킷 생성."""
    work = df.copy() if df is not None else pd.DataFrame()
    if work.empty:
        return work
    try:
        idx = work.get('index_label', pd.Series('', index=work.index)).astype(str).str.strip()
        mcap_b = pd.to_numeric(work.get('marcap', pd.Series(0, index=work.index)), errors='coerce').fillna(0) / 100000000.0
        amount_b = pd.to_numeric(work.get('amount_b', pd.Series(0, index=work.index)), errors='coerce').fillna(0)
        work['_sf_index'] = np.select(
            [idx.eq('코스피200'), idx.eq('코스닥150'), idx.str.contains('KOSPI', case=False, na=False), idx.str.contains('KOSDAQ|KQ', case=False, na=False)],
            ['KOSPI200', 'KOSDAQ150', 'KOSPI계열', 'KOSDAQ계열'],
            default=np.where(idx.ne(''), idx, '지수외/시총편입')
        )
        work['_sf_mcap_b'] = mcap_b
        work['_sf_amount_b'] = amount_b
        work['_sf_mcap_bucket'] = np.select(
            [mcap_b.lt(1000), mcap_b.lt(3000), mcap_b.lt(10000), mcap_b.lt(50000), mcap_b.ge(50000)],
            ['<1000억', '1000~3000억', '3000억~1조', '1~5조', '5조+'],
            default='시총불명'
        )
        work['_sf_amount_bucket'] = np.select(
            [amount_b.lt(100), amount_b.lt(300), amount_b.lt(1000), amount_b.lt(3000), amount_b.lt(5000), amount_b.ge(5000)],
            ['<100억', '100~300억', '300~1000억', '1000~3000억', '3000~5000억', '5000억+'],
            default='거래대금불명'
        )
        work['_sf_leader_proxy'] = np.select(
            [amount_b.ge(5000) | mcap_b.ge(50000) | idx.eq('코스피200'), amount_b.ge(1000) | mcap_b.ge(10000), amount_b.ge(300) | mcap_b.ge(3000), amount_b.ge(100) | mcap_b.ge(1000)],
            ['대형/기관수급권', '중형/고유동성', '중소형/실전가능', '소형/저유동성주의'],
            default='초소형/호가주의'
        )
        # I-MAIN에서 쓰는 재료/거래대금 프록시 점수를 종목 속성으로 함께 버킷화한다.
        mat = pd.to_numeric(work.get('i_material_proxy_score', pd.Series(np.nan, index=work.index)), errors='coerce')
        work['_sf_material_bucket'] = np.select(
            [mat.ge(4), mat.ge(3), mat.ge(2), mat.ge(0)],
            ['재료/대금 4점+', '재료/대금 3점', '재료/대금 2점', '재료/대금 약함'],
            default='재료점수없음'
        )
        if 'flow_label' in work.columns:
            flow = work['flow_label'].astype(str).replace('', '수급미조회')
        else:
            flow = pd.Series('수급미조회', index=work.index)
        work['_sf_flow'] = flow
    except Exception as e:
        log_error(f"⚠️ 종목특성 버킷 생성 실패: {e}")
    return work


def _stock_feature_group_stats(df: pd.DataFrame, feature_col: str, metric_mode: str = 'short') -> pd.DataFrame:
    """v4.3.8: 종목특성 구간별 성과/손절 통계."""
    if df is None or df.empty or feature_col not in df.columns:
        return pd.DataFrame()
    rows = []
    try:
        for key, sub in df.groupby(feature_col, dropna=False):
            if len(sub) == 0:
                continue
            if metric_mode == 'i':
                ret60 = pd.to_numeric(sub.get('i_ret_close_60d', sub.get('ret_close_hd', 0)), errors='coerce').fillna(0)
                ret20 = pd.to_numeric(sub.get('i_ret_close_20d', sub.get('ret_close_hd', 0)), errors='coerce').fillna(0)
                hit20 = _bt_bool(sub, 'i_hit20_60d') if 'i_hit20_60d' in sub.columns else ret60.ge(20)
                box_fail = _bt_bool(sub, 'i_box_fail_close') if 'i_box_fail_close' in sub.columns else pd.Series(False, index=sub.index)
                negative = ret60.lt(0) | box_fail
                rows.append({
                    '구간': str(key), '건수': int(len(sub)), '평균20일': float(ret20.mean()), '평균60일': float(ret60.mean()),
                    '승률': float(ret60.gt(0).mean()*100), '+20도달': float(hit20.mean()*100), '실패율': float(negative.mean()*100),
                    '평균거래대금': float(pd.to_numeric(sub.get('amount_b', 0), errors='coerce').fillna(0).mean()),
                    '평균시총억': float((pd.to_numeric(sub.get('marcap', 0), errors='coerce').fillna(0)/100000000).mean()),
                })
            else:
                pnl = pd.to_numeric(sub.get('rule35_pnl', 0), errors='coerce').fillna(0)
                win = _bt_bool(sub, 'rule35_win') if 'rule35_win' in sub.columns else pnl.gt(0)
                stop = _bt_bool(sub, 'stop_before_3') if 'stop_before_3' in sub.columns else pnl.lt(0)
                hit3 = _bt_bool(sub, 'hit3_before_stop') if 'hit3_before_stop' in sub.columns else pnl.gt(0)
                rows.append({
                    '구간': str(key), '건수': int(len(sub)), '평균3_5': float(pnl.mean()), '승률': float(win.mean()*100),
                    '+3선행': float(hit3.mean()*100), '손절선행': float(stop.mean()*100),
                    '평균거래대금': float(pd.to_numeric(sub.get('amount_b', 0), errors='coerce').fillna(0).mean()),
                    '평균시총억': float((pd.to_numeric(sub.get('marcap', 0), errors='coerce').fillna(0)/100000000).mean()),
                })
        return pd.DataFrame(rows)
    except Exception as e:
        log_error(f"⚠️ 종목특성 통계 생성 실패({feature_col}): {e}")
        return pd.DataFrame()


def _fmt_feature_rows(stat: pd.DataFrame, mode: str = 'short', min_n: int | None = None, top: int = 3) -> tuple[list[str], list[str]]:
    """좋은 구간/주의 구간을 짧게 뽑는다."""
    if stat is None or stat.empty:
        return [], []
    min_n = CLOSING_BET_STOCK_FEATURE_MIN_N if min_n is None else int(min_n)
    s = stat[pd.to_numeric(stat.get('건수', 0), errors='coerce').fillna(0).ge(min_n)].copy()
    if s.empty:
        s = stat.copy()
    if mode == 'i':
        good = s.sort_values(['평균60일', '+20도달', '건수'], ascending=[False, False, False]).head(top)
        bad = s.sort_values(['실패율', '평균60일'], ascending=[False, True]).head(top)
        good_lines = [f"{str(r.get('구간'))}: {int(r.get('건수', 0))}건 | 60일 {float(r.get('평균60일', 0)):+.2f}% | +20 {float(r.get('+20도달', 0)):.1f}% | 실패 {float(r.get('실패율', 0)):.1f}%" for _, r in good.iterrows()]
        bad_lines = [f"{str(r.get('구간'))}: {int(r.get('건수', 0))}건 | 실패 {float(r.get('실패율', 0)):.1f}% | 60일 {float(r.get('평균60일', 0)):+.2f}%" for _, r in bad.iterrows()]
    else:
        good = s.sort_values(['평균3_5', '승률', '건수'], ascending=[False, False, False]).head(top)
        bad = s.sort_values(['손절선행', '평균3_5'], ascending=[False, True]).head(top)
        good_lines = [f"{r.구간}: {int(r.건수)}건 | 3/5 {r.평균3_5:+.2f}% | 승률 {r.승률:.1f}% | 손절 {r.손절선행:.1f}%" for r in good.itertuples(index=False)]
        bad_lines = [f"{r.구간}: {int(r.건수)}건 | 손절 {r.손절선행:.1f}% | 3/5 {r.평균3_5:+.2f}%" for r in bad.itertuples(index=False)]
    return good_lines, bad_lines




def _safe_feature_mask(mask_fn, df: pd.DataFrame, fallback_mode: str | None = None) -> pd.Series:
    """종목특성 리포트용 안전 마스크. 일부 컬럼이 없는 샘플/구버전 CSV에서도 리포트를 중단하지 않는다."""
    try:
        m = mask_fn(df)
        if isinstance(m, pd.Series):
            return m.reindex(df.index).fillna(False).astype(bool)
    except Exception:
        pass
    if fallback_mode is not None and df is not None and 'mode' in df.columns:
        return df['mode'].astype(str).eq(fallback_mode)
    return pd.Series(False, index=df.index if df is not None else [])

def _format_stock_feature_scope(work: pd.DataFrame, label: str, scope_df: pd.DataFrame, metric_mode: str = 'short', min_n: int = 5) -> list[str]:
    lines = []
    if scope_df is None or scope_df.empty:
        return lines
    lines.append(f"\n[{label}]")
    feature_defs = [
        ('_sf_index', '시장/지수'),
        ('_sf_mcap_bucket', '시총'),
        ('_sf_amount_bucket', '거래대금'),
        ('_sf_leader_proxy', '대표성/유동성'),
    ]
    if metric_mode == 'i':
        feature_defs.append(('_sf_material_bucket', '재료/거래대금 프록시'))
    if '_sf_flow' in scope_df.columns and not scope_df['_sf_flow'].astype(str).eq('수급미조회').all():
        feature_defs.append(('_sf_flow', '외인/기관 수급'))
    for col, name in feature_defs:
        stat = _stock_feature_group_stats(scope_df, col, metric_mode=metric_mode)
        good, bad = _fmt_feature_rows(stat, mode=metric_mode, min_n=min_n, top=2)
        if not good and not bad:
            continue
        lines.append(f"- {name} 좋은 구간: " + (" / ".join(good) if good else '표본부족'))
        lines.append(f"- {name} 손절/실패주의: " + (" / ".join(bad) if bad else '표본부족'))
    return lines


def _format_stock_feature_report(df: pd.DataFrame) -> str:
    """v4.3.8: 차트 외 종목특성별 성공/손절 분석 리포트."""
    lines = ["[🧬 종목 특성별 성공/손절 분석 — 차트 외 요인 v4.3.8]"]
    if not CLOSING_BET_STOCK_FEATURE_REPORT:
        lines.append("- CLOSING_BET_STOCK_FEATURE_REPORT=0 상태라 생략합니다.")
        return "\n".join(lines)
    if df is None or df.empty:
        lines.append("- 데이터 없음")
        return "\n".join(lines)
    try:
        work = _stock_feature_bucket_columns(df)
        lines.append("- 비교 기준: 시장/지수소속, 시총, 거래대금, 대표성·유동성, I-MAIN 재료/거래대금 프록시입니다. 차트 모양이 아니라 종목 자체의 체급과 관심도를 보는 보조 리포트입니다.")
        # 전체 후보의 체급별 손절 위험을 먼저 압축 표시한다.
        for col, label in [('_sf_mcap_bucket', '전체 시총'), ('_sf_amount_bucket', '전체 거래대금'), ('_sf_leader_proxy', '전체 대표성/유동성')]:
            stat = _stock_feature_group_stats(work, col, metric_mode='short')
            good, bad = _fmt_feature_rows(stat, mode='short', min_n=max(CLOSING_BET_STOCK_FEATURE_MIN_N, 20), top=2)
            if good or bad:
                lines.append(f"- {label} 우세: " + (" / ".join(good) if good else '표본부족'))
                lines.append(f"- {label} 손절주의: " + (" / ".join(bad) if bad else '표본부족'))

        # 패턴별로 종목 특성 차이를 본다. 넓은 A/B/C는 표본이 커서 경고 구간 위주로 해석한다.
        try:
            scopes = []
            scopes.append(('🟡 S-CORE 종목특성', work[_safe_feature_mask(_bt_mask_s_core, work, 'S')], 'short', 5))
            scopes.append(('👑 L 리더갭 종목특성', work[_safe_feature_mask(_bt_mask_leader_gap_all, work, 'L')], 'short', 5))
            i_all = work[_safe_feature_mask(_bt_mask_i_core_all, work, 'I')]
            if not i_all.empty:
                i_main = i_all[_safe_feature_mask(_bt_mask_i_core_main, i_all, 'I')]
                i_core = i_all[_bt_mask_i_main_core(i_all)] if '_bt_mask_i_main_core' in globals() else pd.DataFrame()
                i_accel = i_all[_bt_mask_i_main_accel(i_all)] if '_bt_mask_i_main_accel' in globals() else pd.DataFrame()
                # 위 두 마스크가 없는 구버전 호환용: 리포트 내부 정의와 같은 조건으로 직접 계산
                if i_core.empty:
                    phase = i_main.get('i_phase', pd.Series('', index=i_main.index)).astype(str)
                    anchor = pd.to_numeric(i_main.get('i_anchor_days', 0), errors='coerce').fillna(0)
                    monthly = pd.to_numeric(i_main.get('i_monthly_vol_rebuild', 0), errors='coerce').fillna(0)
                    i_core = i_main[phase.eq('I-4') & anchor.between(120, 180) & monthly.eq(1)]
                if i_accel.empty:
                    anchor = pd.to_numeric(i_main.get('i_anchor_days', 0), errors='coerce').fillna(0)
                    dist = pd.to_numeric(i_main.get('i_long_ma_dist_pct', 999), errors='coerce').fillna(999)
                    i_accel = i_main[dist.gt(10) & dist.le(18) & anchor.between(120, 180)]
                scopes.append(('✅ I-MAIN CORE 종목특성', i_core, 'i', 3))
                scopes.append(('🚀 I-MAIN ACCEL 종목특성', i_accel, 'i', 3))
            scopes.append(('A 돌파형 넓은 후보 종목특성', work[work.get('mode', '').astype(str).eq('A')], 'short', 50))
            b_df = work[work.get('mode', '').astype(str).isin(['B1', 'B2'])]
            scopes.append(('B1/B2 하단형 넓은 후보 종목특성', b_df, 'short', 30))
            scopes.append(('C 역매공파 넓은 후보 종목특성', work[work.get('mode', '').astype(str).eq('C')], 'short', 50))
            for label, sub, mode, mn in scopes:
                lines.extend(_format_stock_feature_scope(work, label, sub, metric_mode=mode, min_n=mn))
        except Exception as e:
            lines.append(f"- 패턴별 종목특성 분석 오류: {e}")

        lines.append("\n[종목특성 해석 가이드]")
        lines.append("- 성공 쪽이 대형/기관수급권·5000억+ 거래대금·KOSPI200/KOSDAQ150에 몰리면, 같은 차트라도 대표주/준대장 우선이 맞습니다.")
        lines.append("- 손절 쪽이 소형/저유동성·<100억 거래대금·지수외 후보에 몰리면, 후발주/호가 얇은 종목은 같은 패턴이라도 강등합니다.")
        lines.append("- I-MAIN은 단기 손절보다 60일 음수/박스실패/벤치마크 열위를 보며, 재료·거래대금 프록시가 약하면 관찰로 낮춥니다.")
    except Exception as e:
        lines.append(f"- 종목특성 리포트 생성 오류: {type(e).__name__}: {e}")
    return "\n".join(lines)


def _build_stock_feature_summary_df(selected_df: pd.DataFrame) -> pd.DataFrame:
    """v4.3.8: 종목특성별 통계를 CSV/HTML로 저장하기 위한 DF."""
    if selected_df is None or selected_df.empty:
        return pd.DataFrame()
    try:
        work = _stock_feature_bucket_columns(selected_df)
        rows = []
        scopes = [('전체', work, 'short')]
        try:
            scopes += [
                ('S-CORE', work[_safe_feature_mask(_bt_mask_s_core, work, 'S')], 'short'),
                ('L-리더갭', work[_safe_feature_mask(_bt_mask_leader_gap_all, work, 'L')], 'short'),
                ('A-돌파형', work[work.get('mode', '').astype(str).eq('A')], 'short'),
                ('B1/B2-하단형', work[work.get('mode', '').astype(str).isin(['B1','B2'])], 'short'),
                ('C-역매공파', work[work.get('mode', '').astype(str).eq('C')], 'short'),
            ]
            i_all = work[_safe_feature_mask(_bt_mask_i_core_all, work, 'I')]
            if not i_all.empty:
                i_main = i_all[_safe_feature_mask(_bt_mask_i_core_main, i_all, 'I')]
                scopes.append(('I-CORE 전체', i_all, 'i'))
                scopes.append(('I-CORE MAIN', i_main, 'i'))
        except Exception:
            pass
        features = [('_sf_index','시장/지수'), ('_sf_mcap_bucket','시총'), ('_sf_amount_bucket','거래대금'), ('_sf_leader_proxy','대표성/유동성'), ('_sf_material_bucket','재료/대금프록시'), ('_sf_flow','수급')]
        for scope_name, sub, mode in scopes:
            if sub is None or sub.empty:
                continue
            for col, fname in features:
                if col not in sub.columns:
                    continue
                stat = _stock_feature_group_stats(sub, col, metric_mode=mode)
                if stat.empty:
                    continue
                stat.insert(0, '범위', scope_name)
                stat.insert(1, '특성', fname)
                stat.insert(2, '평가기준', 'I-60일' if mode == 'i' else '3/5규칙')
                rows.append(stat)
        return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    except Exception as e:
        log_error(f"⚠️ 종목특성 요약 DF 생성 실패: {e}")
        return pd.DataFrame()

def _format_weekly_pattern_report(df: pd.DataFrame, scope_label: str = '현재 선택군') -> str:
    """v4.1: 계산 범위별로 주차별 패턴 강도와 약화/회복 신호를 감지."""
    lines = [f"[📡 주차별 패턴 변화 감지 — {scope_label}]"]
    if df is None or df.empty:
        lines.append("- 데이터 없음")
        return "\n".join(lines)
    try:
        work = df.copy()
        if 'signal_week' not in work.columns and 'signal_date' in work.columns:
            work['signal_week'] = work['signal_date'].map(_week_label_from_date)
        if 'signal_week' not in work.columns:
            lines.append("- 주차 정보가 없어 패턴 변화 감지를 생략합니다.")
            return "\n".join(lines)
        rows = []
        for week, sub in work.groupby('signal_week', dropna=False):
            if len(sub) == 0:
                continue
            rows.append({
                'week': str(week),
                'cnt': len(sub),
                'pnl': _bt_num(sub, 'rule35_pnl', 0).mean(),
                'win': _bt_bool(sub, 'rule35_win').mean() * 100,
                'p3': _bt_bool(sub, 'hit3_before_stop').mean() * 100,
                'p5': _bt_bool(sub, 'hit5_before_stop').mean() * 100,
                'stop': _bt_bool(sub, 'stop_before_3').mean() * 100,
                'maxup': _bt_num(sub, 'ret_max_high_hd', 0).mean(),
                'close_ret': _bt_num(sub, 'ret_close_hd', 0).mean(),
            })
        if not rows:
            lines.append("- 주차별 집계 데이터 없음")
            return "\n".join(lines)
        wk = pd.DataFrame(rows).sort_values('week')
        pos_weeks = int((wk['pnl'] > 0).sum())
        valid_weeks = len(wk)
        avg_pnl = wk['pnl'].mean()
        avg_stop = wk['stop'].mean()
        recent = wk.tail(min(3, len(wk)))
        prior = wk.iloc[:-len(recent)] if len(wk) > len(recent) else pd.DataFrame()
        lines.append(f"- {scope_label} 주차 유효성: {valid_weeks}주 중 {pos_weeks}주 플러스 | 주간평균 3/5수익 {avg_pnl:.2f}% | 평균 손절선행 {avg_stop:.1f}%")
        if not prior.empty:
            recent_pnl = recent['pnl'].mean()
            prior_pnl = prior['pnl'].mean()
            recent_stop = recent['stop'].mean()
            prior_stop = prior['stop'].mean()
            if recent_pnl > prior_pnl + 0.5 and recent_stop <= prior_stop + 5:
                state = "최근 회복/강화"
            elif recent_pnl < prior_pnl - 0.5 or recent_stop > prior_stop + 8:
                state = "최근 약화 주의"
            else:
                state = "유효 유지"
            lines.append(f"- 최근 3주 변화: {state} | 최근평균 {recent_pnl:.2f}% / 이전평균 {prior_pnl:.2f}% | 최근손절 {recent_stop:.1f}% / 이전손절 {prior_stop:.1f}%")
        best = wk.sort_values('pnl', ascending=False).head(2)
        worst = wk.sort_values('pnl', ascending=True).head(2)
        lines.append("- 강했던 주차: " + ", ".join([f"{r.week}({r.pnl:.2f}%, 손절 {r.stop:.1f}%)" for r in best.itertuples()]))
        lines.append("- 약했던 주차: " + ", ".join([f"{r.week}({r.pnl:.2f}%, 손절 {r.stop:.1f}%)" for r in worst.itertuples()]))

        s_df = work[work.get('mode', pd.Series('', index=work.index)).astype(str).eq('S')].copy()
        if not s_df.empty:
            vol_low = s_df[_bt_num(s_df, 'vol_ratio', 0).lt(1.5)]
            vol_hi = s_df[_bt_num(s_df, 'vol_ratio', 0).ge(1.5)]
            close_good = s_df[_bt_num(s_df, 'close_loc_pct', 0).ge(70)]
            close_low = s_df[_bt_num(s_df, 'close_loc_pct', 0).lt(70)]
            notes = []
            if len(vol_low) >= 5 and len(vol_hi) >= 3:
                low_pnl = _bt_num(vol_low, 'rule35_pnl', 0).mean()
                hi_pnl = _bt_num(vol_hi, 'rule35_pnl', 0).mean()
                hi_stop = _bt_bool(vol_hi, 'stop_before_3').mean() * 100
                if low_pnl > hi_pnl + 1.0:
                    notes.append(f"거래량비 1.5 미만이 우세({low_pnl:.2f}% vs {hi_pnl:.2f}%) → 과열 재점화형 주의")
                if hi_stop >= 20:
                    notes.append(f"거래량비 1.5+ 손절선행 {hi_stop:.1f}% → 추격 가점 축소 유지")
            if len(close_good) >= 5 and len(close_low) >= 3:
                good_pnl = _bt_num(close_good, 'rule35_pnl', 0).mean()
                low_pnl = _bt_num(close_low, 'rule35_pnl', 0).mean()
                if good_pnl > low_pnl + 0.5:
                    notes.append(f"종가위치 70% 이상 우세({good_pnl:.2f}% vs {low_pnl:.2f}%) → 70% 미만 강등 유지")
            if not notes:
                notes.append("거래량/종가위치 미세 패턴은 추가 표본 확인 필요")
            lines.append("- 미세 패턴:")
            for note in notes[:5]:
                lines.append(f"  · {note}")

        if avg_pnl > 0 and avg_stop <= 15:
            lines.append(f"- 판정: {scope_label} 유효. 단, 약한 주에는 +3/+5 익절 우선으로 수익 반납을 줄이는 대응이 적절합니다.")
        elif avg_pnl > 0:
            lines.append(f"- 판정: {scope_label}는 플러스지만 손절선행 관리가 필요합니다. 거래량 과열/종가위치 낮은 후보를 줄이세요.")
        else:
            lines.append(f"- 판정: {scope_label} 약화 구간입니다. 신규 진입 축소 또는 다음날 확인형 전환이 필요합니다.")
    except Exception as e:
        lines.append(f"- 주차별 패턴 변화 감지 오류: {e}")
    return "\n".join(lines)


def _bt_mask_i_core_all(df: pd.DataFrame):
    if df is None or df.empty or 'mode' not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [])
    return df['mode'].astype(str).eq('I') | (pd.to_numeric(df.get('i_core', pd.Series(0, index=df.index)), errors='coerce').fillna(0).astype(int) == 1)


def _bt_mask_i_phase(df: pd.DataFrame, phase: str):
    if df is None or df.empty:
        return pd.Series(False, index=df.index if df is not None else [])
    return _bt_mask_i_core_all(df) & df.get('i_phase', pd.Series('', index=df.index)).astype(str).eq(str(phase))


def _bt_mask_i_core_main(df: pd.DataFrame):
    """v4.3.3 I-CORE MAIN 후보 마스크.
    기준: I-4~I-6 + 재료/거래대금 프록시 + 장기선 이격 제한 + OBV20/거래대금 재증가.
    이전 결과에서 외국인/기관 수급은 표본이 적어 필수 조건이 아니라 참고 태그로 둔다.
    """
    if df is None or df.empty:
        return pd.Series(False, index=df.index if df is not None else [])
    base = _bt_mask_i_core_all(df)
    phase = df.get('i_phase', pd.Series('', index=df.index)).astype(str).isin(['I-4', 'I-5', 'I-6'])
    material = pd.to_numeric(df.get('i_material_proxy_score', 0), errors='coerce').fillna(0) >= I_CORE_MAIN_MIN_MATERIAL
    long_dist = pd.to_numeric(df.get('i_long_ma_dist_pct', 999), errors='coerce').fillna(999)
    long_ok = (long_dist >= I_CORE_MAIN_LONG_MIN) & (long_dist <= I_CORE_MAIN_LONG_MAX)
    if I_CORE_MAIN_REQUIRE_OBV_AMOUNT:
        obv_amount = (pd.to_numeric(df.get('i_amount20_rebuild', 0), errors='coerce').fillna(0) == 1) & (pd.to_numeric(df.get('i_obv20_up', 0), errors='coerce').fillna(0) == 1)
    else:
        obv_amount = pd.Series(True, index=df.index)
    return base & phase & material & long_ok & obv_amount


def _format_i_core_stat_block(sub: pd.DataFrame, label: str) -> str:
    try:
        if sub is None or sub.empty:
            return f"- {label}: 0건"
        n = len(sub)
        def mean_col(c):
            return pd.to_numeric(sub.get(c, np.nan), errors='coerce').mean()
        def rate_col(c):
            return pd.to_numeric(sub.get(c, 0), errors='coerce').fillna(0).mean() * 100.0
        return (
            f"- {label}: {n}건 | "
            f"20일종가 {mean_col('i_ret_close_20d'):.2f}% | 40일종가 {mean_col('i_ret_close_40d'):.2f}% | 60일종가 {mean_col('i_ret_close_60d'):.2f}% | "
            f"60일최대 {mean_col('i_ret_max_high_60d'):.2f}% | +10도달 {rate_col('i_hit10_60d'):.1f}% | +20도달 {rate_col('i_hit20_60d'):.1f}% | "
            f"+30도달 {rate_col('i_hit30_60d'):.1f}% | +50도달 {rate_col('i_hit50_60d'):.1f}% | "
            f"20MA이탈 {rate_col('i_ma20_break_close'):.1f}% | 50MA이탈 {rate_col('i_ma50_break_close'):.1f}% | 박스실패 {rate_col('i_box_fail_close'):.1f}%"
        )
    except Exception as e:
        return f"- {label}: 통계오류 {type(e).__name__}"


def _format_i_core_excess_stat_block(sub: pd.DataFrame, label: str) -> str:
    try:
        if sub is None or sub.empty:
            return f"- {label}: 0건"
        n = len(sub)
        def mean_col(c):
            return pd.to_numeric(sub.get(c, np.nan), errors='coerce').mean()
        return (
            f"- {label}: {n}건 | "
            f"20일 I {mean_col('i_ret_close_20d'):.2f}% / KOSPI {mean_col('i_kospi_ret_close_20d'):.2f}% / 초과 {mean_col('i_excess_close_20d'):.2f}% | "
            f"40일 I {mean_col('i_ret_close_40d'):.2f}% / KOSPI {mean_col('i_kospi_ret_close_40d'):.2f}% / 초과 {mean_col('i_excess_close_40d'):.2f}% | "
            f"60일 I {mean_col('i_ret_close_60d'):.2f}% / KOSPI {mean_col('i_kospi_ret_close_60d'):.2f}% / 초과 {mean_col('i_excess_close_60d'):.2f}%"
        )
    except Exception as e:
        return f"- {label}: 초과수익 통계오류 {type(e).__name__}"

def _format_i_core_excess_stat_block_by_prefix(sub: pd.DataFrame, label: str, prefix: str = 'bench', bench_label: str = 'BENCH') -> str:
    try:
        if sub is None or sub.empty:
            return f"- {label}: 0건"
        n = len(sub)
        def mean_col(c):
            return pd.to_numeric(sub.get(c, np.nan), errors='coerce').mean()
        return (
            f"- {label}: {n}건 | "
            f"20일 I {mean_col('i_ret_close_20d'):.2f}% / {bench_label} {mean_col(f'i_{prefix}_ret_close_20d'):.2f}% / 초과 {mean_col(f'i_{prefix}_excess_close_20d'):.2f}% | "
            f"40일 I {mean_col('i_ret_close_40d'):.2f}% / {bench_label} {mean_col(f'i_{prefix}_ret_close_40d'):.2f}% / 초과 {mean_col(f'i_{prefix}_excess_close_40d'):.2f}% | "
            f"60일 I {mean_col('i_ret_close_60d'):.2f}% / {bench_label} {mean_col(f'i_{prefix}_ret_close_60d'):.2f}% / 초과 {mean_col(f'i_{prefix}_excess_close_60d'):.2f}%"
        )
    except Exception as e:
        return f"- {label}: {bench_label} 초과수익 통계오류 {type(e).__name__}"



def _i_main_enriched_df(df: pd.DataFrame) -> pd.DataFrame:
    """v4.3.7: I-MAIN 발생 종목 상세용 분류 플래그/대표분류를 부여한다."""
    try:
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()
        idx = out.index
        main = _bt_mask_i_core_main(out)
        phase = out.get('i_phase', pd.Series('', index=idx)).astype(str)
        long_dist = pd.to_numeric(out.get('i_long_ma_dist_pct', pd.Series(999, index=idx)), errors='coerce').fillna(999)
        anchor_days = pd.to_numeric(out.get('i_anchor_days', pd.Series(0, index=idx)), errors='coerce').fillna(0)
        monthly = pd.to_numeric(out.get('i_monthly_vol_rebuild', pd.Series(0, index=idx)), errors='coerce').fillna(0)
        out['imain_is_main'] = main.astype(int)
        out['imain_core'] = (main & phase.eq('I-4') & anchor_days.between(120, 180) & monthly.eq(1)).astype(int)
        out['imain_accel'] = (main & long_dist.gt(10) & long_dist.le(18) & anchor_days.between(120, 180)).astype(int)
        out['imain_watch'] = (main & phase.eq('I-4')).astype(int)
        out['imain_confirm'] = (main & phase.eq('I-5')).astype(int)
        out['imain_add'] = (main & phase.eq('I-6')).astype(int)
        def _primary(row):
            # 운용상 고수익형 ACCEL을 최우선으로 표기하고, 중복 시 보조 플래그는 별도 컬럼으로 보존한다.
            if int(row.get('imain_accel', 0)) == 1:
                return '🚀 I-MAIN ACCEL'
            if int(row.get('imain_core', 0)) == 1:
                return '✅ I-MAIN CORE'
            if int(row.get('imain_watch', 0)) == 1:
                return '🟡 I-MAIN WATCH'
            if int(row.get('imain_add', 0)) == 1:
                return '➕ I-MAIN ADD'
            if int(row.get('imain_confirm', 0)) == 1:
                return '🔎 I-MAIN CONFIRM'
            if int(row.get('imain_is_main', 0)) == 1:
                return '📈 I-MAIN MAIN'
            return str(row.get('mode_label', row.get('mode', '')))
        out['imain_primary_class'] = out.apply(_primary, axis=1)
        out['imain_detail_tags'] = out.apply(lambda r: ','.join([t for t, flag in [
            ('CORE', r.get('imain_core', 0)),
            ('ACCEL', r.get('imain_accel', 0)),
            ('WATCH', r.get('imain_watch', 0)),
            ('ADD', r.get('imain_add', 0)),
            ('CONFIRM', r.get('imain_confirm', 0)),
        ] if int(flag) == 1]), axis=1)
        return out
    except Exception as e:
        log_debug(f"I-MAIN 상세 분류 오류: {type(e).__name__}: {e}")
        return df.copy() if df is not None else pd.DataFrame()


def _fmt_pct_cell(x, suffix='%') -> str:
    try:
        v = pd.to_numeric(pd.Series([x]), errors='coerce').iloc[0]
        if pd.isna(v):
            return '-'
        return f"{v:+.1f}{suffix}"
    except Exception:
        return '-'


def _fmt_price_cell(x) -> str:
    try:
        v = _safe_float(x, 0.0)
        return f"{int(round(v)):,}원" if v > 0 else '-'
    except Exception:
        return '-'


def _format_i_main_signal_samples(sub: pd.DataFrame, label: str, max_rows: int | None = None, sort_col: str = 'i_ret_close_60d', ascending: bool = False) -> str:
    """v4.3.7: 텔레그램용 I-MAIN 발생일자/종목명/성과 샘플."""
    try:
        if sub is None or sub.empty:
            return f"- {label}: 해당 없음"
        n = BACKTEST_DETAIL_TOP_N if max_rows is None else int(max_rows)
        view = sub.copy()
        if sort_col in view.columns:
            view['_sort_sample'] = pd.to_numeric(view.get(sort_col), errors='coerce')
            view = view.sort_values(['_sort_sample', 'signal_date'], ascending=[ascending, False], na_position='last')
        else:
            view = view.sort_values('signal_date', ascending=False) if 'signal_date' in view.columns else view
        view = view.head(max(1, n))
        lines = [f"- {label}: 상위/최근 {len(view)}개"]
        for _, r in view.iterrows():
            date = str(r.get('signal_date', ''))[:10]
            name = str(r.get('name', ''))
            code = _normalize_code(str(r.get('code', '')))
            phase = str(r.get('i_phase', r.get('mode', '')))
            cls = str(r.get('imain_primary_class', r.get('mode_label', '')))
            entry = _fmt_price_cell(r.get('close', np.nan))
            ret20 = _fmt_pct_cell(r.get('i_ret_close_20d', np.nan))
            ret40 = _fmt_pct_cell(r.get('i_ret_close_40d', np.nan))
            ret60 = _fmt_pct_cell(r.get('i_ret_close_60d', r.get('ret_close_hd', np.nan)))
            max60 = _fmt_pct_cell(r.get('i_ret_max_high_60d', r.get('ret_max_high_hd', np.nan)))
            dd = _fmt_pct_cell(r.get('ret_min_low_hd', np.nan))
            h10 = 'O' if _safe_float(r.get('i_hit10_60d', 0), 0) >= 1 else 'X'
            h20 = 'O' if _safe_float(r.get('i_hit20_60d', 0), 0) >= 1 else 'X'
            h30 = 'O' if _safe_float(r.get('i_hit30_60d', 0), 0) >= 1 else 'X'
            h50 = 'O' if _safe_float(r.get('i_hit50_60d', 0), 0) >= 1 else 'X'
            bench_ex = _fmt_pct_cell(r.get('i_bench_excess_close_60d', np.nan))
            kosdaq_ex = _fmt_pct_cell(r.get('i_kosdaq_excess_close_60d', np.nan))
            first_event = str(r.get('i_first_event', r.get('first_event', '')))
            tags = str(r.get('imain_detail_tags', ''))
            lines.append(
                f"  · {date} | {name}({code}) | {cls} / {phase} | 진입 {entry} | "
                f"20/40/60 {ret20}/{ret40}/{ret60} | 최대 {max60} / 하락 {dd} | "
                f"+10/+20/+30/+50 {h10}/{h20}/{h30}/{h50} | 초과 BENCH {bench_ex}, KQ {kosdaq_ex} | {first_event} {tags}".strip()
            )
        return "\n".join(lines)
    except Exception as e:
        return f"- {label}: 샘플 생성 오류 {type(e).__name__}"


def _format_i_main_signal_detail_report(df: pd.DataFrame) -> str:
    """v4.3.7: 백테스트 리포트 본문에 발생 종목명/발생일자 샘플을 넣는다."""
    try:
        i = _i_main_enriched_df(df[_bt_mask_i_core_all(df)].copy()) if df is not None and not df.empty else pd.DataFrame()
        lines = ["\n[📋 I-MAIN 발생 종목 상세 샘플 — 날짜/타점/성과]"]
        if i.empty:
            lines.append('- I-MAIN/I-CORE 발생 상세 후보 없음')
            return "\n".join(lines)
        core = i[pd.to_numeric(i.get('imain_core', 0), errors='coerce').fillna(0).eq(1)]
        accel = i[pd.to_numeric(i.get('imain_accel', 0), errors='coerce').fillna(0).eq(1)]
        watch = i[pd.to_numeric(i.get('imain_watch', 0), errors='coerce').fillna(0).eq(1)]
        confirm = i[pd.to_numeric(i.get('imain_confirm', 0), errors='coerce').fillna(0).eq(1)]
        add = i[pd.to_numeric(i.get('imain_add', 0), errors='coerce').fillna(0).eq(1)]
        # 텔레그램 길이를 줄이기 위해 핵심 분류만 상위 샘플 출력한다.
        lines.append(_format_i_main_signal_samples(accel, '🚀 ACCEL 성과상위', max_rows=min(BACKTEST_DETAIL_TOP_N, 8), sort_col='i_ret_close_60d', ascending=False))
        lines.append(_format_i_main_signal_samples(core, '✅ CORE 성과상위', max_rows=min(BACKTEST_DETAIL_TOP_N, 8), sort_col='i_ret_close_40d', ascending=False))
        # 최신 발생 리스트는 복기/차트 확인용
        recent_main = i[pd.to_numeric(i.get('imain_is_main', 0), errors='coerce').fillna(0).eq(1)].copy()
        if not recent_main.empty and 'signal_date' in recent_main.columns:
            recent_main = recent_main.sort_values('signal_date', ascending=False)
        lines.append(_format_i_main_signal_samples(recent_main, '🕒 MAIN 최근발생', max_rows=min(BACKTEST_DETAIL_TOP_N, 8), sort_col='signal_date', ascending=False))
        # 실패/약한 후보도 같이 보여야 조건 보완 가능
        weak = i[pd.to_numeric(i.get('i_ret_close_60d', np.nan), errors='coerce').fillna(999).lt(0)].copy()
        lines.append(_format_i_main_signal_samples(weak, '⚠️ 60일 종가수익 음수 샘플', max_rows=min(5, BACKTEST_DETAIL_TOP_N), sort_col='i_ret_close_60d', ascending=True))
        lines.append('- 전체 발생 상세는 closing_bet_logs의 `i_main_backtest_detail_*.csv/html` 파일로 저장됩니다.')
        return "\n".join(lines)
    except Exception as e:
        return f"\n[📋 I-MAIN 발생 종목 상세 샘플]\n- 생성 오류: {type(e).__name__}: {e}"


def _build_backtest_detail_df(df: pd.DataFrame) -> pd.DataFrame:
    """v4.3.7: 선택 백테스트 후보를 사례 복기용 컬럼으로 정리한다."""
    try:
        if df is None or df.empty:
            return pd.DataFrame()
        enriched = _i_main_enriched_df(df)
        keep_cols = [
            'signal_date', 'code', 'name', 'mode', 'mode_label', 'strategy', 'grade', 'score',
            'imain_primary_class', 'imain_detail_tags', 'i_phase',
            'close', 'amount_b', 'vol_ratio', 'rr', 'close_loc_pct', 'wick_pct',
            'st30_label', 'st30_k', 'st30_d', 'st30_recent_min_k', 'st30_reclaim_pass', 'st30_weekly_confirm', 'st30_reason',
            'i_anchor_days', 'i_long_ma_dist_pct', 'i_monthly_vol_rebuild', 'i_material_proxy_score',
            'i_obv20_up', 'i_obv60_up', 'i_amount20_rebuild', 'index_label', 'marcap',
            'i_ret_close_20d', 'i_ret_close_40d', 'i_ret_close_60d',
            'i_ret_max_high_20d', 'i_ret_max_high_40d', 'i_ret_max_high_60d', 'ret_min_low_hd',
            'i_hit10_60d', 'i_hit20_60d', 'i_hit30_60d', 'i_hit50_60d',
            'i_ma20_break_close', 'i_ma50_break_close', 'i_box_fail_close', 'i_first_event', 'i_first_event_date',
            'i_mkt_regime', 'i_kospi_ret_close_60d', 'i_excess_close_60d',
            'i_kosdaq_ret_close_60d', 'i_kosdaq_excess_close_60d',
            'i_bench_name', 'i_bench_ret_close_60d', 'i_bench_excess_close_60d',
            'rule35_pnl', 'rule35_win', 'rule35_hit3', 'rule35_hit5', 'rule35_stop', 'rule35_exit', 'rule35_exit_date',
            'passed', 'band_reason', 'sell_rule', 'core_filter'
        ]
        for c in keep_cols:
            if c not in enriched.columns:
                enriched[c] = np.nan
        out = enriched[keep_cols].copy()
        out = out.rename(columns={
            'signal_date': '발생일자', 'code': '종목코드', 'name': '종목명', 'mode': '전략코드',
            'mode_label': '전략명', 'grade': '등급', 'score': '점수',
            'imain_primary_class': 'I-MAIN_대표분류', 'imain_detail_tags': 'I-MAIN_세부태그', 'i_phase': 'I타점',
            'close': '진입종가', 'amount_b': '거래대금_억',
            'st30_label': 'ST30라벨', 'st30_k': 'STOCH_K', 'st30_d': 'STOCH_D',
            'st30_recent_min_k': 'ST30최근저점K', 'st30_reclaim_pass': 'ST30통과',
            'st30_weekly_confirm': 'ST30주봉확인', 'st30_reason': 'ST30사유',
            'i_anchor_days': '기준봉후_거래일',
            'i_long_ma_dist_pct': '장기선이격_pct', 'i_monthly_vol_rebuild': '월봉거래량재증가',
            'i_material_proxy_score': '재료거래대금점수', 'i_ret_close_20d': '20일종가수익_pct',
            'i_ret_close_40d': '40일종가수익_pct', 'i_ret_close_60d': '60일종가수익_pct',
            'i_ret_max_high_60d': '60일최대상승_pct', 'ret_min_low_hd': '평가기간최대하락_pct',
            'i_hit10_60d': '+10도달', 'i_hit20_60d': '+20도달', 'i_hit30_60d': '+30도달', 'i_hit50_60d': '+50도달',
            'i_ma20_break_close': '20MA이탈', 'i_ma50_break_close': '50MA이탈', 'i_box_fail_close': '박스실패',
            'i_first_event': 'I첫이벤트', 'i_first_event_date': 'I첫이벤트일',
            'i_mkt_regime': '시장국면', 'i_bench_name': '선택벤치',
            'i_bench_ret_close_60d': '선택벤치60일수익_pct', 'i_bench_excess_close_60d': '선택벤치60일초과_pct',
            'i_kosdaq_excess_close_60d': 'KOSDAQ60일초과_pct', 'rule35_pnl': '3_5규칙수익_pct'
        })
        if len(out) > BACKTEST_DETAIL_MAX_ROWS:
            out = out.head(BACKTEST_DETAIL_MAX_ROWS)
        return out
    except Exception as e:
        log_error(f"⚠️ 백테스트 상세 DF 생성 실패: {e}")
        return pd.DataFrame()


def _export_backtest_detail_artifacts(selected_df: pd.DataFrame, stamp: str) -> dict:
    """v4.3.7: 발생 종목명/발생일자/성과 상세 CSV+HTML 저장."""
    paths = {}
    try:
        detail = _build_backtest_detail_df(selected_df)
        if detail.empty:
            return paths
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        all_csv = LOG_DIR / f"closing_bet_backtest_detail_{stamp}.csv"
        all_html = LOG_DIR / f"closing_bet_backtest_detail_{stamp}.html"
        detail.to_csv(all_csv, index=False, encoding='utf-8-sig')
        detail.to_html(all_html, index=False, escape=False)
        paths['detail_csv'] = str(all_csv)
        paths['detail_html'] = str(all_html)
        # I-MAIN만 별도 저장
        if '전략코드' in detail.columns:
            imain = detail[detail['전략코드'].astype(str).eq('I')].copy()
        else:
            imain = pd.DataFrame()
        if not imain.empty:
            imain_csv = LOG_DIR / f"i_main_backtest_detail_{stamp}.csv"
            imain_html = LOG_DIR / f"i_main_backtest_detail_{stamp}.html"
            imain.to_csv(imain_csv, index=False, encoding='utf-8-sig')
            imain.to_html(imain_html, index=False, escape=False)
            paths['i_main_csv'] = str(imain_csv)
            paths['i_main_html'] = str(imain_html)
        # v4.3.8: 차트 외 종목특성별 성공/손절 요약도 별도 저장
        try:
            feature = _build_stock_feature_summary_df(selected_df)
            if feature is not None and not feature.empty:
                feature_csv = LOG_DIR / f"stock_feature_backtest_summary_{stamp}.csv"
                feature_html = LOG_DIR / f"stock_feature_backtest_summary_{stamp}.html"
                feature.to_csv(feature_csv, index=False, encoding='utf-8-sig')
                feature.to_html(feature_html, index=False, escape=False)
                paths['stock_feature_csv'] = str(feature_csv)
                paths['stock_feature_html'] = str(feature_html)
        except Exception as e:
            log_error(f"⚠️ 종목특성 요약 파일 저장 실패: {e}")
        return paths
    except Exception as e:
        log_error(f"⚠️ 백테스트 상세 파일 저장 실패: {e}")
        return paths

def _format_i_core_report(df: pd.DataFrame) -> str:
    try:
        i = df[_bt_mask_i_core_all(df)].copy()
        lines = []
        lines.append("[📈 I-CORE 시대중심주 150/200일 시세분출 — v4.3.8 I-MAIN CORE/ACCEL·발생상세·종목특성]")
        if i.empty:
            lines.append("- 해당 후보 없음")
            return "\n".join(lines)
        lines.append(_format_i_core_stat_block(i, 'I-CORE 전체'))
        for phase, label in [
            ('I-3', 'I-3 관찰: 150/200일선 근처 생존'),
            ('I-4', 'I-4 1차: 5MA가 150/200MA 회복'),
            ('I-5', 'I-5 2차: 박스/120일 고점 돌파'),
            ('I-6', 'I-6 3차: 돌파 후 첫 눌림 재지지'),
        ]:
            lines.append(_format_i_core_stat_block(i[_bt_mask_i_phase(i, phase)], label))
        # 분할매집 관점 프록시: I-4/I-5/I-6만 실전 누적 타점으로 본다.
        accum = i[i.get('i_phase', pd.Series('', index=i.index)).astype(str).isin(['I-4', 'I-5', 'I-6'])]
        lines.append(_format_i_core_stat_block(accum, 'I-4~I-6 분할매집 후보군'))
        strict = i[(pd.to_numeric(i.get('score', 0), errors='coerce').fillna(0) >= I_CORE_MIN_SCORE) & (i.get('i_phase', pd.Series('', index=i.index)).astype(str).isin(['I-4','I-5','I-6']))]
        lines.append(_format_i_core_stat_block(strict, f'I-CORE STRICT {I_CORE_MIN_SCORE}점+·I-4~I-6'))

        # v4.3.3 MAIN FILTER: v4.3.2에서 가장 의미 있었던 조합을 별도 검증한다.
        main_mask = _bt_mask_i_core_main(i)
        main = i[main_mask].copy()
        lines.append("\n[I-CORE MAIN FILTER — 실전 누적관찰 후보]")
        lines.append(_format_i_core_stat_block(main, f'I-CORE MAIN: I-4~I-6·재료/거래대금 {I_CORE_MAIN_MIN_MATERIAL}점+·OBV20+거래대금재증가'))
        main_long_dist = pd.to_numeric(main.get('i_long_ma_dist_pct', 0), errors='coerce').fillna(999) if not main.empty else pd.Series(dtype=float)
        lines.append(_format_i_core_stat_block(main[(main_long_dist >= -3) & (main_long_dist <= 10)] if not main.empty else main, 'MAIN 초기: 장기선 이격 -3~+10%'))
        lines.append(_format_i_core_stat_block(main[(main_long_dist > 10) & (main_long_dist <= 18)] if not main.empty else main, 'MAIN 가속: 장기선 이격 +10~+18%'))
        for phase, label in [
            ('I-4', 'MAIN I-4 1차매집: 5MA 장기선 회복'),
            ('I-5', 'MAIN I-5 본매수: 박스/전고점 돌파'),
            ('I-6', 'MAIN I-6 추가매수: 첫 눌림 재지지'),
        ]:
            lines.append(_format_i_core_stat_block(main[main.get('i_phase', pd.Series('', index=main.index)).astype(str).eq(phase)] if not main.empty else main, label))
        if I_CORE_MAIN_ONLY:
            lines.append('- 현재 실행은 I_CORE_MAIN_ONLY=1 상태입니다. I-CORE 전용 백테스트 결과가 MAIN 후보로 제한됩니다.')
        else:
            lines.append('- I_CORE_MAIN_ONLY=1로 켜면 --backtest-i-core-only 결과를 위 MAIN 후보만으로 제한할 수 있습니다.')

        # v4.3.5 MAIN MATRIX: v4.3.3 MAIN_ONLY에서 가장 강했던 조합을 직접 검증한다.
        main_anchor = pd.to_numeric(main.get('i_anchor_days', pd.Series(0, index=main.index)), errors='coerce').fillna(0) if not main.empty else pd.Series(dtype=float)
        main_monthly = pd.to_numeric(main.get('i_monthly_vol_rebuild', pd.Series(0, index=main.index)), errors='coerce').fillna(0) if not main.empty else pd.Series(dtype=float)
        main_phase = main.get('i_phase', pd.Series('', index=main.index)).astype(str) if not main.empty else pd.Series(dtype=str)
        lines.append("\n[I-CORE MAIN MATRIX — I-4 × 기준봉 120~180일 × 월봉거래량]")
        main_i4 = main[main_phase.eq('I-4')].copy() if not main.empty else main
        main_i4_anchor = pd.to_numeric(main_i4.get('i_anchor_days', pd.Series(0, index=main_i4.index)), errors='coerce').fillna(0) if not main_i4.empty else pd.Series(dtype=float)
        main_i4_monthly = pd.to_numeric(main_i4.get('i_monthly_vol_rebuild', pd.Series(0, index=main_i4.index)), errors='coerce').fillna(0) if not main_i4.empty else pd.Series(dtype=float)
        lines.append(_format_i_core_stat_block(main_i4, 'MAIN I-4 전체'))
        lines.append(_format_i_core_stat_block(main_i4[(main_i4_anchor >= 120) & (main_i4_anchor <= 180)] if not main_i4.empty else main_i4, 'MAIN I-4 × 기준봉 120~180일'))
        lines.append(_format_i_core_stat_block(main_i4[main_i4_monthly == 1] if not main_i4.empty else main_i4, 'MAIN I-4 × 월봉 거래량 재증가'))
        lines.append(_format_i_core_stat_block(main_i4[(main_i4_anchor >= 120) & (main_i4_anchor <= 180) & (main_i4_monthly == 1)] if not main_i4.empty else main_i4, 'MAIN I-4 × 120~180일 × 월봉거래량'))
        imain_core = main_i4[(main_i4_anchor >= 120) & (main_i4_anchor <= 180) & (main_i4_monthly == 1)] if not main_i4.empty else main_i4
        imain_accel = main[(main_long_dist > 10) & (main_long_dist <= 18) & (main_anchor >= 120) & (main_anchor <= 180)] if not main.empty else main
        imain_i6_anchor = main[(main_phase.eq('I-6')) & (main_anchor >= 120) & (main_anchor <= 180)] if not main.empty else main
        lines.append(_format_i_core_stat_block(imain_accel, 'MAIN 가속(+10~18%) × 기준봉 120~180일'))
        lines.append(_format_i_core_stat_block(imain_i6_anchor, 'MAIN I-6 × 기준봉 120~180일'))

        # v4.3.5 운용분류: 넓은 I-CORE를 그대로 쓰지 않고 CORE/ACCEL/WATCH로 분리한다.
        lines.append("\n[I-MAIN 운용분류 — CORE / ACCEL / WATCH]")
        lines.append(_format_i_core_stat_block(imain_core, '✅ I-MAIN CORE: MAIN I-4 × 120~180일 × 월봉거래량'))
        lines.append(_format_i_core_stat_block(imain_accel, '🚀 I-MAIN ACCEL: MAIN 가속(+10~18%) × 120~180일'))
        lines.append(_format_i_core_stat_block(main_i4, '🟡 I-MAIN WATCH: MAIN I-4 전체'))
        lines.append(_format_i_core_stat_block(main[main_phase.eq('I-5')] if not main.empty else main, '🔎 I-MAIN CONFIRM: I-5 돌파확인'))
        lines.append(_format_i_core_stat_block(main[main_phase.eq('I-6')] if not main.empty else main, '➕ I-MAIN ADD: I-6 첫눌림'))
        lines.append(_format_i_main_signal_detail_report(i))

        # v4.3.5 KOSPI/KOSDAQ 국면/초과수익: 상승장 착시를 분리한다.
        lines.append("\n[I-CORE 시장국면/초과수익 검증 — KOSPI/KOSDAQ/선택벤치 보정]")
        regime = i.get('i_mkt_regime', pd.Series('시장데이터부족', index=i.index)).astype(str)
        if regime.eq('시장데이터부족').all() or regime.eq('시장검증OFF').all():
            lines.append('- KOSPI 시장 데이터가 없거나 I_CORE_REGIME_VALIDATE=0 상태입니다. 시장 대비 초과수익은 계산되지 않았습니다.')
        else:
            lines.append(_format_i_core_excess_stat_block(i, 'I-CORE 전체 vs KOSPI'))
            lines.append(_format_i_core_excess_stat_block(main, 'I-CORE MAIN vs KOSPI'))
            for rg in ['상승장', '횡보장', '하락장']:
                lines.append(_format_i_core_excess_stat_block(i[regime.eq(rg)], f'I-CORE 전체 · {rg}'))
            main_regime = main.get('i_mkt_regime', pd.Series('시장데이터부족', index=main.index)).astype(str) if not main.empty else pd.Series(dtype=str)
            for rg in ['상승장', '횡보장', '하락장']:
                lines.append(_format_i_core_excess_stat_block(main[main_regime.eq(rg)] if not main.empty else main, f'MAIN · {rg}'))
            lines.append(_format_i_core_excess_stat_block(main_i4, 'MAIN I-4 vs KOSPI'))
            lines.append(_format_i_core_excess_stat_block(main_i4[(main_i4_anchor >= 120) & (main_i4_anchor <= 180)] if not main_i4.empty else main_i4, 'MAIN I-4 × 120~180일 vs KOSPI'))
            if I_CORE_COMPARE_KOSDAQ:
                lines.append(_format_i_core_excess_stat_block_by_prefix(i, 'I-CORE 전체 vs KOSDAQ', 'kosdaq', 'KOSDAQ'))
                lines.append(_format_i_core_excess_stat_block_by_prefix(main, 'I-CORE MAIN vs KOSDAQ', 'kosdaq', 'KOSDAQ'))
                lines.append(_format_i_core_excess_stat_block_by_prefix(imain_core, 'I-MAIN CORE vs KOSDAQ', 'kosdaq', 'KOSDAQ'))
                lines.append(_format_i_core_excess_stat_block_by_prefix(imain_accel, 'I-MAIN ACCEL vs KOSDAQ', 'kosdaq', 'KOSDAQ'))
            lines.append(_format_i_core_excess_stat_block_by_prefix(i, 'I-CORE 전체 vs 선택벤치', 'bench', 'BENCH'))
            lines.append(_format_i_core_excess_stat_block_by_prefix(main, 'I-CORE MAIN vs 선택벤치', 'bench', 'BENCH'))
            lines.append(_format_i_core_excess_stat_block_by_prefix(imain_core, 'I-MAIN CORE vs 선택벤치', 'bench', 'BENCH'))
            lines.append(_format_i_core_excess_stat_block_by_prefix(imain_accel, 'I-MAIN ACCEL vs 선택벤치', 'bench', 'BENCH'))

        # 세부 구간
        long_dist = pd.to_numeric(i.get('i_long_ma_dist_pct', 0), errors='coerce').fillna(999)
        anchor_days = pd.to_numeric(i.get('i_anchor_days', 0), errors='coerce').fillna(0)
        material = pd.to_numeric(i.get('i_material_proxy_score', 0), errors='coerce').fillna(0)
        monthly = pd.to_numeric(i.get('i_monthly_vol_rebuild', 0), errors='coerce').fillna(0)
        lines.append("\n[I-CORE 세부 진단]")
        lines.append(_format_i_core_stat_block(i[(long_dist >= -3) & (long_dist <= 10)], '장기선 이격 -3~+10%'))
        lines.append(_format_i_core_stat_block(i[(long_dist > 10) & (long_dist <= 18)], '장기선 이격 +10~+18%'))
        lines.append(_format_i_core_stat_block(i[(anchor_days >= 120) & (anchor_days <= 180)], '기준봉 후 120~180일'))
        lines.append(_format_i_core_stat_block(i[(anchor_days > 180) & (anchor_days <= 240)], '기준봉 후 180~240일'))
        lines.append(_format_i_core_stat_block(i[monthly == 1], '월봉 거래량 재증가'))
        lines.append(_format_i_core_stat_block(i[material >= 3], '재료/거래대금 프록시 3점+'))
        supply_score = pd.to_numeric(i.get('i_supply_score', 0), errors='coerce').fillna(0)
        obv20 = pd.to_numeric(i.get('i_obv20_up', 0), errors='coerce').fillna(0)
        obv60 = pd.to_numeric(i.get('i_obv60_up', 0), errors='coerce').fillna(0)
        amount_re = pd.to_numeric(i.get('i_amount20_rebuild', 0), errors='coerce').fillna(0)
        flow_fetch = pd.to_numeric(i.get('i_flow_fetched', 0), errors='coerce').fillna(0)
        fi = pd.to_numeric(i.get('i_fi_20d_b', 0), errors='coerce').fillna(0)
        inst = pd.to_numeric(i.get('i_inst_20d_b', 0), errors='coerce').fillna(0)
        frgn = pd.to_numeric(i.get('i_frgn_20d_b', 0), errors='coerce').fillna(0)
        lines.append("\n[I-CORE 수급/OBV 진단]")
        lines.append(_format_i_core_stat_block(i[supply_score >= 2], '수급프록시 2점+'))
        lines.append(_format_i_core_stat_block(i[(obv20 == 1) & (obv60 == 1)], 'OBV 20/60일 동시상승'))
        lines.append(_format_i_core_stat_block(i[(amount_re == 1) & (obv20 == 1)], '거래대금 재증가+OBV20상승'))
        if int(flow_fetch.sum()) > 0:
            lines.append(_format_i_core_stat_block(i[fi > 0], '외국인+기관 20일 합산 순매수'))
            lines.append(_format_i_core_stat_block(i[inst > 0], '기관 20일 순매수'))
            lines.append(_format_i_core_stat_block(i[frgn > 0], '외국인 20일 순매수'))
        else:
            lines.append('- pykrx 외국인/기관 수급: 기본 OFF입니다. I_CORE_FETCH_KRX_FLOW=1로 켜면 I-CORE 후보에 20일 외국인/기관 순매수 진단을 추가합니다.')
        lines.append("\n[제외/태그 로직]")
        lines.append("- 최근 15거래일 내 거래량 없는 -8% 급락 후 5일 이내 빠른 회복형은 제외하지 않고 `무거래단기급락회복` 태그로 표시합니다. 장기 박스/150·200선 생존 구조라면 흔들기·유동성 공백 후 회복으로 별도 관찰합니다.")
        lines.append("- I-CORE는 종가배팅 +3/+5가 아니라 20/40/60거래일, +10/+20/+30/+50, 20/50MA 이탈 기준으로 해석합니다.")
        lines.append("- 뉴스/재료 지속성은 이번 v4.3.0에서는 거래대금·월봉거래량·대표성 프록시로만 측정하며, 후속 버전에서 뉴스 누적 저장소와 연결합니다. v4.3.7은 MAIN 후보를 I-MAIN CORE/ACCEL/WATCH로 운용분류하고 KOSPI/KOSDAQ/선택벤치 초과수익과 발생 종목 상세를 함께 검증합니다.")
        return "\n".join(lines)
    except Exception as e:
        return f"[📈 I-CORE 시대중심주 150/200일 시세분출]\n- 리포트 생성 오류: {type(e).__name__}: {e}"

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
    core_only: bool = False,
    safe_only: bool = False,
    neutral_only: bool = False,
    risk_only: bool = False,
    c_only: bool = False,
    c_swing_only: bool = False,
    c_strict_only: bool = False,
    c_pullback_only: bool = False,
    g_only: bool = False,
    h_only: bool = False,
    h_watch_only: bool = False,
    leader_gap_only: bool = False,
    i_core_only: bool = False,
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
    sel_text = '전체후보' if all_candidates else f'날짜별 전략별 TOP{top_per_strategy}'
    if c_pullback_only:
        sel_text += ' → C-SWING 눌림재상승형 전용 필터'
    elif c_swing_only:
        sel_text += ' → C-SWING 역매공파 스윙 전용 필터(+5/+10·장기선 종가이탈)'
    elif c_strict_only:
        sel_text += ' → C 엄격형만 필터(스윙 검증)'
    elif c_only:
        sel_text += ' → C 역매공파만 필터(C1/C2/C3 단계별 검증)'
    elif leader_gap_only:
        sel_text += ' → 대형주 리더갭 WATCH만 필터(SK하이닉스형 초대형 거래대금 갭상승)'
    elif h_watch_only:
        sel_text += ' → H 눌림반등 WATCH만 필터(신고가/전고점 돌파 후 2~8일 눌림 관전형)'
    elif h_only:
        sel_text += ' → H 신고가거자름 STRICT만 필터(장대양봉 신고가 돌파 후 거래량 마른 짧은 타점봉)'
    elif safe_only:
        sel_text += ' → S-CORE SAFE만 필터(RR 1.0~1.5·거래량비<1.5·종가위치70%+)'
    elif neutral_only:
        sel_text += ' → S-CORE NEUTRAL만 필터(SAFE/RISK 제외 중립군)'
    elif risk_only:
        sel_text += ' → S-CORE RISK만 필터(위험태그 후보)'
    elif core_only:
        sel_text += ' → S-CORE만 필터(S1우수응축+S2)'
    lines.append(f"원신호 {len(raw_df)}건 | 최종검증 {len(df)}건 | 선택방식: {sel_text}")
    lines.append("")

    lines.append("[전체]")
    lines.append(_format_backtest_stat_block(df, '전체', hold_days))
    lines.append("")

    lines.append("[실전형 3/5 익절·손절]")
    lines.append(_format_backtest_trade_rule_block(df, '전체'))
    try:
        _st30_report = _format_st30_reclaim_abtest_report(df)
        if _st30_report:
            lines.append("")
            lines.append(_st30_report)
        _st30_cross_lines = _v44916_pattern_cross_audit_lines(df, compact=False)
        if _st30_cross_lines:
            lines.append("")
            lines += _st30_cross_lines
        _ssafe_st30_lines = _v44917_s_safe_st30_drilldown_lines(df, compact=False)
        if _ssafe_st30_lines:
            lines.append("")
            lines += _ssafe_st30_lines
        _ssafe_miss_audit_lines = _v44918_s_safe_miss_success_fail_audit_lines(df, compact=False)
        if _ssafe_miss_audit_lines:
            lines.append("")
            lines += _ssafe_miss_audit_lines
        _ssafe_liq_repeat_lines = _v44919_s_safe_liquidity_rule_repeat_lines(df, compact=False)
        if _ssafe_liq_repeat_lines:
            lines.append("")
            lines += _ssafe_liq_repeat_lines
    except Exception as e:
        lines.append(f"[ST30-RECLAIM/패턴교차 리포트 오류] {type(e).__name__}: {e}")
    try:
        s_core_mask = _bt_mask_s_core(df)
        s_core_df = df[s_core_mask]
        s_safe_df = df[_bt_mask_s_core_safe(df)]
        s_neutral_df = df[_bt_mask_s_core_neutral(df)]
        s_risk_df = df[_bt_mask_s_core_risk(df)]
        a_aux_df = df[_bt_mask_a_strong(df)]
        core_plus_a_df = df[s_core_mask | _bt_mask_a_strong(df)]
        observe_mask = ((df['mode'].astype(str).eq('S')) & (~_bt_mask_s1_good(df)) & (~_bt_mask_s2(df))) | (df['mode'].astype(str).isin(['B1', 'B2']))
        observe_df = df[observe_mask]
        lines.append(_format_backtest_trade_rule_block(s_core_df, 'S-CORE 전체(S1우수응축+S2)'))
        lines.append(_format_backtest_trade_rule_block(s_safe_df, '🟢 S-CORE SAFE'))
        lines.append(_format_backtest_trade_rule_block(s_neutral_df, '🟡 S-CORE NEUTRAL'))
        lines.append(_format_backtest_trade_rule_block(s_risk_df, '⚠️ S-CORE RISK'))
        lines.append(_format_backtest_trade_rule_block(a_aux_df, 'A 보조돌파'))
        lines.append(_format_backtest_trade_rule_block(core_plus_a_df, '실전 전체군(S-CORE+A보조)'))
        lines.append(_format_backtest_trade_rule_block(observe_df, '관찰/확인군(S1일반+B1+B2)'))
        g_all_df = df[_bt_mask_g_all(df)]
        if len(g_all_df) > 0:
            lines.append(_format_backtest_trade_rule_block(g_all_df, 'G 모랄레스갭 전체'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_g_safe(df)], '🟢 G-SAFE 모랄레스갭'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_g_neutral(df)], '🟡 G-NEUTRAL 모랄레스갭'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_g_risk(df)], '⚠️ G-AGGRESSIVE 모랄레스갭'))
        h_all_df = df[_bt_mask_h_all(df)]
        if len(h_all_df) > 0:
            lines.append(_format_backtest_trade_rule_block(h_all_df, 'H 신고가거자름 STRICT 전체'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_struct_safe(df)], 'H-STRUCT 신고가거자름 STRICT'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_triangle(df)], '🧊 H-TRIANGLE SAFE 직전삼각'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_triangle_struct(df)], '🧊 H-TRIANGLE STRUCT 직전삼각+구조통과'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_core_500_1000_vol23(df)], '🟢 H-CORE 500~1000억×2~3배'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_core_300_500_vol35(df)], '🟣 H-CORE 300~500억×3~5배'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_v427_core_union(df)], '✅ H-CORE UNION 삼각/핵심셀'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_safe(df)], '🟢 H-VOL SAFE 2~3배'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_vol_safe_pattern(df)], '🧊 H-PATTERN SAFE 2~3배+저변동+직전구조'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_vol_swing(df)], '🟣 H-VOL SWING 3~5배'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_vol_overheat(df)], '⚠️ H-OVERHEAT 5~8배'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_vol_aggressive(df)], '🔥 H-AGGRESSIVE 8배+'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_neutral(df)], '🟡 H-NEUTRAL 잔여관찰'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_risk(df)], '⚠️ H-RISK 구조불량'))
        h_watch_df = df[_bt_mask_h_watch_all(df)]
        if len(h_watch_df) > 0:
            lines.append(_format_backtest_trade_rule_block(h_watch_df, '👀 H 눌림반등 WATCH 전체'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_h_watch_ready(df)], '🟢 H-WATCH READY'))
        leader_gap_df = df[_bt_mask_leader_gap_all(df)]
        if len(leader_gap_df) > 0:
            lines.append(_format_backtest_trade_rule_block(leader_gap_df, '👑 대형주 리더갭 WATCH 전체'))
            lines.append(_format_backtest_trade_rule_block(df[_bt_mask_leader_gap_ready(df)], '🟢 리더갭 READY'))
        i_core_df = df[_bt_mask_i_core_all(df)]
        if len(i_core_df) > 0:
            lines.append(_format_i_core_stat_block(i_core_df, '📈 I-CORE 전체'))
    except Exception:
        pass
    lines.append("")

    try:
        lines.append(_format_practical_combo_report(df))
        lines.append("")
        lines.append(_format_tuning_report(df))
        lines.append("")
        # v4.1: 손절 특이점 분석은 계산 범위를 명확히 분리한다.
        try:
            s_core_diag_df = df[_bt_mask_s_core(df)]
            s_safe_diag_df = df[_bt_mask_s_core_safe(df)]
            if core_only or safe_only or neutral_only or risk_only or c_only or c_swing_only or c_strict_only or c_pullback_only or g_only or h_only or h_watch_only or leader_gap_only or i_core_only:
                if leader_gap_only:
                    _scope = '대형주 리더갭 WATCH 선택군'
                elif h_watch_only:
                    _scope = 'H 눌림반등 WATCH 선택군'
                elif h_only:
                    _scope = 'H 신고가거자름 선택군'
                elif c_pullback_only:
                    _scope = 'C-SWING 눌림재상승형 선택군'
                elif c_swing_only:
                    _scope = 'C-SWING 역매공파 선택군'
                elif c_strict_only:
                    _scope = 'C 엄격형 선택군'
                elif c_only:
                    _scope = 'C 역매공파 선택군'
                elif g_only:
                    _scope = 'G 모랄레스갭 선택군'
                elif safe_only:
                    _scope = 'SAFE 선택군'
                elif neutral_only:
                    _scope = 'NEUTRAL 선택군'
                elif risk_only:
                    _scope = 'RISK 선택군'
                elif core_only:
                    _scope = 'S-CORE 선택군'
                else:
                    _scope = '현재 선택군'
                lines.append(_format_stoploss_signature_report(df, _scope, include_buckets=True))
            else:
                lines.append(_format_stoploss_signature_report(df, '전체 선택군', include_buckets=False))
                if s_core_diag_df is not None and not s_core_diag_df.empty:
                    lines.append("")
                    lines.append(_format_stoploss_signature_report(s_core_diag_df, 'S-CORE 선택군', include_buckets=True))
                if s_safe_diag_df is not None and not s_safe_diag_df.empty:
                    lines.append("")
                    lines.append(_format_stoploss_signature_report(s_safe_diag_df, 'SAFE 선택군', include_buckets=False))
        except Exception as e:
            lines.append(f"[손절 특이점 범위분리 오류] {e}")
        lines.append("")
    except Exception as e:
        lines.append(f"[튜닝 리포트 오류] {e}")
        lines.append("")

    try:
        lines.append(_format_stock_feature_report(df))
        lines.append("")
    except Exception as e:
        lines.append(f"[종목특성 리포트 오류] {e}")
        lines.append("")

    lines.append("[전략별]")
    mode_order = ['I', 'LP', 'G', 'L', 'H', 'S', 'A', 'B1', 'B2', 'C']
    for mode in mode_order:
        sub = df[df['mode'] == mode]
        label = {
            'LP': '리더갭 눌림재지지(LP)',
            'G': '모랄레스갭(G)',
            'L': '대형주리더갭WATCH(L)',
            'H': '신고가거자름STRICT(H)',
            'S': '고점재응축(S)',
            'A': '돌파형(A)',
            'B1': 'ENV엄격형(B1)',
            'B2': 'BB확장형(B2)',
            'C': '역매공파(C)',
        'I': 'I-MAIN 150/200 시세분출(I)',
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


    l_df = df[df['mode'] == 'L'] if 'mode' in df.columns else pd.DataFrame()
    if not l_df.empty:
        lines.append("")
        lines.append(_format_leader_gap_watch_report(df))

    h_df = df[df['mode'] == 'H'] if 'mode' in df.columns else pd.DataFrame()
    if not h_df.empty:
        h_strict_df = h_df[_bt_mask_h_all(h_df)].copy()
        h_watch_df = h_df[_bt_mask_h_watch_all(h_df)].copy()
        if not h_watch_df.empty:
            lines.append("")
            lines.append(_format_h_pullback_watch_report(h_watch_df))
            lines.append("")
            lines.append("[H-WATCH MA5 전용 — 5/10일선 회복·이탈 검증]")
            lines.append(_format_h_ma5_block(h_watch_df, 'H-WATCH 전체'))
            lines.append(_format_h_ma5_block(h_watch_df[_bt_mask_h_watch_ready(h_watch_df)], 'H-WATCH READY'))
            lines.append(_format_h_ma5_block(h_watch_df[_bt_mask_h_watch_ma5_reclaim(h_watch_df)], 'H-WATCH 5/10일선 근처'))
            lines.append("- 해석: WATCH는 실전 확정 후보가 아니라 다음날 반등 확인 후보입니다. 결과가 좋더라도 추격매수용으로 바로 승격하지 않습니다.")
        if not h_strict_df.empty:
            lines.append("")
            lines.append("[H전략 세부 — 신고가 거자름 STRICT 분리]")
            lines.append(_format_backtest_trade_rule_block(h_strict_df, 'H 신고가거자름 STRICT 전체'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_struct_safe(h_strict_df)], 'H-STRUCT 신고가거자름 STRICT'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_triangle(h_strict_df)], '🧊 H-TRIANGLE SAFE 직전삼각'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_triangle_struct(h_strict_df)], '🧊 H-TRIANGLE STRUCT 직전삼각+구조통과'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_core_500_1000_vol23(h_strict_df)], '🟢 H-CORE 500~1000억×2~3배'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_core_300_500_vol35(h_strict_df)], '🟣 H-CORE 300~500억×3~5배'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_v427_core_union(h_strict_df)], '✅ H-CORE UNION 삼각/핵심셀'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_safe(h_strict_df)], '🟢 H-VOL SAFE 2~3배'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_vol_safe_pattern(h_strict_df)], '🧊 H-PATTERN SAFE 2~3배+저변동+직전구조'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_vol_swing(h_strict_df)], '🟣 H-VOL SWING 3~5배'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_vol_overheat(h_strict_df)], '⚠️ H-OVERHEAT 5~8배'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_vol_aggressive(h_strict_df)], '🔥 H-AGGRESSIVE 8배+'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_neutral(h_strict_df)], '🟡 H-NEUTRAL 잔여관찰'))
            lines.append(_format_backtest_trade_rule_block(h_strict_df[_bt_mask_h_risk(h_strict_df)], '⚠️ H-RISK 구조불량'))
            lines.append(_format_h_v427_core_report(h_strict_df))
            lines.append("")
            lines.append(_format_h_reason_report(h_strict_df))
            lines.append("")
            lines.append(_format_h_breakout_volume_report(h_strict_df))
            lines.append("")
            lines.append(_format_h_amount_report(h_strict_df))
            lines.append("")
            lines.append(_format_h_amount_vol_matrix_report(h_strict_df))
            lines.append(_format_h_500_1000_fine_matrix_report(h_strict_df))
            lines.append("")
            lines.append(_format_h_pre_structure_report(h_strict_df))
            lines.append("")
            lines.append("[H-MA5 전용 — 5일선 종가이탈 손절·+5/+10 검증]")
            lines.append(_format_h_ma5_block(h_strict_df, 'H-MA5 전체'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_struct_safe(h_strict_df)], 'H-MA5 STRUCT'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_triangle(h_strict_df)], 'H-MA5 TRIANGLE SAFE'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_core_500_1000_vol23(h_strict_df)], 'H-MA5 CORE 500~1000억×2~3배'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_core_500_700_vol23(h_strict_df)], 'H-MA5 CORE 500~700억×2~3배'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_core_700_1000_vol23(h_strict_df)], 'H-MA5 CORE 700~1000억×2~3배'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_watch_500_1000_vol30_40(h_strict_df)], 'H-MA5 WATCH 500~1000억×3~4배'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_watch_500_1000_vol40_50(h_strict_df)], 'H-MA5 WATCH 500~1000억×4~5배'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_core_300_500_vol35(h_strict_df)], 'H-MA5 CORE 300~500억×3~5배'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_v427_core_union(h_strict_df)], 'H-MA5 CORE UNION'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_safe(h_strict_df)], 'H-MA5 VOL-SAFE 2~3배'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_vol_safe_pattern(h_strict_df)], 'H-MA5 PATTERN SAFE 2~3배+저변동+직전구조'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_vol_swing(h_strict_df)], 'H-MA5 SWING 3~5배'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_vol_overheat(h_strict_df)], 'H-MA5 OVERHEAT 5~8배'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_vol_aggressive(h_strict_df)], 'H-MA5 AGGRESSIVE 8배+'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_neutral(h_strict_df)], 'H-MA5 NEUTRAL'))
            lines.append(_format_h_ma5_block(h_strict_df[_bt_mask_h_risk(h_strict_df)], 'H-MA5 RISK'))
            lines.append("- 해석: H는 +3/+5 단기 익절과 함께 종가 5일선 이탈 손절이 실제로 유리한지 별도 확인합니다.")

    c_df = df[df['mode'] == 'C'] if 'mode' in df.columns else pd.DataFrame()
    if not c_df.empty:
        lines.append("")
        lines.append("[C전략 세부 — 역매공파 단계별 분리]")
        if 'c_stage' in c_df.columns:
            for cs, label in [('C1', 'C1 1파돌파형'), ('C2', 'C2 눌림진행형'), ('C3', 'C3 눌림완성형')]:
                sub = c_df[c_df['c_stage'].astype(str) == cs]
                lines.append(_format_backtest_stat_block(sub, label, hold_days))
                if not sub.empty and 'rule35_pnl' in sub.columns:
                    lines.append('  ' + _format_backtest_trade_rule_block(sub, label).lstrip('- '))
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
        lines.append("- 해석: C1/C2/C3는 진단용이며, v4.1.6부터는 ymGP 눌림재상승형이 실제 비교 대상입니다.")
        lines.append("- 해석: C 완화형은 실전 신호가 아니라 백테스트 진단용입니다. C 엄격형 안에서 C2/C3가 손절선행을 줄이는지 우선 비교합니다.")

        lines.append("")
        lines.append("[C-SWING 전용 — 장기선 종가이탈 손절·+5/+10 검증]")
        lines.append(_format_c_swing_block(c_df, 'C-SWING 전체'))
        if 'ymgp_pullback_reentry' in c_df.columns:
            c_pr = c_df[pd.to_numeric(c_df['ymgp_pullback_reentry'], errors='coerce').fillna(0).astype(int) == 1]
            lines.append(_format_c_swing_block(c_pr, 'C-SWING 눌림재상승형'))
        if 'c_type' in c_df.columns:
            lines.append(_format_c_swing_block(c_df[c_df['c_type'].astype(str) == 'strict'], 'C-SWING 엄격형'))
            lines.append(_format_c_swing_block(c_df[c_df['c_type'].astype(str) == 'relaxed'], 'C-SWING 완화형(진단용)'))
        if 'c_stage' in c_df.columns:
            for cs, label in [('C1', 'C1 1파돌파형'), ('C2', 'C2 눌림진행형'), ('C3', 'C3 눌림완성형')]:
                lines.append(_format_c_swing_block(c_df[c_df['c_stage'].astype(str) == cs], f'C-SWING {label}'))
        lines.append("- 해석: C-SWING은 3/5 단타룰이 아니라 +5/+10 도달과 장기선 종가이탈 손절을 보는 스윙 검증입니다.")

    i_df = df[df['mode'] == 'I'] if 'mode' in df.columns else pd.DataFrame()
    if not i_df.empty:
        lines.append("")
        lines.append(_format_i_core_report(df))

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
        lines.append("")
        # v4.1: 주차별 변화 감지는 전체/S-CORE/SAFE 계산 범위를 분리한다.
        try:
            if safe_only:
                lines.append(_format_weekly_pattern_report(df, 'SAFE 선택군'))
            elif neutral_only:
                lines.append(_format_weekly_pattern_report(df, 'NEUTRAL 선택군'))
            elif risk_only:
                lines.append(_format_weekly_pattern_report(df, 'RISK 선택군'))
            elif core_only:
                lines.append(_format_weekly_pattern_report(df, 'S-CORE 선택군'))
            elif c_pullback_only:
                lines.append(_format_weekly_pattern_report(df, 'C-SWING 눌림재상승형 선택군'))
            elif c_swing_only:
                lines.append(_format_weekly_pattern_report(df, 'C-SWING 선택군'))
            elif c_strict_only:
                lines.append(_format_weekly_pattern_report(df, 'C 엄격형 선택군'))
            elif c_only:
                lines.append(_format_weekly_pattern_report(df, 'C 역매공파 선택군'))
            elif leader_gap_only:
                lines.append(_format_weekly_pattern_report(df, '대형주 리더갭 WATCH 선택군'))
            else:
                lines.append(_format_weekly_pattern_report(df, '전체 선택군'))
                s_core_week_df = df[_bt_mask_s_core(df)]
                s_safe_week_df = df[_bt_mask_s_core_safe(df)]
                if s_core_week_df is not None and not s_core_week_df.empty:
                    lines.append("")
                    lines.append(_format_weekly_pattern_report(s_core_week_df, 'S-CORE 선택군'))
                if s_safe_week_df is not None and not s_safe_week_df.empty:
                    lines.append("")
                    lines.append(_format_weekly_pattern_report(s_safe_week_df, 'SAFE 선택군'))
        except Exception as e:
            lines.append(f"[주차별 패턴 변화 범위분리 오류] {e}")

    if diag_text:
        lines.append("")
        lines.append(diag_text)

    lines.append("")
    lines.append("[주의]")
    lines.append("- 이 검증은 일봉 종가 기준 재현입니다. 실제 장중 체결가, 슬리피지, 수수료, 호가 공백은 반영하지 않았습니다.")
    lines.append("- 기본값은 날짜별 전략별 TOP5만 검증하므로, 실전 텔레그램 선별 결과에 가깝게 보는 용도입니다.")
    lines.append("- v4.1의 3/5규칙은 보수 모델입니다. 같은 날 손절가와 목표가가 모두 닿으면 손절을 먼저 본 것으로 계산합니다.")
    lines.append("- v4.1의 튜닝/손절특이점 리포트는 최근 백테스트 결과를 조건별 성과로 비교하는 참고용이며, 최소 4주~12주 이상 반복 검증이 필요합니다.")
    lines.append("- C 완화형은 실전 추천 후보가 아니라 C조건 점검용 진단 후보입니다.")
    lines.append("- --backtest-core-only 옵션을 쓰면 S-CORE(S1우수응축+S2)만 따로 검증합니다. A강한돌파는 보조 후보로 별도 해석합니다.")
    lines.append("- --backtest-safe-only 옵션을 쓰면 S-CORE SAFE(RR 1.0~1.5·거래량비<1.5·종가위치70%+)만 따로 검증합니다.")
    lines.append("- --backtest-neutral-only 옵션을 쓰면 S-CORE NEUTRAL(SAFE/RISK 제외 중립군)만 따로 검증합니다.")
    lines.append("- --backtest-risk-only 옵션을 쓰면 S-CORE RISK(위험태그 후보)만 따로 검증합니다.")
    lines.append("- --backtest-c-only 옵션을 쓰면 C 역매공파만 필터링하여 C1/C2/C3 단계별 성과를 빠르게 확인합니다.")
    lines.append("- --backtest-c-swing-only 옵션을 쓰면 C 역매공파를 +5/+10 도달·장기선 종가이탈 손절 기준으로 별도 검증합니다.")
    lines.append("- --backtest-c-strict-only 옵션을 쓰면 C 엄격형만 따로 필터링해 스윙 성과를 확인합니다.")
    lines.append("- --backtest-g-only 옵션을 쓰면 G 모랄레스갭만 필터링하여 G-SAFE/NEUTRAL/RISK 성과를 확인합니다.")
    lines.append("- --backtest-h-only 옵션을 쓰면 H 신고가거자름 STRICT만 필터링하여 H-VOL SAFE/SWING/OVERHEAT/AGGRESSIVE 성과와 5일선 종가이탈 손절 기준을 확인합니다.")
    lines.append("- --backtest-leader-gap-only 옵션을 쓰면 대형주/섹터대장 리더갭 WATCH만 필터링하여 SK하이닉스형 초대형 거래대금 갭상승 성과를 확인합니다.")
    lines.append("- --backtest-i-core-only 옵션을 쓰면 I-CORE 시대중심주 150/200일 시세분출 타점만 필터링하여 20/40/60일, +10/+20/+30/+50 성과를 확인합니다.")
    lines.append("- --backtest-h-watch-only 옵션을 쓰면 H 눌림반등 WATCH만 필터링하여 신고가/전고점 돌파 후 2~8일 눌림 관전형 성과를 확인합니다.")
    lines.append("- v4.2.15부터 실시간 스캔에서도 H-TRIANGLE SAFE와 거래대금×Vol60 핵심셀(500~1000억×2~3배, 500~1000억 세부구간, 300~500억×3~5배)을 S/G/H 통합 운영 후보로 표시하고, 돌파봉 Vol60 배율 운용분류에 더해, 거래대금 구간을 <30억/30~50억/50~100억/100~200억/200~300억/300~500억/500~1000억/1000~2000억/2000~3000억/3000~5000억/5000억~1조/1조+로 세분화해 성과를 분해합니다. H-VOL SAFE는 2~3배이며 v4.2.12에서는 500~1000억 내부를 500~600/600~700/700~850/850~1000억 및 Vol60 1.5~5배로 추가 세분화합니다. H-VOL SAFE는 2~3배, H-VOL SWING은 3~5배, H-OVERHEAT는 5~8배, H-AGGRESSIVE는 8배 이상입니다. 수급 데이터는 이번 버전에서 필수 조건이 아니라 후속 가점 후보입니다. v4.2.15는 대형주 리더갭 5000억+를 L-CORE 실전 보조 후보로 유지하고 3000~5000억은 L-WATCH 관찰로 분리합니다. v4.3.8은 I-MAIN CORE/ACCEL/WATCH/CONFIRM/ADD와 발생 종목명·발생일자·성과 상세에 더해, 시장/지수소속·시총·거래대금·대표성/유동성 등 차트 외 종목특성별 성공/손절 분석을 추가합니다.")
    lines.append("- v4.1.9부터 G 모랄레스갭은 G-SAFE/G-NEUTRAL/G-AGGRESSIVE 분류와 함께 AGGRESSIVE/SAFE 탈락 사유를 출력합니다. G-SAFE는 S-CORE SAFE 다음 보조 실전 후보, C-SWING은 진단용입니다.")
    lines.append("- v4.1는 손절발생 종목의 공통점, S-CORE 3단계, 주차별 패턴 변화 감지를 함께 출력합니다.")
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
    core_only: bool = False,
    safe_only: bool = False,
    neutral_only: bool = False,
    risk_only: bool = False,
    c_only: bool = False,
    c_swing_only: bool = False,
    c_strict_only: bool = False,
    c_pullback_only: bool = False,
    g_only: bool = False,
    h_only: bool = False,
    h_watch_only: bool = False,
    leader_gap_only: bool = False,
    i_core_only: bool = False,
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
    source_codes = [c for c in codes if _is_universe_allowed(c)]
    if not source_codes and codes and CLOSING_BET_ALLOW_UNIVERSE_FALLBACK:
        log_error('⚠️ 유니버스 필터 후 0개 → INDEX/MARCAP 맵 장애로 판단하고 원 유니버스 코드를 fallback 허용')
        source_codes = list(codes)
    name_map = _build_name_map_for_codes(source_codes)
    _set_stock_name_map(name_map)
    names = [_clean_stock_name(c, name_map.get(c, c)) for c in source_codes]
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
            all_candidates, weekly_breakdown=weekly_breakdown, diag=diag, core_only=core_only, safe_only=safe_only, neutral_only=neutral_only, risk_only=risk_only, c_only=c_only, c_swing_only=c_swing_only, c_strict_only=c_strict_only, c_pullback_only=c_pullback_only, g_only=g_only, h_only=h_only, h_watch_only=h_watch_only, leader_gap_only=leader_gap_only,
            i_core_only=i_core_only,
        )
        try:
            BACKTEST_SUMMARY_TXT.write_text(report, encoding='utf-8')
            BACKTEST_DEBUG_TXT.write_text(_build_backtest_diag_text(diag), encoding='utf-8')
        except Exception as e:
            log_error(f"⚠️ 백테스트 0건 요약 저장 실패: {e}")
        return report, str(selected_path), str(raw_path)

    selected_df = _select_backtest_top(raw_df, top_per_strategy=top_per_strategy, all_candidates=all_candidates)
    if (safe_only or neutral_only or risk_only or core_only or c_only or c_swing_only or c_strict_only or c_pullback_only or g_only or h_only or h_watch_only or leader_gap_only or i_core_only) and selected_df is not None and not selected_df.empty:
        try:
            if i_core_only:
                if I_CORE_MAIN_ONLY:
                    core_mask = _bt_mask_i_core_main(selected_df)
                    selected_df = selected_df[core_mask].copy()
                    selected_df['core_filter'] = 'I_CORE_MAIN_ONLY'
                    log_info(f"I-CORE MAIN FILTER 전용 백테스트 필터 적용: {len(selected_df)}건")
                else:
                    core_mask = _bt_mask_i_core_all(selected_df)
                    selected_df = selected_df[core_mask].copy()
                    selected_df['core_filter'] = 'I_CORE_150_200_ONLY'
                    log_info(f"I-CORE 150/200 시세분출 전용 백테스트 필터 적용: {len(selected_df)}건")
            elif leader_gap_only:
                core_mask = _bt_mask_leader_gap_all(selected_df)
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'LEADER_GAP_WATCH_ONLY'
                log_info(f"대형주 리더갭 WATCH 전용 백테스트 필터 적용: {len(selected_df)}건")
            elif h_watch_only:
                core_mask = _bt_mask_h_watch_all(selected_df)
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'H_PULLBACK_WATCH_ONLY'
                log_info(f"H 눌림반등 WATCH 전용 백테스트 필터 적용: {len(selected_df)}건")
            elif h_only:
                core_mask = _bt_mask_h_all(selected_df)
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'H_HIGH_DRYUP_ONLY'
                log_info(f"H 신고가거자름 STRICT 전용 백테스트 필터 적용: {len(selected_df)}건")
            elif g_only:
                core_mask = _bt_mask_g_all(selected_df)
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'G_MORALES_ONLY'
                log_info(f"G 모랄레스갭 전용 백테스트 필터 적용: {len(selected_df)}건")
            elif c_pullback_only:
                core_mask = selected_df['mode'].astype(str).eq('C') & (pd.to_numeric(selected_df.get('ymgp_pullback_reentry', pd.Series(0, index=selected_df.index)), errors='coerce').fillna(0).astype(int) == 1)
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'C_PULLBACK_REENTRY_ONLY'
                log_info(f"C-SWING 눌림재상승형 전용 백테스트 필터 적용: {len(selected_df)}건")
            elif c_swing_only:
                core_mask = selected_df['mode'].astype(str).eq('C')
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'C_SWING_ONLY'
                log_info(f"C-SWING 역매공파 전용 백테스트 필터 적용: {len(selected_df)}건")
            elif c_strict_only:
                core_mask = selected_df['mode'].astype(str).eq('C') & selected_df.get('c_type', pd.Series('', index=selected_df.index)).astype(str).eq('strict')
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'C_STRICT_ONLY'
                log_info(f"C 엄격형 전용 백테스트 필터 적용: {len(selected_df)}건")
            elif c_only:
                core_mask = selected_df['mode'].astype(str).eq('C')
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'C_YMGP_ONLY'
                log_info(f"C 역매공파 전용 백테스트 필터 적용: {len(selected_df)}건")
            elif safe_only:
                core_mask = _bt_mask_s_core_safe(selected_df)
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'S-CORE_SAFE'
                log_info(f"S-CORE SAFE 전용 백테스트 필터 적용: {len(selected_df)}건")
            elif neutral_only:
                core_mask = _bt_mask_s_core_neutral(selected_df)
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'S-CORE_NEUTRAL'
                log_info(f"S-CORE NEUTRAL 전용 백테스트 필터 적용: {len(selected_df)}건")
            elif risk_only:
                core_mask = _bt_mask_s_core_risk(selected_df)
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'S-CORE_RISK'
                log_info(f"S-CORE RISK 전용 백테스트 필터 적용: {len(selected_df)}건")
            else:
                core_mask = _bt_mask_s_core(selected_df)
                selected_df = selected_df[core_mask].copy()
                selected_df['core_filter'] = 'S-CORE(S1우수응축+S2)'
                log_info(f"S-CORE 전용 백테스트 필터 적용: {len(selected_df)}건")
        except Exception as e:
            log_error(f"⚠️ 핵심군 필터 적용 실패: {e}")
    try:
        raw_df.to_csv(raw_path, index=False, encoding='utf-8-sig')
        selected_df.to_csv(selected_path, index=False, encoding='utf-8-sig')
    except Exception as e:
        log_error(f"⚠️ 백테스트 CSV 저장 실패: {e}")
    report = _build_backtest_summary(
        raw_df, selected_df, start_date, end_date, hold_days, top_per_strategy,
        all_candidates, weekly_breakdown=weekly_breakdown, diag=diag if debug else None, core_only=core_only, safe_only=safe_only, neutral_only=neutral_only, risk_only=risk_only, c_only=c_only, c_swing_only=c_swing_only, c_strict_only=c_strict_only, c_pullback_only=c_pullback_only, g_only=g_only, h_only=h_only, h_watch_only=h_watch_only, leader_gap_only=leader_gap_only,
        i_core_only=i_core_only,
    )
    detail_paths = _export_backtest_detail_artifacts(selected_df, stamp)
    if detail_paths:
        path_lines = ["", "[📁 백테스트 발생 종목 상세 파일]"]
        if detail_paths.get('detail_csv'):
            path_lines.append(f"- 전체 상세 CSV: {detail_paths.get('detail_csv')}")
        if detail_paths.get('detail_html'):
            path_lines.append(f"- 전체 상세 HTML: {detail_paths.get('detail_html')}")
        if detail_paths.get('i_main_csv'):
            path_lines.append(f"- I-MAIN 상세 CSV: {detail_paths.get('i_main_csv')}")
        if detail_paths.get('i_main_html'):
            path_lines.append(f"- I-MAIN 상세 HTML: {detail_paths.get('i_main_html')}")
        if detail_paths.get('stock_feature_csv'):
            path_lines.append(f"- 종목특성 요약 CSV: {detail_paths.get('stock_feature_csv')}")
        if detail_paths.get('stock_feature_html'):
            path_lines.append(f"- 종목특성 요약 HTML: {detail_paths.get('stock_feature_html')}")
        report = report + "\n" + "\n".join(path_lines)
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
# v4.3.9 COMPACT OPERATION SUMMARY
# =============================================================
def _v439_bool_env(name: str, default: str = '0') -> bool:
    return str(os.environ.get(name, default)).strip().lower() in ('1', 'true', 'yes', 'y', 'on')


def _v439_num_series(df: pd.DataFrame, col: str, default=0.0) -> pd.Series:
    try:
        if df is None or df.empty:
            return pd.Series(dtype=float)
        if col not in df.columns:
            return pd.Series(default, index=df.index, dtype=float)
        return pd.to_numeric(df[col], errors='coerce').fillna(default)
    except Exception:
        return pd.Series(default, index=df.index if df is not None else [], dtype=float)


def _v439_str_series(df: pd.DataFrame, col: str, default='') -> pd.Series:
    try:
        if df is None or df.empty:
            return pd.Series(dtype=str)
        if col not in df.columns:
            return pd.Series(default, index=df.index, dtype=str)
        return df[col].astype(str).fillna(default)
    except Exception:
        return pd.Series(default, index=df.index if df is not None else [], dtype=str)


def _v439_read_csv_safe(path: str) -> pd.DataFrame:
    try:
        if not path:
            return pd.DataFrame()
        p = Path(str(path))
        if not p.exists():
            return pd.DataFrame()
        return pd.read_csv(p, dtype={'code': str}, encoding='utf-8-sig')
    except Exception:
        try:
            return pd.read_csv(path, dtype={'code': str})
        except Exception:
            return pd.DataFrame()


def _v439_rate(sub: pd.DataFrame, col: str) -> float:
    try:
        if sub is None or sub.empty or col not in sub.columns:
            return float('nan')
        return pd.to_numeric(sub[col], errors='coerce').fillna(0).mean() * 100.0
    except Exception:
        return float('nan')


def _v439_mean(sub: pd.DataFrame, col: str) -> float:
    try:
        if sub is None or sub.empty or col not in sub.columns:
            return float('nan')
        return pd.to_numeric(sub[col], errors='coerce').mean()
    except Exception:
        return float('nan')


def _v439_fmt_pct(v, signed=False) -> str:
    try:
        if pd.isna(v):
            return '-'
        return (f"{float(v):+.2f}%" if signed else f"{float(v):.2f}%")
    except Exception:
        return '-'


def _v439_first_valid_value(row, cols: list[str], default=np.nan):
    """v4.3.9.1: 여러 후보 컬럼 중 실제 값이 있는 첫 값을 반환한다.
    DataFrame에 컬럼은 있지만 NaN인 경우 `초과 -`가 찍히는 문제를 막기 위함.
    """
    try:
        for c in cols:
            if c in row.index:
                v = row.get(c, np.nan)
                try:
                    if not pd.isna(v):
                        return v
                except Exception:
                    if str(v).strip() not in ('', '-', 'nan', 'None'):
                        return v
    except Exception:
        pass
    return default


def _v439_i_outlier_tags(row) -> list[str]:
    """v4.3.9.1: I-MAIN 샘플의 이상치/위험 샘플 태그.
    - +100% 이상: 실제 급등일 수도 있으나 액면분할/데이터 보정/이벤트 확인 필요
    - 20일 -15% 이하 또는 60일 -20% 이하: 중기 구조 실패/급락주의
    """
    if not _v439_bool_env('CLOSING_BET_COMPACT_OUTLIER_TAG', '1'):
        return []
    tags = []
    try:
        r20 = float(pd.to_numeric(pd.Series([row.get('i_ret_close_20d', np.nan)]), errors='coerce').iloc[0])
    except Exception:
        r20 = float('nan')
    try:
        r40 = float(pd.to_numeric(pd.Series([row.get('i_ret_close_40d', np.nan)]), errors='coerce').iloc[0])
    except Exception:
        r40 = float('nan')
    try:
        r60 = float(pd.to_numeric(pd.Series([row.get('i_ret_close_60d', np.nan)]), errors='coerce').iloc[0])
    except Exception:
        r60 = float('nan')

    if not pd.isna(r60) and r60 >= 100.0:
        tags.append('⚠️60d+100%↑ 데이터/이벤트확인')
    elif (not pd.isna(r40) and r40 >= 80.0) or (not pd.isna(r60) and r60 >= 70.0):
        tags.append('⚠️고수익이상치확인')
    if (not pd.isna(r20) and r20 <= -15.0) or (not pd.isna(r60) and r60 <= -20.0):
        tags.append('⚠️급락/구조실패주의')
    return tags


def _v439_i_class_priority_series(df: pd.DataFrame) -> pd.Series:
    """v4.3.9.1: 샘플 정렬용 I-MAIN 운용등급 우선순위.
    ACCEL(0) > CORE(1) > ADD/CONFIRM(2) > WATCH(3) > 기타(9)
    """
    try:
        idx = df.index if df is not None else []
        pri = pd.Series(9, index=idx, dtype=int)
        if df is None or df.empty:
            return pri
        accel = _v439_num_series(df, 'imain_accel', 0).astype(int).eq(1)
        core = _v439_num_series(df, 'imain_core', 0).astype(int).eq(1)
        cls = _v439_str_series(df, 'imain_primary_class', '')
        phase = _v439_str_series(df, 'i_phase', '')
        pri.loc[cls.str.contains('ACCEL', na=False) | accel] = 0
        pri.loc[(pri.eq(9)) & (cls.str.contains('CORE', na=False) | core)] = 1
        pri.loc[(pri.eq(9)) & (cls.str.contains('ADD|CONFIRM', regex=True, na=False) | phase.isin(['I-5', 'I-6']))] = 2
        pri.loc[(pri.eq(9)) & cls.str.contains('WATCH', na=False)] = 3
        return pri
    except Exception:
        return pd.Series(9, index=df.index if df is not None else [], dtype=int)


def _v439_priority_i_pool(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """v4.3.9.1: 성과상위 샘플은 ACCEL/CORE를 우선 노출한다.
    우선군이 부족하면 WATCH/CONFIRM 등으로 보충한다.
    """
    try:
        if df is None or df.empty:
            return pd.DataFrame()
        work = df.copy()
        work['_v439_class_priority'] = _v439_i_class_priority_series(work)
        # ACCEL/CORE 우선. 다만 표본이 부족하면 전체 MAIN 후보로 보충.
        if _v439_bool_env('CLOSING_BET_COMPACT_I_MAIN_PRIORITY_CLASS', '1'):
            primary = work[work['_v439_class_priority'].le(1)].copy()
            # v4.3.9.1 기본값: 성과상위 샘플에서는 WATCH를 제외하고 ACCEL/CORE만 노출.
            # ACCEL/CORE가 전혀 없을 때만 전체 MAIN 후보로 fallback.
            if not primary.empty:
                return primary.drop(columns=['_v439_class_priority'], errors='ignore')
        return work.drop(columns=['_v439_class_priority'], errors='ignore')
    except Exception:
        return df.copy() if df is not None else pd.DataFrame()


def _v439_short_trade_line(sub: pd.DataFrame, label: str) -> str:
    n = len(sub) if sub is not None else 0
    if n <= 0:
        return f"- {label}: 0건"
    pnl = _v439_mean(sub, 'rule35_pnl')
    win = _v439_rate(sub, 'rule35_win') if 'rule35_win' in sub.columns else _v439_rate(sub, 'close_win_hd')
    hit3 = _v439_rate(sub, 'hit3_before_stop') if 'hit3_before_stop' in sub.columns else _v439_rate(sub, 'rule35_hit3')
    hit5 = _v439_rate(sub, 'hit5_before_stop') if 'hit5_before_stop' in sub.columns else _v439_rate(sub, 'rule35_hit5')
    stop = _v439_rate(sub, 'stop_before_3') if 'stop_before_3' in sub.columns else _v439_rate(sub, 'rule35_stop')
    return f"- {label}: {n}건 | 3/5 {pnl:.2f}% | 승률 {win:.1f}% | +3선행 {hit3:.1f}% | +5선행 {hit5:.1f}% | 손절선행 {stop:.1f}%"


def _v439_i_line(sub: pd.DataFrame, label: str) -> str:
    n = len(sub) if sub is not None else 0
    if n <= 0:
        return f"- {label}: 0건"
    return (
        f"- {label}: {n}건 | "
        f"20d {_v439_mean(sub, 'i_ret_close_20d'):.2f}% | 40d {_v439_mean(sub, 'i_ret_close_40d'):.2f}% | 60d {_v439_mean(sub, 'i_ret_close_60d'):.2f}% | "
        f"+10 {_v439_rate(sub, 'i_hit10_60d'):.1f}% | +20 {_v439_rate(sub, 'i_hit20_60d'):.1f}% | +30 {_v439_rate(sub, 'i_hit30_60d'):.1f}% | "
        f"박스실패 {_v439_rate(sub, 'i_box_fail_close'):.1f}%"
    )




def _v44933_i_mean_cols(sub: pd.DataFrame, cols: list[str], cap_upper: float | None = None) -> list[float]:
    vals: list[float] = []
    try:
        if sub is None or sub.empty:
            return [float('nan') for _ in cols]
        for c in cols:
            if c not in sub.columns:
                vals.append(float('nan'))
                continue
            s = pd.to_numeric(sub[c], errors='coerce')
            if cap_upper is not None:
                s = s.clip(upper=float(cap_upper))
            vals.append(float(s.mean()))
    except Exception:
        vals = [float('nan') for _ in cols]
    return vals



def _v44935_horizon_value_text(sub: pd.DataFrame, col: str, horizon: int, hold_days: int = 0, cap_upper: float | None = None) -> str:
    """v4.4.9.35: hold_days보다 긴 중기 성과와 NaN을 사용자에게 nan%로 노출하지 않는다."""
    try:
        if hold_days and int(hold_days) > 0 and int(hold_days) < int(horizon):
            return '평가보류'
        if sub is None or sub.empty or col not in sub.columns:
            return '평가불가'
        ser = pd.to_numeric(sub[col], errors='coerce')
        if cap_upper is not None:
            ser = ser.clip(upper=float(cap_upper))
        v = float(ser.mean())
        if pd.isna(v) or np.isinf(v):
            return '평가불가'
        return f'{v:.2f}%'
    except Exception:
        return '평가불가'


def _v44935_i_horizon_line(sub: pd.DataFrame, label: str, hold_days: int = 0) -> str:
    """v4.4.9.35: I-MAIN 중기성과는 평가기간이 충분할 때만 숫자로 표시한다."""
    n = len(sub) if sub is not None else 0
    if n <= 0:
        return f"- {label}: 0건"
    h20 = _v44935_horizon_value_text(sub, 'i_ret_close_20d', 20, hold_days)
    h40 = _v44935_horizon_value_text(sub, 'i_ret_close_40d', 40, hold_days)
    h60 = _v44935_horizon_value_text(sub, 'i_ret_close_60d', 60, hold_days)
    if hold_days and int(hold_days) < 20:
        return f"- {label}: {n}건 | 보유평가 {hold_days}일 모드 → 20/40/60d 중기성과 평가보류 | 신규추격 금지·재지지 확인형"
    return (
        f"- {label}: {n}건 | 20d {h20} | 40d {h40} | 60d {h60} | "
        f"+10 {_v439_rate(sub, 'i_hit10_60d'):.1f}% | +20 {_v439_rate(sub, 'i_hit20_60d'):.1f}% | +30 {_v439_rate(sub, 'i_hit30_60d'):.1f}% | "
        f"박스실패 {_v439_rate(sub, 'i_box_fail_close'):.1f}%"
    )


def _v44935_i_mode_comment(hold_days: int = 0) -> str:
    try:
        hd = int(hold_days or 0)
    except Exception:
        hd = 0
    if hd and hd < 20:
        return f"- ⚠️ HORIZON GUARD: 보유평가 {hd}일 모드에서는 I-MAIN 20/40/60d 중기성과를 확정평가하지 않습니다. 최근구간은 신규추격보다 5MA/20MA 재지지 확인형으로 낮춥니다."
    if hd and hd < 40:
        return f"- ⚠️ HORIZON GUARD: 보유평가 {hd}일 모드라 40/60d는 평가보류입니다. 20d만 참고하고 중기 판단은 누적검증을 우선합니다."
    if hd and hd < 60:
        return f"- ⚠️ HORIZON GUARD: 보유평가 {hd}일 모드라 60d는 평가보류입니다. 20/40d와 재지지 여부만 참고합니다."
    return "- HORIZON GUARD: 평가기간이 충분한 구간만 숫자로 보고, NaN/미래봉 부족은 평가불가로 표시합니다."

def _v44933_i_adjusted_line(sub: pd.DataFrame, label: str, mode: str = 'raw', hold_days: int = 0) -> str:
    """v4.4.9.33: I-MAIN 성과가 +100% 이상 이상치로 과장되는지 확인하는 압축 라인."""
    try:
        if sub is None or sub.empty:
            return f"- {label}: 0건"
        work = sub.copy()
        if mode == 'exclude100' and 'i_ret_close_60d' in work.columns:
            r60 = pd.to_numeric(work['i_ret_close_60d'], errors='coerce')
            out_n = int(r60.gt(100).sum())
            work = work[~r60.gt(100)].copy()
            if work.empty:
                return f"- {label} +100%초과 제외: 0건 | 제외 {out_n}건"
            a20 = _v44935_horizon_value_text(work, 'i_ret_close_20d', 20, hold_days)
            a40 = _v44935_horizon_value_text(work, 'i_ret_close_40d', 40, hold_days)
            a60 = _v44935_horizon_value_text(work, 'i_ret_close_60d', 60, hold_days)
            return f"- {label} +100%초과 제외: {len(work)}건 | 20d {a20} | 40d {a40} | 60d {a60} | 제외 {out_n}건"
        if mode == 'cap100':
            a20 = _v44935_horizon_value_text(work, 'i_ret_close_20d', 20, hold_days, cap_upper=100.0)
            a40 = _v44935_horizon_value_text(work, 'i_ret_close_40d', 40, hold_days, cap_upper=100.0)
            a60 = _v44935_horizon_value_text(work, 'i_ret_close_60d', 60, hold_days, cap_upper=100.0)
            return f"- {label} +100%상한 보정: {len(work)}건 | 20d {a20} | 40d {a40} | 60d {a60}"
        if mode == 'excess':
            # 벤치마크는 BENCH 우선, 없으면 KOSDAQ, 없으면 일반 excess 컬럼을 사용한다.
            prefixes = [('bench', 'BENCH'), ('kosdaq', 'KOSDAQ'), ('', '초과')]
            for pref, shown in prefixes:
                if pref:
                    cols = [f'i_{pref}_excess_close_20d', f'i_{pref}_excess_close_40d', f'i_{pref}_excess_close_60d']
                else:
                    cols = ['i_excess_close_20d', 'i_excess_close_40d', 'i_excess_close_60d']
                if any(c in work.columns for c in cols):
                    a20 = _v44935_horizon_value_text(work, cols[0], 20, hold_days)
                    a40 = _v44935_horizon_value_text(work, cols[1], 40, hold_days)
                    a60 = _v44935_horizon_value_text(work, cols[2], 60, hold_days)
                    return f"- {label} 지수대비({shown}): {len(work)}건 | 20d {a20} | 40d {a40} | 60d {a60}"
            return f"- {label} 지수대비: 초과수익 컬럼 없음"
        return _v44935_i_horizon_line(work, label, hold_days)
    except Exception as e:
        return f"- {label} 보정 오류: {type(e).__name__}: {e}"


def _v44933_i_outlier_adjustment_lines(i_en: pd.DataFrame, hold_days: int = 0) -> list[str]:
    """v4.4.9.33: I-MAIN 원본/이상치제외/상한보정/초과수익 요약."""
    lines: list[str] = []
    try:
        if i_en is None or i_en.empty:
            return lines
        work = i_en.copy()
        i_main = work[_v439_num_series(work, 'imain_is_main', 0).astype(int).eq(1)]
        i_accel = work[_v439_num_series(work, 'imain_accel', 0).astype(int).eq(1)]
        i_core = work[_v439_num_series(work, 'imain_core', 0).astype(int).eq(1)]
        groups = [('I-MAIN MAIN', i_main), ('I-MAIN ACCEL', i_accel), ('I-MAIN CORE', i_core)]
        if not any(len(g) for _, g in groups):
            return lines
        lines.append('[🧯 I-MAIN HORIZON GUARD + 이상치 보정 — v4.4.9.43]')
        lines.append('- 목적: +100% 이상 이벤트/데이터성 급등이 평균을 과장하는지 원본·제외·상한·지수대비로 나눠 확인합니다.')
        lines.append(_v44935_i_mode_comment(hold_days))
        for label, sub in groups:
            if sub is None or sub.empty:
                continue
            lines.append(_v44933_i_adjusted_line(sub, label, 'raw', hold_days))
            lines.append(_v44933_i_adjusted_line(sub, label, 'exclude100', hold_days))
            lines.append(_v44933_i_adjusted_line(sub, label, 'cap100', hold_days))
            ex = _v44933_i_adjusted_line(sub, label, 'excess', hold_days)
            if '컬럼 없음' not in ex:
                lines.append(ex)
        lines.append('- 운용해석: 원본이 좋아도 보정 후 성과와 초과수익이 약하면 추격하지 않고 5MA/20MA 재지지 분할관찰로 낮춥니다.')
    except Exception as e:
        lines.append(f'[I-MAIN 이상치 보정 오류] {type(e).__name__}: {e}')
    return lines

def _v439_mask(fn_name: str, df: pd.DataFrame) -> pd.Series:
    try:
        fn = globals().get(fn_name)
        if callable(fn):
            m = fn(df)
            if isinstance(m, pd.Series):
                return m.fillna(False).astype(bool)
    except Exception:
        pass
    return pd.Series(False, index=df.index if df is not None else [])


def _v439_stock_feature_quick_lines(df: pd.DataFrame, min_n: int = 5) -> list[str]:
    lines = []
    try:
        if df is None or df.empty:
            return lines
        work = _stock_feature_bucket_columns(df) if callable(globals().get('_stock_feature_bucket_columns')) else df.copy()
        if work is None or work.empty or '_sf_amount_bucket' not in work.columns:
            return lines
        order = ['5000억+', '3000~5000억', '1000~3000억', '300~1000억', '100~300억', '<100억']
        rows = []
        for bucket in order:
            sub = work[work['_sf_amount_bucket'].astype(str).eq(bucket)]
            if len(sub) >= min_n:
                rows.append((bucket, len(sub), _v439_mean(sub, 'rule35_pnl'), _v439_rate(sub, 'rule35_win'), _v439_rate(sub, 'stop_before_3')))
        if rows:
            lines.append("[🧬 종목특성 핵심 — 거래대금/대표성]")
            for b, n, pnl, win, stop in rows[:6]:
                mark = '✅' if b in ('5000억+', '3000~5000억') else ('⚠️' if b in ('100~300억', '<100억') else '·')
                lines.append(f"{mark} {b}: {n}건 | 3/5 {pnl:.2f}% | 승률 {win:.1f}% | 손절 {stop:.1f}%")
            lines.append("- 해석: 시총보다 당일 거래대금·대표성·재료/대금 프록시가 더 중요합니다. 저유동성 A/B/C는 기본 강등합니다.")
    except Exception as e:
        lines.append(f"[종목특성 압축 오류] {type(e).__name__}: {e}")
    return lines


def _v439_dedupe_samples(df: pd.DataFrame, sort_col: str, ascending: bool, top_n: int, negative_only: bool = False) -> pd.DataFrame:
    try:
        if df is None or df.empty:
            return pd.DataFrame()
        work = df.copy()
        if negative_only:
            work = work[pd.to_numeric(work.get('i_ret_close_60d', np.nan), errors='coerce').fillna(999).lt(0)].copy()
        if work.empty:
            return work
        if sort_col not in work.columns:
            sort_col = 'signal_date' if 'signal_date' in work.columns else work.columns[0]
        work['_v439_sort'] = pd.to_numeric(work[sort_col], errors='coerce')
        if work['_v439_sort'].isna().all():
            work['_v439_sort'] = work[sort_col].astype(str)
        if 'code' in work.columns:
            work['code'] = work['code'].astype(str).str.zfill(6)
        work = work.sort_values('_v439_sort', ascending=ascending).drop_duplicates(subset=['code'], keep='first')
        return work.head(int(top_n)).drop(columns=['_v439_sort'], errors='ignore')
    except Exception:
        return pd.DataFrame()


def _v439_format_i_sample_rows(sub: pd.DataFrame, title: str) -> list[str]:
    lines = [title]
    if sub is None or sub.empty:
        lines.append("- 해당 샘플 없음")
        return lines
    for _, r in sub.iterrows():
        code = str(r.get('code', '')).zfill(6)
        name = _clean_stock_name(code, r.get('name', '')) if callable(globals().get('_clean_stock_name')) else str(r.get('name', code))
        dt = str(r.get('signal_date', ''))[:10]
        cls = str(r.get('imain_primary_class', r.get('mode_label', 'I-MAIN')))
        phase = str(r.get('i_phase', ''))
        ret20 = _v439_fmt_pct(r.get('i_ret_close_20d', np.nan), signed=True)
        ret40 = _v439_fmt_pct(r.get('i_ret_close_40d', np.nan), signed=True)
        ret60 = _v439_fmt_pct(r.get('i_ret_close_60d', np.nan), signed=True)
        mat = _safe_int(r.get('i_material_proxy_score', 0), 0) if callable(globals().get('_safe_int')) else int(float(r.get('i_material_proxy_score', 0) or 0))
        excess_val = _v439_first_valid_value(r, ['i_bench_excess_close_60d', 'i_kosdaq_excess_close_60d', 'i_excess_close_60d'], default=np.nan)
        excess = _v439_fmt_pct(excess_val, signed=True)
        extra_parts = []
        if not (_v439_bool_env('CLOSING_BET_COMPACT_HIDE_MISSING_EXCESS', '1') and excess == '-'):
            extra_parts.append(f"초과 {excess}")
        tags = _v439_i_outlier_tags(r)
        # v4.3.9.2: 최근발생 샘플이 음수면 추격 금지/재지지 확인 태그를 따로 표시한다.
        try:
            if '최근발생' in str(title):
                _r20 = float(pd.to_numeric(pd.Series([r.get('i_ret_close_20d', np.nan)]), errors='coerce').iloc[0])
                _r40 = float(pd.to_numeric(pd.Series([r.get('i_ret_close_40d', np.nan)]), errors='coerce').iloc[0])
                _r60 = float(pd.to_numeric(pd.Series([r.get('i_ret_close_60d', np.nan)]), errors='coerce').iloc[0])
                if (not pd.isna(_r20) and _r20 < 0) or (not pd.isna(_r40) and _r40 < 0) or (not pd.isna(_r60) and _r60 < 0):
                    if not any('최근발생 음수' in t for t in tags):
                        tags.append('⚠️최근발생 음수: 추격금지/재지지확인')
        except Exception:
            pass
        extra_parts.extend(tags)
        extra = (" | " + " | ".join(extra_parts)) if extra_parts else ""
        lines.append(f"- {dt} {name}({code}) | {cls} {phase} | 20/40/60 {ret20}/{ret40}/{ret60} | 재료대금 {mat}{extra}")
    return lines


def _v439_i_samples_section(df: pd.DataFrame, top_n: int = 5) -> list[str]:
    lines = []
    try:
        i = df[_v439_mask('_bt_mask_i_core_all', df)].copy()
        if i.empty:
            return lines
        i = _i_main_enriched_df(i) if callable(globals().get('_i_main_enriched_df')) else i
        i = i[_v439_num_series(i, 'imain_is_main', 0).astype(int).eq(1) | _v439_num_series(i, 'imain_core', 0).astype(int).eq(1) | _v439_num_series(i, 'imain_accel', 0).astype(int).eq(1)]
        if i.empty:
            return lines
        lines.append("[📋 I-MAIN 발생 샘플 — 종목별 중복제거 v4.4.9.43]")
        priority_pool = _v439_priority_i_pool(i, top_n)
        best = _v439_dedupe_samples(priority_pool, 'i_ret_close_60d', False, top_n)
        recent = _v439_dedupe_samples(i, 'signal_date', False, top_n)
        weak = _v439_dedupe_samples(i, 'i_ret_close_60d', True, min(3, top_n), negative_only=True)
        lines += _v439_format_i_sample_rows(best, "성과상위: ACCEL/CORE 우선 · 종목별 최고 1개")
        lines += _v439_format_i_sample_rows(recent, "최근발생: 종목별 최근 1개")
        if not weak.empty:
            lines += _v439_format_i_sample_rows(weak, "음수/실패 샘플: 종목별 대표 1개")
    except Exception as e:
        lines.append(f"[I-MAIN 샘플 압축 오류] {type(e).__name__}: {e}")
    return lines






def _v445_mean_positive_day(sub: pd.DataFrame, col: str) -> float:
    try:
        ser = pd.to_numeric(sub.get(col), errors='coerce')
        ser = ser[ser > 0]
        return float(ser.mean()) if len(ser) else np.nan
    except Exception:
        return np.nan


def _v445_path_profile_line(sub: pd.DataFrame, label: str, entry_tip: str, normal: str, danger: str) -> str:
    """v4.4.6: 흐름 프로파일 보정.
    - path_min_low_ret: 평가기간 전체 최대흔들림으로 표기
    - path_first3d_min_low_ret: 1~3일 첫흔들림
    - path_pre_plus3_min_low_ret: +3 도달 전 최대흔들림
    """
    n = len(sub) if sub is not None else 0
    if n <= 0:
        return f"- {label}: 0건"
    path = _v439_str_series(sub, 'path_type', '')
    try:
        top_path = path.value_counts().index[0] if len(path.value_counts()) else '확인필요'
    except Exception:
        top_path = '확인필요'

    first3 = _v439_mean(sub, 'path_first3d_min_low_ret') if 'path_first3d_min_low_ret' in sub.columns else _v439_mean(sub, 'ret_next_low')
    pre3 = _v439_mean(sub, 'path_pre_plus3_min_low_ret') if 'path_pre_plus3_min_low_ret' in sub.columns else np.nan
    pre5 = _v439_mean(sub, 'path_pre_plus5_min_low_ret') if 'path_pre_plus5_min_low_ret' in sub.columns else np.nan
    maxshake = _v439_mean(sub, 'path_min_low_ret') if 'path_min_low_ret' in sub.columns else _v439_mean(sub, 'ret_min_low_hd')
    d3 = _v445_mean_positive_day(sub, 'path_first_plus3_day')
    d5 = _v445_mean_positive_day(sub, 'path_first_plus5_day')
    shake = 0.0
    try:
        pre3_ser = pd.to_numeric(sub.get('path_pre_plus3_min_low_ret', sub.get('path_min_low_ret')), errors='coerce').fillna(0)
        shake = ((path.eq('개미털기후상승형')) | (pre3_ser.le(-3) & pd.to_numeric(sub.get('hit3_before_stop'), errors='coerce').fillna(0).eq(1))).mean() * 100.0
    except Exception:
        pass
    stop = _v439_rate(sub, 'stop_before_3') if 'stop_before_3' in sub.columns else _v439_rate(sub, 'rule35_stop')
    d3_txt = f"{d3:.1f}일" if not pd.isna(d3) else "-"
    d5_txt = f"{d5:.1f}일" if not pd.isna(d5) else "-"

    def _fmt(v):
        return f"{v:.2f}%" if not pd.isna(v) else "-"

    # 데이터 기반 멘탈 기준. 기존 고정문구(-1~-3%)가 너무 좁게 느껴지는 문제를 보정한다.
    ref = first3 if not pd.isna(first3) else pre3
    if pd.isna(ref):
        mental = normal
    elif ref <= -7:
        mental = "초반 -5~-8% 흔들림 가능. 단, 갭하단/전일종가 종가유지와 거래량 감소가 핵심"
    elif ref <= -5:
        mental = "-3~-6% 개미털기 가능. 지지선 유지 시 정상범위, 장대음봉은 경계"
    elif ref <= -3:
        mental = "-1~-3%는 가벼운 흔들림, -3~-5%는 지지선 확인 구간"
    else:
        mental = "-1~-3% 가벼운 흔들림 중심. 지지선 이탈만 아니면 과민반응 금지"

    return (
        f"- {label}: 대표흐름 {top_path} | 1~3일첫흔들림 {_fmt(first3)} | +3전흔들림 {_fmt(pre3)} | "
        f"전체최대흔들림 {_fmt(maxshake)} | +3평균 {d3_txt} | +5평균 {d5_txt} | "
        f"개미털기 {shake:.1f}% | 손절선행 {stop:.1f}%\n"
        f"  ↳ 추천타점: {entry_tip}\n"
        f"  ↳ 멘탈기준: {mental}\n"
        f"  ↳ 위험신호: {danger}"
    )



def _v447_mental_summary_lines() -> list[str]:
    """v4.4.7: 실시간/백테스트 텔레그램에 항상 붙이는 패턴별 멘탈 요약."""
    return [
        "[🧠 패턴별 멘탈 요약 — 매번 확인]",
        "- LP-D23: 핵심 타점. 갭후 2~3일 식힘 뒤 재지지 구간이지만, 갭 6~12%·5000억+·갭하단/전일종가 지지가 붙을 때 강하게 봅니다. 전일종가·5/10일선 동시 이탈 전에는 과민반응 금지.",
        "- LP-D45: 안정 타점. 충분히 식혀 손절은 낮지만 +5까지 시간이 걸릴 수 있음. 조용한 횡보·거래량 감소는 정상 범위.",
        "- LP-D1: 공격 타점. 빠르지만 -3~-6% 흔들림 가능성이 커서 소액만. 갭하단 이탈 시 빠르게 제외.",
        "- L 당일: 강하지만 추격주의. 당일보다 다음날 갭하단/전일종가 지지 확인이 더 안전.",
        "- S2/S-NEUTRAL: 기다림 필요. 2~7일 비빌 수 있고 거래량 마른 횡보는 정상, 거래량 증가 장대음봉은 위험.",
        "- IT-ACCEL/I-MAIN: 단기 몰빵 금지. 20/40/60일 중기 분할 관점, 5MA/20MA 재지지 확인 후 비중 조절.",
        "- 공통 멘탈 기준: +3은 적극 1차 익절, +5는 절반 이하 연장. 지지선 종가이탈+거래량 증가 음봉은 정상 흔들림이 아니라 위험신호.",
    ]

def _v445_pattern_path_profile_lines(df: pd.DataFrame) -> list[str]:
    """v4.4.6: 패턴별 타점 이후 흐름을 텔레그램 백테스트 요약에 압축 표시."""
    lines = []
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            return lines
        mode = _v439_str_series(df, 'mode')
        lines.append('[🧭 패턴별 타점 이후 흐름 프로파일 — v4.4.9.34 보정판]')
        lp = df[mode.eq('LP')].copy()
        if not lp.empty:
            if 'lp_class' not in lp.columns:
                lp['lp_class'] = lp.apply(lambda r: _classify_lp_candidate(r.to_dict()).get('lp_class', 'LP-WATCH'), axis=1)
            lp_safe = lp[_v439_str_series(lp, 'lp_class').eq('LP-SAFE')]
            d1 = lp_safe[_v439_num_series(lp_safe, 'lp_days_since_gap', 0).eq(1)]
            d23 = lp_safe[_v439_num_series(lp_safe, 'lp_days_since_gap', 0).between(2, 3)]
            d45 = lp_safe[_v439_num_series(lp_safe, 'lp_days_since_gap', 0).between(4, 5)]
            lines.append(_v445_path_profile_line(lp_safe, 'LP-SAFE 전체', '갭후 2~5일 식힘 후 갭하단/전일종가 재지지', '-1~-3%는 가벼운 흔들림, -3~-6%는 지지선 확인', '갭하단 종가이탈+거래량 증가 음봉'))
            if not d1.empty:
                lines.append(_v445_path_profile_line(d1, 'LP-D1 공격타점', '갭후 1일 빠른 재지지, 소액만', '-1~-3%는 가벼운 흔들림, -3~-6%는 소액만 감내', '갭하단 이탈 시 빠르게 제외'))
            if not d23.empty:
                lines.append(_v445_path_profile_line(d23, 'LP-D23 핵심타점', '갭후 2~3일 식힘 후 재지지', '첫흔들림보다 +3전흔들림과 양봉전환 확인', '전일종가·5/10일선 동시 이탈'))
            if not d45.empty:
                lines.append(_v445_path_profile_line(d45, 'LP-D45 안정타점', '갭후 4~5일 충분히 식힌 뒤 재상승', '횡보·거래량 감소는 정상, 급락은 지지선 확인', '식혔는데도 갭하단 이탈하면 실패'))
        l = df[_v439_mask('_bt_mask_leader_gap_all', df) | mode.eq('L')]
        if not l.empty:
            lines.append(_v445_path_profile_line(l, 'L 리더갭 당일', '당일 추격보다 다음날 갭하단/전일종가 지지 확인', '갭하단 테스트는 정상 가능', '갭하단·전일종가 동시 이탈'))
        s = df[_v439_mask('_bt_mask_s_core_neutral', df) | _bt_mask_s2(df)] if callable(globals().get('_bt_mask_s2')) else df[mode.eq('S')]
        if not s.empty:
            lines.append(_v445_path_profile_line(s, 'S2/S-NEUTRAL', '응축 유지 중 소액, 2~5일 횡보 감안', '거래량 마른 횡보는 정상', '거래량 증가 장대음봉'))
        it = df[mode.eq('IT')]
        if not it.empty:
            it_en = _i_main_enriched_df(it) if callable(globals().get('_i_main_enriched_df')) else it
            it_accel = it_en[_v439_str_series(it_en, 'i_trigger_class').eq('ACCEL')]
            if not it_accel.empty:
                lines.append(_v445_path_profile_line(it_accel, 'IT-ACCEL', '단기몰빵 금지, 5MA/20MA 재지지 때 분할', '-3~-5% 흔들림도 구조 유지 시 정상 가능', '50MA 이탈·박스하단 이탈'))
        lines.append('- 해석: v4.4.9.34는 1~3일 첫흔들림, +3전흔들림, 전체최대흔들림을 분리합니다. 전체최대흔들림은 목표 도달 후 흔들림까지 포함될 수 있으므로 실전 판단은 +3전흔들림과 지지선 종가유지를 우선 봅니다.')
    except Exception as e:
        lines.append(f'[패턴 흐름 프로파일 오류] {type(e).__name__}: {e}')
    return lines


def _v442_new_pattern_performance_lines(df: pd.DataFrame) -> list[str]:
    """v4.4.6: 신규 검색식 LP/SLOCK/IT 성과를 텔레그램 요약 안에서 바로 확인한다.
    LP는 v4.4.2 결과가 강했으므로 SAFE/WATCH/RISK와 타점 발생일별로 추가 분해한다.
    """
    lines = []
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            return lines
        work = df.copy()
        mode = _v439_str_series(work, 'mode')
        lp = work[mode.eq('LP')].copy()
        slock = work[mode.eq('SLOCK')].copy()
        it = work[mode.eq('IT')].copy()
        lines.append('[🧪 신규 검색식 성과검증 — LP CORE/SLOCK/IT]')
        if lp.empty and slock.empty and it.empty:
            lines.append('- 이번 백테스트 선택군에는 LP/SLOCK/IT 신규 검색식 검증 대상이 없습니다. 실시간 후보 섹션에서 해당 종목 없음이면 정상입니다.')
            return lines

        if not lp.empty:
            if 'lp_class' not in lp.columns:
                def _row_lp_class(r):
                    try:
                        return _classify_lp_candidate(r.to_dict()).get('lp_class', 'LP-WATCH')
                    except Exception:
                        return 'LP-WATCH'
                lp['lp_class'] = lp.apply(_row_lp_class, axis=1)
            lines.append(_v439_short_trade_line(lp, 'LP 리더갭 눌림재지지 전체'))
            lp_safe = lp[_v439_str_series(lp, 'lp_class').eq('LP-SAFE')]
            lp_watch = lp[_v439_str_series(lp, 'lp_class').eq('LP-WATCH')]
            lp_risk = lp[_v439_str_series(lp, 'lp_class').eq('LP-RISK')]
            if not lp_safe.empty:
                lines.append(_v439_short_trade_line(lp_safe, '🥇 LP-SAFE 핵심운용: 갭봉5000억+·갭3~12·재지지'))
                lp_fast = lp_safe[_v439_num_series(lp_safe, 'lp_fast_take_profit', 0).eq(1)]
                if not lp_fast.empty:
                    lines.append(_v439_short_trade_line(lp_fast, '⚡ LP-FAST +3/+5 빠른익절형'))
                d1 = lp_safe[_v439_num_series(lp_safe, 'lp_days_since_gap', 0).eq(1)]
                d23 = lp_safe[_v439_num_series(lp_safe, 'lp_days_since_gap', 0).between(2, 3)]
                d45 = lp_safe[_v439_num_series(lp_safe, 'lp_days_since_gap', 0).between(4, 5)]
                if not d1.empty:
                    lines.append(_v439_short_trade_line(d1, 'LP-SAFE 갭후 1일 타점'))
                if not d23.empty:
                    lines.append(_v439_short_trade_line(d23, 'LP-SAFE 갭후 2~3일 타점'))
                if not d45.empty:
                    lines.append(_v439_short_trade_line(d45, 'LP-SAFE 갭후 4~5일 타점'))
            else:
                lines.append('- LP-SAFE 핵심운용: 검증 대상 없음')
            if not lp_watch.empty:
                lines.append(_v439_short_trade_line(lp_watch, '🔁 LP-WATCH 관찰/승격대기'))
            if not lp_risk.empty:
                lines.append(_v439_short_trade_line(lp_risk, '⚠️ LP-RISK 기본제외'))
        else:
            lines.append('- LP 리더갭 눌림재지지: 검증 대상 없음')

        if not slock.empty:
            lines.append(_v439_short_trade_line(slock, 'SLOCK S2 상단잠김형'))
        else:
            lines.append('- SLOCK S2 상단잠김형: 검증 대상 없음')

        if not it.empty:
            it_en = _i_main_enriched_df(it) if callable(globals().get('_i_main_enriched_df')) else it
            lines.append(_v439_i_line(it_en, 'IT I-MAIN 촉발형'))
            it_accel = it_en[_v439_str_series(it_en, 'i_trigger_class').eq('ACCEL')]
            it_core = it_en[_v439_str_series(it_en, 'i_trigger_class').eq('CORE')]
            if not it_accel.empty:
                lines.append(_v439_i_line(it_accel, 'IT-ACCEL 촉발형'))
            if not it_core.empty:
                lines.append(_v439_i_line(it_core, 'IT-CORE 촉발형'))
        else:
            lines.append('- IT I-MAIN 촉발형: 검증 대상 없음')
        lines.append('- 해석: v4.4.9.13부터 LP는 단기 최우선 운용 후보로 보되, D1/D23/D45 타점과 다음날 대응 시나리오를 함께 확인합니다. LP-SAFE는 실행 후보, LP-WATCH는 다음날 양봉·전일고가 회복·거래대금 유지 시 승격, LP-RISK는 제외입니다. SLOCK은 실시간 기본 숨김, IT는 ACCEL 중심으로만 봅니다.')
    except Exception as e:
        lines.append(f'[신규 검색식 성과검증 오류] {type(e).__name__}: {e}')
    return lines


def _v448_lp_d23_validation_lines(df: pd.DataFrame) -> list[str]:
    """v4.4.8: LP-D23 과최적화 확인용 월/대금/갭 구간 압축 검증."""
    lines = []
    try:
        if df is None or df.empty or 'mode' not in df.columns:
            return lines
        mode = _v439_str_series(df, 'mode')
        lp = df[mode.eq('LP')].copy()
        if lp.empty:
            return lines
        if 'lp_class' not in lp.columns:
            lp['lp_class'] = lp.apply(lambda r: _classify_lp_candidate(r.to_dict()).get('lp_class', 'LP-WATCH'), axis=1)
        d23 = lp[_v439_str_series(lp, 'lp_class').eq('LP-SAFE') & _v439_num_series(lp, 'lp_days_since_gap', 0).between(2, 3)].copy()
        if d23.empty:
            return lines
        lines.append('[🔎 LP-D23 과최적화 체크 — 월/대금/갭 구간]')
        lines.append(_v439_short_trade_line(d23, 'LP-D23 전체'))
        # 최근 월별 분포: 표본이 특정 한 달에 몰렸는지 확인한다.
        if 'signal_date' in d23.columns:
            tmp = d23.copy()
            tmp['_month'] = pd.to_datetime(tmp['signal_date'], errors='coerce').dt.strftime('%Y-%m')
            month_parts = []
            for m, g in tmp.dropna(subset=['_month']).groupby('_month', sort=True):
                n = len(g)
                pnl = _v439_mean(g, 'rule35_pnl')
                stop = _v439_rate(g, 'stop_before_3') if 'stop_before_3' in g.columns else _v439_rate(g, 'rule35_stop')
                month_parts.append(f"{m}:{n}건/{pnl:.2f}%/손{stop:.0f}%")
            if month_parts:
                lines.append('- 월별 분포: ' + ' · '.join(month_parts[-8:]))
        amt = _v439_num_series(d23, 'lp_gap_amount_b', np.nan)
        if amt.isna().all():
            amt = _v439_num_series(d23, 'amount_b', np.nan)
        gap = _v439_num_series(d23, 'lp_gap_pct', np.nan)
        if gap.isna().all():
            gap = _v439_num_series(d23, 'gap_pct', np.nan)
        lines.append(_v439_short_trade_line(d23[amt.between(5000, 10000, inclusive='left')], 'LP-D23 갭봉대금 5000억~1조'))
        lines.append(_v439_short_trade_line(d23[amt.ge(10000)], 'LP-D23 갭봉대금 1조+'))
        lines.append(_v439_short_trade_line(d23[gap.between(3, 6, inclusive='left')], 'LP-D23 갭 3~6%'))
        lines.append(_v439_short_trade_line(d23[gap.between(6, 12, inclusive='both')], 'LP-D23 갭 6~12%'))
        # 시장국면 컬럼이 있으면 같이 분해한다. 없으면 다음 버전에서 지수 기반 국면을 더 정밀 추가한다.
        regime_col = None
        for c in ['market_regime', 'kospi_regime', 'regime', 'i_market_regime']:
            if c in d23.columns:
                regime_col = c; break
        if regime_col:
            parts = []
            for rg, g in d23.groupby(regime_col):
                rg = str(rg)[:12]
                parts.append(f"{rg}:{len(g)}건/{_v439_mean(g,'rule35_pnl'):.2f}%/손{(_v439_rate(g,'stop_before_3') if 'stop_before_3' in g.columns else _v439_rate(g,'rule35_stop')):.0f}%")
            if parts:
                lines.append('- 시장국면: ' + ' · '.join(parts[:6]))
        else:
            lines.append('- 시장국면: 현재 결과 CSV에 국면 컬럼 없음 → 다음 단계에서 KOSPI 20/60/120일 기준 자동분류 추가 가능')
        lines.append('- 해석: LP-D23은 표본이 아직 작으므로 월별/대금/갭 구간이 한쪽에 몰리는지 확인합니다. 손절 0%라도 과신하지 않고 반복검증이 필요합니다.')
    except Exception as e:
        lines.append(f'[LP-D23 검증 오류] {type(e).__name__}: {e}')
    return lines



# =============================================================
# v4.4.9.13 LIVE OPERATION GUARD — A/C/H/B 승격검증
# =============================================================
def _v449_safe_contains_series(df: pd.DataFrame, cols: list[str], pattern: str) -> pd.Series:
    try:
        if df is None or df.empty:
            return pd.Series(False, index=[])
        idx = df.index
        acc = pd.Series('', index=idx, dtype=str)
        for c in cols:
            if c in df.columns:
                acc = acc.str.cat(df[c].astype(str).fillna(''), sep=' ')
        return acc.str.contains(pattern, case=False, regex=True, na=False)
    except Exception:
        return pd.Series(False, index=df.index if df is not None else [])


def _v449_base_liquidity_series(df: pd.DataFrame) -> pd.Series:
    """튜닝용 거래대금: amount_b 우선, 없으면 유사 컬럼 fallback."""
    for c in ['amount_b', 'entry_amount_b', 'breakout_amount_b', 'today_amount_b', '거래대금_억']:
        if df is not None and c in df.columns:
            s = pd.to_numeric(df[c], errors='coerce')
            if s.notna().sum() > 0:
                return s.fillna(0)
    return _v439_num_series(df, 'amount_b', 0)


def _v449_price_series_any(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    """v4.4.9.8: OHLC 유사 컬럼을 여러 이름에서 찾아 숫자 Series로 반환."""
    try:
        if df is None or df.empty:
            return pd.Series(dtype=float)
        for c in candidates:
            if c in df.columns:
                s = pd.to_numeric(df[c], errors='coerce')
                if s.notna().sum() > 0:
                    return s
    except Exception:
        pass
    return pd.Series(np.nan, index=df.index if df is not None else [], dtype=float)


def _v449_close_loc_fallback_series(df: pd.DataFrame) -> pd.Series:
    """
    v4.4.9.8 CLOSE LOC FALLBACK.
    A/C/B 튜닝 백테스트에서 close_loc_pct가 일부 패턴에만 저장되어 0건이 되는 문제를 보완한다.
    우선순위:
      1) 기존 close_loc_pct/유사 컬럼
      2) _high/_low/_close 또는 high/low/close 계열로 직접 계산
      3) high/low가 없으면 open/close만으로 양봉/음봉 위치를 거친 프록시로 계산
    """
    try:
        if df is None or df.empty:
            return pd.Series(dtype=float)
        idx = df.index
        base = pd.Series(np.nan, index=idx, dtype=float)
        for c in ['close_loc_pct', 'close_location_pct', 'close_loc', '종가위치']:
            if c in df.columns:
                s = pd.to_numeric(df[c], errors='coerce')
                # 0은 실제 종가저가일 수도 있지만, 기존 CSV에서 누락값이 0으로 채워진 경우가 많아 fallback 대상으로 둔다.
                if s.notna().sum() > 0:
                    base = s
                    break

        close = _v449_price_series_any(df, ['_close', 'close', 'Close', '종가', 'entry_close', 'today_close', 'current_price', 'price', '현재가'])
        high = _v449_price_series_any(df, ['_high', 'high', 'High', '고가', 'entry_high', 'today_high'])
        low = _v449_price_series_any(df, ['_low', 'low', 'Low', '저가', 'entry_low', 'today_low'])
        open_p = _v449_price_series_any(df, ['_open', 'open', 'Open', '시가', 'entry_open', 'today_open'])

        calc = pd.Series(np.nan, index=idx, dtype=float)
        try:
            rng = (high - low)
            valid = close.gt(0) & high.gt(0) & low.gt(0) & rng.gt(0)
            calc.loc[valid] = ((close.loc[valid] - low.loc[valid]) / rng.loc[valid] * 100.0).clip(lower=0, upper=100)
        except Exception:
            pass

        # high/low가 없는 일부 행은 open/close만으로 최소한 종가강도 프록시를 만든다.
        try:
            proxy_valid = calc.isna() & close.gt(0) & open_p.gt(0)
            # 양봉이면 70, 보합이면 50, 음봉이면 30 정도의 프록시. 정밀평가가 아니라 0건 방지 진단용 fallback.
            proxy = pd.Series(np.nan, index=idx, dtype=float)
            proxy.loc[proxy_valid & close.gt(open_p)] = 70.0
            proxy.loc[proxy_valid & close.eq(open_p)] = 50.0
            proxy.loc[proxy_valid & close.lt(open_p)] = 30.0
            calc = calc.fillna(proxy)
        except Exception:
            pass

        # 기존값이 NaN이거나 0 이하인데 계산값이 있으면 계산값으로 대체.
        out = base.copy()
        replace_mask = (out.isna() | out.le(0)) & calc.notna()
        out.loc[replace_mask] = calc.loc[replace_mask]
        return pd.to_numeric(out, errors='coerce').fillna(0.0).clip(lower=0, upper=100)
    except Exception:
        return pd.Series(0, index=df.index if df is not None else [], dtype=float)


def _v449_close_loc_series(df: pd.DataFrame) -> pd.Series:
    return _v449_close_loc_fallback_series(df)


def _v449_vol_ratio_series(df: pd.DataFrame) -> pd.Series:
    for c in ['vol_ratio', 'volume_ratio', 'vol20_ratio', 'vma20_ratio', '거래량비']:
        if df is not None and c in df.columns:
            s = pd.to_numeric(df[c], errors='coerce')
            if s.notna().sum() > 0:
                return s.fillna(1.0)
    return pd.Series(1.0, index=df.index if df is not None else [], dtype=float)


def _v449_rr_series(df: pd.DataFrame) -> pd.Series:
    for c in ['rr', 'RR', 'risk_reward', 'rr_ratio']:
        if df is not None and c in df.columns:
            s = pd.to_numeric(df[c], errors='coerce')
            if s.notna().sum() > 0:
                return s.fillna(1.0)
    return pd.Series(1.0, index=df.index if df is not None else [], dtype=float)


def _v449_mode_series(df: pd.DataFrame) -> pd.Series:
    return _v439_str_series(df, 'mode', '')


def _v449_mask_a_retest(df: pd.DataFrame) -> pd.Series:
    """A-RETEST 1차 튜닝 프록시.
    A 전체를 바로 쓰지 않고, 고거래대금 + 종가위치 + 과열/RR 과도 회피 조건으로 좁힌다.
    실제 '다음날 전일고가 회복'은 실시간 승격 조건으로 운용하고, 백테스트에서는 이 프록시로 압축 검증한다.
    """
    if df is None or df.empty:
        return pd.Series(False, index=[])
    mode = _v449_mode_series(df)
    amount = _v449_base_liquidity_series(df)
    close_loc = _v449_close_loc_series(df)
    vol = _v449_vol_ratio_series(df)
    rr = _v449_rr_series(df)
    risk_txt = _v449_safe_contains_series(df, ['risk_tags', 'tags', 'reason', 'final_reason', 'comment'], r'RISK|위험|과열|종가위치약함|RR낮음|RR과대')
    return mode.eq('A') & amount.ge(5000) & close_loc.ge(60) & vol.le(2.5) & rr.between(0.6, 2.2) & (~risk_txt)


def _v449_mask_a_retest_safe(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(False, index=[])
    amount = _v449_base_liquidity_series(df)
    close_loc = _v449_close_loc_series(df)
    vol = _v449_vol_ratio_series(df)
    rr = _v449_rr_series(df)
    return _v449_mask_a_retest(df) & amount.ge(5000) & close_loc.ge(70) & vol.le(1.8) & rr.between(0.8, 1.8)


def _v449_mask_c_reclaim(df: pd.DataFrame) -> pd.Series:
    """C-RECLAIM/C-SWING 1차 튜닝 프록시.
    넓은 C 전체가 아니라 거래대금·종가위치·재지지/회복 힌트가 있는 후보만 본다.
    """
    if df is None or df.empty:
        return pd.Series(False, index=[])
    mode = _v449_mode_series(df)
    amount = _v449_base_liquidity_series(df)
    close_loc = _v449_close_loc_series(df)
    vol = _v449_vol_ratio_series(df)
    reclaim_hint = _v449_safe_contains_series(df, ['c_subtype', 'tags', 'reason', 'final_reason', 'comment', 'label'], r'재지지|재상승|회복|reclaim|눌림|안착|5MA|20MA')
    # 힌트 컬럼이 없는 CSV에서도 최소한 고거래대금 C는 튜닝 후보로 분해한다.
    return mode.eq('C') & amount.ge(3000) & close_loc.ge(55) & vol.le(2.5) & (reclaim_hint | amount.ge(5000))


def _v449_mask_c_reclaim_safe(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(False, index=[])
    amount = _v449_base_liquidity_series(df)
    close_loc = _v449_close_loc_series(df)
    return _v449_mask_c_reclaim(df) & amount.ge(5000) & close_loc.ge(65)


def _v449_mask_h_core_tuning(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(False, index=[])
    return (
        _v439_mask('_bt_mask_h_core_500_1000_vol23', df)
        | _v439_mask('_bt_mask_h_core_300_500_vol35', df)
        | _v439_mask('_bt_mask_h_v427_core_union', df)
        | _v439_mask('_bt_mask_h_triangle_safe', df)
    )


def _v449_mask_b_confirm_proxy(df: pd.DataFrame) -> pd.Series:
    """B1/B2 확인형 1차 프록시.
    B 자체 즉시매수는 제외하고, 거래대금·종가위치·거래량 과열 방지가 붙는 경우만 검증한다.
    """
    if df is None or df.empty:
        return pd.Series(False, index=[])
    mode = _v449_mode_series(df)
    amount = _v449_base_liquidity_series(df)
    close_loc = _v449_close_loc_series(df)
    vol = _v449_vol_ratio_series(df)
    bmode = mode.str.contains(r'B1|B2|B', regex=True, na=False)
    return bmode & amount.ge(1000) & close_loc.ge(65) & vol.between(0.7, 2.0)


def _v449_promotion_decision(sub: pd.DataFrame, short_term: bool = True, min_n: int = 10) -> str:
    n = len(sub) if sub is not None else 0
    if n <= 0:
        return '데이터없음'
    try:
        if short_term:
            pnl = _v439_mean(sub, 'rule35_pnl')
            win = _v439_rate(sub, 'rule35_win') if 'rule35_win' in sub.columns else _v439_rate(sub, 'close_win_hd')
            stop = _v439_rate(sub, 'stop_before_3') if 'stop_before_3' in sub.columns else _v439_rate(sub, 'rule35_stop')
            if n < min_n:
                if pnl >= 1.2 and win >= 65 and stop <= 25:
                    return f'🧪 표본부족 우수후보({n}건)'
                return f'표본부족({n}건)'
            if pnl >= 1.2 and win >= 65 and stop <= 25:
                return '✅ 승격검토'
            if pnl >= 0.5 and win >= 55 and stop <= 40:
                return '🟡 튜닝유지'
            return '⚠️ 숨김/재정의'
        r60 = _v439_mean(sub, 'i_ret_close_60d') if 'i_ret_close_60d' in sub.columns else _v439_mean(sub, 'ret_close_hd')
        if n < min_n:
            return f'표본부족({n}건)'
        if r60 >= 15:
            return '✅ 중기승격검토'
        if r60 >= 5:
            return '🟡 중기튜닝유지'
        return '⚠️ 중기숨김/재정의'
    except Exception:
        return '판정오류'


def _v449_tuning_line(sub: pd.DataFrame, label: str, short_term: bool = True, min_n: int = 10) -> str:
    base = _v439_short_trade_line(sub, label) if short_term else _v439_i_line(sub, label)
    return f"{base} | {_v449_promotion_decision(sub, short_term=short_term, min_n=min_n)}"


def _v449_pattern_tuning_lines(df: pd.DataFrame) -> list[str]:
    """A/B/C/H를 실전 승격 후보로 만들 수 있는지 텔레그램에서 바로 보는 압축 검증."""
    lines: list[str] = []
    try:
        if df is None or df.empty or not _v439_bool_env('CLOSING_BET_PATTERN_TUNING_BACKTEST', '1'):
            return lines
        lines.append('[🔬 튜닝 패턴 승격검증 — A/C/H/B]')
        a_retest = df[_v449_mask_a_retest(df)]
        a_safe = df[_v449_mask_a_retest_safe(df)]
        c_reclaim = df[_v449_mask_c_reclaim(df)]
        c_safe = df[_v449_mask_c_reclaim_safe(df)]
        h_core = df[_v449_mask_h_core_tuning(df)]
        b_confirm = df[_v449_mask_b_confirm_proxy(df)]
        lines.append(_v449_tuning_line(a_retest, 'A-RETEST 고거래대금 돌파재지지 프록시', True, 10))
        lines.append(_v449_tuning_line(a_safe, 'A-RETEST SAFE 후보', True, 10))
        lines.append(_v449_tuning_line(c_reclaim, 'C-RECLAIM/C-SWING 재지지 프록시', True, 10))
        lines.append(_v449_tuning_line(c_safe, 'C-RECLAIM SAFE 후보', True, 10))
        lines.append(_v449_tuning_line(h_core, 'H-CORE/H-TRIANGLE 핵심셀', True, 10))
        lines.append(_v449_tuning_line(b_confirm, 'B-CONFIRM B1/B2 확인형 프록시', True, 10))
        lines.append('- 승격 기준: 표본 10건 이상 기준 3/5 +1.2%↑, 승률 65%↑, 손절선행 25%↓이면 승격검토. 표본 부족은 실시간 숨김 유지.')
        lines.append('- 운용 원칙: 통과 전까지 FAST 실시간에는 숨김/관찰, 백테스트 모드에서만 넓게 검증합니다.')
    except Exception as e:
        lines.append(f'[튜닝 패턴 검증 오류] {type(e).__name__}: {e}')
    return lines




# =============================================================
# v4.4.9.13 LIVE OPERATION GUARD — 손절률 25% 이하 후보 찾기
# =============================================================
def _v4493_bucket_line(sub: pd.DataFrame, label: str, min_n: int = 5) -> str:
    """A-RETEST 세부 버킷은 표본이 작을 수 있어 min_n을 5로 낮춰 참고 판정한다."""
    try:
        base = _v439_short_trade_line(sub, label)
        n = len(sub) if sub is not None else 0
        if n <= 0:
            return base + " | 데이터없음"
        pnl = _v439_mean(sub, 'rule35_pnl')
        win = _v439_rate(sub, 'rule35_win') if 'rule35_win' in sub.columns else _v439_rate(sub, 'close_win_hd')
        stop = _v439_rate(sub, 'stop_before_3') if 'stop_before_3' in sub.columns else _v439_rate(sub, 'rule35_stop')
        if n < min_n:
            if pnl >= 1.2 and win >= 65 and stop <= 25:
                return base + f" | 🧪 표본부족 우수후보({n}건)"
            return base + f" | 표본부족({n}건)"
        if pnl >= 1.2 and win >= 65 and stop <= 25:
            verdict = '✅ 승격후보'
        elif pnl >= 1.2 and win >= 65 and stop <= 30:
            verdict = '🟢 근접후보'
        elif pnl >= 0.5 and win >= 55 and stop <= 40:
            verdict = '🟡 튜닝유지'
        else:
            verdict = '⚠️ 제외/재정의'
        return base + f" | {verdict}"
    except Exception as e:
        return f"- {label}: 판정오류 {type(e).__name__}: {e}"


def _v4493_next_confirm_proxy(df: pd.DataFrame, base_mask: pd.Series) -> tuple[pd.Series, str]:
    """다음날 양봉/전일고가 회복에 가까운 컬럼이 있으면 확인형 프록시를 만든다."""
    try:
        idx = df.index if df is not None else []
        if df is None or df.empty:
            return pd.Series(False, index=idx), '다음날 확인 컬럼 없음'
        base = base_mask.reindex(df.index).fillna(False).astype(bool)
        bool_cols = [
            'next_reclaim_prev_high', 'next_day_reclaim_high', 'next_high_reclaim',
            'next_bullish', 'next_day_bullish', 'next_positive_candle', 'next_close_above_signal_high'
        ]
        masks = []
        used = []
        for c in bool_cols:
            if c in df.columns:
                s = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(float).gt(0)
                masks.append(s)
                used.append(c)
        for c in ['next_close_ret', 'ret_next_close', 'next_day_ret', 'ret_1d', 'close_ret_1d']:
            if c in df.columns:
                s = pd.to_numeric(df[c], errors='coerce').fillna(-999).gt(0)
                masks.append(s)
                used.append(c + '>0')
        if not masks:
            return pd.Series(False, index=df.index), '다음날 확인 컬럼 없음 → 실시간 조건으로만 사용'
        out = masks[0].copy()
        for m in masks[1:]:
            out = out | m
        return base & out.fillna(False).astype(bool), '사용컬럼 ' + ','.join(used[:4])
    except Exception as e:
        return pd.Series(False, index=df.index if df is not None else []), f'다음날 확인 프록시 오류 {type(e).__name__}'


def _v4493_a_retest_refinement_lines(df: pd.DataFrame) -> list[str]:
    """A-RETEST SAFE를 실시간 승격 가능한 수준으로 좁히기 위한 버킷 분석."""
    lines: list[str] = []
    try:
        if df is None or df.empty or not _v439_bool_env('CLOSING_BET_A_RETEST_REFINEMENT', '1'):
            return lines
        mode = _v449_mode_series(df)
        amount = _v449_base_liquidity_series(df)
        close_loc = _v449_close_loc_series(df)
        vol = _v449_vol_ratio_series(df)
        rr = _v449_rr_series(df)
        risk_txt = _v449_safe_contains_series(df, ['risk_tags', 'tags', 'reason', 'final_reason', 'comment'], r'RISK|위험|과열|종가위치약함|RR낮음|RR과대')
        a0 = mode.eq('A')
        a_retest = _v449_mask_a_retest(df)
        a_safe = _v449_mask_a_retest_safe(df)
        lines.append('[🎯 A-RETEST SAFE 세분화 — 손절 25% 이하 후보 찾기]')
        lines.append(_v4493_bucket_line(df[a_safe], 'A-RETEST SAFE 현재 기준', min_n=10))
        lines.append('- 목표: 3/5 +1.2%↑, 승률 65%↑ 유지하면서 손절선행 25%↓ 조합을 찾습니다.')
        lines.append(_v4493_bucket_line(df[a_retest & close_loc.ge(70)], 'A 종가위치70+'))
        lines.append(_v4493_bucket_line(df[a_retest & close_loc.ge(80)], 'A 종가위치80+'))
        lines.append(_v4493_bucket_line(df[a_retest & close_loc.ge(70) & amount.ge(5000)], 'A 5000억+·종가70+'))
        lines.append(_v4493_bucket_line(df[a_retest & close_loc.ge(80) & amount.ge(5000)], 'A 5000억+·종가80+'))
        lines.append(_v4493_bucket_line(df[a_safe & amount.between(5000, 10000, inclusive='left')], 'A-SAFE 대금5000억~1조'))
        lines.append(_v4493_bucket_line(df[a_safe & amount.ge(10000)], 'A-SAFE 대금1조+'))
        lines.append(_v4493_bucket_line(df[a_retest & amount.between(3000, 5000, inclusive='left') & close_loc.ge(70) & vol.le(1.8) & rr.between(0.8, 1.8)], 'A 후보 대금3000~5000억·종가70+'))
        lines.append(_v4493_bucket_line(df[a_retest & close_loc.ge(70) & amount.ge(5000) & vol.between(0.0, 1.2, inclusive='both') & rr.between(0.8, 1.8)], 'A 5000억+·종가70+·거래량≤1.2'))
        lines.append(_v4493_bucket_line(df[a_retest & close_loc.ge(70) & amount.ge(5000) & vol.between(1.2, 1.8, inclusive='right') & rr.between(0.8, 1.8)], 'A 5000억+·종가70+·거래량1.2~1.8'))
        lines.append(_v4493_bucket_line(df[a_retest & close_loc.ge(70) & amount.ge(5000) & vol.between(1.8, 2.5, inclusive='right') & rr.between(0.8, 2.2)], 'A 5000억+·종가70+·거래량1.8~2.5'))
        lines.append(_v4493_bucket_line(df[a_retest & close_loc.ge(70) & amount.ge(5000) & vol.le(1.8) & rr.between(0.8, 1.5, inclusive='both')], 'A 5000억+·종가70+·RR0.8~1.5'))
        lines.append(_v4493_bucket_line(df[a_retest & close_loc.ge(70) & amount.ge(5000) & vol.le(1.8) & rr.between(1.5, 1.8, inclusive='right')], 'A 5000억+·종가70+·RR1.5~1.8'))
        lines.append(_v4493_bucket_line(df[a_retest & close_loc.ge(70) & amount.ge(5000) & vol.le(2.5) & rr.gt(1.8)], 'A 5000억+·종가70+·RR1.8초과'))
        core1 = a_retest & amount.between(5000, 10000, inclusive='left') & close_loc.ge(70) & vol.le(1.8) & rr.between(0.8, 1.5) & (~risk_txt)
        core2 = a_retest & amount.ge(5000) & close_loc.ge(80) & vol.le(1.8) & rr.between(0.8, 1.5) & (~risk_txt)
        core3 = a_retest & amount.ge(5000) & close_loc.ge(70) & vol.le(1.2) & rr.between(0.8, 1.5) & (~risk_txt)
        lines.append(_v4493_bucket_line(df[core1], 'A-RETEST CORE 후보① 5000억~1조·종가70+·거래량≤1.8·RR0.8~1.5'))
        lines.append(_v4493_bucket_line(df[core2], 'A-RETEST CORE 후보② 5000억+·종가80+·거래량≤1.8·RR0.8~1.5'))
        lines.append(_v4493_bucket_line(df[core3], 'A-RETEST CORE 후보③ 5000억+·종가70+·거래량≤1.2·RR0.8~1.5'))
        risk_a = a0 & amount.ge(5000) & close_loc.ge(50) & risk_txt
        lines.append(_v4493_bucket_line(df[risk_a], 'A 위험태그 포함 5000억+·종가50+'))
        next_mask, next_note = _v4493_next_confirm_proxy(df, a_safe)
        if next_mask.any():
            lines.append(_v4493_bucket_line(df[next_mask], 'A-SAFE 다음날확인 프록시 통과'))
        lines.append(f"- 다음날 전일고가/양봉 확인: {next_note}. 없으면 실시간 운용에서만 승격 조건으로 사용합니다.")
        lines.append('- 해석: A는 바로 승격하지 않고, 위 버킷 중 손절 25% 이하·표본 10건 이상 조합만 FAST 후보 승격을 검토합니다.')
    except Exception as e:
        lines.append(f'[A-RETEST 세분화 오류] {type(e).__name__}: {e}')
    return lines

# =============================================================
# v4.4.9.13 LIVE OPERATION GUARD — A/C/B/H 0건 원인 진단
# =============================================================
def _v4491_num_with_source(df: pd.DataFrame, candidates: list[str], default: float = 0.0) -> tuple[pd.Series, str, int]:
    """진단용 숫자 컬럼 탐색: 실제 사용한 컬럼명과 유효값 개수를 함께 반환한다."""
    try:
        if df is None:
            return pd.Series(dtype=float), 'df없음', 0
        for c in candidates:
            if c in df.columns:
                s = pd.to_numeric(df[c], errors='coerce')
                valid = int(s.notna().sum())
                if valid > 0:
                    return s.fillna(default), c, valid
        return pd.Series(default, index=df.index, dtype=float), '미검출', 0
    except Exception:
        return pd.Series(default, index=df.index if df is not None else [], dtype=float), '오류', 0


def _v4491_mode_counts_line(df: pd.DataFrame) -> str:
    try:
        mode = _v449_mode_series(df)
        vc = mode.value_counts(dropna=False).head(12)
        parts = [f"{str(k) or '공백'}:{int(v)}" for k, v in vc.items()]
        return '- mode 분포: ' + ' · '.join(parts) if parts else '- mode 분포: 확인불가'
    except Exception as e:
        return f'- mode 분포 오류: {type(e).__name__}: {e}'


def _v4491_metric_source_lines(df: pd.DataFrame) -> list[str]:
    lines: list[str] = []
    try:
        amount, amount_src, amount_n = _v4491_num_with_source(df, ['amount_b', 'entry_amount_b', 'breakout_amount_b', 'today_amount_b', '거래대금_억'], 0)
        close_loc, close_src, close_n = _v4491_num_with_source(df, ['close_loc_pct', 'close_location_pct', 'close_loc', 'close_location', '종가위치'], 0)
        vol, vol_src, vol_n = _v4491_num_with_source(df, ['vol_ratio', 'volume_ratio', 'vol20_ratio', 'vma20_ratio', '거래량비'], 1)
        rr, rr_src, rr_n = _v4491_num_with_source(df, ['rr', 'RR', 'risk_reward', 'rr_ratio'], 1)
        lines.append(f"- 사용 컬럼: 거래대금={amount_src}({amount_n}개), 종가위치={close_src}({close_n}개), 거래량비={vol_src}({vol_n}개), RR={rr_src}({rr_n}개)")
        if close_src == '미검출' or close_n == 0:
            lines.append("  ↳ 경고: 종가위치 컬럼을 못 찾으면 A/C/B 튜닝 조건이 0건으로 줄어들 수 있습니다.")
        if vol_src == '미검출' or vol_n == 0:
            lines.append("  ↳ 참고: 거래량비 컬럼 미검출 시 기본값 1.0으로 처리됩니다.")
        if rr_src == '미검출' or rr_n == 0:
            lines.append("  ↳ 참고: RR 컬럼 미검출 시 기본값 1.0으로 처리됩니다.")
    except Exception as e:
        lines.append(f"- 사용 컬럼 진단 오류: {type(e).__name__}: {e}")
    return lines


def _v4491_count(label: str, mask: pd.Series) -> str:
    try:
        return f"{label} {int(mask.fillna(False).sum())}건"
    except Exception:
        return f"{label} 오류"


def _v4491_step_line(prefix: str, steps: list[tuple[str, pd.Series]]) -> str:
    return f"- {prefix}: " + " → ".join(_v4491_count(label, mask) for label, mask in steps)



def _v4492_close_loc_fallback_diagnostic_lines(df: pd.DataFrame) -> list[str]:
    """v4.4.9.3: close_loc 기존값과 fallback 적용 후 커버리지 비교."""
    lines = []
    try:
        if df is None or df.empty:
            return lines
        mode = _v449_mode_series(df)
        old = pd.Series(np.nan, index=df.index, dtype=float)
        old_col = None
        for c in ['close_loc_pct', 'close_location_pct', 'close_loc', '종가위치']:
            if c in df.columns:
                old_col = c
                old = pd.to_numeric(df[c], errors='coerce')
                break
        fb = _v449_close_loc_fallback_series(df)
        amount = _v449_base_liquidity_series(df)
        lines.append('[🧩 종가위치 fallback 보정 — v4.4.9.34]')
        lines.append(f"- 기존 종가위치 컬럼: {old_col or '없음'} | 유효값 {int(old.notna().sum())}건 / {len(df)}건 | fallback 후 50%+ {int(fb.ge(50).sum())}건, 60%+ {int(fb.ge(60).sum())}건, 70%+ {int(fb.ge(70).sum())}건")
        for m in ['A', 'C', 'B1', 'B2', 'B']:
            if m == 'B':
                mask = mode.str.contains(r'B1|B2|B', regex=True, na=False)
                label = 'B계열 전체'
            else:
                mask = mode.eq(m)
                label = m
            if int(mask.sum()) <= 0:
                continue
            old50 = int(old.loc[mask].ge(50).sum()) if old_col else 0
            fb50 = int(fb.loc[mask].ge(50).sum())
            fb60 = int(fb.loc[mask].ge(60).sum())
            liq5000 = int((mask & amount.ge(5000)).sum())
            liq5000_50 = int((mask & amount.ge(5000) & fb.ge(50)).sum())
            lines.append(f"- {label}: 원신호 {int(mask.sum())}건 | 기존종가50+ {old50}건 → fallback종가50+ {fb50}건 / 60+ {fb60}건 | 대금5000+ {liq5000}건, 대금5000+·종가50+ {liq5000_50}건")
        lines.append('- 해석: 기존 close_loc_pct가 없는 A/C/B 행은 _high/_low/_close로 종가위치를 직접 계산해 튜닝 마스크에 반영합니다.')
    except Exception as e:
        lines.append(f'[종가위치 fallback 진단 오류] {type(e).__name__}: {e}')
    return lines

def _v4491_tuning_mask_diagnostic_lines(df: pd.DataFrame) -> list[str]:
    """A/C/B 튜닝 프록시가 0건일 때 원인을 찾기 위한 단계별 후보 수 리포트."""
    lines: list[str] = []
    try:
        if df is None or df.empty or not _v439_bool_env('CLOSING_BET_TUNING_MASK_DIAGNOSTIC', '1'):
            return lines
        lines.append('[🔍 튜닝 마스크 진단 — A/C/B/H fallback 적용 후 재점검]')
        lines.append(_v4491_mode_counts_line(df))
        lines += _v4491_metric_source_lines(df)

        mode = _v449_mode_series(df)
        amount = _v449_base_liquidity_series(df)
        close_loc = _v449_close_loc_series(df)
        vol = _v449_vol_ratio_series(df)
        rr = _v449_rr_series(df)
        risk_txt = _v449_safe_contains_series(df, ['risk_tags', 'tags', 'reason', 'final_reason', 'comment'], r'RISK|위험|과열|종가위치약함|RR낮음|RR과대')
        reclaim_hint = _v449_safe_contains_series(df, ['c_subtype', 'tags', 'reason', 'final_reason', 'comment', 'label'], r'재지지|재상승|회복|reclaim|눌림|안착|5MA|20MA')

        # A 단계별 진단
        a0 = mode.eq('A')
        a1 = a0 & amount.ge(3000)
        a2 = a0 & amount.ge(5000)
        a3 = a2 & close_loc.ge(50)
        a4 = a2 & close_loc.ge(60)
        a5 = a4 & vol.le(2.5)
        a6 = a5 & rr.between(0.6, 2.2)
        a7 = a6 & (~risk_txt)
        lines.append(_v4491_step_line('A-RETEST 단계', [
            ('A원신호', a0), ('대금3000+', a1), ('대금5000+', a2), ('종가50+', a3), ('종가60+', a4), ('거래량≤2.5', a5), ('RR정상', a6), ('위험제외', a7)
        ]))
        if int(a0.sum()) > 0 and int(a7.sum()) == 0:
            if int(a2.sum()) == 0:
                lines.append('  ↳ A 진단: 5000억+ 거래대금 조건에서 대부분 탈락합니다. 3000억+ 완화 비교가 필요합니다.')
            elif int(a4.sum()) == 0:
                lines.append('  ↳ A 진단: 종가위치 조건 또는 종가위치 컬럼 매핑이 핵심 병목입니다.')
            elif int(a5.sum()) == 0:
                lines.append('  ↳ A 진단: 거래량비/과열 조건이 너무 빡셀 수 있습니다.')
            elif int(a6.sum()) == 0:
                lines.append('  ↳ A 진단: RR 조건이 너무 빡셀 수 있습니다.')
            elif int(a7.sum()) == 0:
                lines.append('  ↳ A 진단: risk_tags/사유 텍스트가 과하게 위험 처리되는지 확인 필요합니다.')
        # A 완화 프록시 성과
        lines.append(_v449_tuning_line(df[a3], 'A 완화1: A 5000억+·종가50+', True, 10))
        lines.append(_v449_tuning_line(df[a4], 'A 완화2: A 5000억+·종가60+', True, 10))

        # C 단계별 진단
        c0 = mode.eq('C')
        c1 = c0 & amount.ge(1000)
        c2 = c0 & amount.ge(3000)
        c3 = c0 & amount.ge(5000)
        c4 = c2 & close_loc.ge(50)
        c5 = c2 & close_loc.ge(55)
        c6 = c5 & vol.le(2.5)
        c7 = c6 & (reclaim_hint | amount.ge(5000))
        c8 = c7 & close_loc.ge(65) & amount.ge(5000)
        lines.append(_v4491_step_line('C-RECLAIM 단계', [
            ('C원신호', c0), ('대금1000+', c1), ('대금3000+', c2), ('대금5000+', c3), ('종가50+', c4), ('종가55+', c5), ('거래량≤2.5', c6), ('재지지힌트/5000+', c7), ('SAFE조건', c8)
        ]))
        if int(c0.sum()) > 0 and int(c7.sum()) == 0:
            if int(c2.sum()) == 0:
                lines.append('  ↳ C 진단: C는 3000억+ 조건에서 거의 사라집니다. 스윙형은 1000억+ 또는 5~20일 평가로 분리 검토가 필요합니다.')
            elif int(c5.sum()) == 0:
                lines.append('  ↳ C 진단: 종가위치 조건/컬럼 매핑 확인이 필요합니다.')
            elif int(c6.sum()) == 0:
                lines.append('  ↳ C 진단: 거래량비 조건이 너무 빡셀 수 있습니다.')
            else:
                lines.append('  ↳ C 진단: 재지지/회복 힌트 텍스트가 실제 컬럼명과 맞지 않을 가능성이 큽니다.')
        lines.append(_v449_tuning_line(df[c4], 'C 완화1: C 3000억+·종가50+', True, 10))
        lines.append(_v449_tuning_line(df[c1 & close_loc.ge(50)], 'C 완화2: C 1000억+·종가50+', True, 10))

        # B 단계별 진단
        b0 = mode.str.contains(r'B1|B2|B', regex=True, na=False)
        b1 = b0 & amount.ge(300)
        b2 = b0 & amount.ge(1000)
        b3 = b2 & close_loc.ge(50)
        b4 = b2 & close_loc.ge(65)
        b5 = b4 & vol.between(0.7, 2.0)
        lines.append(_v4491_step_line('B-CONFIRM 단계', [
            ('B원신호', b0), ('대금300+', b1), ('대금1000+', b2), ('종가50+', b3), ('종가65+', b4), ('거래량0.7~2.0', b5)
        ]))
        if int(b0.sum()) > 0 and int(b5.sum()) == 0:
            if int(b2.sum()) == 0:
                lines.append('  ↳ B 진단: B는 유동성 조건에서 대부분 탈락합니다. 실시간 매수보다 다음날 확인형/저유동성 제외 유지가 맞습니다.')
            elif int(b4.sum()) == 0:
                lines.append('  ↳ B 진단: 종가위치 65+ 확인 조건이 너무 빡세거나 컬럼 매핑이 필요합니다.')
            else:
                lines.append('  ↳ B 진단: 거래량비 확인 조건이 병목입니다.')
        lines.append(_v449_tuning_line(df[b3], 'B 완화1: B 1000억+·종가50+', True, 10))
        lines.append(_v449_tuning_line(df[b1 & close_loc.ge(50)], 'B 완화2: B 300억+·종가50+', True, 10))

        # H 참고 진단
        h0 = mode.eq('H') | _v439_mask('_bt_mask_h_strict_all', df) | _v439_mask('_bt_mask_h_v427_core_union', df)
        h_core = _v449_mask_h_core_tuning(df)
        lines.append(_v4491_step_line('H-CORE 단계', [('H원신호/STRICT', h0), ('H핵심셀', h_core)]))
        if int(h_core.sum()) > 0:
            lines.append(_v449_tuning_line(df[h_core], 'H 핵심셀 재확인', True, 5))

        lines.append('- 진단 해석: 0건은 “성과가 나쁨”이 아니라 기존 종가위치 누락은 fallback으로 보정했습니다. 그래도 0건이면 조건 자체가 병목입니다.')
    except Exception as e:
        lines.append(f'[튜닝 마스크 진단 오류] {type(e).__name__}: {e}')
    return lines


# =============================================================
# v4.4.9.13 LIVE OPERATION GUARD — 재료/시황 동반 성과검증
# =============================================================
def _v4494_text_blob_series(df: pd.DataFrame) -> pd.Series:
    """재료/테마/글로벌시황 힌트를 찾기 위해 주요 텍스트 컬럼을 합친다."""
    try:
        if df is None or df.empty:
            return pd.Series('', index=[])
        idx = df.index
        cols = [
            'theme', 'sector', 'industry', 'theme_name', 'sector_name', 'issue', 'issue_title',
            'news_title', 'news_summary', 'news', 'material_reason', 'material_type', 'tags',
            'passed', 'reason', 'final_reason', 'comment', 'label', 'strategy_label', 'name', '종목명'
        ]
        acc = pd.Series('', index=idx, dtype=str)
        for c in cols:
            if c in df.columns:
                try:
                    acc = acc.str.cat(df[c].astype(str).fillna(''), sep=' ')
                except Exception:
                    pass
        return acc.fillna('').astype(str)
    except Exception:
        return pd.Series('', index=df.index if df is not None else [], dtype=str)


def _v4494_material_proxy_score(df: pd.DataFrame) -> pd.Series:
    """A-RETEST용 재료/대금 프록시 점수.
    과거 뉴스 DB가 없을 때도 기존 i_material_proxy_score/뉴스점수/거래대금/대표성/텍스트 힌트로 0~5점대 점수를 만든다.
    """
    try:
        if df is None or df.empty:
            return pd.Series(dtype=float)
        idx = df.index
        # 1) 이미 계산된 재료 점수 우선
        for c in ['i_material_proxy_score', 'material_proxy_score', 'material_score', 'news_material_score', 'theme_score', 'issue_score', '재료대금점수', '재료점수']:
            if c in df.columns:
                s = pd.to_numeric(df[c], errors='coerce')
                if s.notna().sum() > 0 and float(s.fillna(0).max()) > 0:
                    return s.fillna(0).clip(lower=0)
        amount = _v449_base_liquidity_series(df).reindex(idx).fillna(0)
        vol = _v449_vol_ratio_series(df).reindex(idx).fillna(1)
        txt = _v4494_text_blob_series(df)
        score = pd.Series(0.0, index=idx)
        # 거래대금/유동성 관심도
        score += amount.ge(300).astype(float)
        score += amount.ge(1000).astype(float)
        score += amount.ge(5000).astype(float)
        # 거래량 재증가/관심도
        score += vol.ge(1.2).astype(float)
        # 지수/대표성/테마 텍스트 힌트
        score += txt.str.contains(r'코스피200|KOSPI200|코스닥150|KOSDAQ150|대장|대표|leader|주도', case=False, regex=True, na=False).astype(float)
        # 뉴스/재료성 텍스트 힌트
        score += txt.str.contains(r'뉴스|재료|이슈|테마|수주|실적|공시|계약|공급|정책|정부|글로벌|미국|AI|반도체|전력|원전|방산|조선|유가|금리|환율|구리|로봇', case=False, regex=True, na=False).astype(float)
        return score.clip(lower=0, upper=6)
    except Exception:
        return pd.Series(0.0, index=df.index if df is not None else [], dtype=float)


def _v4494_material_type_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    """단기테마/글로벌시황/개별재료 프록시를 만든다. 실제 뉴스 저장소가 없으면 텍스트+대금 기반 보조 판단으로 사용한다."""
    idx = df.index if df is not None else []
    false = pd.Series(False, index=idx)
    try:
        if df is None or df.empty:
            return {'theme': false, 'global': false, 'company': false}
        txt = _v4494_text_blob_series(df)
        amount = _v449_base_liquidity_series(df).reindex(df.index).fillna(0)
        vol = _v449_vol_ratio_series(df).reindex(df.index).fillna(1)
        # 단기 테마성: 정책/테마/관련주/순환/섹터 키워드 또는 대금+거래량이 동반되는 테마성 후보
        theme_kw = txt.str.contains(
            r'테마|관련주|정책|정부|수혜|대장|후발|순환|로봇|전력|전선|원전|방산|조선|화장품|바이오|AI|반도체|데이터센터|재건|우크라|원전|2차전지|수소|원자력',
            case=False, regex=True, na=False
        )
        # 글로벌 시황 연동: 미국/ETF/매크로/원자재/환율/금리/글로벌 섹터 키워드
        global_kw = txt.str.contains(
            r'글로벌|미국|나스닥|다우|S&P|SOXX|SMH|엔비디아|NVIDIA|필라델피아|반도체|AI|전력|데이터센터|유가|WTI|브렌트|금리|달러|환율|구리|원자재|은행|보험|조선|해운',
            case=False, regex=True, na=False
        )
        # 개별 기업/공시성: 공시/수주/실적/계약/증설/투자/인수합병 등
        company_kw = txt.str.contains(
            r'공시|수주|계약|공급계약|실적|영업이익|흑자|흑자전환|증설|투자|M&A|인수|합병|자사주|배당|신규|승인|임상',
            case=False, regex=True, na=False
        )
        # 뉴스 텍스트가 없는 경우에도 초대금+거래량은 재료/이슈성 가능성으로 theme 보조 처리한다.
        theme_proxy = theme_kw | (amount.ge(5000) & vol.ge(1.2))
        return {'theme': theme_proxy.fillna(False), 'global': global_kw.fillna(False), 'company': company_kw.fillna(False)}
    except Exception:
        return {'theme': false, 'global': false, 'company': false}


def _v4494_material_check_lines(df: pd.DataFrame) -> list[str]:
    """A-RETEST CORE/다음날확인 후보가 재료·대금/테마/글로벌시황과 결합될 때 성과가 좋아지는지 검증한다."""
    lines: list[str] = []
    try:
        if df is None or df.empty or not _v439_bool_env('CLOSING_BET_A_RETEST_MATERIAL_CHECK', '1'):
            return lines
        amount = _v449_base_liquidity_series(df)
        close_loc = _v449_close_loc_series(df)
        vol = _v449_vol_ratio_series(df)
        rr = _v449_rr_series(df)
        risk_txt = _v449_safe_contains_series(df, ['risk_tags', 'tags', 'reason', 'final_reason', 'comment'], r'RISK|위험|과열|종가위치약함|RR낮음|RR과대')
        a_retest = _v449_mask_a_retest(df)
        a_safe = _v449_mask_a_retest_safe(df)
        core2 = a_retest & amount.ge(5000) & close_loc.ge(80) & vol.le(1.8) & rr.between(0.8, 1.5) & (~risk_txt)
        next_mask, next_note = _v4493_next_confirm_proxy(df, a_safe)
        mat = _v4494_material_proxy_score(df).reindex(df.index).fillna(0)
        type_masks = _v4494_material_type_masks(df)
        theme = type_masks.get('theme', pd.Series(False, index=df.index)).reindex(df.index).fillna(False)
        glob = type_masks.get('global', pd.Series(False, index=df.index)).reindex(df.index).fillna(False)
        company = type_masks.get('company', pd.Series(False, index=df.index)).reindex(df.index).fillna(False)
        rr_bad = _v449_mode_series(df).eq('A') & amount.ge(5000) & close_loc.ge(70) & rr.gt(1.8)

        lines.append('[🧪 A-RETEST 재료/시황 동반 성과검증]')
        lines.append(_v4493_bucket_line(df[core2], 'A-RETEST CORE② 전체 5000억+·종가80+·거래량≤1.8·RR0.8~1.5', min_n=10))
        lines.append(_v4493_bucket_line(df[core2 & mat.ge(3)], 'A-RETEST CORE② + 재료대금 3점+', min_n=5))
        lines.append(_v4493_bucket_line(df[core2 & mat.ge(4)], 'A-RETEST CORE② + 재료대금 4점+', min_n=5))
        lines.append(_v4493_bucket_line(df[core2 & theme], 'A-RETEST CORE② + 단기테마성', min_n=5))
        lines.append(_v4493_bucket_line(df[core2 & glob], 'A-RETEST CORE② + 글로벌시황연동', min_n=5))
        lines.append(_v4493_bucket_line(df[core2 & company], 'A-RETEST CORE② + 개별재료/공시성', min_n=5))
        if next_mask.any():
            lines.append(_v4493_bucket_line(df[next_mask], 'A-SAFE 다음날확인 전체', min_n=10))
            lines.append(_v4493_bucket_line(df[next_mask & mat.ge(3)], 'A-SAFE 다음날확인 + 재료대금 3점+', min_n=5))
            lines.append(_v4493_bucket_line(df[next_mask & mat.ge(4)], 'A-SAFE 다음날확인 + 재료대금 4점+', min_n=5))
            lines.append(_v4493_bucket_line(df[next_mask & theme], 'A-SAFE 다음날확인 + 단기테마성', min_n=5))
            lines.append(_v4493_bucket_line(df[next_mask & glob], 'A-SAFE 다음날확인 + 글로벌시황연동', min_n=5))
        lines.append(_v4493_bucket_line(df[rr_bad], 'A RR1.8초과 전체 — 제외 재확인', min_n=5))
        lines.append(_v4493_bucket_line(df[rr_bad & mat.ge(3)], 'A RR1.8초과 + 재료대금3점+ — 재료가 있어도 위험한지 확인', min_n=5))
        lines.append(f'- 다음날확인 프록시: {next_note}')
        lines.append(f'- 재료/시황 프록시 커버리지: 재료3점+ {int(mat.ge(3).sum())}건, 재료4점+ {int(mat.ge(4).sum())}건, 단기테마 {int(theme.sum())}건, 글로벌연동 {int(glob.sum())}건, 개별재료 {int(company.sum())}건')
        lines.append('- 해석: 과거 뉴스 원문 저장소가 없으므로 재료/시황은 거래대금·대표성·텍스트 힌트 기반 프록시입니다. 결과가 좋으면 실시간에서는 뉴스/테마 확인 후 보수적으로 승격합니다.')
    except Exception as e:
        lines.append(f'[A-RETEST 재료검증 오류] {type(e).__name__}: {e}')
    return lines


# =============================================================
# v4.4.9.13 LIVE OPERATION GUARD — 다음날 진입가별 성과검증
# =============================================================
def _v4495_simulate_rule35_bars(bars: pd.DataFrame, entry: float, stoploss: float, hold_days: int = 60) -> dict:
    """주어진 entry 기준 +3/+5/손절 시뮬레이션.
    일봉 OHLC만 사용하므로 같은 날 목표/손절 충돌은 보수적으로 손절 우선 처리한다.
    """
    try:
        if bars is None or bars.empty or entry <= 0:
            return {'ok': 0}
        work = bars.copy().head(max(1, int(hold_days or 60)))
        target3 = entry * 1.03
        target5 = entry * 1.05
        pos_remain = 1.0
        realized = 0.0
        hit3 = 0
        hit5 = 0
        stop = 0
        first_event = '기간종료'
        first_event_date = ''
        def _ret_price(x):
            try:
                x = float(x)
                return (x / entry - 1.0) * 100.0 if entry > 0 and x > 0 else np.nan
            except Exception:
                return np.nan
        for _, bar in work.iterrows():
            bdate = ''
            try:
                bdate = pd.Timestamp(bar.get('Date')).strftime('%Y-%m-%d') if not pd.isna(bar.get('Date')) else ''
            except Exception:
                pass
            low = _safe_float(bar.get('Low', 0), 0.0)
            high = _safe_float(bar.get('High', 0), 0.0)
            if stoploss > 0 and low > 0 and low <= stoploss:
                realized += pos_remain * _ret_price(stoploss)
                pos_remain = 0.0
                stop = 1
                first_event = '손절'
                first_event_date = bdate
                break
            if hit3 == 0 and high >= target3:
                realized += 0.5 * 3.0
                pos_remain -= 0.5
                hit3 = 1
                first_event = '+3절반익절'
                first_event_date = bdate
            if hit3 == 1 and pos_remain > 0 and high >= target5:
                realized += pos_remain * 5.0
                pos_remain = 0.0
                hit5 = 1
                first_event = '+5추가익절'
                first_event_date = bdate
                break
        if pos_remain > 0:
            last_close = _safe_float(work.iloc[-1].get('Close', 0), 0.0)
            close_ret = _ret_price(last_close)
            realized += pos_remain * (0.0 if pd.isna(close_ret) else close_ret)
        max_high = _safe_float(work['High'].max(), 0.0) if 'High' in work.columns else 0.0
        min_low = _safe_float(work['Low'].min(), 0.0) if 'Low' in work.columns else 0.0
        return {
            'ok': 1,
            'pnl': round(realized, 2),
            'win': int(realized > 0),
            'hit3': int(hit3),
            'hit5': int(hit5),
            'stop': int(stop),
            'max_high_ret': round(_ret_price(max_high), 2) if max_high > 0 else np.nan,
            'min_low_ret': round(_ret_price(min_low), 2) if min_low > 0 else np.nan,
            'first_event': first_event,
            'first_event_date': first_event_date,
        }
    except Exception:
        return {'ok': 0}


def _v4495_fetch_price_bars_for_signal(row: pd.Series, hold_days: int = 60) -> dict | None:
    """A-CONFIRM 다음날 진입가별 검증용 가격 데이터 로딩."""
    try:
        code = str(row.get('code', '')).zfill(6)
        sig = str(row.get('signal_date', row.get('scan_date', '')))
        if not code or not sig or sig.lower() in ('nan', 'nat'):
            return None
        sig_ts = pd.Timestamp(sig)
        start = (sig_ts - pd.Timedelta(days=10)).strftime('%Y-%m-%d')
        # 60거래일 확보를 위해 달력일은 넉넉히 잡는다.
        end = (sig_ts + pd.Timedelta(days=int(max(hold_days, 60) * 2 + 30))).strftime('%Y-%m-%d')
        price = fdr.DataReader(code, start=start, end=end)
        if price is None or price.empty:
            return None
        price = price.sort_index()
        price = price[~price.index.duplicated(keep='last')].copy()
        price['Date'] = price.index
        # 신호일이 휴장/데이터 누락인 경우 신호일 이하 마지막 거래일을 사용한다.
        before = price[price.index.date <= sig_ts.date()]
        if before.empty:
            return None
        sig_idx = before.index[-1]
        loc = price.index.get_loc(sig_idx)
        signal_bar = price.iloc[int(loc)]
        future = price.iloc[int(loc) + 1:].copy()
        if future.empty:
            return None
        next_bar = future.iloc[0]
        return {'signal_bar': signal_bar, 'future': future, 'next_bar': next_bar}
    except Exception as e:
        log_debug(f"A-CONFIRM entry fetch 실패: {e}")
        return None


def _v4495_entry_summary_line(rows: list[dict], label: str) -> str:
    try:
        ok_rows = [r for r in rows if r and int(r.get('ok', 0)) == 1]
        n = len(ok_rows)
        if n <= 0:
            return f"- {label}: 0건 | 데이터없음"
        pnl = float(np.nanmean([_safe_float(r.get('pnl', np.nan), np.nan) for r in ok_rows]))
        win = float(np.nanmean([_safe_float(r.get('win', 0), 0) for r in ok_rows]) * 100.0)
        hit3 = float(np.nanmean([_safe_float(r.get('hit3', 0), 0) for r in ok_rows]) * 100.0)
        hit5 = float(np.nanmean([_safe_float(r.get('hit5', 0), 0) for r in ok_rows]) * 100.0)
        stop = float(np.nanmean([_safe_float(r.get('stop', 0), 0) for r in ok_rows]) * 100.0)
        mark = '✅ 승격후보' if (n >= 10 and pnl >= 1.2 and win >= 65 and stop <= 25) else ('🟡 확인필요' if n >= 5 else f'표본부족({n}건)')
        return f"- {label}: {n}건 | 3/5 {pnl:.2f}% | 승률 {win:.1f}% | +3선행 {hit3:.1f}% | +5선행 {hit5:.1f}% | 손절선행 {stop:.1f}% | {mark}"
    except Exception as e:
        return f"- {label}: 집계오류 {type(e).__name__}: {e}"


def _v4495_a_confirm_entry_price_test_lines(df: pd.DataFrame, hold_days: int = 60) -> list[str]:
    """A-SAFE 다음날확인형을 실제 어느 가격에 들어갔을 때 좋은지 검증한다.
    기존 A-SAFE 다음날확인 프록시는 '전일 종가 진입 + 다음날 생존 필터'였으므로,
    다음날 시가/종가/전일고가 회복가 진입을 별도 검증한다.
    """
    lines: list[str] = []
    try:
        if df is None or df.empty or not _v439_bool_env('CLOSING_BET_A_CONFIRM_ENTRY_TEST', '1'):
            return lines
        amount = _v449_base_liquidity_series(df)
        close_loc = _v449_close_loc_series(df)
        vol = _v449_vol_ratio_series(df)
        rr = _v449_rr_series(df)
        risk_txt = _v449_safe_contains_series(df, ['risk_tags', 'tags', 'reason', 'final_reason', 'comment'], r'RISK|위험|과열|종가위치약함|RR낮음|RR과대')
        a_retest = _v449_mask_a_retest(df)
        a_safe = _v449_mask_a_retest_safe(df)
        core2 = a_retest & amount.ge(5000) & close_loc.ge(80) & vol.le(1.8) & rr.between(0.8, 1.5) & (~risk_txt)
        next_mask, next_note = _v4493_next_confirm_proxy(df, a_safe)
        target_mask = next_mask | core2
        cand = df[target_mask].copy()
        max_n = _env_int('CLOSING_BET_A_CONFIRM_ENTRY_MAX', 80)
        if len(cand) > max_n:
            cand = cand.head(max_n)
        rows_prev_close = []
        rows_next_open = []
        rows_next_close = []
        rows_prev_high = []
        fetch_ok = 0
        prev_high_possible = 0
        for _, row in cand.iterrows():
            entry_signal = _safe_float(row.get('close_entry', row.get('close', row.get('_close', 0))), 0.0)
            stoploss = _safe_float(row.get('stoploss', 0), 0.0)
            if stoploss <= 0 and entry_signal > 0:
                stoploss = entry_signal * 0.95
            # 기존 전일종가 진입 성과는 CSV 계산값을 그대로 비교 기준으로 사용한다.
            if int(row.name in df.index) and not pd.isna(row.get('rule35_pnl', np.nan)):
                rows_prev_close.append({
                    'ok': 1,
                    'pnl': _safe_float(row.get('rule35_pnl', np.nan), np.nan),
                    'win': _safe_float(row.get('rule35_win', 0), 0),
                    'hit3': _safe_float(row.get('rule35_hit3', row.get('hit3_before_stop', 0)), 0),
                    'hit5': _safe_float(row.get('rule35_hit5', row.get('hit5_before_stop', 0)), 0),
                    'stop': _safe_float(row.get('stop_before_3', row.get('rule35_stop', 0)), 0),
                })
            price_pack = _v4495_fetch_price_bars_for_signal(row, hold_days=hold_days)
            if not price_pack:
                continue
            fetch_ok += 1
            signal_bar = price_pack['signal_bar']
            future = price_pack['future'].copy()
            next_bar = price_pack['next_bar']
            next_open = _safe_float(next_bar.get('Open', 0), 0.0)
            next_close = _safe_float(next_bar.get('Close', 0), 0.0)
            signal_high = _safe_float(signal_bar.get('High', 0), 0.0)
            next_high = _safe_float(next_bar.get('High', 0), 0.0)
            if next_open > 0:
                rows_next_open.append(_v4495_simulate_rule35_bars(future, next_open, stoploss, hold_days=hold_days))
            if next_close > 0:
                # 종가 확인 후 진입은 다음 거래일부터 평가한다.
                rows_next_close.append(_v4495_simulate_rule35_bars(future.iloc[1:].copy(), next_close, stoploss, hold_days=hold_days))
            if signal_high > 0 and next_high >= signal_high:
                prev_high_possible += 1
                # 전일고가 회복가 진입은 intraday 순서를 알 수 없어 프록시로 다음날 포함 평가한다.
                rows_prev_high.append(_v4495_simulate_rule35_bars(future, signal_high, stoploss, hold_days=hold_days))
        lines.append('[🎯 A-SAFE 다음날확인 진입가별 백테스트 — v4.4.9.34]')
        lines.append(f'- 대상: A-RETEST CORE② 또는 A-SAFE 다음날확인 프록시 {len(cand)}건 | 가격데이터 확보 {fetch_ok}건 | 전일고가 회복 가능 {prev_high_possible}건')
        lines.append(_v4495_entry_summary_line(rows_prev_close, '① 전일 종가 진입 기존기준'))
        lines.append(_v4495_entry_summary_line(rows_next_open, '② 다음날 시가 진입'))
        lines.append(_v4495_entry_summary_line(rows_next_close, '③ 다음날 종가 확인 후 진입'))
        lines.append(_v4495_entry_summary_line(rows_prev_high, '④ 다음날 전일고가 회복가 진입 프록시'))
        lines.append(f'- 다음날확인 필터 기준: {next_note}')
        lines.append('- 해석: 기존 A-SAFE 다음날확인은 “전일 종가 진입 + 다음날 생존 필터”였습니다. 이 섹션은 실제 다음날 시가/종가/전일고가 회복가 진입 시 성과가 유지되는지 확인합니다.')
        lines.append('- 운용 기준: 다음날 시가 진입이 약하면 시초 추격 금지, 다음날 종가/전일고가 회복가가 양호하면 확인형으로만 운용합니다.')
    except Exception as e:
        lines.append(f'[A-CONFIRM 진입가 검증 오류] {type(e).__name__}: {e}')
    return lines


# =============================================================
# v4.4.9.13 LIVE OPERATION GUARD — VC 태그별 성과검증
# =============================================================

def _v4499_vc_performance_lines(df: pd.DataFrame) -> list[str]:
    """A-CONFIRM VC 라벨 재보정판.

    v4.4.9.10 결과에서 기존 VC-WATCH가 VC-SAFE보다 승률/손절이 좋았다.
    따라서 v4.4.9.13은 내부 계산은 유지하되 실전 라벨을 재정의한다.
    - 기존 VC-WATCH  -> A-CONFIRM PRIME: 적당히 식었고 가격지지 유지, 힘이 남은 후보
    - 기존 VC-SAFE   -> A-CONFIRM CALM: 안정적으로 식었지만 상대적으로 힘이 약할 수 있는 후보
    VC 라벨은 하드필터가 아니라 우선순위/해석 보조 태그로 사용한다.
    """
    lines: list[str] = []
    try:
        if df is None or df.empty or not _v439_bool_env('CLOSING_BET_A_CONFIRM_VC_PERF_CHECK', '1'):
            return lines

        amount = _v449_base_liquidity_series(df)
        close_loc = _v449_close_loc_series(df)
        vol = _v449_vol_ratio_series(df)
        rr = _v449_rr_series(df)
        risk_txt = _v449_safe_contains_series(df, ['risk_tags', 'tags', 'reason', 'final_reason', 'comment'], r'RISK|위험|과열|종가위치약함|RR낮음|RR과대')

        a_retest = _v449_mask_a_retest(df)
        a_safe = _v449_mask_a_retest_safe(df)
        core2 = a_retest & amount.ge(5000) & close_loc.ge(80) & vol.le(1.8) & rr.between(0.8, 1.5) & (~risk_txt)
        next_mask, next_note = _v4493_next_confirm_proxy(df, a_safe)
        a_confirm = next_mask | (_v439_num_series(df, 'a_confirm_live', 0).astype(int).eq(1))
        target = (a_confirm | core2)

        # 1) 저장된 실시간 VC 컬럼 우선 사용.
        stored_safe = _v439_num_series(df, 'a_vc_safe', 0).astype(int).eq(1)
        stored_watch_raw = _v439_num_series(df, 'a_vc_watch', 0).astype(int).eq(1)
        stored_watch = stored_watch_raw & (~stored_safe)
        stored_cnt = int((target & (stored_safe | stored_watch)).sum())

        # 2) 전용 컬럼이 없거나 모두 0건이면 백테스트용 프록시로 직접 계산.
        support_col = _v439_num_series(df, 'a_support_hold', np.nan)
        amount_hold_col = _v439_num_series(df, 'a_amount_hold', np.nan)
        no_bear_col = _v439_num_series(df, 'a_no_bear_expand', np.nan)
        vcp_col = _v439_num_series(df, 'a_vol_contract_prev', np.nan)
        vc3_col = _v439_num_series(df, 'a_vol_contract_3d', np.nan)
        today_vr_col = _v439_num_series(df, 'a_today_vol_ratio', np.nan)

        has_support_data = bool(('a_support_hold' in df.columns) and support_col.notna().any() and int(support_col.fillna(0).sum()) > 0)
        has_no_bear_data = bool(('a_no_bear_expand' in df.columns) and no_bear_col.notna().any() and int(no_bear_col.fillna(0).sum()) > 0)

        support = support_col.astype(float).fillna(0).eq(1) if has_support_data else close_loc.ge(70)
        amount_hold = amount_hold_col.astype(float).fillna(0).eq(1) if 'a_amount_hold' in df.columns and int(amount_hold_col.fillna(0).sum()) > 0 else amount.ge(3000)
        no_bear = no_bear_col.astype(float).fillna(0).eq(1) if has_no_bear_data else ((~risk_txt) & vol.le(2.5))
        today_vr = today_vr_col.where(today_vr_col.notna() & today_vr_col.gt(0), vol)
        vol_cool_strong = vcp_col.fillna(0).astype(float).eq(1) | vc3_col.fillna(0).astype(float).eq(1) | today_vr.between(0.01, 1.20) | vol.between(0.01, 1.20)
        vol_cool_ok = vcp_col.fillna(0).astype(float).eq(1) | vc3_col.fillna(0).astype(float).eq(1) | today_vr.between(0.01, 1.80) | vol.between(0.01, 1.80)

        if stored_cnt > 0:
            legacy_safe = target & stored_safe
            legacy_watch = target & stored_watch
            fallback_used = False
        else:
            # legacy_safe: v4.4.9.10의 기존 VC-SAFE. 너무 차분해져 힘이 약해질 수 있으므로 CALM으로 재라벨링한다.
            legacy_safe = target & close_loc.ge(80) & support & amount_hold & no_bear & vol_cool_strong & rr.between(0.8, 1.5) & (~risk_txt)
            # legacy_watch: v4.4.9.10에서 더 좋은 성과를 보인 중간 냉각/가격지지 구간. PRIME으로 재라벨링한다.
            legacy_watch = target & (~legacy_safe) & close_loc.ge(70) & support & no_bear & vol_cool_ok & rr.between(0.5, 1.8) & (~risk_txt)
            fallback_used = True

        prime = legacy_watch
        calm = legacy_safe
        need = target & (~prime) & (~calm)

        target_df = df[target].copy()
        prime_df = df[target & prime].copy()
        calm_df = df[target & calm].copy()
        need_df = df[need].copy()
        core2_df = df[core2].copy()
        core2_prime_df = df[core2 & prime].copy()
        core2_calm_df = df[core2 & calm].copy()
        confirm_df = df[a_confirm].copy()
        confirm_prime_df = df[a_confirm & prime].copy()
        confirm_calm_df = df[a_confirm & calm].copy()

        def _line(sub: pd.DataFrame, label: str, min_n: int = 10) -> str:
            base = _v439_short_trade_line(sub, label)
            n = len(sub) if sub is not None else 0
            if n <= 0:
                return base + ' | 데이터없음'
            if n < min_n:
                return base + f' | 표본부족({n}건)'
            return base + ' | ' + _v449_promotion_decision(sub, short_term=True, min_n=min_n)

        lines.append('[🧊 A-CONFIRM VC 라벨 재보정 — v4.4.9.34]')
        lines.append(_line(target_df, 'A-CONFIRM/CORE 전체'))
        lines.append(_line(prime_df, '🔥 A-CONFIRM PRIME — 기존 VC-WATCH', min_n=5))
        lines.append(_line(calm_df, '🟢 A-CONFIRM CALM — 기존 VC-SAFE', min_n=5))
        lines.append(_line(need_df, '🟡 A-CONFIRM 확인필요', min_n=5))
        lines.append(_line(core2_df, 'A-RETEST CORE② 전체'))
        lines.append(_line(core2_prime_df, 'A-RETEST CORE② PRIME', min_n=5))
        lines.append(_line(core2_calm_df, 'A-RETEST CORE② CALM', min_n=5))
        lines.append(_line(confirm_df, 'A-SAFE 다음날확인 전체'))
        lines.append(_line(confirm_prime_df, 'A-SAFE 다음날확인 PRIME', min_n=5))
        lines.append(_line(confirm_calm_df, 'A-SAFE 다음날확인 CALM', min_n=5))
        lines.append(
            f"- VC 라벨 진단: 대상 {int(target.sum())}건 | 저장VC {stored_cnt}건 | "
            f"fallback {'사용' if fallback_used else '미사용'} | "
            f"PRIME {int((target & prime).sum())}건 | CALM {int((target & calm).sum())}건 | 확인필요 {int(need.sum())}건 | "
            f"가격지지80+ {int((target & close_loc.ge(80)).sum())}건 | "
            f"거래량냉각≤1.2 {int((target & vol_cool_strong).sum())}건 | "
            f"거래량냉각≤1.8 {int((target & vol_cool_ok).sum())}건"
        )
        lines.append(f'- 라벨 기준: 기존 VC-WATCH를 PRIME, 기존 VC-SAFE를 CALM으로 재정의합니다. 다음날확인 기준: {next_note}')
        lines.append('- 해석: v4.4.9.13은 VC를 하드필터로 쓰지 않고 A-CONFIRM 안에서 우선순위를 나누는 태그로 사용합니다. PRIME이 전체 대비 손절을 낮추고 승률을 높이는지 반복 확인합니다.')
    except Exception as e:
        lines.append(f'[A-CONFIRM VC 라벨재보정 오류] {type(e).__name__}: {e}')
    return lines


# =============================================================
# v4.4.9.24 H-CORE PROMOTION AUDIT — 신고가거자름 핵심셀 승격검증
# =============================================================
def _v44924_h_core_promotion_audit_lines(df: pd.DataFrame, compact: bool = True) -> list[str]:
    """H-CORE/H-TRIANGLE 핵심셀을 실전 FINAL KICK 후보로 올릴 수 있는지 검증한다.

    목적은 전략을 바로 매수 신호로 승격하는 것이 아니라,
    1) H-CORE 12건 이상 확보 여부,
    2) 돌파봉 거래대금 × Vol60 핵심셀별 성과,
    3) 다음날 확인/지정가 운용 가능성,
    4) 실패 샘플의 공통 위험 요인
    을 텔레그램 요약에서 바로 보이게 만드는 것이다.
    """
    lines: list[str] = []
    try:
        if df is None or df.empty or not _v439_bool_env('CLOSING_BET_H_CORE_PROMOTION_AUDIT', '1'):
            return lines
        if 'mode' not in df.columns:
            return lines

        def _mask(fn_name: str) -> pd.Series:
            return _v439_mask(fn_name, df).reindex(df.index).fillna(False).astype(bool)

        h_all_m = _mask('_bt_mask_h_all')
        if int(h_all_m.sum()) <= 0:
            return lines

        h_struct_m = _mask('_bt_mask_h_struct_safe')
        h_tri_m = _mask('_bt_mask_h_triangle')
        h_tri_struct_m = _mask('_bt_mask_h_triangle_struct')
        h_tri_lowvol_m = _mask('_bt_mask_h_triangle_lowvol')
        h_core_500_1000_m = _mask('_bt_mask_h_core_500_1000_vol23')
        h_core_500_700_m = _mask('_bt_mask_h_core_500_700_vol23')
        h_core_700_1000_m = _mask('_bt_mask_h_core_700_1000_vol23')
        h_core_300_500_m = _mask('_bt_mask_h_core_300_500_vol35')
        h_core_1000_2000_m = _mask('_bt_mask_h_core_1000_2000_vol23')
        h_watch_3_4_m = _mask('_bt_mask_h_watch_500_1000_vol30_40')
        h_overheat_m = _mask('_bt_mask_h_vol_overheat')
        h_fast_m = _mask('_bt_mask_h_fast_8x')
        h_core_m = _mask('_bt_mask_h_v427_core_union') | h_tri_m | h_core_500_1000_m | h_core_300_500_m | h_core_1000_2000_m

        h_all = df[h_all_m].copy()
        h_core = df[h_core_m].copy()
        if h_core.empty and int(h_fast_m.sum()) <= 0:
            lines.append('[🧪 H-CORE/H-FAST 타이밍 감사 — v4.4.9.34]')
            lines.append('- H 전체 후보는 있으나 H-CORE/삼각/거래대금×Vol60 핵심셀과 H-FAST 8배+ 후보가 없습니다. 실시간에서는 H를 계속 숨김/관찰로 유지합니다.')
            return lines
        if h_core.empty:
            lines.append('[🧪 H-CORE/H-FAST 타이밍 감사 — v4.4.9.34]')
            lines.append('- H-CORE/삼각/거래대금×Vol60 핵심셀은 없지만, H-FAST 8배+ 후보가 있어 빠른익절형만 별도로 검증합니다.')

        def _nser(xdf: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
            if xdf is None or xdf.empty:
                return pd.Series(dtype=float)
            if col in xdf.columns:
                return pd.to_numeric(xdf[col], errors='coerce').fillna(default)
            return pd.Series(default, index=xdf.index, dtype=float)

        def _first_numeric_series(xdf: pd.DataFrame, cols: list[str], default: float = 0.0) -> pd.Series:
            if xdf is None or xdf.empty:
                return pd.Series(dtype=float)
            for c in cols:
                if c in xdf.columns:
                    s = pd.to_numeric(xdf[c], errors='coerce')
                    if s.notna().sum() > 0:
                        return s.fillna(default)
            return pd.Series(default, index=xdf.index, dtype=float)

        def _close_loc_series(xdf: pd.DataFrame) -> pd.Series:
            if xdf is None or xdf.empty:
                return pd.Series(dtype=float)
            for c in ['close_loc_pct', 'close_loc', 'close_location', '종가위치']:
                if c in xdf.columns:
                    s = pd.to_numeric(xdf[c], errors='coerce')
                    if s.notna().sum() > 0:
                        return s.fillna(0)
            hi = _first_numeric_series(xdf, ['_high', 'High', 'high'], np.nan)
            lo = _first_numeric_series(xdf, ['_low', 'Low', 'low'], np.nan)
            cl = _first_numeric_series(xdf, ['_close', 'Close', 'close'], np.nan)
            rng = (hi - lo).replace(0, np.nan)
            return ((cl - lo) / rng * 100.0).replace([np.inf, -np.inf], np.nan).fillna(0)

        amount_all = _first_numeric_series(df, ['breakout_amount_b', 'amount_b', 'amount', 'amount_100m', '거래대금'], 0)
        entry_amount_all = _first_numeric_series(df, ['amount_b', 'entry_amount_b', 'amount', 'amount_100m', '거래대금'], 0)
        bvol_all = _first_numeric_series(df, ['breakout_vol60_ratio', 'breakout_vol_ratio', 'h_breakout_vol60'], 0)
        days_all = _first_numeric_series(df, ['days_since_high_breakout', 'h_days_since_breakout'], 999)
        pullback_all = _first_numeric_series(df, ['high_dryup_pullback_pct', 'high_pullback_pullback_pct', 'h_pullback_pct'], 999)
        bret_all = _first_numeric_series(df, ['breakout_day_ret_pct', 'h_breakout_day_ret_pct'], 0)
        bclose_all = _first_numeric_series(df, ['breakout_close_loc_pct', 'h_breakout_close_loc_pct'], 0)
        bwick_all = _first_numeric_series(df, ['breakout_upper_wick_pct', 'h_breakout_upper_wick_pct'], 999)
        close_loc_all = _close_loc_series(df)
        vol_all = _first_numeric_series(df, ['vol_ratio', 'volume_ratio', 'vma_ratio', '거래량비'], 0)
        rr_all = _first_numeric_series(df, ['rr', 'RR', 'risk_reward'], 0)
        highvol_all = _first_numeric_series(df, ['h_high_volatility'], 0).gt(0)
        pre_score_all = _first_numeric_series(df, ['h_pre_structure_score'], 0)

        def _rate(mask: pd.Series | None) -> float:
            try:
                if mask is None or len(mask) == 0:
                    return 0.0
                return float(mask.mean() * 100.0)
            except Exception:
                return 0.0

        def _verdict(xdf: pd.DataFrame, min_n: int = 10) -> str:
            n = len(xdf) if xdf is not None else 0
            if n <= 0:
                return '데이터없음'
            pnl = _v439_mean(xdf, 'rule35_pnl')
            win = _v439_rate(xdf, 'rule35_win') if 'rule35_win' in xdf.columns else _v439_rate(xdf, 'close_win_hd')
            stop = _v439_rate(xdf, 'stop_before_3') if 'stop_before_3' in xdf.columns else _v439_rate(xdf, 'rule35_stop')
            if n < min_n:
                if pnl >= 1.2 and win >= 65 and stop <= 25:
                    return f'🧪 표본부족 우수({n}건)'
                return f'표본부족({n}건)'
            if pnl >= 1.2 and win >= 65 and stop <= 25:
                return '✅ FINAL_KICK WATCH 승격검토'
            if pnl >= 0.5 and win >= 55 and stop <= 40:
                return '🟡 연구/관찰 유지'
            return '⚠️ 실시간 숨김 유지'

        def _quality_line(xdf: pd.DataFrame, label: str, min_n: int = 10) -> str:
            n = len(xdf) if xdf is not None else 0
            if n <= 0:
                return f'- {label}: 0건'
            base = _v439_short_trade_line(xdf, label)
            ix = xdf.index
            bamt = amount_all.reindex(ix).fillna(0)
            entry_amt = entry_amount_all.reindex(ix).fillna(0)
            bvol = bvol_all.reindex(ix).fillna(0)
            days = days_all.reindex(ix).fillna(999)
            pull = pullback_all.reindex(ix).fillna(999)
            bret = bret_all.reindex(ix).fillna(0)
            bclose = bclose_all.reindex(ix).fillna(0)
            bwick = bwick_all.reindex(ix).fillna(999)
            highvol = highvol_all.reindex(ix).fillna(False)
            pre_score = pre_score_all.reindex(ix).fillna(0)
            max_col = 'path_max_high_ret' if 'path_max_high_ret' in xdf.columns else ('ret_max_high_hd' if 'ret_max_high_hd' in xdf.columns else 'rule35_pnl')
            dd_col = 'path_pre_plus3_min_low_ret' if 'path_pre_plus3_min_low_ret' in xdf.columns else ('path_min_low_ret' if 'path_min_low_ret' in xdf.columns else 'rule35_pnl')
            close_col = 'ret_close_hd' if 'ret_close_hd' in xdf.columns else ('ret_next_close' if 'ret_next_close' in xdf.columns else 'rule35_pnl')
            maxr = _nser(xdf, max_col, 0)
            dd = _nser(xdf, dd_col, 0)
            close_ret = _nser(xdf, close_col, 0)
            return (
                f'{base} | 돌파대금중앙 {bamt.median():.0f}억 / 진입대금중앙 {entry_amt.median():.0f}억 | '
                f'Vol60중앙 {bvol.median():.2f}배 | 돌파후일수중앙 {days.median():.1f}일 | 눌림중앙 {pull.median():.1f}% | '
                f'돌파봉 등락/종가위치/윗꼬리 중앙 {bret.median():.1f}%/{bclose.median():.0f}%/{bwick.median():.0f}% | '
                f'최대평균/중앙 {maxr.mean():.2f}/{maxr.median():.2f}% | 종가평균 {close_ret.mean():.2f}% | +3전흔들림중앙 {dd.median():.2f}% | '
                f'고변동 {_rate(highvol):.1f}% | 직전구조점수중앙 {pre_score.median():.1f} | {_verdict(xdf, min_n=min_n)}'
            )

        lines.append('[🧪 H-CORE/H-FAST 타이밍 감사 — v4.4.9.34]')
        lines.append('- 목적: H-CORE는 WATCH로 유지하면서, H-FAST 8배+가 +3 빠른익절 전용 후보로 분리 가능한지 타이밍·거래대금·눌림 구간을 별도 검증합니다.')
        lines.append('- 원칙: H는 신고가/돌파 계열이라 추격위험이 큽니다. 통과하더라도 “즉시 강매수”가 아니라 H-CORE WATCH/지정가/다음날 지지확인 후보로 먼저 올립니다.')
        lines.append(f'- 표본: H 전체 {int(h_all_m.sum())}건 | H-STRUCT {int(h_struct_m.sum())}건 | H-CORE UNION {int(h_core_m.sum())}건 | TRIANGLE {int(h_tri_m.sum())}건 | 500~1000×2~3 {int(h_core_500_1000_m.sum())}건 | 300~500×3~5 {int(h_core_300_500_m.sum())}건 | 1000~2000×2~3 {int(h_core_1000_2000_m.sum())}건')
        lines.append(_quality_line(h_all, 'H 전체 STRICT'))
        lines.append(_quality_line(df[h_struct_m], 'H-STRUCT SAFE 모수'))
        lines.append(_quality_line(h_core, '✅ H-CORE UNION 삼각/핵심셀', min_n=10))
        lines.append('')
        lines.append('H-CORE 구성별 성과')
        parts: list[tuple[str, pd.Series, int]] = [
            ('🧊 H-TRIANGLE 직전삼각', h_tri_m, 5),
            ('🧊 H-TRIANGLE STRUCT', h_tri_struct_m, 5),
            ('🧊 H-TRIANGLE LOWVOL', h_tri_lowvol_m, 5),
            ('🟢 H-CORE 500~1000억×2~3배', h_core_500_1000_m, 5),
            ('  · H-CORE 500~700억×2~3배', h_core_500_700_m, 5),
            ('  · H-CORE 700~1000억×2~3배', h_core_700_1000_m, 5),
            ('🟣 H-CORE 300~500억×3~5배', h_core_300_500_m, 5),
            ('🔵 H-CORE 1000~2000억×2~3배', h_core_1000_2000_m, 5),
            ('🟡 비교 H-WATCH 500~1000억×3~4배', h_watch_3_4_m, 5),
            ('🔥 비교 H-FAST 8배+', h_fast_m, 5),
            ('⚠️ 비교 H-OVERHEAT 5~8배', h_overheat_m, 5),
        ]
        for label, m, mn in parts:
            sub = df[m.reindex(df.index).fillna(False)]
            if not sub.empty or not compact:
                lines.append(_quality_line(sub, label, min_n=mn))

        ix = h_core.index
        bamt = amount_all.reindex(ix).fillna(0)
        bvol = bvol_all.reindex(ix).fillna(0)
        days = days_all.reindex(ix).fillna(999)
        pull = pullback_all.reindex(ix).fillna(999)
        cloc = close_loc_all.reindex(ix).fillna(0)
        evol = vol_all.reindex(ix).fillna(0)
        err = rr_all.reindex(ix).fillna(0)

        def _core_sub(mask: pd.Series) -> pd.DataFrame:
            return h_core[mask.reindex(ix).fillna(False)]

        lines.append('')
        lines.append('H-CORE 거래대금/Vol60/타점 버킷')
        bucket_defs: list[tuple[str, pd.Series]] = [
            ('H-CORE 돌파대금 300~500억', bamt.ge(300) & bamt.lt(500)),
            ('H-CORE 돌파대금 500~700억', bamt.ge(500) & bamt.lt(700)),
            ('H-CORE 돌파대금 700~1000억', bamt.ge(700) & bamt.lt(1000)),
            ('H-CORE 돌파대금 1000~2000억', bamt.ge(1000) & bamt.lt(2000)),
            ('H-CORE 돌파대금 2000억+', bamt.ge(2000)),
            ('H-CORE Vol60 1.5~2배', bvol.ge(1.5) & bvol.lt(2.0)),
            ('H-CORE Vol60 2~3배', bvol.ge(2.0) & bvol.lt(3.0)),
            ('H-CORE Vol60 3~5배', bvol.ge(3.0) & bvol.lt(5.0)),
            ('H-CORE Vol60 5배+', bvol.ge(5.0)),
            ('H-CORE 돌파후 1일', days.eq(1)),
            ('H-CORE 돌파후 2~3일', days.ge(2) & days.le(3)),
            ('H-CORE 돌파후 4~5일', days.ge(4) & days.le(5)),
            ('H-CORE 돌파후 6~7일', days.ge(6) & days.le(7)),
            ('H-CORE 눌림 1~3%', pull.ge(1) & pull.lt(3)),
            ('H-CORE 눌림 3~6%', pull.ge(3) & pull.lt(6)),
            ('H-CORE 눌림 6~10%', pull.ge(6) & pull.le(10)),
            ('H-CORE 진입종가위치 70%+', cloc.ge(70)),
            ('H-CORE 진입거래량 ≤1.2', evol.gt(0) & evol.le(1.2)),
            ('H-CORE RR 0.8~1.8', err.between(0.8, 1.8)),
        ]
        for label, m in bucket_defs:
            sub = _core_sub(m)
            if not sub.empty or not compact:
                lines.append(_quality_line(sub, label, min_n=5))

        # 다음날 확인 프록시가 있는 경우만 추가한다. 없으면 실시간 조건으로 남긴다.
        next_masks: list[tuple[str, pd.Series]] = []
        for c in ['ret_next_close', 'next_close_ret', 'next_day_ret', 'ret_1d', 'close_ret_1d']:
            if c in df.columns:
                s = pd.to_numeric(df[c], errors='coerce')
                if s.notna().sum() > 0:
                    next_masks.append((f'H-CORE 다음날종가 + ({c}>0)', s.reindex(ix).fillna(-999).gt(0)))
                    break
        for c in ['next_reclaim_prev_high', 'next_day_reclaim_high', 'next_high_reclaim', 'next_close_above_signal_high']:
            if c in df.columns:
                s = pd.to_numeric(df[c], errors='coerce')
                if s.notna().sum() > 0:
                    next_masks.append((f'H-CORE 다음날 전일고가/신호고가 회복({c})', s.reindex(ix).fillna(0).gt(0)))
                    break
        if next_masks:
            lines.append('')
            lines.append('H-CORE 다음날 확인 프록시')
            for label, m in next_masks:
                lines.append(_quality_line(_core_sub(m), label, min_n=5))
        else:
            lines.append('- 다음날 확인 프록시: 사용 가능한 컬럼 없음 → 실시간에서는 다음날 전일고가 회복·양봉·돌파권 지지를 확인 조건으로 둡니다.')

        # v4.4.9.28: H-FAST 8배+는 H-CORE와 성격이 다르므로 별도 타이밍 감사로 분리한다.
        # 목표는 보유형 승격이 아니라 '+3 빠른익절 전용' 후보인지 검증하는 것이다.
        if _v439_bool_env('CLOSING_BET_H_FAST_TIMING_AUDIT', '1'):
            h_fast = df[h_fast_m].copy()
            h_overheat = df[h_overheat_m].copy()

            def _fast_timing_line(xdf: pd.DataFrame, label: str, min_n: int = 5) -> str:
                base = _quality_line(xdf, label, min_n=min_n)
                n = len(xdf) if xdf is not None else 0
                if n <= 0:
                    return base
                d3 = _nser(xdf, 'path_first_plus3_day', 0)
                d5 = _nser(xdf, 'path_first_plus5_day', 0)
                d3_pos = d3[d3.gt(0)]
                d5_pos = d5[d5.gt(0)]
                d3_avg = float(d3_pos.mean()) if len(d3_pos) else 0.0
                d5_avg = float(d5_pos.mean()) if len(d5_pos) else 0.0
                d3_fast = float(d3.between(1, 2).mean() * 100.0) if len(d3) else 0.0
                d3_5 = float(d3.between(1, 5).mean() * 100.0) if len(d3) else 0.0
                return base + f' | +3평균 {d3_avg:.1f}일 | +3 1~2일 {d3_fast:.1f}% | +3 1~5일 {d3_5:.1f}% | +5평균 {d5_avg:.1f}일'

            lines.append('')
            lines.append('[🔥 H-FAST 8배+ 타이밍 감사 — v4.4.9.28]')
            lines.append('- 목적: H-FAST 8배+를 H-CORE 보유형과 분리해, 보유가 아니라 +3 빠른익절 전용으로 쓸 수 있는지 확인합니다.')
            lines.append('- 운용가정: 시초/종가 추격 금지, 지정가만 가능, +3 우선 자동익절, +5는 일부만, 5일선·돌파권 이탈 시 빠른 제외입니다.')
            lines.append(_fast_timing_line(h_fast, '🔥 H-FAST 전체 8배+', min_n=5))
            lines.append(_fast_timing_line(h_overheat, '⚠️ 비교 H-OVERHEAT 5~8배', min_n=5))

            if not h_fast.empty:
                fx = h_fast.index
                fbamt = amount_all.reindex(fx).fillna(0)
                feamt = entry_amount_all.reindex(fx).fillna(0)
                fbvol = bvol_all.reindex(fx).fillna(0)
                fdays = days_all.reindex(fx).fillna(999)
                fpull = pullback_all.reindex(fx).fillna(999)
                fcloc = close_loc_all.reindex(fx).fillna(0)
                fevol = vol_all.reindex(fx).fillna(0)
                frr = rr_all.reindex(fx).fillna(0)
                fhighvol = highvol_all.reindex(fx).fillna(False)
                fpre = pre_score_all.reindex(fx).fillna(0)
                fplus3 = _nser(h_fast, 'path_first_plus3_day', 0)
                fpre3 = _nser(h_fast, 'path_pre_plus3_min_low_ret', 0)

                def _fast_sub(mask: pd.Series) -> pd.DataFrame:
                    return h_fast[mask.reindex(fx).fillna(False)]

                lines.append('H-FAST 거래대금/Vol60/타점 버킷')
                fast_defs: list[tuple[str, pd.Series]] = [
                    ('H-FAST 돌파대금 <300억', fbamt.lt(300)),
                    ('H-FAST 돌파대금 300~500억', fbamt.ge(300) & fbamt.lt(500)),
                    ('H-FAST 돌파대금 500~1000억', fbamt.ge(500) & fbamt.lt(1000)),
                    ('H-FAST 돌파대금 1000억+', fbamt.ge(1000)),
                    ('H-FAST 진입대금 <100억', feamt.lt(100)),
                    ('H-FAST 진입대금 100~300억', feamt.ge(100) & feamt.lt(300)),
                    ('H-FAST 진입대금 300~500억', feamt.ge(300) & feamt.lt(500)),
                    ('H-FAST 진입대금 500억+', feamt.ge(500)),
                    ('H-FAST Vol60 8~12배', fbvol.ge(8) & fbvol.lt(12)),
                    ('H-FAST Vol60 12배+', fbvol.ge(12)),
                    ('H-FAST 돌파후 1일', fdays.eq(1)),
                    ('H-FAST 돌파후 2~3일', fdays.ge(2) & fdays.le(3)),
                    ('H-FAST 돌파후 4~5일', fdays.ge(4) & fdays.le(5)),
                    ('H-FAST 눌림 0~3%', fpull.ge(0) & fpull.lt(3)),
                    ('H-FAST 눌림 3~6%', fpull.ge(3) & fpull.lt(6)),
                    ('H-FAST 눌림 6~10%', fpull.ge(6) & fpull.le(10)),
                    ('H-FAST 진입종가위치 70%+', fcloc.ge(70)),
                    ('H-FAST 진입거래량 ≤1.2', fevol.gt(0) & fevol.le(1.2)),
                    ('H-FAST RR 0.8~1.8', frr.between(0.8, 1.8)),
                    ('H-FAST +3 1~2일 도달', fplus3.between(1, 2)),
                    ('H-FAST +3전 흔들림 -3% 이내', fpre3.ge(-3)),
                    ('H-FAST 고변동 제외', ~fhighvol),
                    ('H-FAST 직전구조점수 1+', fpre.ge(1)),
                ]
                for label, m in fast_defs:
                    sub = _fast_sub(m)
                    if not sub.empty or not compact:
                        lines.append(_fast_timing_line(sub, label, min_n=5))

                # H-FAST 샘플은 성과상위/실패를 분리해 과최적화를 확인한다.
                def _fmt_fast_row(r: pd.Series) -> str:
                    dt = str(r.get('signal_date', r.get('date', '')) or '')[:10]
                    nm = str(r.get('name', r.get('종목명', '')) or '').strip()
                    cd = str(r.get('code', r.get('ticker', '')) or '').strip().zfill(6) if str(r.get('code', r.get('ticker', '')) or '').strip() else ''
                    title = f'{nm}({cd})' if nm and cd else (nm or cd or '종목')
                    ixr = r.name
                    pnl = _safe_float(r.get('rule35_pnl', 0), 0.0)
                    max_col_f = 'path_max_high_ret' if 'path_max_high_ret' in r.index else ('ret_max_high_hd' if 'ret_max_high_hd' in r.index else 'rule35_pnl')
                    dd_col_f = 'path_pre_plus3_min_low_ret' if 'path_pre_plus3_min_low_ret' in r.index else ('path_min_low_ret' if 'path_min_low_ret' in r.index else 'rule35_pnl')
                    maxr = _safe_float(r.get(max_col_f, 0), 0.0)
                    dd = _safe_float(r.get(dd_col_f, 0), 0.0)
                    d3 = _safe_int(r.get('path_first_plus3_day', 0), 0)
                    return (
                        f'- {dt} {title} | H-FAST 8배+ | 3/5 {pnl:.2f}% | 최대 {maxr:.2f}% | 흔들림 {dd:.2f}% | +3일 {d3} | '
                        f'돌파대금 {amount_all.reindex([ixr]).fillna(0).iloc[0]:.0f}억 | 진입대금 {entry_amount_all.reindex([ixr]).fillna(0).iloc[0]:.0f}억 | '
                        f'Vol60 {bvol_all.reindex([ixr]).fillna(0).iloc[0]:.2f}배 | 돌파후 {days_all.reindex([ixr]).fillna(0).iloc[0]:.0f}일 | '
                        f'눌림 {pullback_all.reindex([ixr]).fillna(0).iloc[0]:.1f}% | 종가위치 {close_loc_all.reindex([ixr]).fillna(0).iloc[0]:.0f}%'
                    )

                detail_n_fast = _env_int('CLOSING_BET_H_FAST_AUDIT_DETAIL_N', _env_int('CLOSING_BET_H_CORE_AUDIT_DETAIL_N', '5' if compact else '20'))
                if detail_n_fast > 0:
                    sort_col_f = 'path_max_high_ret' if 'path_max_high_ret' in h_fast.columns else ('ret_max_high_hd' if 'ret_max_high_hd' in h_fast.columns else 'rule35_pnl')
                    ft = h_fast.copy()
                    ft['_sort_max'] = _nser(ft, sort_col_f, 0)
                    lines.append('H-FAST 대표 샘플 — 성과상위')
                    for _, r in ft.sort_values('_sort_max', ascending=False).head(detail_n_fast).iterrows():
                        lines.append('  ' + _fmt_fast_row(r))
                    ft['_sort_pnl'] = _nser(ft, 'rule35_pnl', 0)
                    fbad = ft[ft['_sort_pnl'].lt(0)]
                    if not fbad.empty:
                        lines.append('H-FAST 실패/손절 샘플')
                        for _, r in fbad.sort_values('_sort_pnl', ascending=True).head(detail_n_fast).iterrows():
                            lines.append('  ' + _fmt_fast_row(r))

            lines.append('- H-FAST 판정: 8배+라고 무조건 좋은 것이 아니라, 5~8배 H-OVERHEAT와 분리해 +3 빠른익절·짧은 보유·지정가 전용으로만 추적합니다.')

        def _class_for_index(idx) -> str:
            tags = []
            if bool(h_tri_m.reindex([idx]).fillna(False).iloc[0]): tags.append('TRI')
            if bool(h_core_500_1000_m.reindex([idx]).fillna(False).iloc[0]): tags.append('500~1000×2~3')
            if bool(h_core_300_500_m.reindex([idx]).fillna(False).iloc[0]): tags.append('300~500×3~5')
            if bool(h_core_1000_2000_m.reindex([idx]).fillna(False).iloc[0]): tags.append('1000~2000×2~3')
            return '+'.join(tags) or 'H-CORE'

        def _fmt_row(r: pd.Series) -> str:
            dt = str(r.get('signal_date', r.get('date', '')) or '')[:10]
            nm = str(r.get('name', r.get('종목명', '')) or '').strip()
            cd = str(r.get('code', r.get('ticker', '')) or '').strip().zfill(6) if str(r.get('code', r.get('ticker', '')) or '').strip() else ''
            title = f'{nm}({cd})' if nm and cd else (nm or cd or '종목')
            ixr = r.name
            pnl = _safe_float(r.get('rule35_pnl', 0), 0.0)
            max_col = 'path_max_high_ret' if 'path_max_high_ret' in r.index else ('ret_max_high_hd' if 'ret_max_high_hd' in r.index else 'rule35_pnl')
            dd_col = 'path_pre_plus3_min_low_ret' if 'path_pre_plus3_min_low_ret' in r.index else ('path_min_low_ret' if 'path_min_low_ret' in r.index else 'rule35_pnl')
            maxr = _safe_float(r.get(max_col, 0), 0.0)
            dd = _safe_float(r.get(dd_col, 0), 0.0)
            return (
                f'- {dt} {title} | {_class_for_index(ixr)} | 3/5 {pnl:.2f}% | 최대 {maxr:.2f}% | 흔들림 {dd:.2f}% | '
                f'돌파대금 {amount_all.reindex([ixr]).fillna(0).iloc[0]:.0f}억 | Vol60 {bvol_all.reindex([ixr]).fillna(0).iloc[0]:.2f}배 | '
                f'돌파후 {days_all.reindex([ixr]).fillna(0).iloc[0]:.0f}일 | 눌림 {pullback_all.reindex([ixr]).fillna(0).iloc[0]:.1f}% | 종가위치 {close_loc_all.reindex([ixr]).fillna(0).iloc[0]:.0f}%'
            )

        detail_n = _env_int('CLOSING_BET_H_CORE_AUDIT_DETAIL_N', '6' if compact else '20')
        if detail_n > 0 and not h_core.empty:
            sort_col = 'path_max_high_ret' if 'path_max_high_ret' in h_core.columns else ('ret_max_high_hd' if 'ret_max_high_hd' in h_core.columns else 'rule35_pnl')
            tmp = h_core.copy()
            tmp['_sort_max'] = _nser(tmp, sort_col, 0)
            lines.append('')
            lines.append('H-CORE 대표 샘플 — 성과상위')
            for _, r in tmp.sort_values('_sort_max', ascending=False).head(detail_n).iterrows():
                lines.append('  ' + _fmt_row(r))
            bad_col = 'rule35_pnl'
            tmp['_sort_pnl'] = _nser(tmp, bad_col, 0)
            bad = tmp[tmp['_sort_pnl'].lt(0)]
            if not bad.empty:
                lines.append('H-CORE 실패/손절 샘플')
                for _, r in bad.sort_values('_sort_pnl', ascending=True).head(detail_n).iterrows():
                    lines.append('  ' + _fmt_row(r))

        core_decision = _verdict(h_core, min_n=10)
        lines.append('')
        lines.append(f'- 승격판정: H-CORE UNION = {core_decision}')
        lines.append('- 운용안: 통과 시에도 FINAL KICK에서는 ✅ 즉시매수보다 🟡 H-CORE WATCH/지정가만 가능으로 시작합니다. 다음날 시초 +2% 이상 갭상승 추격 금지, 전일고가/돌파권 재지지 확인 후 +3 우선 익절을 기본으로 둡니다.')
        lines.append("- 다음 단계: H-CORE는 반복 통과 전까지 WATCH 유지, H-FAST는 8배+·짧은 눌림·빠른 +3 도달이 반복되면 '초단기 +3 전용' 후보로 별도 승격 검토합니다. H-OVERHEAT 5~8배는 계속 실패하면 H-RISK로 고정합니다.")
    except Exception as e:
        lines.append(f'[H-CORE 승격검증 오류] {type(e).__name__}: {e}')
    return lines

def _v449_fast_live_policy_lines() -> list[str]:
    return [
        '[⚡ FAST LIVE 운용분리 원칙]',
        '- 실시간 FAST: LP-SAFE/LP-D23, L 5000억+, S2/S-NEUTRAL, IT-ACCEL/I-MAIN 핵심만 우선 출력.',
        '- H-CORE는 WATCH를 유지하고, H-FAST는 PRIME 조건을 만족한 경우에만 +3 초단타 전용 후보로 표시합니다.',
        '- 승격 조건을 반복 통과한 패턴만 이후 SAFE/CORE 섹션으로 올립니다. 현재 H-CORE는 WATCH 감사 중, H-FAST는 +3 전용 감사 중, A-RETEST/C-RECLAIM/B-CONFIRM은 검증 단계입니다.',
    ]


def _v44934_cleanup_legacy_labels(text: str) -> str:
    """v4.4.9.34: 텔레그램/로그에 남는 과거 패치 버전명을 현재 운용 기준으로 통일한다."""
    try:
        s = str(text or '')
        replacements = {
            'v4.4.8.1': '누적검증',
            'v4.4.9.13': '누적검증',
            'v4.4.9.16': '누적검증',
            'v4.4.9.17': '누적검증',
            'v4.4.9.18': '누적검증',
            'v4.4.9.24': '누적검증',
            'v4.4.9.28': '누적검증',
            'v4.4.9.33': 'v4.4.9.43',
            'v4.4.9.34': 'v4.4.9.43',
            'v4.4.9.35': 'v4.4.9.43',
            'v4.4.9.36': 'v4.4.9.43',
            'v4.4.9.37': 'v4.4.9.43',
            'v4.4.9.38': 'v4.4.9.43',
            'v4.4.9.39': 'v4.4.9.43',
            'v4.4.9.40': 'v4.4.9.43',
            'v4.4.9.41': 'v4.4.9.43',
        }
        for old, new in replacements.items():
            s = s.replace(old, new)
        # 과거 패치번호가 제목 안에 중복으로 남는 경우 가독성 보정
        s = s.replace('— 누적검증]', '— 누적검증]')
        return s
    except Exception:
        return str(text or '')



def _v44937_first_num_series(df: pd.DataFrame, cols: list[str], default: float = 0.0) -> pd.Series:
    """v4.4.9.39: 백테스트 CSV 컬럼명이 버전별로 달라도 첫 유효 숫자열을 안전하게 가져온다."""
    try:
        for c in cols:
            if c in df.columns:
                s = pd.to_numeric(df[c], errors='coerce')
                if s.notna().sum() > 0:
                    return s.fillna(default)
    except Exception:
        pass
    return pd.Series(default, index=df.index)


def _v44937_first_str_series(df: pd.DataFrame, cols: list[str], default: str = '') -> pd.Series:
    try:
        for c in cols:
            if c in df.columns:
                s = df[c].astype(str).fillna(default)
                if len(s) > 0:
                    return s
    except Exception:
        pass
    return pd.Series(default, index=df.index)


def _v44937_lp_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """v4.4.9.39: LP-SAFE 모수를 백테스트 CSV에서 최대한 재구성한다."""
    if df is None or df.empty:
        return df.iloc[0:0].copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    mode = _v439_str_series(df, 'mode')
    m = _v439_mask('_bt_mask_l_pullback_safe', df)
    if bool(m.any()):
        return df[m].copy()
    cls = _v44937_first_str_series(df, ['lp_class', 'lp_label', 'lp_decision'], '')
    m2 = mode.eq('LP') & cls.str.contains('SAFE', na=False)
    if bool(m2.any()):
        return df[m2].copy()
    return df[mode.eq('LP')].copy()


def _v44937_lp_masks(lp: pd.DataFrame) -> dict[str, pd.Series]:
    """v4.4.9.39: LP를 STABLE / POWER-A / POWER-B / POWER-STABLE / CALM-STABLE로 나눈다."""
    idx = lp.index
    if lp is None or lp.empty:
        z = pd.Series(False, index=idx)
        return {'d1': z, 'd23': z, 'd45': z, 'explosion': z, 'exp_a': z, 'exp_b': z, 'stable': z, 'power_stable': z, 'calm_stable': z}
    days = _v44937_first_num_series(lp, ['lp_days_after_gap', 'lp_days_since_gap', 'lp_gap_days', 'days_after_gap'], 0)
    tb = _v44937_first_str_series(lp, ['lp_timing_bucket', 'lp_timing', 'lp_bucket'], '')
    d1 = days.eq(1) | tb.str.contains('D1', na=False)
    d23 = days.between(2, 3) | tb.str.contains('D23', na=False)
    d45 = days.between(4, 5) | tb.str.contains('D45', na=False)
    gap_amt = _v44937_first_num_series(lp, ['lp_gap_amount_b', 'leader_gap_amount_b', 'gap_amount_b', 'gap_bar_amount_b'], 0)
    gap_pct = _v44937_first_num_series(lp, ['lp_gap_pct', 'gap_pct'], 0)
    amount = _v44937_first_num_series(lp, ['amount_b', 'trade_amount_b', 'entry_amount_b'], 0)
    close_loc = _v44937_first_num_series(lp, ['close_loc_pct', 'lp_entry_close_loc_pct', 'entry_close_loc_pct'], 0)
    volr = _v44937_first_num_series(lp, ['vol_ratio', 'today_vol_ratio', 'entry_vol_ratio'], 0)
    lp_vs_gap = _v44937_first_num_series(lp, ['lp_volume_vs_gap', 'volume_vs_gap', 'lp_vol_vs_gap'], 0)
    pullback = _v44937_first_num_series(lp, ['lp_pullback_pct', 'pullback_pct'], 0)
    hold_gap_s = _v44937_first_num_series(lp, ['lp_gap_zone_hold', 'gap_zone_hold'], 1)
    ma_hold_s = _v44937_first_num_series(lp, ['lp_ma_hold', 'ma_hold'], 1)
    common = (d23 | d45) & days.between(1, 5) & hold_gap_s.fillna(1).ge(1) & ma_hold_s.fillna(1).ge(1)

    exp_a = (
        common
        & gap_amt.ge(5000)
        & gap_pct.between(3, 12)
        & amount.ge(1000)
        & close_loc.between(65, 85)
        & lp_vs_gap.between(0.45, 0.95)
        & volr.between(1.15, 2.30)
        & pullback.between(0, 12)
    )
    exp_b_raw = (
        common
        & gap_amt.ge(3000)
        & gap_pct.between(2.5, 15)
        & amount.ge(1000)
        & close_loc.between(68, 92)
        & lp_vs_gap.between(0.35, 1.10)
        & volr.between(1.05, 2.80)
        & pullback.between(0, 14)
    )
    exp_b = exp_b_raw & ~exp_a
    explosion = exp_a | exp_b
    stable = ~explosion

    # POWER-STABLE: 폭발형 거래량 재점화는 아니지만, D23/D45·리더갭대금·종가위치가 강해 다음날 고가가 잘 나올 수 있는 안정형.
    power_stable = (
        stable
        & (d23 | d45)
        & gap_amt.ge(5000)
        & amount.ge(1000)
        & close_loc.ge(70)
        & lp_vs_gap.between(0.35, 1.05)
        & volr.between(0.80, 1.15)
    )
    # CALM-STABLE: 거래량이 확실히 식고 종가가 버틴 저손절 안정형.
    calm_stable = (
        stable
        & (d23 | d45)
        & amount.ge(1000)
        & close_loc.ge(65)
        & volr.lt(0.80)
        & lp_vs_gap.le(0.85)
    )
    return {
        'd1': d1.fillna(False),
        'd23': d23.fillna(False),
        'd45': d45.fillna(False),
        'explosion': explosion.fillna(False),
        'exp_a': exp_a.fillna(False),
        'exp_b': exp_b.fillna(False),
        'stable': stable.fillna(False),
        'power_stable': power_stable.fillna(False),
        'calm_stable': calm_stable.fillna(False),
    }


def _v44937_lp_flow_line(sub: pd.DataFrame, label: str, min_n: int = 5) -> str:
    """v4.4.9.39: LP 분리검증용 속도/흔들림/다음날 지표."""
    try:
        base = _v439_short_trade_line(sub, label)
        if sub is None or sub.empty:
            return base
        d3 = _v44937_first_num_series(sub, ['path_first_plus3_day', 'first_plus3_day', 'plus3_day'], 0)
        d5 = _v44937_first_num_series(sub, ['path_first_plus5_day', 'first_plus5_day', 'plus5_day'], 0)
        pre3 = _v44937_first_num_series(sub, ['path_pre_plus3_min_low_ret', 'pre_plus3_min_low_ret', 'path_first3d_min_low_ret'], np.nan)
        maxr = _v44937_first_num_series(sub, ['path_max_high_ret', 'ret_max_high_hd', 'max_high_ret'], np.nan)
        next_high = _v44937_first_num_series(sub, ['ret_next_high', 'next_high_ret', 'ret_next_day_high'], np.nan)
        min_low = _v44937_first_num_series(sub, ['path_min_low_ret', 'ret_min_low_hd', 'min_low_ret'], np.nan)
        # v4.4.9.43: 감사 지표 정합성 보정. 흔들림/최대하락은 양수로 표시되면 오해가 커서 0 이하로 클립한다.
        try:
            pre3 = pre3.where(pre3.le(0) | pre3.isna(), 0.0)
            min_low = min_low.where(min_low.le(0) | min_low.isna(), 0.0)
        except Exception:
            pass
        d3_pos = d3[d3.gt(0)]
        d5_pos = d5[d5.gt(0)]
        d3_avg = float(d3_pos.mean()) if len(d3_pos) else 0.0
        d5_avg = float(d5_pos.mean()) if len(d5_pos) else 0.0
        d3_12 = float(d3.between(1, 2).mean() * 100.0) if len(d3) else 0.0
        d3_15 = float(d3.between(1, 5).mean() * 100.0) if len(d3) else 0.0
        d5_15 = float(d5.between(1, 5).mean() * 100.0) if len(d5) else 0.0
        extras = [
            f'+3평균 {d3_avg:.1f}일',
            f'+3 1~2일 {d3_12:.1f}%',
            f'+3 1~5일 {d3_15:.1f}%',
            f'+5 1~5일 {d5_15:.1f}%',
        ]
        if pre3.notna().sum() > 0:
            extras.append(f'+3전흔들림중앙 {pre3.median():.2f}%')
        if next_high.notna().sum() > 0:
            extras.append(f'다음날고가평균 {next_high.mean():.2f}%')
        if maxr.notna().sum() > 0:
            extras.append(f'최대상승중앙 {maxr.median():.2f}%')
        if min_low.notna().sum() > 0:
            extras.append(f'최대하락중앙 {min_low.median():.2f}%')
        return base + ' | ' + ' | '.join(extras)
    except Exception as e:
        return f'- {label}: LP 흐름 계산 오류 {type(e).__name__}: {e}'


def _v44937_lp_sample_lines(sub: pd.DataFrame, title: str, top: bool = True, n: int = 3) -> list[str]:
    """v4.4.9.39: LP-POWER PRIME 성공/실패 대표 샘플."""
    lines: list[str] = []
    try:
        if sub is None or sub.empty:
            return lines
        score = _v44937_first_num_series(sub, ['path_max_high_ret', 'ret_max_high_hd', 'rule35_pnl'], 0) if top else _v44937_first_num_series(sub, ['rule35_pnl', 'ret_close_hd', 'path_min_low_ret'], 0)
        order = score.sort_values(ascending=not top).index[:max(1, n)]
        if len(order) == 0:
            return lines
        lines.append(title)
        for ix in order:
            row = sub.loc[ix]
            date = str(row.get('signal_date', row.get('date', '')))[:10]
            code = str(row.get('code', '')).zfill(6) if str(row.get('code', '')).strip() else ''
            name = str(row.get('name', row.get('종목명', code)) or code)
            pnl = float(pd.to_numeric(pd.Series([row.get('rule35_pnl', 0)]), errors='coerce').fillna(0).iloc[0])
            maxr = float(pd.to_numeric(pd.Series([row.get('path_max_high_ret', row.get('ret_max_high_hd', 0))]), errors='coerce').fillna(0).iloc[0])
            d3 = float(pd.to_numeric(pd.Series([row.get('path_first_plus3_day', 0)]), errors='coerce').fillna(0).iloc[0])
            amount = float(pd.to_numeric(pd.Series([row.get('amount_b', 0)]), errors='coerce').fillna(0).iloc[0])
            cloc = float(pd.to_numeric(pd.Series([row.get('close_loc_pct', 0)]), errors='coerce').fillna(0).iloc[0])
            volr = float(pd.to_numeric(pd.Series([row.get('vol_ratio', 0)]), errors='coerce').fillna(0).iloc[0])
            days = float(pd.to_numeric(pd.Series([row.get('lp_days_after_gap', row.get('lp_days_since_gap', 0))]), errors='coerce').fillna(0).iloc[0])
            lines.append(f'  - {date} {name}({code}) | 3/5 {pnl:.2f}% | 최대 {maxr:.2f}% | +3일 {d3:.0f} | 대금 {amount:.0f}억 | 종가위치 {cloc:.0f}% | Vol {volr:.2f} | 갭후 {days:.0f}일')
    except Exception as e:
        lines.append(f'{title} 생성 오류: {type(e).__name__}: {e}')
    return lines


def _v44937_lp_explosion_audit_lines(df: pd.DataFrame) -> list[str]:
    """v4.4.9.39: v37/v38의 단일 폭발형을 A/B와 POWER/CALM 안정형으로 확장 검증한다."""
    lines: list[str] = []
    try:
        lp = _v44937_lp_safe_df(df)
        if lp is None or lp.empty:
            return lines
        masks = _v44937_lp_masks(lp)
        stable = lp[masks['stable']]
        explosion = lp[masks['explosion']]
        exp_a = lp[masks.get('exp_a', pd.Series(False, index=lp.index))]
        exp_b = lp[masks.get('exp_b', pd.Series(False, index=lp.index))]
        power_stable = lp[masks.get('power_stable', pd.Series(False, index=lp.index))]
        calm_stable = lp[masks.get('calm_stable', pd.Series(False, index=lp.index))]
        d23 = masks['d23']
        d45 = masks['d45']

        lines.append('[🔥 LP POWER PRIME + STABLE 분리검증 — v4.4.9.43]')
        lines.append('- 목적: v37 폭발형이 1건으로 너무 좁게 잡힌 문제를 보완해, POWER-A 엄격형·POWER-B 강력추천 지정가·POWER STABLE·CALM STABLE로 나눠 봅니다.')
        lines.append(_v44937_lp_flow_line(lp, 'LP-SAFE 전체'))
        lines.append(_v44937_lp_flow_line(stable, 'LP-STABLE 전체'))
        lines.append(_v44937_lp_flow_line(power_stable, '⚡ LP-POWER STABLE'))
        lines.append(_v44937_lp_flow_line(calm_stable, '🧊 LP-CALM STABLE'))
        lines.append(_v44937_lp_flow_line(explosion, '🔥 LP-POWER PRIME 전체'))
        lines.append(_v44937_lp_flow_line(exp_a, '🔥 LP-POWER PRIME A 엄격형'))
        lines.append(_v44937_lp_flow_line(exp_b, '🔥 LP-POWER PRIME 확장형'))

        if len(lp[d23]) > 0:
            lines.append(_v44937_lp_flow_line(lp[d23], 'LP-D23 전체'))
        if len(stable[d23.reindex(stable.index).fillna(False)]) > 0:
            lines.append(_v44937_lp_flow_line(stable[d23.reindex(stable.index).fillna(False)], 'LP-D23 STABLE'))
        if len(explosion[d23.reindex(explosion.index).fillna(False)]) > 0:
            lines.append(_v44937_lp_flow_line(explosion[d23.reindex(explosion.index).fillna(False)], '🔥 LP-D23 EXPLOSION A/B'))
        if len(lp[d45]) > 0:
            lines.append(_v44937_lp_flow_line(lp[d45], 'LP-D45 전체'))
        if len(stable[d45.reindex(stable.index).fillna(False)]) > 0:
            lines.append(_v44937_lp_flow_line(stable[d45.reindex(stable.index).fillna(False)], 'LP-D45 STABLE'))
        if len(explosion[d45.reindex(explosion.index).fillna(False)]) > 0:
            lines.append(_v44937_lp_flow_line(explosion[d45.reindex(explosion.index).fillna(False)], '🔥 LP-D45 EXPLOSION A/B'))

        # 감사 대상과 핵심성과 LP-SAFE 모수가 다를 수 있음을 명시한다.
        lines.append(f'- 감사대상: LP-AUDIT {len(lp)}건. 도달일/다음날고가/흔들림 컬럼이 없는 신호는 일부 속도지표가 0 또는 평가불가로 표시될 수 있습니다.')
        if len(explosion) == 0:
            lines.append('- 판정: 이번 구간에서는 확장 폭발형 A/B도 없습니다. 폭발형은 최근 강력추천 지정가 후보 리플레이 구간에서 별도 확인이 필요합니다.')
        else:
            lines.append('- 판정기준: POWER-A는 엄격형, POWER-B는 강력추천 지정가 확장형입니다. B의 손절선행이 20~25%를 넘으면 종가진입 승격 금지, 강력추천 지정가/다음날 관찰만 유지합니다.')
            lines += _v44937_lp_sample_lines(explosion, 'LP-POWER PRIME 성공·강세 샘플', top=True, n=5)
            fail_col = _v44937_first_num_series(explosion, ['rule35_pnl', 'path_min_low_ret'], 0)
            fail_sub = explosion[fail_col.lt(0)] if bool(fail_col.lt(0).any()) else explosion.tail(0)
            if not fail_sub.empty:
                lines += _v44937_lp_sample_lines(fail_sub, 'LP-POWER PRIME 실패·손절 샘플', top=False, n=5)
        if len(power_stable) > 0:
            lines += _v44937_lp_sample_lines(power_stable, 'LP-POWER STABLE 대표 샘플', top=True, n=3)
        lines.append('- 운용해석: STABLE은 종가진입 메인, POWER-STABLE은 안정형 중 빠른 +3 후보, EXPLOSION A/B는 종가추격 금지·다음날 폭발 관찰 1순위로 분리합니다.')
    except Exception as e:
        lines.append(f'[LP-POWER PRIME 분리검증 오류] {type(e).__name__}: {e}')
    return lines


def _v44934_s_momentum_core_lines(df: pd.DataFrame) -> list[str]:
    """v4.4.9.34: 장문의 ST30 원문감사 대신 실전 S 라벨만 압축한다."""
    lines: list[str] = []
    try:
        if df is None or df.empty:
            return lines
        s_safe = df[_v439_mask('_bt_mask_s_core_safe', df)].copy()
        if s_safe.empty:
            # S-SAFE 마스크가 비어도 S/NEUTRAL 핵심 줄은 보여준다.
            s_safe = df[_v439_mask('_bt_mask_s_core_neutral', df) | _v439_mask('_bt_mask_s2', df)].copy()
        if s_safe.empty:
            return lines
        amount = _v439_num_series(s_safe, 'amount_b')
        close_loc = _v439_num_series(s_safe, 'close_loc_pct')
        vol = _v439_num_series(s_safe, 'vol_ratio')
        rr = _v439_num_series(s_safe, 'rr')
        st30_pass = _v439_mask('_bt_mask_st30_reclaim', s_safe)
        st30_miss = ~st30_pass
        quality = close_loc.ge(75) & vol.le(1.8) & rr.between(0.8, 1.8)
        calm = close_loc.ge(75) & vol.le(1.2) & rr.between(0.8, 1.5)
        lines.append('[🔥 S-CALM / S-MOMENTUM / S-RECLAIM 핵심압축 — v4.4.9.43]')
        lines.append(_v439_short_trade_line(s_safe, 'S-SAFE 전체'))
        reclaim_prime = s_safe[st30_pass & amount.ge(1000) & quality]
        reclaim_watch = s_safe[st30_pass & amount.between(300, 1000, inclusive='left') & quality]
        momentum_prime = s_safe[st30_miss & amount.ge(1000) & quality]
        momentum_calm = s_safe[st30_miss & amount.ge(1000) & calm]
        momentum_watch = s_safe[st30_miss & amount.between(300, 1000, inclusive='left') & quality]
        liquidity_risk = s_safe[st30_miss & amount.lt(1000)]
        liquidity_exclude = s_safe[st30_miss & amount.lt(300)]
        lines.append(_v439_short_trade_line(reclaim_prime, 'S-RECLAIM PRIME: ST30통과·1000억+·품질군'))
        if len(reclaim_watch) > 0:
            lines.append(_v439_short_trade_line(reclaim_watch, 'S-RECLAIM WATCH: ST30통과·300~1000억'))
        if len(momentum_calm) > 0:
            lines.append(_v439_short_trade_line(momentum_calm, 'S-MOMENTUM CALM: ST30미통과·1000억+·거래량냉각'))
        lines.append(_v439_short_trade_line(momentum_prime, 'S-MOMENTUM PRIME: ST30미통과·1000억+·품질군'))
        if len(momentum_watch) > 0:
            lines.append(_v439_short_trade_line(momentum_watch, 'S-MOMENTUM WATCH: ST30미통과·300~1000억'))
        lines.append(_v439_short_trade_line(liquidity_risk, 'S-LIQUIDITY RISK: ST30미통과·1000억 미만'))
        if len(liquidity_exclude) > 0:
            lines.append(_v439_short_trade_line(liquidity_exclude, 'S-LIQUIDITY EXCLUDE: ST30미통과·300억 미만'))
        lines.append('- 운용해석: ST30은 하드필터가 아니라 가점/강등 태그입니다. ST30 미통과라도 1000억+·종가75%+·RR정상이면 S-MOMENTUM으로 살립니다. 그중 Vol≤1.2·RR0.8~1.5의 CALM을 PRIME보다 우선하고, 저거래대금은 RISK/EXCLUDE로 낮춥니다.')
    except Exception as e:
        lines.append(f'[S-MOMENTUM 핵심압축 오류] {type(e).__name__}: {e}')
    return lines



def _v44935_geo_risk_interpretation_lines(df: pd.DataFrame) -> list[str]:
    """v4.4.9.35: 사용자가 중동/전쟁 리스크 구간을 검증할 때 보기 위한 패턴 해석 요약.
    외부뉴스를 직접 판정하지 않고, 백테스트 창 안에서 어떤 패턴이 방어력/수익성을 보였는지만 압축한다.
    """
    lines: list[str] = []
    try:
        if df is None or df.empty:
            return lines
        mode = _v439_str_series(df, 'mode')
        amount = _v439_num_series(df, 'amount_b')
        gap = _v439_num_series(df, 'gap_pct')
        close_loc = _v439_num_series(df, 'close_loc_pct')
        lp_safe = df[_v439_mask('_bt_mask_l_pullback_safe', df)] if callable(globals().get('_bt_mask_l_pullback_safe')) else df[mode.eq('LP')]
        if lp_safe.empty:
            lp_safe = df[mode.eq('LP')]
        l_all = df[_v439_mask('_bt_mask_leader_gap_all', df) | mode.eq('L')]
        l_geo = l_all[amount.loc[l_all.index].ge(5000) & gap.loc[l_all.index].between(6, 12) & close_loc.loc[l_all.index].ge(70)] if len(l_all) else pd.DataFrame()
        s_safe = df.iloc[0:0]
        # direct mask only: avoid expensive row-wise apply in large backtests.
        try:
            sm = _bt_mask_s_safe(df) if callable(globals().get('_bt_mask_s_safe')) else pd.Series(False, index=df.index)
            if isinstance(sm, pd.Series):
                s_safe = df[sm.fillna(False).astype(bool)]
        except Exception:
            pass
        if not s_safe.empty:
            st30 = _v439_str_series(s_safe, 'st30_label', '')
            st30_pass = st30.str.contains('ST30-RECLAIM|ST30-WEEKLY', na=False)
            st30_miss = ~st30_pass
            amt = _v439_num_series(s_safe, 'amount_b')
            q75 = _v439_num_series(s_safe, 'close_loc_pct').ge(75)
            vol12 = _v439_num_series(s_safe, 'vol_ratio').le(1.2)
            vol18 = _v439_num_series(s_safe, 'vol_ratio').le(1.8)
            rr = _v439_num_series(s_safe, 'rr')
            rr15 = rr.between(0.8, 1.5)
            rr18 = rr.between(0.8, 1.8)
            s_calm = s_safe[st30_miss & amt.ge(1000) & q75 & vol12 & rr15]
            s_prime = s_safe[st30_miss & amt.ge(1000) & q75 & vol18 & rr18]
            s_low = s_safe[st30_miss & amt.lt(300)]
        else:
            s_calm = s_prime = s_low = pd.DataFrame()
        i_all = df[_v439_mask('_bt_mask_i_core_all', df)] if callable(globals().get('_bt_mask_i_core_all')) else pd.DataFrame()
        i_note = ''
        if not i_all.empty:
            try:
                i_en = _i_main_enriched_df(i_all) if callable(globals().get('_i_main_enriched_df')) else i_all
                i_accel = i_en[_v439_num_series(i_en, 'imain_accel', 0).astype(int).eq(1)]
                i20 = _v439_mean(i_accel, 'i_ret_close_20d') if not i_accel.empty else np.nan
                if not pd.isna(i20) and i20 < 0:
                    i_note = 'I-MAIN ACCEL 최근성과 음수 → 신규추격 강등'
                elif not i_accel.empty:
                    i_note = 'I-MAIN ACCEL은 재지지 확인형으로만 참고'
            except Exception:
                pass
        lines.append('[🌍 GEO-RISK / 헤드라인 리스크 구간 해석 — v4.4.9.43]')
        lines.append('- 목적: 전쟁·유가·환율·VIX 같은 헤드라인 리스크가 큰 구간에서 어떤 패턴이 버텼는지 비교합니다. 외부뉴스를 자동판정하지 않고, 현재 백테스트 창의 패턴 성과만 해석합니다.')
        if len(lp_safe) > 0:
            lines.append(_v439_short_trade_line(lp_safe, 'GEO 우선 1: LP-SAFE 리더갭 눌림재지지'))
        if len(l_geo) > 0:
            lines.append(_v439_short_trade_line(l_geo, 'GEO 우선 2: L-GEO PRIME 갭6~12·5000억+·종가70+'))
        if len(s_calm) > 0:
            lines.append(_v439_short_trade_line(s_calm, 'GEO 우선 3: S-MOMENTUM CALM 거래량냉각'))
        if len(s_prime) > 0:
            lines.append(_v439_short_trade_line(s_prime, 'GEO 보조: S-MOMENTUM PRIME 1000억+ 품질군'))
        if len(s_low) > 0:
            lines.append(_v439_short_trade_line(s_low, 'GEO 경계: S 저유동성 300억 미만'))
        if i_note:
            lines.append(f'- 중기 I 해석: {i_note}. 리스크 장세에서는 중기 신규추격보다 5MA/20MA 재지지를 기다립니다.')
        lines.append('- 운용해석: 헤드라인 리스크 장세에서는 선취형 중기추격보다 “대금 붙은 리더갭 눌림재지지”와 “거래량이 식은 상단 응축”을 우선합니다.')
    except Exception as e:
        lines.append(f'[GEO-RISK 해석 오류] {type(e).__name__}: {e}')
    return lines


def _v440_l_mega_upper_followup_lines(df: pd.DataFrame) -> list[str]:
    """v4.4.9.43: L-MEGA 상한가성 후속관찰 감사 섹션."""
    lines: list[str] = []
    try:
        if df is None or df.empty:
            return lines
        mode = _v439_str_series(df, 'mode')
        amount = _v439_num_series(df, 'amount_b')
        if 'leader_gap_amount_b' in df.columns:
            amount = amount.where(amount.gt(0), _v439_num_series(df, 'leader_gap_amount_b'))
        gap = _v439_num_series(df, 'gap_pct')
        close_loc = _v439_num_series(df, 'close_loc_pct')
        wick = _v439_num_series(df, 'upper_wick_pct') if 'upper_wick_pct' in df.columns else _v439_num_series(df, 'wick_pct')
        l_all = df[(mode.eq('L')) | _v439_mask('_bt_mask_leader_gap_all', df)]
        if l_all.empty:
            return lines
        l_mega = l_all[(amount.loc[l_all.index].ge(10000)) | (gap.loc[l_all.index].ge(6))]
        upper = l_mega[(amount.loc[l_mega.index].ge(5000)) & (close_loc.loc[l_mega.index].ge(98)) & (wick.loc[l_mega.index].le(3))]
        non_upper = l_mega.drop(index=upper.index, errors='ignore')
        if upper.empty and l_mega.empty:
            return lines
        lines.append('[💰 L-MEGA 상한가 후속관찰 감사 — v4.4.9.43]')
        lines.append('- 목적: 상한가성 L-MEGA를 신규 종가추격 후보가 아니라 보유자 대응/다음날 기준선 지지 확인 후보로 분리합니다.')
        if len(l_mega) > 0:
            lines.append(_v439_short_trade_line(l_mega, 'L-MEGA 전체'))
        if len(upper) > 0:
            lines.append(_v439_short_trade_line(upper, 'L-MEGA 상한가성 잠금형'))
            try:
                next_high_col = 'next_high_ret' if 'next_high_ret' in upper.columns else 'd1_high_ret'
                if next_high_col in upper.columns:
                    nh = pd.to_numeric(upper[next_high_col], errors='coerce').dropna()
                    if len(nh) > 0:
                        lines.append(f'- 다음날 고가 평균: {nh.mean():.2f}% | 중앙: {nh.median():.2f}%')
            except Exception:
                pass
        if len(non_upper) > 0:
            lines.append(_v439_short_trade_line(non_upper, 'L-MEGA 일반형(상한잠금 제외)'))
        # 시초가 컬럼이 있는 경우에만 갭출발별 참고를 추가한다.
        open_candidates = ['next_open_ret', 'd1_open_ret', 'next_day_open_ret']
        open_col = next((c for c in open_candidates if c in upper.columns), '') if len(upper) > 0 else ''
        if open_col:
            opn = pd.to_numeric(upper[open_col], errors='coerce')
            for label, mask in [
                ('다음날 +5% 이상 갭출발', opn.ge(5)),
                ('다음날 보합~+3% 출발', opn.between(0, 3)),
                ('다음날 -3~0% 눌림출발', opn.between(-3, 0)),
            ]:
                sub = upper[mask]
                if len(sub) > 0:
                    lines.append(_v439_short_trade_line(sub, label))
        lines.append('- 운용해석: 상한가성 잠금형은 강도는 높지만 신규 종가추격보다 다음날 전일 종가/상한가 기준선 지지, VWAP 위 첫 눌림 재돌파, 거래량 증가 장대음봉 회피가 핵심입니다.')
    except Exception as e:
        lines.append(f'[L-MEGA 상한가 후속 감사 오류] {type(e).__name__}: {e}')
    return lines



def _v44942_sj6_score_series(df: pd.DataFrame) -> pd.Series:
    """v4.4.9.43: 신·좁·깔·거·조·재 점수 산출. 후보 생성에는 절대 개입하지 않는 overlay 전용."""
    try:
        if df is None or df.empty:
            return pd.Series(dtype='float64')
        close_loc = _v439_num_series(df, 'close_loc_pct')
        wick = _v439_num_series(df, 'wick_pct', 999)
        if wick.eq(999).all():
            wick = _v439_num_series(df, 'upper_wick_pct', 999)
        amount = _v439_num_series(df, 'amount_b')
        if 'leader_gap_amount_b' in df.columns:
            amount = amount.where(amount.gt(0), _v439_num_series(df, 'leader_gap_amount_b'))
        vol = _v439_num_series(df, 'vol_ratio')
        vol50 = _v439_num_series(df, 'vol50_ratio')
        rr = _v439_num_series(df, 'rr')
        disp = _v439_num_series(df, 'disparity20')
        near = _v439_num_series(df, 'near_high120')
        runup20 = _v439_num_series(df, 'runup20')
        mode = _v439_str_series(df, 'mode')
        lp_days = _v439_num_series(df, 'lp_days_after_gap', 0)
        if lp_days.eq(0).all():
            lp_days = _v439_num_series(df, 'lp_days_since_gap', 0)
        mat = _v439_num_series(df, 'i_material_proxy_score', 0)
        sin = (near.ge(98) | close_loc.ge(90) | _v439_num_series(df, 'leader_gap_new_high_120', 0).ge(1) | _v439_num_series(df, 'new_high_52w', 0).ge(1))
        narrow = (disp.between(100, 108) | rr.between(0.85, 1.45) | (mode.eq('LP') & lp_days.between(1, 5)))
        clean = close_loc.ge(85) & wick.le(10)
        money = amount.ge(1000) & (vol.ge(1.15) | vol50.ge(2.0) | amount.ge(3000))
        adjust = ((mode.eq('LP') & lp_days.between(1, 5)) | mat.ge(3) | (runup20.le(20) & (disp.eq(0) | disp.le(112))))
        material = (mat.ge(3) | amount.ge(5000))
        return (sin.astype(int) + narrow.astype(int) + clean.astype(int) + money.astype(int) + adjust.astype(int) + material.astype(int)).reindex(df.index).fillna(0).astype(int)
    except Exception:
        return pd.Series(0, index=df.index if isinstance(df, pd.DataFrame) else None)


def _v44942_count_parity_lines(df: pd.DataFrame, raw_count=None) -> list[str]:
    """v4.4.9.43: 후보 수 보존 감사. SJ-6가 하드필터처럼 작동하지 않았는지 텔레그램에 명시."""
    lines: list[str] = []
    try:
        final_n = len(df) if isinstance(df, pd.DataFrame) else 0
        rc = raw_count
        raw_unknown = isinstance(rc, str) or rc is None
        if raw_unknown:
            rc_txt = str(rc or '확인필요')
        else:
            rc = int(rc)
            rc_txt = f'{rc}건'
        lines.append('[🧮 COUNT PARITY / SJ OVERLAY 감사 — v4.4.9.43]')
        lines.append(f'- 원신호 {rc_txt} | 최종검증 {final_n}건 | SJ-6 적용방식: 후보제외 없음, 라벨/가점 전용')
        if not raw_unknown:
            diff = int(rc) - int(final_n)
            if abs(diff) == 0:
                lines.append('- 후보 수 보존: PASS — SJ-6가 하드필터로 작동하지 않았습니다.')
            elif abs(diff) <= max(3, int(rc) * 0.01):
                lines.append(f'- 후보 수 차이 {diff:+d}건: 허용범위 — 중복제거/평가불가 컬럼 차이 가능성.')
            else:
                lines.append(f'- ⚠️ 후보 수 차이 {diff:+d}건: 입력값·유니버스·기간·선택방식 또는 SJ-6 하드필터 개입 여부 점검 필요.')
        expected = _env_int('CLOSING_BET_PARITY_EXPECT_MIN_RAW', 0)
        if expected and (not raw_unknown) and int(rc) < expected:
            lines.append(f'- ⚠️ 기대 원신호 하한 {expected}건 대비 부족: workflow 입력값/유니버스/데이터 로딩 차이를 확인하세요.')
    except Exception as e:
        lines.append(f'[COUNT PARITY 감사 오류] {type(e).__name__}: {e}')
    return lines


def _v44942_sj_overlay_audit_lines(df: pd.DataFrame, raw_count=None) -> list[str]:
    """v4.4.9.43: SJ-6 단독성과보다 기존 패턴과의 교차 성과를 우선 출력."""
    lines: list[str] = []
    try:
        if df is None or df.empty:
            return lines
        sj_score = _v44942_sj6_score_series(df)
        sj5 = sj_score.ge(5)
        sj4 = sj_score.eq(4)
        sj_fail = sj_score.le(3)
        lines.append('[📊 신정재 SJ-6 문맥형 OVERLAY 감사 — v4.4.9.43]')
        lines.append('- 목적: 신·좁·깔·거·조·재를 단독 매수신호가 아니라 패턴별 문맥에 맞는 품질가점·위험강등 라벨로 검증합니다. LP-SAFE에는 강등을 걸지 않습니다.')
        for label, mask in [
            ('🔥 SJ-PRIME 6/6', sj_score.ge(6)),
            ('✅ SJ-SAFE 5/6', sj_score.eq(5)),
            ('🟡 SJ-WATCH 4/6', sj4),
            ('❌ SJ-FAIL 0~3/6', sj_fail),
            ('신좁깔거조재 5/6+ 전체', sj5),
        ]:
            sub = df[mask.fillna(False)]
            if len(sub) > 0:
                lines.append(_v439_short_trade_line(sub, label))

        mode = _v439_str_series(df, 'mode')
        amount = _v439_num_series(df, 'amount_b')
        if 'leader_gap_amount_b' in df.columns:
            amount = amount.where(amount.gt(0), _v439_num_series(df, 'leader_gap_amount_b'))
        gap = _v439_num_series(df, 'gap_pct')
        close_loc = _v439_num_series(df, 'close_loc_pct')
        wick = _v439_num_series(df, 'upper_wick_pct') if 'upper_wick_pct' in df.columns else _v439_num_series(df, 'wick_pct')
        lp_safe = _v44937_lp_safe_df(df)
        lp_idx = lp_safe.index if lp_safe is not None and not lp_safe.empty else []
        l_all_mask = _v439_mask('_bt_mask_leader_gap_all', df) | mode.eq('L')
        l5000_mask = l_all_mask & amount.ge(5000)
        l_geo_mask = l5000_mask & gap.between(6, 12) & close_loc.ge(70)
        upper_mask = l_all_mask & ((amount.ge(10000)) | (gap.ge(6))) & amount.ge(5000) & close_loc.ge(98) & wick.le(3)
        s_mom_mask = pd.Series(False, index=df.index)
        try:
            s_safe = df[_v439_mask('_bt_mask_s_safe', df)] if callable(globals().get('_bt_mask_s_safe')) else df[mode.eq('S')]
            if not s_safe.empty:
                st30 = _v439_str_series(s_safe, 'st30_label', '')
                miss = ~st30.str.contains('ST30-RECLAIM|ST30-WEEKLY', na=False)
                amt_s = _v439_num_series(s_safe, 'amount_b')
                cl_s = _v439_num_series(s_safe, 'close_loc_pct')
                vol_s = _v439_num_series(s_safe, 'vol_ratio')
                rr_s = _v439_num_series(s_safe, 'rr')
                good = miss & amt_s.ge(1000) & cl_s.ge(75) & vol_s.le(1.8) & rr_s.between(0.8, 1.8)
                s_mom_mask.loc[s_safe.index] = good.reindex(s_safe.index).fillna(False).astype(bool)
        except Exception:
            pass

        lines.append('- 교차검증: SJ는 패턴별로 다르게 적용합니다. LP는 참고, L/S는 품질가점, A/B/C는 위험강등 중심입니다.')
        cross_items = []
        if len(lp_idx) > 0:
            cross_items.append(('LP-SAFE + SJ5+', df.index.isin(lp_idx) & sj5))
            cross_items.append(('LP-SAFE + SJ0~3 강등감사', df.index.isin(lp_idx) & sj_fail))
        cross_items += [
            ('L 5000억+ + SJ5+', l5000_mask & sj5),
            ('L-GEO PRIME + SJ5+', l_geo_mask & sj5),
            ('S-MOMENTUM + SJ5+', s_mom_mask & sj5),
            ('L-MEGA 상한후속 + SJ5+', upper_mask & sj5),
            ('L-MEGA 상한후속 + SJ0~3 경계', upper_mask & sj_fail),
        ]
        for label, mask in cross_items:
            sub = df[pd.Series(mask, index=df.index).fillna(False).astype(bool)]
            if len(sub) > 0:
                lines.append(_v439_short_trade_line(sub, label))
        lines.append('- 운용해석: SJ 5/6+는 L/S/A 후보의 추천강도 가점으로만 쓰고, LP-SAFE는 SJ0~3이어도 강등하지 않습니다. SJ0~3은 A/B/C·저유동성·상투성 후보의 위험강등 신호입니다. 상한가성 L-MEGA는 SJ가 좋아도 신규 종가추격이 아니라 다음날 기준선 확인형으로 둡니다.')
    except Exception as e:
        lines.append(f'[SJ OVERLAY 감사 오류] {type(e).__name__}: {e}')
    return lines


def _v44943_first_num_series(df: pd.DataFrame, cols: list[str], default=0.0) -> pd.Series:
    """v4.4.9.43: 여러 컬럼 중 첫 유효 숫자 Series를 반환한다."""
    try:
        idx = df.index if isinstance(df, pd.DataFrame) else []
        out = pd.Series(default, index=idx, dtype=float)
        if df is None or df.empty:
            return out
        filled = pd.Series(False, index=idx)
        for c in cols:
            if c not in df.columns:
                continue
            s = pd.to_numeric(df[c], errors='coerce')
            m = (~filled) & s.notna() & s.ne(0)
            out.loc[m] = s.loc[m]
            filled.loc[m] = True
        return out
    except Exception:
        return pd.Series(default, index=df.index if isinstance(df, pd.DataFrame) else [], dtype=float)


def _v44943_stop_dist_series(df: pd.DataFrame) -> pd.Series:
    """v4.4.9.43: 진입가 대비 가장 가까운 실전 무효/지지선 거리(%)를 추정한다.

    명시 stop 컬럼이 없을 수 있어 close 아래의 prev_close/gap_low/lp support 중 가장 가까운 값을 사용한다.
    """
    try:
        if df is None or df.empty:
            return pd.Series(dtype=float)
        close = _v44943_first_num_series(df, ['entry_price', 'current_price', 'close', '_close'], 0.0)
        explicit = _v44943_first_num_series(df, [
            'stop_price', 'stop_loss_price', 'rule35_stop_price', 'final_stop_price',
            'practical_stop_price', 'calc_stop_price', 'invalid_price', 'invalid_line'
        ], 0.0)
        supports = []
        for cols in [
            ['lp_prev_close', 'prev_close', 'a_prev_close'],
            ['lp_gap_low', 'gap_low', 'day_low', 'low'],
            ['ma5', 'ma10', 'ma20'],
        ]:
            supports.append(_v44943_first_num_series(df, cols, 0.0))
        stop = explicit.copy()
        # 명시 stop이 없으면 close 아래에 있는 지지선 중 가장 가까운 선을 쓴다.
        nearest = pd.Series(0.0, index=df.index, dtype=float)
        for sup in supports:
            valid = (sup > 0) & (close > 0) & (sup < close)
            nearest = nearest.where(~(valid & (sup > nearest)), sup)
        stop = stop.where(stop.gt(0), nearest)
        dist = ((close - stop) / close * 100.0).where((close > 0) & (stop > 0) & (stop < close), np.nan)
        return dist.replace([np.inf, -np.inf], np.nan).clip(lower=0)
    except Exception:
        return pd.Series(np.nan, index=df.index if isinstance(df, pd.DataFrame) else [], dtype=float)


def _v44943_material_score_series(df: pd.DataFrame) -> pd.Series:
    """v4.4.9.43: 재료 강도 프록시. 글로벌/섹터/수급 데이터가 없으면 대금·재료대금 점수로 대체한다."""
    try:
        mat = _v439_num_series(df, 'i_material_proxy_score', 0)
        amount = _v439_num_series(df, 'amount_b')
        if 'leader_gap_amount_b' in df.columns:
            amount = amount.where(amount.gt(0), _v439_num_series(df, 'leader_gap_amount_b'))
        score = mat.clip(lower=0, upper=4).copy()
        score = score.where(score.gt(0), np.select([amount.ge(5000), amount.ge(1000), amount.ge(300)], [3, 2, 1], default=0))
        # 섹터/테마 텍스트가 있으면 1단계 보강
        text_cols = [c for c in ['theme', 'theme_name', 'sector', 'sector_name', 'tags', 'reason', 'material_hint', 'news_hint'] if c in df.columns]
        if text_cols:
            txt = pd.Series('', index=df.index, dtype=str)
            for c in text_cols:
                txt = txt.str.cat(df[c].astype(str).fillna(''), sep=' ')
            kw = txt.str.contains(r'AI|반도체|전력|원전|방산|로봇|바이오|조선|수주|정책|공급|계약|실적', regex=True, case=False, na=False)
            score = score.where(~kw, np.maximum(score, 2))
        return pd.Series(score, index=df.index).astype(float)
    except Exception:
        return pd.Series(0.0, index=df.index if isinstance(df, pd.DataFrame) else [], dtype=float)


def _v44943_bin_line(df: pd.DataFrame, label: str, mask: pd.Series, min_n: int = 1) -> str | None:
    try:
        sub = df[pd.Series(mask, index=df.index).fillna(False).astype(bool)]
        if len(sub) < int(min_n):
            return None
        return _v439_short_trade_line(sub, label)
    except Exception:
        return None


def _v44943_threshold_fail_common_audit_lines(df: pd.DataFrame) -> list[str]:
    """v4.4.9.43: SJ 세부 임계값과 손절선행 후보 공통점을 감사한다."""
    lines: list[str] = []
    try:
        if df is None or df.empty:
            return lines
        lines.append('[🔬 SJ 임계값/손절공통점 감사 — v4.4.9.43]')
        lines.append('- 목적: “의미있는 신고가·좁은 이격·깔끔한 양봉·긴 조정·재료”를 넓게 보지 않고, 손절이 커지는 공통 원인을 구간별로 확인합니다.')
        mode = _v439_str_series(df, 'mode')
        amount = _v439_num_series(df, 'amount_b')
        if 'leader_gap_amount_b' in df.columns:
            amount = amount.where(amount.gt(0), _v439_num_series(df, 'leader_gap_amount_b'))
        wick = _v439_num_series(df, 'wick_pct', 999)
        if wick.eq(999).all():
            wick = _v439_num_series(df, 'upper_wick_pct', 999)
        close_loc = _v439_num_series(df, 'close_loc_pct')
        disp = _v439_num_series(df, 'disparity20')
        rr = _v439_num_series(df, 'rr')
        vol = _v439_num_series(df, 'vol_ratio')
        vol50 = _v439_num_series(df, 'vol50_ratio')
        gap = _v439_num_series(df, 'gap_pct')
        runup20 = _v439_num_series(df, 'runup20')
        near = _v439_num_series(df, 'near_high120')
        mat = _v44943_material_score_series(df)
        stop_dist = _v44943_stop_dist_series(df)
        sj_score = _v44942_sj6_score_series(df)
        stop_col = 'stop_before_3' if 'stop_before_3' in df.columns else ('rule35_stop' if 'rule35_stop' in df.columns else '')
        fail = pd.to_numeric(df[stop_col], errors='coerce').fillna(0).gt(0) if stop_col else pd.Series(False, index=df.index)

        lines.append('- [좁은 이격/손익비] 전고점 이격 컬럼이 없을 때는 20일선 이격·RR·진입가 대비 무효가 거리로 대체합니다.')
        for label, mask in [
            ('무효가 거리 0~3%', stop_dist.between(0, 3, inclusive='both')),
            ('무효가 거리 3~5%', stop_dist.gt(3) & stop_dist.le(5)),
            ('무효가 거리 5~8%', stop_dist.gt(5) & stop_dist.le(8)),
            ('무효가 거리 8% 초과', stop_dist.gt(8)),
            ('20일선 이격 100~106', disp.between(100, 106)),
            ('20일선 이격 106~112', disp.gt(106) & disp.le(112)),
            ('20일선 이격 112 초과', disp.gt(112)),
        ]:
            line = _v44943_bin_line(df, label, mask, min_n=3)
            if line: lines.append(line)

        lines.append('- [깔끔한 양봉] 종가베팅형은 윗꼬리 0~5%가 PRIME, 5~10%가 SAFE, 10% 이상부터 감점 후보로 봅니다.')
        for label, mask in [
            ('윗꼬리 0~5%', wick.between(0, 5, inclusive='both')),
            ('윗꼬리 5~10%', wick.gt(5) & wick.le(10)),
            ('윗꼬리 10~15%', wick.gt(10) & wick.le(15)),
            ('윗꼬리 15% 초과', wick.gt(15)),
        ]:
            line = _v44943_bin_line(df, label, mask, min_n=3)
            if line: lines.append(line)

        lines.append('- [조정/과열] 긴 조정은 단순 기간보다 “20일 상승 과열이 낮고, 거래량 재증가가 붙는지”를 먼저 봅니다.')
        for label, mask in [
            ('20일 상승 0~10%', runup20.between(0, 10, inclusive='both')),
            ('20일 상승 10~20%', runup20.gt(10) & runup20.le(20)),
            ('20일 상승 20~35%', runup20.gt(20) & runup20.le(35)),
            ('20일 상승 35% 초과', runup20.gt(35)),
        ]:
            line = _v44943_bin_line(df, label, mask, min_n=3)
            if line: lines.append(line)

        lines.append('- [재료 프록시] 글로벌/섹터/수급 원자료가 없으면 재료대금 점수와 5000억+ 대금으로 대체합니다.')
        for label, mask in [
            ('재료 약함 0~1', mat.le(1)),
            ('재료 보통 2', mat.eq(2)),
            ('재료 강함 3+', mat.ge(3)),
        ]:
            line = _v44943_bin_line(df, label, mask, min_n=3)
            if line: lines.append(line)

        # 손절선행 후보 공통점 TOP
        if fail.any():
            fail_n = int(fail.sum())
            total_n = len(df)
            features = [
                ('무효가거리 8%초과', stop_dist.gt(8)),
                ('윗꼬리 15%초과', wick.gt(15)),
                ('종가위치 70%미만', close_loc.lt(70)),
                ('거래량 과열 Vol50 5배+', vol50.ge(5) | vol.ge(3)),
                ('거래대금 1000억 미만', amount.lt(1000)),
                ('거래대금 300억 미만', amount.lt(300)),
                ('20일선 이격 112초과', disp.gt(112)),
                ('20일 상승 35%초과', runup20.gt(35)),
                ('갭 8%초과', gap.gt(8)),
                ('RR 정상범위 이탈', ~(rr.between(0.8, 1.8)) & rr.ne(0)),
                ('재료 프록시 약함', mat.le(1)),
                ('SJ 0~3', sj_score.le(3)),
                ('신고가/고점근접 미달', near.lt(95) & close_loc.lt(80)),
            ]
            rows = []
            for name, m in features:
                m = pd.Series(m, index=df.index).fillna(False).astype(bool)
                in_fail = int((m & fail).sum())
                if in_fail <= 0:
                    continue
                fail_rate = in_fail / max(fail_n, 1) * 100.0
                all_rate = int(m.sum()) / max(total_n, 1) * 100.0
                lift = fail_rate / all_rate if all_rate > 0 else 0.0
                rows.append((lift, fail_rate, in_fail, name, all_rate))
            rows.sort(reverse=True)
            lines.append(f'- [손절선행 공통점 TOP] 손절선행 {fail_n}건 기준, 손절군에서 과대표현되는 원인을 봅니다.')
            for lift, fail_rate, in_fail, name, all_rate in rows[:7]:
                lines.append(f'  · {name}: 손절군 {in_fail}건/{fail_n}건({fail_rate:.1f}%) | 전체비중 {all_rate:.1f}% | lift {lift:.1f}배')
        lines.append('- 운용해석: SJ는 “좋은 모양” 확인보다 손절이 커지는 자리(무효가 멂·윗꼬리·과열·재료약함·저유동성)를 낮추는 문맥형 보조계기판으로 사용합니다.')
    except Exception as e:
        lines.append(f'[SJ 임계값/손절공통점 감사 오류] {type(e).__name__}: {e}')
    return lines


def _v44943_sj_context_policy_lines() -> list[str]:
    return [
        '[🧭 SJ 문맥별 적용 원칙 — v4.4.9.43]',
        '- LP-SAFE: SJ 점수가 낮아도 강등하지 않습니다. LP는 거래량 식힘·눌림재지지가 정상이라 SJ-6 당일형 조건과 충돌할 수 있습니다.',
        '- LP-POWER PRIME: SJ5+면 “강력추천 지정가” 문구를 강화하되, SJ가 낮아도 구조가 좋으면 유지합니다. 종가추격 금지는 유지합니다.',
        '- L 5000억+/L-GEO: SJ5+면 추천강도 우대, SJ0~3이면 당일 추격 금지·다음날 지지확인형으로 낮춥니다.',
        '- S-MOMENTUM: SJ5+면 소액 우대, SJ0~3이면 WATCH 강등 또는 비중 축소로 봅니다.',
        '- A/B/C: SJ0~3이면 기본 숨김/강등을 강화합니다.',
    ]


# 하위호환: 기존 호출부가 남아 있어도 v43 overlay 감사로 연결한다.
def _v44941_sj6_backtest_lines(df: pd.DataFrame) -> list[str]:
    return _v44942_sj_overlay_audit_lines(df)


def _v44934_build_core_compact_backtest_report(original_report: str, selected_csv: str = '', raw_csv: str = '', start_date: str = '', end_date: str = '', hold_days: int = 0, top_per_strategy: int = 5, all_candidates: bool = False) -> str:
    """v4.4.9.34: 백테스트 텔레그램 기본 리포트를 핵심 섹션만 3~5파트 수준으로 압축한다."""
    df = _v439_read_csv_safe(selected_csv)
    raw = _v439_read_csv_safe(raw_csv)
    if df is None or df.empty:
        return _v44934_cleanup_legacy_labels(original_report)
    try:
        if 'code' in df.columns:
            df['code'] = df['code'].astype(str).str.zfill(6)
        raw_count = len(raw) if raw is not None and not raw.empty else '확인필요'
        if (not start_date or not end_date) and 'signal_date' in df.columns:
            try:
                _dts = pd.to_datetime(df['signal_date'], errors='coerce').dropna()
                if len(_dts) > 0:
                    start_date = start_date or _dts.min().strftime('%Y-%m-%d')
                    end_date = end_date or _dts.max().strftime('%Y-%m-%d')
            except Exception:
                pass
        sel_text = '전체후보' if all_candidates else f'날짜별 전략별 TOP{top_per_strategy}'
        lines: list[str] = []
        lines.append('🧪 v4.4.9.43 SJ THRESHOLD + FAIL COMMON AUDIT')
        lines.append(f'버전: {CLOSING_BET_SCANNER_VERSION}')
        lines.append(f'기간: {start_date} ~ {end_date} | 보유평가: 다음 {hold_days}거래일')
        lines.append(f'원신호 {raw_count}건 | 최종검증 {len(df)}건 | 선택방식: {sel_text}')
        parity_lines = _v44942_count_parity_lines(df, raw_count=raw_count)
        if parity_lines:
            lines.append('')
            lines += parity_lines
        lines.append('')
        lines.append('[🎯 운용 결론]')
        lines.append('1순위 단기: LP-SAFE — 리더갭 후 1~5일 눌림재지지, +3/+5 우선')
        lines.append('2순위 단기: L 5000억+ / S-MOMENTUM CALM·PRIME / S2 — SJ는 문맥별 품질가점·위험강등으로만 사용, 추격보다 지정가·지지확인')
        lines.append('1순위 중기: I-MAIN ACCEL — 이상치 보정 후에도 중기 우선 유지')
        lines.append('관찰/확인: I-MAIN CORE, H-CORE/H-FAST, A-CONFIRM — 즉시매수보다 확인형')
        lines.append('강등/제외: 넓은 A/C/B, 저거래대금 A/B/C, H-OVERHEAT/RISK')
        lines.append('')
        lines.append('[🚦 최종 운용판정]')
        lines.append('단기 실행 우선순위는 LP-STABLE 안정형과 LP-POWER PRIME 강력추천 지정가를 분리하고, LP-SAFE > LP-POWER STABLE > L 5000억+ > S-MOMENTUM CALM > S-MOMENTUM PRIME/S2 > A-CONFIRM입니다. SJ-6는 LP-SAFE 강등에는 쓰지 않고, L/S/A/B/C의 품질가점·위험강등에만 문맥 적용합니다. 중기 I-MAIN은 평가기간이 충분할 때만 숫자를 확정하고, 최근구간이 약하면 5MA/20MA 재지지 확인형으로 낮춥니다.')
        lines.append('')
        lines += _v447_mental_summary_lines()
        lines.append('')

        mode = _v439_str_series(df, 'mode')
        amount = _v439_num_series(df, 'amount_b')
        gap = _v439_num_series(df, 'gap_pct')
        close_loc = _v439_num_series(df, 'close_loc_pct')
        lines.append('[📌 핵심 성과만 보기]')
        lp_safe = df[_v439_mask('_bt_mask_l_pullback_safe', df)] if callable(globals().get('_bt_mask_l_pullback_safe')) else df[mode.eq('LP')]
        if lp_safe.empty:
            lp_safe = df[mode.eq('LP')]
        if not lp_safe.empty:
            lines.append(_v439_short_trade_line(lp_safe, '단기 1순위 LP-SAFE'))
            for label, m in [
                ('LP-D1 공격타점', _v439_num_series(lp_safe, 'lp_days_after_gap', 0).eq(1)),
                ('LP-D23 핵심타점', _v439_num_series(lp_safe, 'lp_days_after_gap', 0).between(2, 3)),
                ('LP-D45 안정타점', _v439_num_series(lp_safe, 'lp_days_after_gap', 0).between(4, 5)),
            ]:
                sub = lp_safe[m]
                if len(sub) > 0:
                    lines.append(_v439_short_trade_line(sub, label))
        lp_audit_lines = _v44937_lp_explosion_audit_lines(df)
        if lp_audit_lines:
            lines.append('')
            lines += lp_audit_lines
        l_all = df[_v439_mask('_bt_mask_leader_gap_all', df) | mode.eq('L')]
        lines.append(_v439_short_trade_line(l_all, '단기 2순위 L 리더갭 전체'))
        lines.append(_v439_short_trade_line(l_all[amount.loc[l_all.index].ge(5000)], 'L 5000억+'))
        l_combo = l_all[amount.loc[l_all.index].ge(5000) & gap.loc[l_all.index].between(6, 12) & close_loc.loc[l_all.index].ge(70)]
        if len(l_combo) > 0:
            lines.append(_v439_short_trade_line(l_combo, 'L 갭6~12×5000억+×종가70+'))
        l_upper_lines = _v440_l_mega_upper_followup_lines(df)
        if l_upper_lines:
            lines.append('')
            lines += l_upper_lines
        sj6_lines = _v44942_sj_overlay_audit_lines(df, raw_count=raw_count)
        if sj6_lines:
            lines.append('')
            lines += sj6_lines
        sj43_lines = _v44943_threshold_fail_common_audit_lines(df)
        if sj43_lines:
            lines.append('')
            lines += sj43_lines
        lines.append('')
        lines += _v44943_sj_context_policy_lines()
        s_neutral = df[_v439_mask('_bt_mask_s_core_neutral', df)]
        s2 = df[mode.eq('S') & _v439_str_series(df, 's_type').eq('S2')]
        if len(s2) > 0:
            lines.append(_v439_short_trade_line(s2, 'S2 실행형'))
        if len(s_neutral) > 0:
            lines.append(_v439_short_trade_line(s_neutral, 'S-CORE NEUTRAL'))

        i_all = df[_v439_mask('_bt_mask_i_core_all', df)]
        if not i_all.empty:
            i_en = _i_main_enriched_df(i_all) if callable(globals().get('_i_main_enriched_df')) else i_all
            i_core = i_en[_v439_num_series(i_en, 'imain_core', 0).astype(int).eq(1)]
            i_accel = i_en[_v439_num_series(i_en, 'imain_accel', 0).astype(int).eq(1)]
            i_main = i_en[_v439_num_series(i_en, 'imain_is_main', 0).astype(int).eq(1)]
            lines.append(_v44935_i_horizon_line(i_main, 'I-MAIN MAIN', hold_days))
            lines.append(_v44935_i_horizon_line(i_accel, '중기 1순위 I-MAIN ACCEL', hold_days))
            lines.append(_v44935_i_horizon_line(i_core, '중기 2순위 I-MAIN CORE', hold_days))
            i_adj_lines = _v44933_i_outlier_adjustment_lines(i_en, hold_days)
            if i_adj_lines:
                lines.append('')
                lines += i_adj_lines
        lines.append('')

        s_lines = _v44934_s_momentum_core_lines(df)
        if s_lines:
            lines += s_lines
            lines.append('')

        geo_lines = _v44935_geo_risk_interpretation_lines(df)
        if geo_lines:
            lines += geo_lines
            lines.append('')

        lines.append('[🧭 실전 해석 압축]')
        lines.append('- LP-SAFE: 여전히 단기 최우선. D1은 소액, D23/D45는 갭하단·전일종가 지지 확인 후 +3/+5 자동익절. 막판 흔들림+거래량 재점화가 붙은 후보는 LP-POWER PRIME으로 분리해 강력추천 지정가·다음날 재돌파 1순위로 두고, 거래량은 식었지만 D23/D45·대금·종가위치가 좋은 후보는 LP-POWER STABLE로 봅니다.')
        lines.append('- L 5000억+: 강하지만 당일 추격 위험. 특히 L-MEGA 상한가성 잠금형은 신규 종가추격이 아니라 보유자 대응/다음날 기준선 지지·첫 눌림 재돌파 확인형입니다.')
        lines.append('- S-MOMENTUM CALM/PRIME: ST30 미통과라도 1000억+·종가75%+·RR정상이면 제외하지 않습니다. 거래량이 더 식은 CALM을 PRIME보다 우선합니다.')
        lines.append('- I-MAIN: HORIZON GUARD 적용. 보유평가가 짧거나 최근구간이 약하면 ACCEL도 신규추격 금지, 5MA/20MA 재지지 분할관찰.')
        lines.append('- A/H/B/C: 성과검증은 유지하되 FAST 실전에서는 확인형·관찰형으로 둡니다.')
        lines.append('')

        lines += _v449_fast_live_policy_lines()
        lines.append('')
        lines += _v439_stock_feature_quick_lines(df, min_n=_env_int('CLOSING_BET_STOCK_FEATURE_MIN_N', 5))
        if lines and lines[-1] != '':
            lines.append('')

        lines.append('[✅ 실전 추천 종목특성 필터]')
        lines.append('- 단기 LP: 5000억+ 리더갭 이후 1~5일 눌림재지지. D1/D23/D45 타점과 다음날 시초가별 대응을 같이 확인. LP-SAFE는 SJ 점수가 낮아도 강등하지 않고 참고 라벨로만 둡니다.')
        lines.append('- 단기 L: 거래대금 5000억+, 갭 3~12%, 종가위치 70%+ 우선. 5000억~1조와 갭 6~12%는 가점. 종가위치98%+·윗꼬리3% 이하 상한가성 L-MEGA는 보유자 대응/다음날 기준선 확인형으로 분리.')
        lines.append('- 단기 S: S-MOMENTUM CALM > S-MOMENTUM PRIME > S2 중심. ST30은 필터가 아니라 가점·비중조절 태그.')
        lines.append('- 중기 I: 평가기간 충분 시 ACCEL 우선, hold<20/40/60이면 해당 horizon은 평가보류. 추격보다 5MA/20MA 재지지 분할.')
        lines.append('- 신좁깔거조재: 단독 매수신호가 아니라 문맥형 품질라벨입니다. L은 SJ5+ 우대, S는 비중조절, A/B/C는 SJ0~3 강등, LP는 강등금지로 적용합니다.')
        lines.append('- 제외/강등: 저거래대금 A/B/C, 넓은 C, B1/B2 즉시매수, H-RISK/과열 H.')
        lines.append('')

        sample_n = _env_int('CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N', _env_int('CLOSING_BET_BACKTEST_DETAIL_TOP_N', 3))
        if _v439_bool_env('CLOSING_BET_COMPACT_SHOW_I_SAMPLES', '1'):
            sample_lines = _v439_i_samples_section(df, top_n=max(1, min(sample_n, 3)))
            if sample_lines:
                lines += sample_lines
                lines.append('')

        lines.append('[📌 확인 안내]')
        lines.append('- 기본 백테스트 리포트는 핵심압축 모드입니다. 원문 전체가 필요하면 CLOSING_BET_BACKTEST_REPORT_COMPACT_CORE=0 또는 --full-backtest-summary를 사용합니다.')
        lines.append('- 과거 패치 버전명은 텔레그램 혼선을 줄이기 위해 v4.4.9.43/누적검증 문구로 정리했습니다.')
        compact = '\n'.join([str(x) for x in lines if x is not None])
        compact = _v44934_cleanup_legacy_labels(compact)
        try:
            out_path = LOG_DIR / 'closing_bet_backtest_summary_v4_4_9_43_sj_threshold_fail_common_audit.txt'
            out_path.write_text(compact, encoding='utf-8')
        except Exception:
            pass
        if _v439_bool_env('CLOSING_BET_SHOW_FULL_BACKTEST_REPORT', '0'):
            compact += '\n\n[원문 상세 리포트]\n' + _v44934_cleanup_legacy_labels(str(original_report or ''))
        return compact
    except Exception as e:
        return (_v44934_cleanup_legacy_labels(str(original_report or '')) + f'\n\n[v4.4.9.43 핵심압축 요약 생성 실패] {type(e).__name__}: {e}')

def _v439_build_compact_backtest_report(original_report: str, selected_csv: str = '', raw_csv: str = '', start_date: str = '', end_date: str = '', hold_days: int = 0, top_per_strategy: int = 5, all_candidates: bool = False) -> str:
    """v4.3.9 Telegram 전송용 압축 리포트.
    원본 v4.3.8 백테스트 계산/저장 로직은 그대로 두고, 전송 메시지만 실전 운용 결론 중심으로 재구성한다.
    """
    if not _v439_bool_env('CLOSING_BET_COMPACT_OPERATION_SUMMARY', '1'):
        return _v44934_cleanup_legacy_labels(original_report)
    if _v439_bool_env('CLOSING_BET_BACKTEST_REPORT_COMPACT_CORE', '1') and not _v439_bool_env('CLOSING_BET_SHOW_FULL_BACKTEST_REPORT', '0'):
        return _v44934_build_core_compact_backtest_report(original_report, selected_csv=selected_csv, raw_csv=raw_csv, start_date=start_date, end_date=end_date, hold_days=hold_days, top_per_strategy=top_per_strategy, all_candidates=all_candidates)
    df = _v439_read_csv_safe(selected_csv)
    raw = _v439_read_csv_safe(raw_csv)
    if df is None or df.empty:
        return _v44934_cleanup_legacy_labels(original_report)
    try:
        if 'code' in df.columns:
            df['code'] = df['code'].astype(str).str.zfill(6)
        raw_count = len(raw) if raw is not None and not raw.empty else '확인필요'
        if (not start_date or not end_date) and 'signal_date' in df.columns:
            try:
                _dts = pd.to_datetime(df['signal_date'], errors='coerce').dropna()
                if len(_dts) > 0:
                    start_date = start_date or _dts.min().strftime('%Y-%m-%d')
                    end_date = end_date or _dts.max().strftime('%Y-%m-%d')
            except Exception:
                pass
        sel_text = '전체후보' if all_candidates else f'날짜별 전략별 TOP{top_per_strategy}'
        lines = []
        lines.append("🧪 v4.4.9.43 SJ THRESHOLD + FAIL COMMON AUDIT")
        lines.append(f"버전: {CLOSING_BET_SCANNER_VERSION}")
        lines.append(f"기간: {start_date} ~ {end_date} | 보유평가: 다음 {hold_days}거래일")
        lines.append(f"원신호 {raw_count}건 | 최종검증 {len(df)}건 | 선택방식: {sel_text}")
        lines.append("")
        lines.append("[🎯 운용 결론]")
        lines.append("1순위 단기: LP-SAFE 리더갭 눌림재지지 — D1/D23/D45 타점과 다음날 대응 시나리오 확인형")
        lines.append("2순위 단기: L 리더갭 5000억+ / S2·S-CORE NEUTRAL — L 당일은 추격보다 지지 확인 우대")
        lines.append("1순위 중기: I-MAIN ACCEL — 20/40/60거래일 시세분출형")
        lines.append("2순위 중기: I-MAIN CORE — 안정형 고확률 누적관찰")
        lines.append("관찰: H 핵심셀, A/C 고거래대금 후보")
        lines.append("강등/제외: B1/B2 즉시매수, 넓은 C, 저거래대금 A, H-RISK")
        lines.append("")
        lines.append("[🚦 최종 운용판정]")
        lines.append("단기 실행 우선순위: LP-SAFE > L 5000억+ > S2/S-NEUTRAL > A-CONFIRM/A-RETEST CORE > 넓은 A/C 관찰. LP는 D1/D23/D45 타점과 패턴별 흐름 프로파일을 함께 확인합니다. A-RETEST CORE②는 당일 보조 후보, A-CONFIRM은 다음날 전일고가 회복·양봉·거래대금 유지 시 승격 후보입니다. 넓은 A/C는 즉시매수 금지입니다. 중기 관찰 우선순위: I-ACCEL > IT-ACCEL > I-CORE. H/B/SLOCK은 검증·관찰만.")
        lines.append("")
        lines += _v447_mental_summary_lines()
        lines.append("")

        # 핵심 성과
        mode = _v439_str_series(df, 'mode')
        amount = _v439_num_series(df, 'amount_b')
        gap = _v439_num_series(df, 'gap_pct')
        close_loc = _v439_num_series(df, 'close_loc_pct')
        lines.append("[📌 핵심 성과만 보기]")
        l_all = df[_v439_mask('_bt_mask_leader_gap_all', df) | mode.eq('L')]
        lines.append(_v439_short_trade_line(l_all, '단기 1순위 L 리더갭 전체'))
        lines.append(_v439_short_trade_line(l_all[amount.loc[l_all.index].ge(5000)], 'L 5000억+'))
        lines.append(_v439_short_trade_line(l_all[amount.loc[l_all.index].between(5000, 10000, inclusive='left')], 'L 5000억~1조'))
        l_combo = l_all[amount.loc[l_all.index].ge(5000) & gap.loc[l_all.index].between(6, 12) & close_loc.loc[l_all.index].ge(70)]
        lines.append(_v439_short_trade_line(l_combo, 'L 갭6~12×5000억+×종가70+'))
        if len(l_combo) > 0 and len(l_combo) < _env_int('CLOSING_BET_COMPACT_SMALL_SAMPLE_N', '10'):
            lines.append(f"  ↳ 표본 {len(l_combo)}건: 최우선 관심 구간이지만 표본이 작아 과신 금지. 당일 갭하단/전일종가 지지 확인 필수.")

        s_neutral = df[_v439_mask('_bt_mask_s_core_neutral', df)]
        s2 = df[mode.eq('S') & _v439_str_series(df, 's_type').eq('S2')]
        lines.append(_v439_short_trade_line(s2, '단기 2순위 S2 실행형'))
        lines.append(_v439_short_trade_line(s_neutral, 'S-CORE NEUTRAL'))

        i_all = df[_v439_mask('_bt_mask_i_core_all', df)]
        if not i_all.empty:
            i_en = _i_main_enriched_df(i_all) if callable(globals().get('_i_main_enriched_df')) else i_all
            i_core = i_en[_v439_num_series(i_en, 'imain_core', 0).astype(int).eq(1)]
            i_accel = i_en[_v439_num_series(i_en, 'imain_accel', 0).astype(int).eq(1)]
            i_main = i_en[_v439_num_series(i_en, 'imain_is_main', 0).astype(int).eq(1)]
            lines.append(_v439_i_line(i_main, 'I-CORE MAIN'))
            lines.append(_v44935_i_horizon_line(i_accel, '중기 1순위 I-MAIN ACCEL', hold_days))
            lines.append(_v44935_i_horizon_line(i_core, '중기 2순위 I-MAIN CORE', hold_days))
            i_adj_lines = _v44933_i_outlier_adjustment_lines(i_en, hold_days)
            if i_adj_lines:
                lines.append('')
                lines += i_adj_lines

        h_core = df[_v439_mask('_bt_mask_h_core_500_1000_vol23', df) | _v439_mask('_bt_mask_h_core_300_500_vol35', df) | _v439_mask('_bt_mask_h_v427_core_union', df)]
        if not h_core.empty:
            lines.append(_v439_short_trade_line(h_core, '관찰 H 핵심셀'))
        a_liq = df[mode.eq('A') & amount.ge(5000)]
        c_liq = df[mode.eq('C') & amount.ge(5000)]
        if not a_liq.empty:
            lines.append(_v439_short_trade_line(a_liq, '관찰 A 5000억+'))
        if not c_liq.empty:
            lines.append(_v439_short_trade_line(c_liq, '관찰 C 5000억+'))
        lines.append("")

        new_pattern_lines = _v442_new_pattern_performance_lines(df)
        if new_pattern_lines:
            lines += new_pattern_lines
            lines.append("")

        tuning_lines = _v449_pattern_tuning_lines(df)
        if tuning_lines:
            lines += tuning_lines
            lines.append("")

        h_audit_lines = _v44924_h_core_promotion_audit_lines(df, compact=True)
        if h_audit_lines:
            lines += h_audit_lines
            lines.append("")

        a_refine_lines = _v4493_a_retest_refinement_lines(df)
        if a_refine_lines:
            lines += a_refine_lines
            lines.append("")

        material_check_lines = _v4494_material_check_lines(df)
        if material_check_lines:
            lines += material_check_lines
            lines.append("")

        entry_test_lines = _v4495_a_confirm_entry_price_test_lines(df, hold_days=hold_days)
        if entry_test_lines:
            lines += entry_test_lines
            lines.append("")

        vc_perf_lines = _v4499_vc_performance_lines(df)
        if vc_perf_lines:
            lines += vc_perf_lines
            lines.append("")

        close_loc_fallback_lines = _v4492_close_loc_fallback_diagnostic_lines(df)
        if close_loc_fallback_lines:
            lines += close_loc_fallback_lines
            lines.append("")

        tuning_diag_lines = _v4491_tuning_mask_diagnostic_lines(df)
        if tuning_diag_lines:
            lines += tuning_diag_lines
            lines.append("")

        st30_compact_lines = _v44915_st30_compact_lines(df)
        if st30_compact_lines:
            lines += st30_compact_lines
            lines.append("")
        st30_cross_lines = _v44916_pattern_cross_audit_lines(df, compact=True)
        if st30_cross_lines:
            lines += st30_cross_lines
            lines.append("")

        ssafe_st30_lines = _v44917_s_safe_st30_drilldown_lines(df, compact=True)
        if ssafe_st30_lines:
            lines += ssafe_st30_lines
            lines.append("")

        ssafe_miss_audit_lines = _v44918_s_safe_miss_success_fail_audit_lines(df, compact=True)
        if ssafe_miss_audit_lines:
            lines += ssafe_miss_audit_lines
            lines.append("")

        ssafe_liq_repeat_lines = _v44919_s_safe_liquidity_rule_repeat_lines(df, compact=True)
        if ssafe_liq_repeat_lines:
            lines += ssafe_liq_repeat_lines
            lines.append("")

        fast_policy_lines = _v449_fast_live_policy_lines()
        if fast_policy_lines:
            lines += fast_policy_lines
            lines.append("")

        path_profile_lines = _v445_pattern_path_profile_lines(df)
        if path_profile_lines:
            lines += path_profile_lines
            lines.append("")

        lp_d23_check_lines = _v448_lp_d23_validation_lines(df)
        if lp_d23_check_lines:
            lines += lp_d23_check_lines
            lines.append("")

        lines += _v439_stock_feature_quick_lines(df, min_n=_env_int('CLOSING_BET_STOCK_FEATURE_MIN_N', 5))
        if lines and lines[-1] != "":
            lines.append("")

        lines.append("[✅ 실전 추천 종목특성 필터]")
        lines.append("- 단기 LP: 5000억+ L 리더갭 후 1~5일 눌림재지지. LP-SAFE는 D1/D23/D45 타점 구분, +3/+5 우선, 다음날 시초가별 대응 시나리오 확인.")
        lines.append("- 단기 L: 거래대금 5000억+, 갭 3~12%, 종가위치 70%+ 우선. 5000억~1조와 갭 6~12%는 가점. 종가위치98%+·윗꼬리3% 이하 상한가성 L-MEGA는 보유자 대응/다음날 기준선 확인형으로 분리.")
        lines.append("- 단기 S: S2 실행형/S-NEUTRAL 중심. KOSPI200, 5조+, 거래대금 3000억+ 우대. 종가위치 70% 미만은 강등.")
        lines.append("- 중기 I: ACCEL/CORE 중심. 재료·대금 프록시 4점+, KOSPI200/5조+ 또는 300~1000억 거래대금 우대.")
        lines.append("- A 운용: A-RETEST CORE②는 당일 보조 후보, A-CONFIRM은 다음날 전일고가 회복/양봉/거래대금 유지 확인형. 넓은 A/C는 5000억+라도 관찰만.")
        lines.append("- 제외/강등: 저거래대금 A/B/C, B1/B2 즉시매수, 넓은 C, H-RISK/과열 H.")
        lines.append("")

        top_n = _env_int('CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N', _env_int('CLOSING_BET_BACKTEST_DETAIL_TOP_N', 5))
        sample_lines = _v439_i_samples_section(df, top_n=top_n)
        if sample_lines:
            lines += sample_lines
            lines.append("")

        lines.append("[📌 확인 안내]")
        lines.append("- 파일 저장/Artifact 확인 없이도 위 텔레그램 요약만으로 운용 결론을 판단하도록 정리했습니다.")
        lines.append("- 신규 검색식 LP/SLOCK/IT는 위 성과검증 섹션과 실시간 후보 출력 섹션에서 함께 확인합니다. v4.4.9.34는 ST30-RECLAIM을 하드필터가 아닌 가점/주의 태그로 유지하고, 실시간 S-SAFE 후보를 S-RECLAIM MEGA·S-RECLAIM 3000~5000·S-RECLAIM PRIME/WATCH/RISK·S-MOMENTUM PRIME(1000억+ 품질군)/WATCH·S-LIQUIDITY RISK로 분리 표시합니다. FAST 실전 출력 후보만 우선 표시하며, A-CONFIRM PRIME/CALM·시장/섹터압력·실전 추적로그를 유지합니다. H-CORE는 WATCH로 유지하고 H-FAST는 PRIME만 +3 초단기 전용 후보로 표시하며, 넓은 A/C/B/SLOCK은 기본 숨김/연구로 유지합니다.")
        compact = "\n".join([str(x) for x in lines if x is not None])
        compact = _v44934_cleanup_legacy_labels(compact)
        try:
            out_path = LOG_DIR / 'closing_bet_backtest_summary_v4_4_9_43_sj_threshold_fail_common_audit.txt'
            out_path.write_text(compact, encoding='utf-8')
        except Exception:
            pass
        if _v439_bool_env('CLOSING_BET_SHOW_FULL_BACKTEST_REPORT', '0'):
            compact += "\n\n[원문 상세 리포트]\n" + _v44934_cleanup_legacy_labels(str(original_report or ''))
        return compact
    except Exception as e:
        return (str(original_report or '') + f"\n\n[v4.4.9.34 압축 요약 생성 실패] {type(e).__name__}: {e}")


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
    parser.add_argument('--backtest-core-only', action='store_true', help='백테스트 결과를 S-CORE(S1우수응축+S2)만 필터링')
    parser.add_argument('--backtest-safe-only', action='store_true', help='백테스트 결과를 S-CORE SAFE(RR 1.0~1.5·거래량비<1.5·종가위치70%+)만 필터링')
    parser.add_argument('--backtest-neutral-only', action='store_true', help='백테스트 결과를 S-CORE NEUTRAL(SAFE/RISK 제외 중립군)만 필터링')
    parser.add_argument('--backtest-risk-only', action='store_true', help='백테스트 결과를 S-CORE RISK(위험태그 후보)만 필터링')
    parser.add_argument('--backtest-c-only', action='store_true', help='백테스트 결과를 C 역매공파(C1/C2/C3)만 필터링')
    parser.add_argument('--backtest-c-swing-only', action='store_true', help='C 역매공파를 스윙 기준(+5/+10·장기선 종가이탈)으로만 필터링/요약')
    parser.add_argument('--backtest-c-strict-only', action='store_true', help='C 엄격형만 필터링하여 스윙 성과를 확인')
    parser.add_argument('--backtest-c-pullback-only', action='store_true', help='C-SWING 눌림재상승형만 필터링하여 성과를 확인')
    parser.add_argument('--backtest-g-only', action='store_true', help='G 모랄레스갭만 필터링하여 G-SAFE/NEUTRAL/RISK 성과를 확인')
    parser.add_argument('--backtest-h-only', action='store_true', help='H 신고가거자름 STRICT만 필터링하여 H-TRIANGLE/CORE/VOL 분류 성과를 확인')
    parser.add_argument('--backtest-h-watch-only', action='store_true', help='H 눌림반등 WATCH만 필터링하여 돌파 후 2~8일 눌림 관전형 성과를 확인')
    parser.add_argument('--backtest-leader-gap-only', action='store_true', help='대형주/섹터대장 리더갭 WATCH만 필터링하여 SK하이닉스형 갭상승 성과를 확인')
    parser.add_argument('--backtest-i-core-only', action='store_true', help='I-CORE 시대중심주 150/200일 시세분출 타점만 필터링하여 20/40/60일 성과를 확인')
    parser.add_argument('--send-backtest-summary', action='store_true', help='백테스트 요약을 텔레그램으로 전송')
    parser.add_argument('--compact-summary', action='store_true', help='v4.4.8.1 후보별 멘탈/LP 검증 압축 요약을 텔레그램 전송/로그에 사용')
    parser.add_argument('--full-backtest-summary', action='store_true', help='압축 요약 뒤에 원문 백테스트 전체 리포트도 함께 출력')
    args = parser.parse_args()
    if getattr(args, 'compact_summary', False):
        os.environ['CLOSING_BET_COMPACT_OPERATION_SUMMARY'] = '1'
    if getattr(args, 'full_backtest_summary', False):
        os.environ['CLOSING_BET_SHOW_FULL_BACKTEST_REPORT'] = '1'

    now = _now_kst()
    log_info(f"✅ BOOTCHECK: {CLOSING_BET_SCANNER_VERSION}")
    log_info(f"종가배팅 스캐너 시작: {now.strftime('%H:%M')} (force={args.force})")
    log_info(f"TELEGRAM_TOKEN: {'✅' if TELEGRAM_TOKEN else '❌ 없음'}")
    log_info(f"CHAT_ID_LIST: {'✅ ' + str(CHAT_ID_LIST) if CHAT_ID_LIST else '❌ 없음'}")
    log_info(f"TELEGRAM_ROUTE: {_telegram_route_status()}")
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
            core_only=args.backtest_core_only,
            safe_only=args.backtest_safe_only,
            neutral_only=args.backtest_neutral_only,
            risk_only=args.backtest_risk_only,
            c_only=args.backtest_c_only,
            c_swing_only=args.backtest_c_swing_only,
            c_strict_only=args.backtest_c_strict_only,
            c_pullback_only=args.backtest_c_pullback_only,
            g_only=args.backtest_g_only,
            h_only=args.backtest_h_only,
            h_watch_only=args.backtest_h_watch_only,
            leader_gap_only=args.backtest_leader_gap_only,
            i_core_only=args.backtest_i_core_only,
        )
        original_report = report
        report = _v439_build_compact_backtest_report(
            original_report,
            selected_csv=selected_csv,
            raw_csv=raw_csv,
            start_date=args.backtest_start,
            end_date=args.backtest_end,
            hold_days=args.backtest_hold_days,
            top_per_strategy=args.backtest_top_per_strategy,
            all_candidates=args.backtest_all_candidates,
        )
        log_info("\n" + report)
        log_info(f"백테스트 선택 CSV: {selected_csv}")
        log_info(f"백테스트 원신호 CSV: {raw_csv}")
        if (args.send_backtest_summary or args.send_summary) and _telegram_route_ready():
            send_telegram_chunks(report, max_len=3500)
        sys.exit(0)

    if args.eval_pending:
        updated = _evaluate_pending_signals()
        log_info(f"평가 완료 건수: {updated}")

    if args.summary:
        summary_text = _build_validation_summary(last_n_days=120)
        log_info("\n" + summary_text)
        if args.send_summary and _telegram_route_ready():
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
            if _telegram_route_ready():
                send_telegram_photo(
                    f"[{TODAY_STR} {now.strftime('%H:%M')}] 종가배팅 후보 없음\n"
                    f"(대상: {SCAN_UNIVERSE} | 조건 미충족)",
                    [],
                )
                log_info("✅ '후보없음' 텔레그램 전송 완료")
        else:
            log_info(f"✅ 종가배팅 후보 {len(hits)}개 텔레그램 전송 완료")

    sys.exit(0)
