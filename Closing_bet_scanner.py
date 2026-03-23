# =============================================================
# 🕯️ closing_bet_scanner.py — 종가배팅 타점 스캐너
# =============================================================
# 실행 시간: 15:00 ~ 15:20 (이 시간대에만 의미 있음)
#
# 왜 이 시간인가:
#   15:00 이전 → 아직 장 중, 종가 확정 안 됨
#   15:20 이후 → 동시호가 진입 타이밍 지남
#   15:00~15:20 → 오늘 종가 흐름 확인 + 진입 결정 가능
#
# 종가배팅 조건 6가지:
#   ① 전고점(20일) 대비 85~100% 구간
#   ② 윗꼬리 비율 20% 이하 (강봉 마감 중)
#   ③ 거래량 VMA20 × 2배 이상 폭발
#   ④ 양봉 마감 (현재가 ≥ 시가)
#   ⑤ 이격도 98~112 (MA20 적정 위치)
#   ⑥ MA20 위 마감
#
# 실행:
#   python closing_bet_scanner.py          # 1회 실행
#   python closing_bet_scanner.py --force  # 시간 무관 강제 실행 (테스트)
# =============================================================

import os
import sys
import argparse
import pytz
import requests
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── main7.py에서 필요한 것만 import
from main7_bugfix import (
    get_indicators,
    _calc_upper_wick_ratio,
    load_krx_listing_safe,
    ANTHROPIC_API_KEY,
    OPENAI_API_KEY,
    GROQ_API_KEY,
    TODAY_STR,
    KST,
)

# ── 종가배팅 전용 텔레그램 설정 (yml에서 별도 지정 가능)
# CLOSING_BET_TOKEN / CLOSING_BET_CHAT_ID 있으면 그걸 우선 사용
# 없으면 기존 TELEGRAM_TOKEN / TELEGRAM_CHAT_ID 사용
TELEGRAM_TOKEN = (
    os.environ.get('CLOSING_BET_TOKEN') or
    os.environ.get('TELEGRAM_TOKEN', '')
)
CHAT_ID_LIST = [
    c.strip() for c in (
        os.environ.get('CLOSING_BET_CHAT_ID') or
        os.environ.get('TELEGRAM_CHAT_ID', '')
    ).split(',') if c.strip()
]

try:
    from scan_logger import set_log_level, log_info, log_error, log_debug
    set_log_level('NORMAL')
except ImportError:
    def log_info(m):  print(m)
    def log_error(m): print(m)
    def log_debug(m): pass

# ── 텔레그램 전송 (자체 구현 — main7 독립)
def send_telegram_photo(message: str, image_paths: list = []):
    if not TELEGRAM_TOKEN or not message.strip():
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        if not chat_id:
            continue
        try:
            requests.post(url, data={
                'chat_id': chat_id,
                'text':    message[:4000],
            }, timeout=5)
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
# 📐 Envelope 계산 유틸
# =============================================================

def _calc_envelope(df: pd.DataFrame, period: int, pct: float) -> dict:
    """
    Envelope 계산.
    상한선 = MA × (1 + pct/100)
    하한선 = MA × (1 - pct/100)

    예) Envelope(20, 20) → MA20 ± 20%
        Envelope(40, 40) → MA40 ± 40%
    """
    ma      = df['Close'].rolling(period).mean()
    upper   = ma * (1 + pct / 100)
    lower   = ma * (1 - pct / 100)
    return {
        'ma':    ma,
        'upper': upper,
        'lower': lower,
    }


def _check_envelope_bottom(row: pd.Series, df: pd.DataFrame) -> dict:
    """
    Envelope 하한선 근접 여부 체크.
    조건:
      - Envelope(20, 20%) 하한선 2% 이내
      - Envelope(40, 40%) 하한선 10% 이내
    """
    close = float(row.get('Close', 0))
    if close <= 0:
        return {'env20_near': False, 'env40_near': False,
                'env20_pct': 0, 'env40_pct': 0}

    # Envelope(20, 20%)
    env20  = _calc_envelope(df, 20, 2)
    lower20 = float(env20['lower'].iloc[-1])
    env20_pct = (close - lower20) / lower20 * 100  # 하한선 대비 얼마나 위에 있나

    # Envelope(40, 40%)
    env40  = _calc_envelope(df, 40, 10)
    lower40 = float(env40['lower'].iloc[-1])
    env40_pct = (close - lower40) / lower40 * 100

    return {
        'env20_near':  env20_pct <= 2.0,   # 하한선 2% 이내
        'env40_near':  env40_pct <= 10.0,  # 하한선 10% 이내
        'env20_pct':   round(env20_pct, 1),
        'env40_pct':   round(env40_pct, 1),
        'lower20':     round(lower20),
        'lower40':     round(lower40),
    }


# =============================================================
# ⚙️ 설정
# =============================================================
MIN_PRICE        = 5_000
MIN_AMOUNT       = 3_000_000_000    # 거래대금 30억 이상 (지수 구성 종목 기준 완화)
MIN_MARCAP       = 50_000_000_000   # 시총 500억 이상
TOP_N            = 400              # 거래대금 상위 N종목 (amount_top400 모드용)

# 스캔 유니버스 선택
# 'kospi200+kosdaq150' : 코스피200 + 코스닥150 (기본값, 신뢰도 높음)
# 'amount_top400'      : 거래대금 상위 400개 (오늘 활발한 종목)
# 'kospi200'           : 코스피200만
SCAN_UNIVERSE    = 'kospi200+kosdaq150'
MAX_WORKERS      = 20

# 종가배팅 조건 임계값
NEAR_HIGH20_MIN  = 85.0   # 전고점 대비 최소 85%
NEAR_HIGH20_MAX  = 100.0  # 전고점 대비 최대 100% (신고가 직전)
UPPER_WICK_MAX   = 0.20   # 윗꼬리 비율 최대 20%
VOL_MULT         = 2.0    # 거래량 VMA20 대비 배율
DISPARITY_MIN    = 98.0   # 이격도 최소
DISPARITY_MAX    = 112.0  # 이격도 최대

# 실행 가능 시간대
SCAN_START_HOUR  = 14
SCAN_START_MIN   = 50    # 14:50부터 (5분 여유)
SCAN_END_HOUR    = 15
SCAN_END_MIN     = 25    # 15:25까지

ALERTED_FILE     = '/tmp/closing_bet_alerted.json'


# =============================================================
# 📋 스캔 유니버스 로딩
# =============================================================

def _get_index_tickers_naver(index_code: str) -> list:
    """
    네이버 금융에서 지수 구성 종목 코드 수집.
    index_code:
      'KOSPI200' → 코스피200 구성 종목
      'KQ150'    → 코스닥150 구성 종목
    """
    try:
        from bs4 import BeautifulSoup
        HEADERS = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://finance.naver.com/',
        }
        # 네이버 금융 지수 구성 종목 페이지
        url_map = {
            'KOSPI200': 'https://finance.naver.com/sise/entryJongmok.naver?kospiCode=KOSPI200',
            'KQ150':    'https://finance.naver.com/sise/entryJongmok.naver?kospiCode=KQ150',
        }
        url = url_map.get(index_code, '')
        if not url:
            return []

        tickers = []
        for page in range(1, 30):  # 최대 29페이지
            res = requests.get(f"{url}&page={page}", headers=HEADERS, timeout=10)
            res.encoding = 'euc-kr'
            soup = BeautifulSoup(res.text, 'html.parser')

            # 종목 링크에서 코드 추출
            links = soup.select('td.ctg a[href*="code="]')
            if not links:
                break

            for a in links:
                href = a.get('href', '')
                code = href.split('code=')[-1].strip()
                if code and len(code) == 6 and code.isdigit():
                    tickers.append(code)

        tickers = list(dict.fromkeys(tickers))  # 중복 제거
        if tickers:
            log_info(f"  네이버 {index_code}: {len(tickers)}개 ✅")
        return tickers

    except Exception as e:
        log_error(f"⚠️ 네이버 {index_code} 실패: {e}")
        return []


def _get_index_tickers_krx(market: str, top_n: int) -> list:
    """FDR / pykrx 시총 기반 폴백"""
    # pykrx 시도
    try:
        from pykrx import stock as _pk
        today = datetime.now().strftime('%Y%m%d')
        idx_code = '1028' if market == 'KOSPI' else '2203'
        tickers = _pk.get_index_portfolio_deposit_file(idx_code, today)
        if tickers and len(tickers) > 50:
            log_info(f"  pykrx {market}: {len(tickers)}개 ✅")
            return list(tickers)[:top_n]
    except Exception:
        pass

    # FDR 시총 기반
    try:
        df = fdr.StockListing(market)
        if df is not None and not df.empty:
            mcap_col = next((c for c in df.columns if 'cap' in c.lower()), None)
            sym_col  = next((c for c in df.columns
                             if c in ('Code','Symbol','코드','종목코드')), None)
            if mcap_col and sym_col:
                df = df.nlargest(top_n, mcap_col)
                tickers = [str(c).zfill(6) for c in df[sym_col].tolist()]
                log_info(f"  FDR {market} 시총상위{top_n}: {len(tickers)}개 ✅")
                return tickers
    except Exception as e:
        log_error(f"⚠️ FDR {market} 실패: {e}")
    return []


def _get_kospi200() -> list:
    """
    코스피200 구성 종목.
    우선순위: ① 네이버금융 ② pykrx ③ FDR 시총상위200
    """
    # ① 네이버 금융
    tickers = _get_index_tickers_naver('KOSPI200')
    if len(tickers) >= 150:
        return tickers

    # ② ③ pykrx / FDR 폴백
    log_info("  코스피200 네이버 실패 → pykrx/FDR 폴백")
    return _get_index_tickers_krx('KOSPI', 200)


def _get_kosdaq150() -> list:
    """
    코스닥150 구성 종목.
    우선순위: ① 네이버금융 ② pykrx ③ FDR 시총상위150
    """
    # ① 네이버 금융
    tickers = _get_index_tickers_naver('KQ150')
    if len(tickers) >= 100:
        return tickers

    # ② ③ pykrx / FDR 폴백
    log_info("  코스닥150 네이버 실패 → pykrx/FDR 폴백")
    return _get_index_tickers_krx('KOSDAQ', 150)


# 전역 지수 소속 맵 (코드 → 지수명)
INDEX_MAP: dict = {}

def _load_universe(mode: str = 'kospi200+kosdaq150') -> list:
    """
    스캔 대상 종목 코드 리스트 반환.
    동시에 INDEX_MAP(코드→지수) 글로벌 변수 업데이트.

    mode:
      'kospi200+kosdaq150' — 코스피200 + 코스닥150 (기본, 약 350개)
      'kospi200'           — 코스피200만 (200개)
      'amount_top400'      — 거래대금 상위 400개
    """
    global INDEX_MAP
    INDEX_MAP = {}
    log_info(f"📋 유니버스 로딩: {mode}")

    if mode == 'kospi200':
        codes = _get_kospi200()
        for c in codes:
            INDEX_MAP[c] = '🏛️코스피200'

    elif mode == 'kospi200+kosdaq150':
        kospi  = _get_kospi200()
        kosdaq = _get_kosdaq150()
        for c in kospi:
            INDEX_MAP[c] = '🏛️코스피200'
        for c in kosdaq:
            if c not in INDEX_MAP:
                # 코스피200에 없는 것만 코스닥150
                INDEX_MAP[c] = '📊코스닥150'
            # 코스피200에 이미 있으면 덮어쓰지 않음 (코스피200 우선)
        codes = list(dict.fromkeys(kospi + kosdaq))

    else:  # amount_top400 또는 기타
        return []

    log_info(f"✅ 유니버스: {len(codes)}개 종목")
    return codes


# =============================================================
# 🕐 시간 체크
# =============================================================

def _is_closing_time(force: bool = False) -> bool:
    """종가배팅 유효 시간 (14:50~15:25)"""
    if force:
        return True
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    start = SCAN_START_HOUR * 60 + SCAN_START_MIN
    end   = SCAN_END_HOUR   * 60 + SCAN_END_MIN
    return start <= t <= end


def _time_to_close() -> int:
    """마감까지 남은 분"""
    now   = datetime.now(KST)
    close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return max(0, int((close - now).total_seconds() / 60))


# =============================================================
# 📊 종가배팅 조건 체크
# =============================================================

def _load_df(code: str) -> pd.DataFrame | None:
    """종목 일봉 데이터 로드 + 지표 계산"""
    try:
        start = (datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d')
        df    = fdr.DataReader(code, start=start)
        if df is None or len(df) < 60:
            return None
        df = get_indicators(df)
        if df is None or df.empty:
            return None
        # 전고점 컬럼
        if 'NearHigh20_Pct' not in df.columns:
            df['High20']         = df['High'].rolling(20).max()
            df['NearHigh20_Pct'] = (df['Close'] / df['High20'] * 100)
        return df
    except Exception:
        return None


def _base_info(row, df) -> dict:
    """공통 기본 정보 추출"""
    close  = float(row['Close'])
    open_p = float(row['Open'])
    high   = float(row['High'])
    vol    = float(row['Volume'])
    vma20  = float(row.get('VMA20', row.get('Vol_Avg', 0)) or 0)
    atr    = float(row.get('ATR', 0) or 0)
    target1  = round(close + atr * 2)   if atr > 0 else round(close * 1.05)
    stoploss = round(close - atr * 1.5) if atr > 0 else round(close * 0.97)
    rr = round((target1 - close) / (close - stoploss), 1) if close > stoploss else 0
    return {
        'close':     int(close),
        'open':      int(open_p),
        'high':      int(high),
        'vol_ratio': round(vol / vma20, 1) if vma20 > 0 else 0,
        'wick_pct':  round(_calc_upper_wick_ratio(row) * 100, 1),
        'amount_b':  round(close * vol / 1e8, 1),
        'atr':       int(atr),
        'target1':   target1,
        'stoploss':  stoploss,
        'rr':        rr,
        '_close':    close,
        '_open':     open_p,
        '_vol':      vol,
        '_vma20':    vma20,
        '_ma20':     float(row.get('MA20', 0) or 0),
        '_disp':     float(row.get('Disparity', 100) or 100),
        '_near20':   float(row.get('NearHigh20_Pct', 0) or 0),
        '_upper_wick': _calc_upper_wick_ratio(row),
    }


# ── 전략 A: 전고점 돌파 종가배팅 ──────────────────────────────

def _check_breakout_bet(code: str, name: str) -> dict | None:
    """
    전략 A — 전고점 돌파형 종가배팅
    강한 종목이 전고점 부근에서 거래량 터지며 강봉 마감

    조건:
      ① 전고점(20일) 대비 85~100%
      ② 윗꼬리 20% 이하
      ③ 거래량 VMA20 × 2배
      ④ 양봉 마감
      ⑤ 이격도 98~112
      ⑥ MA20 위 마감
    """
    try:
        df = _load_df(code)
        if df is None: return None

        row    = df.iloc[-1]
        info   = _base_info(row, df)

        if info['_close'] < MIN_PRICE or info['amount_b'] < MIN_AMOUNT / 1e8:
            return None

        cond = {
            '①전고점85~100%': NEAR_HIGH20_MIN <= info['_near20'] <= NEAR_HIGH20_MAX,
            '②윗꼬리20%이하':  info['_upper_wick'] <= UPPER_WICK_MAX,
            '③거래량2배폭발':  info['_vma20'] > 0 and info['_vol'] >= info['_vma20'] * VOL_MULT,
            '④양봉마감':       info['_close'] >= info['_open'],
            '⑤이격도98~112':   DISPARITY_MIN <= info['_disp'] <= DISPARITY_MAX,
            '⑥MA20위마감':     info['_ma20'] > 0 and info['_close'] >= info['_ma20'],
        }
        passed = [k for k, v in cond.items() if v]
        score  = len(passed)
        if score < 4: return None

        return {
            **info,
            'code':        code, 'name': name,
            'mode':        'A', 'mode_label': '📈돌파형',
            'index_label': INDEX_MAP.get(code, ''),
            'near20':      round(info['_near20'], 1),
            'disp':        round(info['_disp'], 1),
            'score':       score,
            'grade':       '🏆완전체' if score==6 else ('✅A급' if score==5 else '📋B급'),
            'passed':      passed,
        }
    except Exception as e:
        log_debug(f"  [A/{name}] {e}")
        return None


# ── 전략 B: Envelope 하한선 반등 종가배팅 ─────────────────────

def _check_envelope_bet(code: str, name: str) -> dict | None:
    """
    전략 B — Envelope 하한선 반등형 종가배팅
    많이 빠진 바닥 구간에서 반등 초동을 포착.

    핵심 조건 (필수):
      ① Envelope(20,20%) 하한선 2% 이내  → 단기 과매도 바닥
      ② Envelope(40,40%) 하한선 10% 이내 → 중기 과매도 바닥
      (①② 중 하나 이상 필수)

    보조 조건 (가점용):
      + RSI 40 이하         → 과매도 수치 확인
      + 아랫꼬리 > 윗꼬리   → 하방 지지 신호 (세력이 받쳐줌)
      + 거래량 감소 추세     → 매도세 소진 중 (반등 임박)
    """
    try:
        df = _load_df(code)
        if df is None: return None

        row  = df.iloc[-1]
        info = _base_info(row, df)

        # ✅ 전략 B: 코스피200 or 코스닥150 소속 종목만
        # - 시총/거래대금/주가 필터 없음 (지수 편입이 품질 보증)
        # - 단, INDEX_MAP에 없으면 지수 외 → 전략B 스킵
        # - 상장폐지 방어용 1,000원 미만 제외
        idx = INDEX_MAP.get(code, '')
        if not idx:
            return None   # 코스피200도 코스닥150도 아니면 전략B 제외
        if info['_close'] < 1_000:
            return None

        # Envelope 계산
        env = _check_envelope_bottom(row, df)
        rsi = float(row.get('RSI', 50) or 50)

        # ══ 필수: Envelope 하한선 근접 (① 또는 ② 중 하나)
        if not (env['env20_near'] or env['env40_near']):
            return None

        close  = info['_close']
        open_p = info['_open']
        high   = float(row.get('High', close))
        low    = float(row.get('Low',  close))
        vol    = info['_vol']
        vma20  = info['_vma20']

        # ── 아랫꼬리 > 윗꼬리 (하방 지지 신호)
        total    = high - low if high > low else 1
        body_bot = min(close, open_p)
        body_top = max(close, open_p)
        lower_wick     = body_bot - low
        upper_wick_len = high - body_top
        lower_wick_long = lower_wick > upper_wick_len

        # ── 거래량 소진 (최근 3일 평균 < 10일 평균)
        vma3  = float(df['Volume'].tail(3).mean())
        vma10 = float(df['Volume'].tail(10).mean())
        vol_drying = vma3 < vma10 * 0.85   # 10일 평균의 85% 미만

        # ── 바닥 구간 세력 매집 탐지 (최근 5거래일)
        # 기존 maejip_cond는 MA20 위를 요구 → 바닥 구간에 안 맞음
        # 바닥 전용 조건:
        #   OBV 5일 이동평균 > 10일 이동평균 (매수세 유입)
        #   + 거래량이 평균보다 많은 날 종가 > 시가 (세력 매수봉)
        obv        = (df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
                      * df['Volume']).cumsum()
        obv_ma5    = obv.rolling(5).mean()
        obv_ma10   = obv.rolling(10).mean()
        obv_rising = float(obv_ma5.iloc[-1]) > float(obv_ma10.iloc[-1])

        # 최근 5일 중 세력 매수봉 (거래량 VMA10↑ + 양봉) 발생 횟수
        recent5 = df.tail(5)
        vma10_val = float(df['Volume'].rolling(10).mean().iloc[-1])
        maejip_5d = int(((recent5['Volume'] > vma10_val) &
                         (recent5['Close'] > recent5['Open'])).sum())

        # ── 오늘 거래량 / 3일 평균 비율 (코멘트용)
        vma3_val  = float(df['Volume'].tail(3).mean())
        vol_vs_3d = round(vol / vma3_val * 100, 1) if vma3_val > 0 else 0
        # 예) 67% = 오늘 거래량이 3일 평균의 67% → 소진 중
        #     130% = 오늘 거래량이 3일 평균보다 30% 많음

        # ── 아랫꼬리/윗꼬리 코멘트용 (필터 아님)
        lower_wick_comment = '아랫꼬리↑' if lower_wick_long else '아랫꼬리↓'

        # ── 점수 조건 (필터만 남김)
        bonus = {
            '①Env20하한2%이내':   env['env20_near'],
            '②Env40하한10%이내':  env['env40_near'],
            '③RSI40이하':         rsi <= 40,
            '④OBV매수세유입':     obv_rising,
            '⑤5일내매집봉1회↑':   maejip_5d >= 1,
        }
        passed = [k for k, v in bonus.items() if v]
        score  = len(passed)

        # ①② 중 하나 필수, 전체 3개 이상
        if score < 3:
            return None

        # 등급
        if env['env20_near'] and env['env40_near'] and score >= 4:
            grade = '🏆완전체'
        elif score >= 4:
            grade = '✅A급'
        else:
            grade = '📋B급'

        # 목표가: Envelope 중심선(MA20) — 바닥→평균 회귀 목표
        env20_ma   = float(_calc_envelope(df, 20, 20)['ma'].iloc[-1])
        target_env = round(env20_ma)

        # 매집 차트
        maejip_chart = _build_maejip_chart(df)

        return {
            **info,
            'code':             code, 'name': name,
            'mode':             'B', 'mode_label': '📉반등형',
            'index_label':      INDEX_MAP.get(code, ''),
            'env20_pct':        env['env20_pct'],
            'env40_pct':        env['env40_pct'],
            'lower20':          env['lower20'],
            'lower40':          env['lower40'],
            'rsi':              round(rsi, 1),
            'obv_rising':       obv_rising,
            'maejip_5d':        maejip_5d,
            # 코멘트용 (필터 아님)
            'vol_vs_3d':        vol_vs_3d,       # 오늘 거래량/3일평균 %
            'lower_wick_comment': lower_wick_comment,  # 아랫꼬리 방향
            'lower_wick_pct':   round(lower_wick / total * 100, 1),
            'upper_wick_pct':   round(upper_wick_len / total * 100, 1),
            'target1':          target_env,
            'score':            score,
            'grade':            grade,
            'passed':           passed,
            'maejip_chart':     maejip_chart,
        }
    except Exception as e:
        log_debug(f"  [B/{name}] {e}")
        return None


def _check_closing_bet(code: str, name: str) -> dict | None:
    """두 전략 모두 체크, 점수 높은 것 반환"""
    a = _check_breakout_bet(code, name)
    b = _check_envelope_bet(code, name)

    if a and b:
        # 둘 다 해당 → 점수 높은 것 (동점이면 A 우선)
        return a if a['score'] >= b['score'] else b
    return a or b

def _build_maejip_chart(df: pd.DataFrame) -> str:
    """
    최근 5거래일 매집 현황을 텍스트 차트로 표현.

    예시:
      📅 매집 현황 (최근 5일)
      D-4  🟢 +2.1% | 거래량 1.8배 | 매집✅
      D-3  🔴 -0.5% | 거래량 0.7배 |
      D-2  🟢 +1.3% | 거래량 2.1배 | 매집✅
      D-1  🟢 +0.8% | 거래량 1.5배 | 매집✅
      D-0  🟡  0.0% | 거래량 0.9배 |  (오늘)
    """
    if df is None or len(df) < 6:
        return ''

    recent = df.tail(6).copy()
    vma10  = float(df['Volume'].rolling(10).mean().iloc[-1]) or 1

    lines = ['📅 최근 5일 매집 현황']
    rows  = list(recent.iterrows())

    for idx, (date, row) in enumerate(rows[-5:], start=1):
        label  = f'D-{5-idx}' if idx < 5 else 'D-0(오늘)'
        close  = float(row['Close'])
        open_p = float(row['Open'])
        vol    = float(row['Volume'])
        pct    = (close - open_p) / open_p * 100 if open_p > 0 else 0
        v_ratio = vol / vma10

        # 양봉/음봉/도지
        if pct > 0.3:   candle = '🟢'
        elif pct < -0.3: candle = '🔴'
        else:            candle = '🟡'

        # 매집 여부 (거래량 VMA10↑ + 양봉)
        is_maejip = v_ratio > 1.0 and close > open_p
        maejip_mark = ' 🔵매집' if is_maejip else ''

        lines.append(
            f'{label:<9} {candle} {pct:+.1f}% | '
            f'거래량{v_ratio:.1f}배{maejip_mark}'
        )

    return '\n'.join(lines)




# =============================================================
# 📱 텔레그램 포맷
# =============================================================

def _format_hit(hit: dict, rank: int, mins_left: int) -> str:
    passed_str = ' '.join(hit['passed'])
    mode_label = hit.get('mode_label', '')

    # 모드별 추가 정보
    extra = ''
    if hit.get('mode') == 'A':
        extra = (f"전고점:{hit.get('near20',0)}% | 이격:{hit.get('disp',0)}")
    elif hit.get('mode') == 'B':
        vol3d    = hit.get('vol_vs_3d', 0)
        vol3d_comment = (
            f"거래량소진({vol3d:.0f}%)" if vol3d < 85
            else f"거래량보통({vol3d:.0f}%)" if vol3d < 120
            else f"거래량증가({vol3d:.0f}%)"
        )
        extra = (
            f"Env20:{hit.get('env20_pct',0):+.1f}% | "
            f"Env40:{hit.get('env40_pct',0):+.1f}% | "
            f"RSI:{hit.get('rsi',0)} | "
            f"5일매집:{hit.get('maejip_5d',0)}회 | "
            f"{'OBV↑' if hit.get('obv_rising') else 'OBV↓'} | "
            f"{vol3d_comment} | "
            f"{hit.get('lower_wick_comment','')} "
            f"(아랫:{hit.get('lower_wick_pct',0):.0f}% 윗:{hit.get('upper_wick_pct',0):.0f}%)"
        )

    # 매집 차트 (전략 B만)
    chart_str = ''
    if hit.get('mode') == 'B' and hit.get('maejip_chart'):
        chart_str = f"\n{hit['maejip_chart']}\n"

    idx_raw   = hit.get('index_label', '')
    # 지수 소속 표시 정리
    if idx_raw == '🏛️코스피200':
        idx_str = ' | 🏛️코스피200'
    elif idx_raw == '📊코스닥150':
        idx_str = ' | 📊코스닥150'
    else:
        # 지수 외 종목 → 비지수 표시
        idx_str = ' | 📋비지수'

    return (
        f"{'─'*28}\n"
        f"🕯️ {mode_label} {hit['grade']}  [{hit['name']}({hit['code']})]  {hit['close']:,}원{idx_str}\n"
        f"✅ {passed_str}\n"
        f"📊 거래량:{hit['vol_ratio']}배 | 윗꼬리:{hit['wick_pct']}% | {extra}\n"
        f"💰 거래대금:{hit['amount_b']}억 | ATR:{hit['atr']:,}원\n"
        f"📌 목표:{hit['target1']:,} | 손절:{hit['stoploss']:,} (RR {hit['rr']}){chart_str}\n"
        f"⏰ 마감까지 {mins_left}분\n"
    )


def _send_results(hits: list, mins_left: int):
    """
    결과 텔레그램 전송.
    전략별 상위 5개씩 선별 → AI 분석 포함해서 전송.
    """
    log_info(f"📨 _send_results 호출: {len(hits)}개 | TOKEN={'✅' if TELEGRAM_TOKEN else '❌'}")
    if not hits:
        log_info("  → 후보 없음 메시지 전송")
        send_telegram_photo(
            f"🕯️ [{TODAY_STR}] 종가배팅 후보 없음\n"
            f"(대상: {SCAN_UNIVERSE} | 조건 미충족)",
            []
        )
        return

    # ── 전략별 완전체 우선, 상위 5개씩 선별
    def _pick_top5(mode: str) -> list:
        pool = [h for h in hits if h.get('mode') == mode]
        # 완전체 → A급 → B급 → 점수 → 거래량 순
        def _priority(h):
            grade = h.get('grade', '')
            g_rank = 0 if '완전체' in grade else (1 if 'A급' in grade else 2)
            return (g_rank, -h.get('score', 0), -h.get('vol_ratio', 0))
        pool.sort(key=_priority)
        return pool[:5]

    hits_a = _pick_top5('A')
    hits_b = _pick_top5('B')
    total  = len(hits_a) + len(hits_b)

    log_info(f"  📈 돌파형 {len(hits_a)}개 (완전체:{sum(1 for h in hits_a if '완전체' in h.get('grade',''))}개)")
    log_info(f"  📉 반등형 {len(hits_b)}개 (완전체:{sum(1 for h in hits_b if '완전체' in h.get('grade',''))}개)")

    # ── 헤더
    header = (
        f"🕯️ 종가배팅 선별 TOP {total} ({TODAY_STR})\n"
        f"⏰ 마감까지 {mins_left}분\n"
        f"📈 돌파형(A): {len(hits_a)}개 | 📉 반등형(B): {len(hits_b)}개\n"
    )
    send_telegram_photo(header, [])

    # ── 전략 A — 종목 카드 전송
    if hits_a:
        send_telegram_photo("── 📈 돌파형(A) TOP5 ──", [])
        current_msg = ''
        MAX_CHAR = 3800
        for hit in hits_a:
            entry = _format_hit(hit, 0, mins_left)
            if len(current_msg) + len(entry) > MAX_CHAR:
                send_telegram_photo(current_msg, [])
                current_msg = entry
            else:
                current_msg += entry
        if current_msg.strip():
            send_telegram_photo(current_msg, [])

    # ── 전략 B — 종목 카드 전송
    if hits_b:
        send_telegram_photo("── 📉 반등형(B) TOP5 ──", [])
        current_msg = ''
        for hit in hits_b:
            entry = _format_hit(hit, 0, mins_left)
            if len(current_msg) + len(entry) > MAX_CHAR:
                send_telegram_photo(current_msg, [])
                current_msg = entry
            else:
                current_msg += entry
        if current_msg.strip():
            send_telegram_photo(current_msg, [])

    # ── AI 분석 (전략별 각각)
    if ANTHROPIC_API_KEY or OPENAI_API_KEY:
        if hits_a:
            _send_ai_comment(hits_a, mins_left, strategy='A')
        if hits_b:
            _send_ai_comment(hits_b, mins_left, strategy='B')


def _send_ai_comment(hits: list, mins_left: int, strategy: str = 'A'):
    """전략별 AI 종가배팅 분석 (각각 전송)"""
    try:
        strategy_name = '돌파형(A)' if strategy == 'A' else '반등형(B)'

        # 전략별 데이터 포맷
        if strategy == 'A':
            data_lines = '\n'.join([
                f"- {h['name']}({h['code']}): 현재가={h['close']:,}원 | "
                f"거래량={h['vol_ratio']}배 | 전고점={h.get('near20',0)}% | "
                f"이격={h.get('disp',0)} | 윗꼬리={h['wick_pct']}% | "
                f"목표={h['target1']:,} 손절={h['stoploss']:,} | "
                f"지수={h.get('index_label','')}"
                for h in hits
            ])
            strategy_context = (
                "전략 A는 전고점 돌파형이야. "
                "전고점 85~100% 근처에서 거래량 폭발하며 강봉 마감하는 패턴. "
                "오늘 종가에 진입하면 내일 전고점 돌파 기대."
            )
        else:
            data_lines = '\n'.join([
                f"- {h['name']}({h['code']}): 현재가={h['close']:,}원 | "
                f"Env20={h.get('env20_pct',0):+.1f}% | Env40={h.get('env40_pct',0):+.1f}% | "
                f"RSI={h.get('rsi',0)} | 5일매집={h.get('maejip_5d',0)}회 | "
                f"OBV={'↑' if h.get('obv_rising') else '↓'} | "
                f"거래량vs3일평균={h.get('vol_vs_3d',0):.0f}% | "
                f"목표={h['target1']:,}(MA20) 손절={h['stoploss']:,} | "
                f"지수={h.get('index_label','')}"
                for h in hits
            ])
            strategy_context = (
                "전략 B는 Envelope 하한선 반등형이야. "
                "많이 빠진 바닥 구간에서 OBV 매수세 유입 + 세력 매집 징후 포착. "
                "오늘 종가에 진입하면 Envelope 중심선(MA20) 회귀 기대."
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

        # Claude 우선
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
                    timeout=30
                )
                data = res.json()
                if 'content' in data and data['content']:
                    comment = data['content'][0].get('text', '').strip()
                    log_info(f"✅ Claude {strategy_name} 코멘트 완료")
            except Exception as e:
                log_error(f"⚠️ Claude 실패: {e}")

        # OpenAI 폴백
        if not comment and OPENAI_API_KEY:
            try:
                from openai import OpenAI as _OAI
                client = _OAI(api_key=OPENAI_API_KEY)
                res = client.chat.completions.create(
                    model='gpt-4o-mini',
                    messages=[
                        {'role': 'system', 'content': system_msg},
                        {'role': 'user',   'content': user_msg},
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
                f"🤖 {emoji} {strategy_name} AI 분석\n\n{comment}",
                max_len=3500
            )

    except Exception as e:
        log_error(f"⚠️ AI 코멘트 실패: {e}")


# =============================================================
# 🚀 메인 스캔
# =============================================================

def run_closing_bet_scan(force: bool = False) -> list:
    """종가배팅 스캔 실행"""
    now      = datetime.now(KST)
    now_str  = now.strftime('%H:%M')
    mins_left = _time_to_close()

    if not _is_closing_time(force):
        log_info(
            f"⏸️ 종가배팅 스캐너는 14:50~15:25에만 실행 (현재 {now_str})\n"
            f"   테스트: python closing_bet_scanner.py --force"
        )
        return []

    log_info(f"\n{'='*55}")
    log_info(f"🕯️ 종가배팅 스캔 시작: {now_str} (마감 {mins_left}분 전)")
    log_info(f"{'='*55}")

    # ── 유니버스 선택
    universe_codes = _load_universe(SCAN_UNIVERSE)

    if universe_codes:
        # 코스피200+코스닥150 모드 — 종목명 보강
        codes = universe_codes
        try:
            from pykrx import stock as _pk
            today_str = datetime.now(KST).strftime('%Y%m%d')
            name_map  = {}
            for c in codes[:500]:
                try: name_map[c] = _pk.get_market_ticker_name(c)
                except: name_map[c] = c
            names = [name_map.get(c, c) for c in codes]
        except Exception:
            names = codes
    else:
        # amount_top400 모드 — 기존 방식
        df_krx = load_krx_listing_safe()
        if df_krx is None or df_krx.empty:
            log_error("⚠️ 종목 리스트 로드 실패")
            return []

        col_map = {}
        for c in df_krx.columns:
            cs = str(c).strip()
            if   cs in ('Code','code','티커','종목코드'): col_map[c] = 'Code'
            elif cs in ('Name','name','종목명'):          col_map[c] = 'Name'
            elif cs in ('Amount','amount','거래대금'):    col_map[c] = 'Amount'
            elif cs in ('Market','market'):               col_map[c] = 'Market'
        df_krx = df_krx.rename(columns=col_map)

        if 'Market' in df_krx.columns:
            df_krx = df_krx[df_krx['Market'].isin(['KOSPI','KOSDAQ','코스피','코스닥','유가'])]
        if 'Name' in df_krx.columns:
            df_krx = df_krx[~df_krx['Name'].astype(str).str.contains(
                'ETF|ETN|스팩|제[0-9]+호|우$|우A|우B', na=False
            )]
        if 'Amount' in df_krx.columns:
            df_krx = df_krx.nlargest(TOP_N, 'Amount')

        codes = df_krx['Code'].tolist() if 'Code' in df_krx.columns else []
        names = df_krx['Name'].tolist() if 'Name' in df_krx.columns else codes

    log_info(f"📊 대상: {len(codes)}개 ({SCAN_UNIVERSE})")

    # 병렬 분석
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
                log_info(f"  진행: {done}/{len(codes)} | 후보: {len(hits)}개")

    # ── 전략별 분리 정렬
    hits_a = [h for h in hits if h.get('mode') == 'A']
    hits_b = [h for h in hits if h.get('mode') == 'B']

    # 전략 A: 점수 → 거래량 배율 (폭발적 거래량 우선)
    hits_a.sort(key=lambda x: (x['score'], x['vol_ratio']), reverse=True)

    # 전략 B: 점수 → 거래대금 절대값 (관심 받는 종목, 거래량 배율 무관)
    hits_b.sort(key=lambda x: (x['score'], x['amount_b']), reverse=True)

    # A 먼저, B 뒤에
    hits = hits_a + hits_b

    log_info(f"\n🕯️ 종가배팅 후보: {len(hits)}개")
    log_info(f"  📈 돌파형(A): {len(hits_a)}개 | 📉 반등형(B): {len(hits_b)}개")
    log_info(f"  🏆완전체: {sum(1 for h in hits if h['score']>=5)}개")
    log_info(f"  ✅A급:    {sum(1 for h in hits if h['score']==4)}개")
    log_info(f"  📋B급:    {sum(1 for h in hits if h['score']==3)}개")

    # 텔레그램 전송 (전체 넘기고 _send_results 내부에서 완전체 우선 5개 선별)
    _send_results(hits, mins_left)

    return hits


# =============================================================
# 🚀 엔트리포인트
# =============================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='종가배팅 타점 스캐너')
    parser.add_argument('--force', action='store_true', help='시간 무관 강제 실행')
    args = parser.parse_args()

    # 진단 정보 출력
    now = datetime.now(KST)
    log_info(f"🕯️ 종가배팅 스캐너 시작: {now.strftime('%H:%M')} (force={args.force})")
    log_info(f"  TELEGRAM_TOKEN:    {'✅' if TELEGRAM_TOKEN else '❌ 없음'}")
    log_info(f"  CHAT_ID_LIST:      {'✅ ' + str(CHAT_ID_LIST) if CHAT_ID_LIST else '❌ 없음'}")
    log_info(f"  SCAN_UNIVERSE:     {SCAN_UNIVERSE}")
    log_info(f"  시간 체크:         {'✅ 통과' if _is_closing_time(args.force) else '❌ 시간 외'}")

    # 시간 체크 먼저 — 시간 외면 텔레그램 없이 종료
    if not _is_closing_time(args.force):
        log_info(f"⏸️ 종가배팅 유효 시간 아님 ({now.strftime('%H:%M')}) — 텔레그램 전송 안 함")
        log_info("  유효 시간: 14:50~15:25 | 강제 실행: --force")
        sys.exit(0)

    hits = run_closing_bet_scan(force=args.force)

    if not hits:
        log_info("✅ 종가배팅 후보 없음")
        # 유효 시간 내 실행인데 후보가 없는 경우만 텔레그램
        if TELEGRAM_TOKEN:
            send_telegram_photo(
                f"🕯️ [{TODAY_STR} {now.strftime('%H:%M')}] 종가배팅 후보 없음\n"
                f"(대상: {SCAN_UNIVERSE} | 조건 미충족)",
                []
            )
            log_info("✅ '후보없음' 텔레그램 전송 완료")
    else:
        log_info(f"✅ 종가배팅 후보 {len(hits)}개 텔레그램 전송 완료")

    sys.exit(0)
