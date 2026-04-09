# -*- coding: utf-8 -*-
# ================================================================
# usStockScanner.py — 미국주식 수박지표 스캐너 (main7_bugfix_2 연동 통합본)
# ================================================================
# 핵심 변경사항
# - main7_bugfix_2.py 공용 엔진 사용 (브리지 경유)
# - 유니버스 확장: us_all = NASDAQ + NYSE + AMEX
# - ETF/ETN 추정 제외 옵션 추가
# - 탈락 사유 통계 로그 추가
# - 환경변수 기반으로 모드/필터 튜닝 가능
#
# 권장 환경변수
#   US_UNIVERSE_MODE=us_all
#   US_EXCLUDE_ETF=1
#   US_MIN_AMOUNT_USD=20000000
#   US_MIN_PRICE=5
#   US_MAX_WORKERS=12
#   US_SCAN_TIMEOUT=900
# ================================================================

import os
import signal as _sig
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from datetime import datetime, timedelta

import FinanceDataReader as fdr
import pandas as pd
import requests

from main7_us_bridge_v2 import (
    get_indicators,
    classify_style,
    judge_trade_with_sequence,
    build_default_signals,
    inject_tri_result,
    calc_pivot_levels,
    calc_fibonacci_levels,
    calc_atr_targets,
    send_telegram_photo,
    send_tournament_results,
    get_ai_summary_batch,
    run_ai_tournament,
    _fetch_stock_news,
    update_google_sheet,
    OPENAI_API_KEY,
    ANTHROPIC_API_KEY,
    GEMINI_API_KEY,
    GROQ_API_KEY,
    TODAY_STR,
    prepare_historical_weather,
)

try:
    from triangle_combo_analyzer import jongbe_triangle_combo_v3
except Exception:
    def jongbe_triangle_combo_v3(df):
        return {}


# ================================================================
# ⚙️ 설정
# ================================================================

def _env_bool(name: str, default: bool) -> bool:
    val = str(os.environ.get(name, '')).strip().lower()
    if not val:
        return default
    return val in ('1', 'true', 'y', 'yes', 'on')


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.environ.get(name, default)).replace(',', '').strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.environ.get(name, default)).replace(',', '').strip())
    except Exception:
        return default


US_UNIVERSE_MODE = os.environ.get('US_UNIVERSE_MODE', 'us_all').strip().lower()
US_MIN_AMOUNT_USD = _env_float('US_MIN_AMOUNT_USD', 20_000_000)
US_MIN_PRICE = _env_float('US_MIN_PRICE', 5.0)
US_MAX_WORKERS = _env_int('US_MAX_WORKERS', 12)
US_SCAN_TIMEOUT = _env_int('US_SCAN_TIMEOUT', 900)
US_LOOKBACK_DAYS = _env_int('US_LOOKBACK_DAYS', 320)
US_EXCLUDE_ETF = _env_bool('US_EXCLUDE_ETF', True)
US_NEWS_TIMEOUT = _env_int('US_NEWS_TIMEOUT', 5)
US_PROGRESS_STEP = _env_int('US_PROGRESS_STEP', 50)
US_MEGA_TOP_N = _env_int('US_MEGA_TOP_N', 400)

RN_LIST_US = [
    5, 10, 15, 20, 25, 30, 40, 50, 75, 100,
    125, 150, 200, 250, 300, 400, 500, 750, 1000, 1500, 2000
]


def sf(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def get_target_levels_us(price):
    upper = [r for r in RN_LIST_US if r > price]
    lower = [r for r in RN_LIST_US if r <= price]
    return (lower[-1] if lower else None, upper[0] if upper else None)


# ================================================================
# 📋 티커 로딩
# ================================================================

_NASDAQ100_FALLBACK = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'GOOG', 'TSLA',
    'AVGO', 'COST', 'NFLX', 'AMD', 'PEP', 'CSCO', 'ADBE', 'QCOM',
    'TXN', 'INTU', 'AMGN', 'CMCSA', 'AMAT', 'BKNG', 'ISRG', 'VRTX',
    'ADP', 'REGN', 'PANW', 'SBUX', 'LRCX', 'MU', 'KLAC', 'SNPS',
    'CDNS', 'MELI', 'FTNT', 'ABNB', 'ORLY', 'CTAS', 'MNST', 'MRVL',
    'PYPL', 'CRWD', 'DXCM', 'PCAR', 'WDAY', 'ROST', 'ODFL', 'IDXX',
    'FAST', 'MRNA', 'ON', 'DDOG', 'ZS', 'TEAM', 'TTD', 'PLTR', 'ARM',
    'ORCL', 'CRM', 'NOW', 'SNOW', 'UBER', 'NET', 'COIN',
]

_SP500_EXTRA_FALLBACK = [
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'BLK', 'BRK-B',
    'JNJ', 'UNH', 'PFE', 'LLY', 'ABBV', 'MRK',
    'XOM', 'CVX', 'COP', 'SLB',
    'V', 'MA', 'AXP', 'PYPL',
    'HD', 'LOW', 'TGT', 'WMT',
    'BA', 'LMT', 'RTX', 'NOC', 'GD',
    'CAT', 'DE', 'MMM', 'HON', 'GE',
    'DIS', 'NFLX', 'PARA', 'WBD',
    'T', 'VZ', 'TMUS',
    'NEE', 'DUK', 'SO', 'AEP',
    'AMT', 'PLD', 'EQIX', 'CCI',
]

_ETF_NAME_KEYWORDS = (
    ' ETF', ' ETN', ' FUND', ' TRUST', ' SHARES', ' INDEX', ' INVERSE', ' LEVERAGED',
    ' ULTRA', ' BEAR', ' BULL', ' 2X', ' 3X'
)


def normalize_ticker(ticker: str) -> str:
    return str(ticker).strip().upper().replace('.', '-')


def get_nasdaq100_wikipedia() -> list:
    try:
        url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        tables = pd.read_html(res.text)
        for table in tables:
            cols = [str(c).lower() for c in table.columns]
            if any('ticker' in c or 'symbol' in c for c in cols):
                col = next(c for c in table.columns if 'ticker' in str(c).lower() or 'symbol' in str(c).lower())
                tickers = [normalize_ticker(t) for t in table[col].dropna() if str(t).strip()]
                print(f'✅ Wikipedia NASDAQ-100: {len(tickers)}개')
                return tickers
    except Exception as e:
        print(f'⚠️ Wikipedia NASDAQ-100 실패: {e}')
    return []


def get_sp500_wikipedia() -> list:
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        df = pd.read_html(res.text)[0]
        col = next(c for c in df.columns if 'symbol' in str(c).lower() or 'ticker' in str(c).lower())
        tickers = [normalize_ticker(t) for t in df[col].dropna() if str(t).strip()]
        print(f'✅ Wikipedia S&P500: {len(tickers)}개')
        return tickers
    except Exception as e:
        print(f'⚠️ Wikipedia S&P500 실패: {e}')
    return []


def _guess_symbol_column(df: pd.DataFrame) -> str:
    for col in df.columns:
        low = str(col).lower()
        if 'symbol' in low or 'ticker' in low or low == 'code':
            return col
    return df.columns[0]


def _guess_name_column(df: pd.DataFrame):
    for col in df.columns:
        low = str(col).lower()
        if 'name' in low or 'company' in low or 'security' in low:
            return col
    return None


def _guess_mcap_column(df: pd.DataFrame):
    for col in df.columns:
        low = str(col).lower()
        if 'marketcap' in low or 'marcap' in low or 'cap' in low:
            return col
    return None


def _strip_etf_like_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or not US_EXCLUDE_ETF:
        return df

    name_col = _guess_name_column(df)
    if not name_col:
        return df

    work = df.copy()
    name_upper = work[name_col].fillna('').astype(str).str.upper()
    mask = pd.Series(False, index=work.index)
    for kw in _ETF_NAME_KEYWORDS:
        mask = mask | name_upper.str.contains(kw.strip(), regex=False)

    removed = int(mask.sum())
    if removed:
        print(f'  ↳ ETF/ETN 추정 제외: {removed}개')
    return work.loc[~mask].copy()


def load_listing_symbols(market: str) -> list:
    try:
        df = fdr.StockListing(market)
        if df is None or df.empty:
            print(f'⚠️ {market}: 빈 목록')
            return []
        df = _strip_etf_like_rows(df)
        sym_col = _guess_symbol_column(df)
        tickers = [normalize_ticker(t) for t in df[sym_col].dropna().tolist() if str(t).strip()]
        print(f'✅ {market}: {len(tickers)}개')
        return tickers
    except Exception as e:
        print(f'⚠️ {market} 로드 실패: {e}')
        return []


def load_mega_tickers(top_n: int = US_MEGA_TOP_N) -> list:
    all_rows = []
    for market in ('NASDAQ', 'NYSE', 'AMEX'):
        try:
            df = fdr.StockListing(market)
            if df is None or df.empty:
                continue
            df = _strip_etf_like_rows(df)
            df['__market__'] = market
            all_rows.append(df)
        except Exception as e:
            print(f'⚠️ {market} mega 로드 실패: {e}')

    if not all_rows:
        return _NASDAQ100_FALLBACK[:]

    all_df = pd.concat(all_rows, ignore_index=True)
    mcap_col = _guess_mcap_column(all_df)
    sym_col = _guess_symbol_column(all_df)

    if mcap_col:
        all_df[mcap_col] = pd.to_numeric(all_df[mcap_col], errors='coerce').fillna(0)
        all_df = all_df.sort_values(mcap_col, ascending=False)
    tickers = [normalize_ticker(t) for t in all_df[sym_col].dropna().tolist() if str(t).strip()]
    tickers = list(dict.fromkeys(tickers))[:top_n]
    print(f'✅ MEGA 유니버스: {len(tickers)}개 (Top {top_n})')
    return tickers


def load_us_tickers(mode: str = 'us_all') -> list:
    mode = (mode or 'us_all').strip().lower()
    tickers = []

    if mode == 'nasdaq100':
        tickers = get_nasdaq100_wikipedia() or _NASDAQ100_FALLBACK

    elif mode == 'sp500':
        tickers = get_sp500_wikipedia() or load_listing_symbols('S&P500') or _SP500_EXTRA_FALLBACK

    elif mode == 'nasdaq100+sp500':
        nq = get_nasdaq100_wikipedia() or _NASDAQ100_FALLBACK
        sp = get_sp500_wikipedia() or load_listing_symbols('S&P500') or _SP500_EXTRA_FALLBACK
        tickers = nq + [t for t in sp if t not in nq]

    elif mode == 'nasdaq':
        tickers = load_listing_symbols('NASDAQ')

    elif mode == 'nyse':
        tickers = load_listing_symbols('NYSE')

    elif mode == 'amex':
        tickers = load_listing_symbols('AMEX')

    elif mode == 'mega':
        tickers = load_mega_tickers(top_n=US_MEGA_TOP_N)

    elif mode == 'us_all':
        merged = []
        for market in ('NASDAQ', 'NYSE', 'AMEX'):
            merged.extend(load_listing_symbols(market))
        tickers = merged

    else:
        print(f'⚠️ 알 수 없는 mode={mode}, us_all로 대체')
        return load_us_tickers('us_all')

    tickers = list(dict.fromkeys([normalize_ticker(t) for t in tickers if str(t).strip()]))
    print(f'📋 스캔 대상: {len(tickers)}개 ({mode})')
    return tickers


# ================================================================
# 🔍 종목 분석
# ================================================================

def _reject(status: str, ticker: str, extra=None):
    return {'status': status, 'ticker': ticker, 'hits': [], 'extra': extra or {}}


def analyze_final_us(ticker: str, historical_indices: pd.DataFrame) -> dict:
    try:
        df = fdr.DataReader(
            ticker,
            start=(datetime.now() - timedelta(days=US_LOOKBACK_DAYS)).strftime('%Y-%m-%d')
        )
        if df is None or df.empty:
            return _reject('fetch_empty', ticker)

        if len(df) < 100:
            return _reject('too_short', ticker, {'rows': len(df)})

        close_p = sf(df['Close'].iloc[-1])
        if close_p < US_MIN_PRICE:
            return _reject('low_price', ticker, {'close': close_p})

        avg_amount = sf((df['Close'] * df['Volume']).tail(5).mean())
        if avg_amount < US_MIN_AMOUNT_USD:
            return _reject('low_amount', ticker, {'avg_amount': avg_amount})

        df = get_indicators(df)
        if df is None or df.empty:
            return _reject('indicator_fail', ticker)

        if historical_indices is not None and not historical_indices.empty:
            df = df.join(historical_indices, how='left').ffill()

        if len(df) < 2:
            return _reject('too_short_after_ind', ticker, {'rows': len(df)})

        row = df.iloc[-1]
        prev = df.iloc[-2]
        curr_idx = df.index[-1]
        close_p = sf(row.get('Close', close_p))
        raw_idx = len(df) - 1
        temp_df = df.iloc[:raw_idx + 1]

        signals = build_default_signals(row, close_p, prev)
        new_tags = []

        try:
            tri_result = jongbe_triangle_combo_v3(temp_df) or {}
        except Exception:
            tri_result = {}
        signals, new_tags = inject_tri_result(signals, tri_result, new_tags)

        has_signal = any([
            signals.get('watermelon_signal'),
            signals.get('watermelon_red'),
            signals.get('dolbanzi'),
            signals.get('Real_Viper_Hook'),
            signals.get('force_pullback'),
            signals.get('bb40_reclaim_rsi_div'),
            signals.get('explosion_ready'),
            signals.get('bb30_shift_gc'),
        ])
        if not has_signal:
            return _reject('pattern_miss', ticker)

        result = judge_trade_with_sequence(temp_df, signals)
        new_tags.extend(result.get('tags', []))

        style = classify_style(row)
        style_label = {'SWING': '📈스윙', 'SCALP': '⚡단타', 'NONE': '➖미분류'}.get(style, '➖미분류')

        storm_count = 0
        for k in ('ixic', 'sp500'):
            try:
                if sf(row.get(f'{k}_close', 0)) <= sf(row.get(f'{k}_ma5', 0)):
                    storm_count += 1
            except Exception:
                pass

        _pivot = calc_pivot_levels(temp_df)
        _fib = calc_fibonacci_levels(temp_df)
        _atr_t = calc_atr_targets(row, close_p)

        atr_target1 = sf(_atr_t.get('target_1', round(close_p * 1.08, 2)))
        atr_target2 = sf(_atr_t.get('target_2', round(close_p * 1.15, 2)))
        atr_stop = sf(_atr_t.get('stop_atr', round(close_p * 0.95, 2)))
        atr_val = sf(_atr_t.get('atr_val', 0))
        rr_ratio = sf(_atr_t.get('risk_reward', 1.3), 1.3)

        tags = [style_label, '🇺🇸미국주식']
        if signals.get('watermelon_signal'):
            tags.append('🍉수박신호')
        if signals.get('dolbanzi'):
            tags.append('💍돌반지')
        if signals.get('Real_Viper_Hook'):
            tags.append('🐍독사훅')
        if signals.get('explosion_ready'):
            tags.append('💎폭발직전')
        if signals.get('force_pullback'):
            tags.append('🧲세력눌림')
        if signals.get('bb30_shift_gc'):
            tags.append('🎯BB30시프트')

        fib_382 = sf(_fib.get('fib_382', 0))
        fib_618 = sf(_fib.get('fib_618', 0))
        tol = 0.02
        if fib_382 > 0 and abs(close_p - fib_382) / fib_382 <= tol:
            tags.append('🔢Fib38.2%지지')
            signals['fib_support_382'] = True
        if fib_618 > 0 and abs(close_p - fib_618) / fib_618 <= tol:
            tags.append('🔢Fib61.8%강지지')
            signals['fib_support_618'] = True

        s_score = max(0, int(sf(result.get('score', 0)) - storm_count * 10))
        print(f'  🇺🇸 {ticker} 포착! 점수:{result.get("score", 0)} | {result.get("combination", "")})')

        hit = {
            '날짜': curr_idx.strftime('%Y-%m-%d'),
            '종목명': ticker,
            'code': ticker,
            '시장': '🇺🇸US',
            'N등급': f"{result.get('type', '')}{result.get('grade', '')}",
            'N조합': result.get('combination', ''),
            'N점수': int(sf(result.get('score', 0))),
            'N구분': ' '.join([str(x) for x in new_tags if str(x).strip()]),
            '👑등급': result.get('grade', ''),
            '📜서사히스토리': f"🇺🇸{ticker} | {result.get('combination', '')}",
            '확신점수': 0,
            '🎯목표타점': round(atr_target1, 2),
            '🎯목표2차': round(atr_target2, 2),
            '🚨손절가': round(atr_stop, 2),
            'RR비율': round(rr_ratio, 1),
            'PP': sf(_pivot.get('PP', 0)),
            'R1': sf(_pivot.get('R1', 0)),
            'R2': sf(_pivot.get('R2', 0)),
            'S1': sf(_pivot.get('S1', 0)),
            'S2': sf(_pivot.get('S2', 0)),
            'Fib382': fib_382,
            'Fib618': fib_618,
            'ATR값': round(atr_val, 2),
            '기상': '☀️' * max(0, 2 - storm_count) + '🌪️' * storm_count,
            '안전점수': s_score,
            'RSI': int(sf(row.get('RSI', 0))),
            '점수': s_score,
            '에너지': '🔋' if sf(row.get('MACD_Hist', 0)) > 0 else '🪫',
            '현재가': round(close_p, 2),
            '구분': ' '.join(tags),
            '재무': 'N/A',
            '수급': '🇺🇸거래량기반',
            '이격': int(sf(row.get('Disparity', 0))),
            'BB40': f"{sf(row.get('BB40_Width', 0)):.1f}",
            'MA수렴': f"{sf(row.get('MA_Convergence', 0)):.1f}",
            '매집': 'N/A',
            'OBV기울기': int(sf(row.get('OBV_Slope', 0))),
            '평균거래대금': int(avg_amount),
            'news_sentiment': '',
        }
        return {'status': 'hit', 'ticker': ticker, 'hits': [hit], 'extra': {'avg_amount': avg_amount}}

    except Exception as e:
        return _reject('exception', ticker, {'error': str(e)})


# ================================================================
# 🚀 스캔 실행
# ================================================================

def run_us_scanner(weather_data, mode: str = None):
    mode = (mode or US_UNIVERSE_MODE).strip().lower()
    tickers = load_us_tickers(mode=mode)
    if not tickers:
        print('❌ 티커 로드 실패')
        return [], Counter()

    print(f'\n🇺🇸 미국주식 스캔 시작: {len(tickers)}개 | mode={mode}')
    print(
        f'  필터: 가격>={US_MIN_PRICE:.2f} / 평균거래대금>={US_MIN_AMOUNT_USD:,.0f} / '
        f'ETF제외={US_EXCLUDE_ETF} / workers={US_MAX_WORKERS}'
    )

    stats = Counter()
    all_hits = []

    with ThreadPoolExecutor(max_workers=US_MAX_WORKERS) as executor:
        future_map = {executor.submit(analyze_final_us, ticker, weather_data): ticker for ticker in tickers}
        done = 0
        try:
            for future in as_completed(future_map, timeout=US_SCAN_TIMEOUT):
                ticker = future_map[future]
                done += 1
                try:
                    res = future.result(timeout=45)
                    status = res.get('status', 'unknown')
                    stats[status] += 1
                    if res.get('hits'):
                        all_hits.extend(res['hits'])
                except FuturesTimeoutError:
                    stats['future_timeout'] += 1
                except Exception as e:
                    stats['future_exception'] += 1
                    print(f'  ⚠️ {ticker} future 실패: {e}')

                if done % max(1, US_PROGRESS_STEP) == 0:
                    print(
                        f'  진행: {done}/{len(tickers)} | 포착:{len(all_hits)} | '
                        f'저유동:{stats.get("low_amount",0)} | 패턴없음:{stats.get("pattern_miss",0)}'
                    )
        except FuturesTimeoutError:
            stats['scan_timeout'] += 1
            print(f'⚠️ 전체 스캔 타임아웃({US_SCAN_TIMEOUT}s) 도달 — 완료된 결과만 사용합니다.')

    all_hits.sort(key=lambda x: x.get('N점수', 0), reverse=True)

    print('\n📊 미국 스캔 탈락/통과 요약')
    ordered = [
        'hit', 'fetch_empty', 'too_short', 'low_price', 'low_amount',
        'indicator_fail', 'too_short_after_ind', 'pattern_miss',
        'exception', 'future_timeout', 'future_exception', 'scan_timeout'
    ]
    for key in ordered:
        if stats.get(key):
            print(f'  - {key}: {stats[key]}')
    print(f'✅ 스캔 완료: {len(all_hits)}개 포착')
    return all_hits, stats


# ================================================================
# 📱 텔레그램 포맷
# ================================================================

def format_us_entry(item: dict) -> str:
    pp = sf(item.get('PP', 0))
    r1 = sf(item.get('R1', 0))
    s1 = sf(item.get('S1', 0))
    f382 = sf(item.get('Fib382', 0))
    f618 = sf(item.get('Fib618', 0))
    atr = sf(item.get('ATR값', 0))
    tgt1 = sf(item.get('🎯목표타점', 0))
    tgt2 = sf(item.get('🎯목표2차', 0))
    stp = sf(item.get('🚨손절가', 0))
    rr = sf(item.get('RR비율', 1.3), 1.3)
    avg_amount = sf(item.get('평균거래대금', 0))

    line = (
        '────────────────────────\n'
        f"⭐ {item.get('N등급', '?')}  [{item.get('종목명', '?')}]  ${sf(item.get('현재가', 0)):.2f}\n"
        f"🎯 {item.get('N조합', '')}\n"
        f"🏷️ {item.get('N구분', '')}\n"
        f"💰 이격:{item.get('이격', 0)} | RSI:{item.get('RSI', 0)} | {item.get('에너지', '')}\n"
        f"📊 MA수렴:{item.get('MA수렴', 0)} | BB40:{item.get('BB40', 0)} | OBV:{item.get('OBV기울기', 0)}\n"
        f"💵 5일평균 거래대금:${avg_amount/1_000_000:.1f}M\n"
    )
    if tgt1 > 0:
        line += f'📌 목표1:${tgt1:.2f} → 목표2:${tgt2:.2f} | 손절:${stp:.2f} (RR {rr:.1f})\n'
    if pp > 0:
        line += f'📐 PP:${pp:.2f} | R1:${r1:.2f} | S1:${s1:.2f}\n'
    if f382 > 0:
        line += f'🔢 Fib38.2%:${f382:.2f} | Fib61.8%:${f618:.2f}\n'
    if atr > 0:
        line += f'📏 ATR:${atr:.2f}\n'

    news = str(item.get('news_sentiment', '')).strip()
    if news:
        line += f'📰 {news[:120]}\n'

    ai_tip = str(item.get('ai_tip', '')).strip()
    if ai_tip:
        line += f'💡 {ai_tip[:200]}\n'

    return line


# ================================================================
# 📰 뉴스 / AI 보강
# ================================================================

def enrich_news(ai_candidates: pd.DataFrame) -> pd.DataFrame:
    if ai_candidates is None or ai_candidates.empty:
        return ai_candidates

    print('📰 뉴스 조회 중...')
    for idx, row_n in ai_candidates.iterrows():
        try:
            def _timeout_handler(signum, frame):
                raise TimeoutError()

            _sig.signal(_sig.SIGALRM, _timeout_handler)
            _sig.alarm(max(1, US_NEWS_TIMEOUT))
            news = _fetch_stock_news(str(row_n.get('code', '')), str(row_n.get('종목명', '')))
            _sig.alarm(0)
        except Exception:
            news = ''
            try:
                _sig.alarm(0)
            except Exception:
                pass
        ai_candidates.at[idx, 'news_sentiment'] = news
    return ai_candidates


def enrich_ai_tips(ai_candidates: pd.DataFrame) -> pd.DataFrame:
    if ai_candidates is None or ai_candidates.empty:
        return ai_candidates

    print('🧠 AI 코멘트 생성 중...')
    try:
        ai_result_text = get_ai_summary_batch(ai_candidates, issues=None)
        ai_map = {}
        current_key, current_lines = None, []

        for line in str(ai_result_text).splitlines():
            if line.startswith('[') and '(' in line and line.endswith(']'):
                if current_key and current_lines:
                    ai_map[current_key] = '\n'.join(current_lines).strip()
                current_key = line[1:-1]
                current_lines = []
            elif current_key:
                current_lines.append(line)

        if current_key and current_lines:
            ai_map[current_key] = '\n'.join(current_lines).strip()

        for idx, item in ai_candidates.iterrows():
            key = f"{item['종목명']}({item['code']})"
            ai_candidates.at[idx, 'ai_tip'] = ai_map.get(key, '')

    except Exception as e:
        print(f'⚠️ AI 코멘트 실패: {e}')
        ai_candidates['ai_tip'] = ''

    return ai_candidates


def send_top_cards(ai_candidates: pd.DataFrame, title_date: str):
    print('📨 텔레그램 전송 중...')
    if ai_candidates is None or ai_candidates.empty:
        send_telegram_photo(f'🇺🇸 [{title_date}] 미국주식 수박신호 해당 없음', [])
        return

    max_char = 3800
    top_n = min(10, len(ai_candidates))
    current_msg = f'🇺🇸 [미국주식 수박신호 TOP {top_n}] {title_date}\n\n'

    for _, item in ai_candidates.head(top_n).iterrows():
        entry = format_us_entry(item.to_dict())
        if len(current_msg) + len(entry) > max_char:
            send_telegram_photo(current_msg, [])
            current_msg = '🇺🇸 [미국주식 - 이어서]\n\n' + entry
        else:
            current_msg += entry

    send_telegram_photo(current_msg, [])


# ================================================================
# 🚀 메인
# ================================================================

def main():
    print('🇺🇸 미국주식 수박지표 스캐너 (main7_bugfix_2 연동) 가동...')
    print(
        f"  API: GPT={'✅' if OPENAI_API_KEY else '❌'} "
        f"Claude={'✅' if ANTHROPIC_API_KEY else '❌'} "
        f"Gemini={'✅' if GEMINI_API_KEY else '❌'} "
        f"Groq={'✅' if GROQ_API_KEY else '❌'}"
    )
    print(
        f'  MODE={US_UNIVERSE_MODE} | MIN_PRICE={US_MIN_PRICE} | '
        f'MIN_AMOUNT={US_MIN_AMOUNT_USD:,.0f} | EXCLUDE_ETF={US_EXCLUDE_ETF}'
    )

    weather_data = prepare_historical_weather()
    us_hits, stats = run_us_scanner(weather_data, mode=US_UNIVERSE_MODE)

    if not us_hits:
        msg = (
            f'🇺🇸 [{TODAY_STR}] 미국주식 수박신호 해당 없음\n'
            f"mode={US_UNIVERSE_MODE} / 저유동탈락={stats.get('low_amount', 0)} / 패턴없음={stats.get('pattern_miss', 0)}"
        )
        send_telegram_photo(msg, [])
        print('✅ 종료')
        raise SystemExit(0)

    ai_candidates = pd.DataFrame(us_hits[:15]).copy()
    ai_candidates = enrich_news(ai_candidates)
    ai_candidates = enrich_ai_tips(ai_candidates)

    send_top_cards(ai_candidates, TODAY_STR)

    print('🏆 AI 토너먼트 실행 중...')
    print(
        f"  GPT={bool(OPENAI_API_KEY)} Claude={bool(ANTHROPIC_API_KEY)} "
        f"Gemini={bool(GEMINI_API_KEY)} Groq={bool(GROQ_API_KEY)}"
    )
    try:
        tournament = run_ai_tournament(ai_candidates, issues=None)
        if tournament:
            send_tournament_results(tournament)
            print('✅ 토너먼트 전송 완료')
        else:
            print('⚠️ 토너먼트 결과 없음')
    except Exception as e:
        print(f'⚠️ 토너먼트 실패: {e}')

    try:
        update_google_sheet(us_hits, TODAY_STR + '_US', '')
        print(f'💾 구글시트 {len(us_hits)}개 저장 완료')
    except Exception as e:
        print(f'⚠️ 시트 저장 실패: {e}')

    print('✅ 미국주식 스캐너 종료')


if __name__ == '__main__':
    main()
