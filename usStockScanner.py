# ================================================================
# us_scanner.py — 미국주식 수박지표 스캐너 (Ver 2.0)
# ================================================================
# main7.py 최신 변경사항 완전 반영:
#   - AI 토너먼트 4종 (GPT + Claude + Gemini + Groq)
#   - 피봇/피보나치/ATR 타점 계산
#   - 종목 유니버스 확장 (NASDAQ100 + S&P500 + 동적 로딩)
#   - send_tournament_results 분할 전송
#   - 뉴스 감성 조회
#
# 실행: python us_scanner.py
# ================================================================

import os
import sys
import time
import pandas as pd
import numpy as np
import requests
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ────────────────────────────────────────────────────────────────
# main7.py에서 import
# ────────────────────────────────────────────────────────────────
from main7 import (
    # 지표 계산
    get_indicators,
    classify_style,
    judge_trade_with_sequence,
    build_default_signals,
    inject_tri_result,

    # 피봇/피보나치/ATR (Ver 27.18+)
    calc_pivot_levels,
    calc_fibonacci_levels,
    calc_atr_targets,

    # 텔레그램
    send_telegram_photo,
    send_telegram_chunks,
    send_tournament_results,      # AI별 분리 + 분할 전송

    # AI
    get_ai_summary_batch,
    run_ai_tournament,

    # 뉴스
    _fetch_stock_news,

    # 구글시트
    update_google_sheet,

    # 환경변수
    TELEGRAM_TOKEN,
    CHAT_ID_LIST,
    OPENAI_API_KEY,
    ANTHROPIC_API_KEY,
    GEMINI_API_KEY,
    GROQ_API_KEY,
    TODAY_STR,
    KST,

    # 기상
    prepare_historical_weather,
)

# 삼각수렴
try:
    from triangle_combo_analyzer import jongbe_triangle_combo_v3
except ImportError:
    def jongbe_triangle_combo_v3(df): return {}


# ================================================================
# ⚙️ 설정
# ================================================================

US_MIN_AMOUNT_USD  = 50_000_000    # 거래대금 최소 5천만 달러
US_MIN_PRICE       = 5.0           # 최소 $5 (페니스톡 제외)
MAX_WORKERS        = 12
SCAN_TIMEOUT       = 900           # 15분

# 달러 기준 라운드넘버
RN_LIST_US = [
    5, 10, 15, 20, 25, 30, 40, 50, 75, 100,
    125, 150, 200, 250, 300, 400, 500, 750, 1000, 1500, 2000
]

def get_target_levels_us(price):
    upper = [r for r in RN_LIST_US if r > price]
    lower = [r for r in RN_LIST_US if r <= price]
    return (lower[-1] if lower else None, upper[0] if upper else None)


# ================================================================
# 📋 종목 유니버스 — 3단계 (NASDAQ100 → S&P500 → FDR)
# ================================================================

# 백업용 하드코딩 (Wikipedia 실패 시 폴백)
_NASDAQ100_FALLBACK = [
    'AAPL','MSFT','NVDA','AMZN','META','GOOGL','GOOG','TSLA',
    'AVGO','COST','NFLX','AMD','PEP','CSCO','ADBE','QCOM',
    'TXN','INTU','AMGN','CMCSA','AMAT','BKNG','ISRG','VRTX',
    'ADP','REGN','PANW','SBUX','LRCX','MU','KLAC','SNPS',
    'CDNS','MELI','FTNT','ABNB','ORLY','CTAS','MNST','MRVL',
    'PYPL','CRWD','DXCM','PCAR','WDAY','ROST','ODFL','IDXX',
    'FAST','MRNA','ON','DDOG','ZS','TEAM','TTD','PLTR','ARM',
    'ORCL','CRM','NOW','SNOW','UBER','NET','COIN',
]

_SP500_EXTRA_FALLBACK = [
    # NASDAQ100 제외 S&P500 주요 종목
    'JPM','BAC','WFC','GS','MS','BLK','BRK-B',
    'JNJ','UNH','PFE','LLY','ABBV','MRK',
    'XOM','CVX','COP','SLB',
    'V','MA','AXP','PYPL',
    'HD','LOW','TGT','WMT',
    'BA','LMT','RTX','NOC','GD',
    'CAT','DE','MMM','HON','GE',
    'DIS','NFLX','PARA','WBD',
    'GOOGL','GOOG','META',
    'T','VZ','TMUS',
    'NEE','DUK','SO','AEP',
    'AMT','PLD','EQIX','CCI',
]


def get_nasdaq100_wikipedia() -> list:
    """Wikipedia에서 NASDAQ-100 정확한 구성 종목"""
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        tables = pd.read_html(res.text)
        for table in tables:
            cols = [str(c).lower() for c in table.columns]
            if any('ticker' in c or 'symbol' in c for c in cols):
                col = next(c for c in table.columns
                           if 'ticker' in str(c).lower() or 'symbol' in str(c).lower())
                tickers = [str(t).strip() for t in table[col].dropna()
                           if isinstance(t, str) and t.strip()]
                print(f"✅ Wikipedia NASDAQ-100: {len(tickers)}개")
                return tickers
    except Exception as e:
        print(f"⚠️ Wikipedia NASDAQ-100 실패: {e}")
    return []


def get_sp500_wikipedia() -> list:
    """Wikipedia에서 S&P500 구성 종목"""
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        df = pd.read_html(res.text)[0]
        col = next(c for c in df.columns
                   if 'symbol' in str(c).lower() or 'ticker' in str(c).lower())
        tickers = [str(t).strip().replace('.', '-') for t in df[col].dropna()]
        print(f"✅ Wikipedia S&P500: {len(tickers)}개")
        return tickers
    except Exception as e:
        print(f"⚠️ Wikipedia S&P500 실패: {e}")
    return []


def load_us_tickers(mode: str = 'nasdaq100+sp500') -> list:
    """
    미국주식 티커 로드.

    mode 옵션:
      'nasdaq100'       — NASDAQ-100 (약 100개)
      'sp500'           — S&P500 (약 500개)
      'nasdaq100+sp500' — 합집합 (약 550개, 기본값)
      'mega'            — 시총 Top 200 (FDR 기반)
    """
    tickers = []

    if mode in ('nasdaq100', 'nasdaq100+sp500'):
        nq = get_nasdaq100_wikipedia() or _NASDAQ100_FALLBACK
        tickers.extend(nq)

    if mode in ('sp500', 'nasdaq100+sp500'):
        sp = get_sp500_wikipedia() or _SP500_EXTRA_FALLBACK
        # 중복 제거
        tickers.extend([t for t in sp if t not in tickers])

    if mode == 'mega':
        # FDR로 나스닥 상위 종목 동적 로드
        try:
            df_fdr = fdr.StockListing('NASDAQ')
            if df_fdr is not None and not df_fdr.empty:
                mcap_col = next((c for c in df_fdr.columns
                                 if 'cap' in c.lower() or 'marcap' in c.lower()), None)
                if mcap_col:
                    df_fdr = df_fdr.nlargest(200, mcap_col)
                sym_col = next((c for c in df_fdr.columns
                                if 'symbol' in c.lower() or 'ticker' in c.lower()), 'Symbol')
                tickers = df_fdr[sym_col].tolist()[:200]
                print(f"✅ FDR NASDAQ 상위 {len(tickers)}개")
        except Exception as e:
            print(f"⚠️ FDR 로드 실패: {e}")
            tickers = _NASDAQ100_FALLBACK

    # 최종 정리 (빈값/특수문자 제거)
    tickers = list(dict.fromkeys([
        t.strip().upper() for t in tickers
        if t and isinstance(t, str) and t.strip()
    ]))

    print(f"📋 스캔 대상: {len(tickers)}개 ({mode})")
    return tickers


# ================================================================
# 🔍 종목 분석 함수
# ================================================================

def analyze_final_us(ticker: str, historical_indices: pd.DataFrame) -> list:
    try:
        df = fdr.DataReader(
            ticker,
            start=(datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d')
        )
        if df is None or len(df) < 100:
            return []

        # 가격 필터
        if float(df['Close'].iloc[-1]) < US_MIN_PRICE:
            return []

        # 거래대금 필터 (달러 기준)
        avg_amount = (df['Close'] * df['Volume']).tail(5).mean()
        if avg_amount < US_MIN_AMOUNT_USD:
            return []

        # 지표 계산
        df = get_indicators(df)
        if df is None or df.empty:
            return []

        df = df.join(historical_indices, how='left').ffill()

        row      = df.iloc[-1]
        prev     = df.iloc[-2]
        curr_idx = df.index[-1]
        close_p  = float(row['Close'])
        raw_idx  = len(df) - 1
        temp_df  = df.iloc[:raw_idx + 1]

        # signals 구성
        signals  = build_default_signals(row, close_p, prev)
        new_tags = []

        # 삼각수렴 주입
        try:
            tri_result = jongbe_triangle_combo_v3(temp_df) or {}
        except Exception:
            tri_result = {}
        signals, new_tags = inject_tri_result(signals, tri_result, new_tags)

        # 핵심 신호 필터
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
            return []

        # 조합 점수
        result = judge_trade_with_sequence(temp_df, signals)
        new_tags.extend(result.get('tags', []))

        # 스타일
        style = classify_style(row)
        style_label = {'SWING': '📈스윙', 'SCALP': '⚡단타', 'NONE': '➖미분류'}[style]

        # 시장 기상
        storm_count = sum(
            1 for k in ['ixic', 'sp500']
            if row.get(f'{k}_close', 0) <= row.get(f'{k}_ma5', 0)
        )

        # ── 피봇/피보나치/ATR (Ver 27.18 추가)
        _pivot = calc_pivot_levels(temp_df)
        _fib   = calc_fibonacci_levels(temp_df)
        _atr_t = calc_atr_targets(row, close_p)

        # 손절/목표가 (ATR 기반)
        atr_target1 = _atr_t.get('target_1', round(close_p * 1.08, 2))
        atr_target2 = _atr_t.get('target_2', round(close_p * 1.15, 2))
        atr_stop    = _atr_t.get('stop_atr',  round(close_p * 0.95, 2))
        atr_val     = _atr_t.get('atr_val', 0)
        rr_ratio    = _atr_t.get('risk_reward', 1.3)

        # 태그
        tags = [style_label, '🇺🇸미국주식']
        if signals.get('watermelon_signal'): tags.append('🍉수박신호')
        if signals.get('dolbanzi'):          tags.append('💍돌반지')
        if signals.get('Real_Viper_Hook'):   tags.append('🐍독사훅')
        if signals.get('explosion_ready'):   tags.append('💎폭발직전')
        if signals.get('force_pullback'):    tags.append('🧲세력눌림')
        if signals.get('bb30_shift_gc'):     tags.append('🎯BB30시프트')

        # 피보나치 신호
        _fib382 = float(_fib.get('fib_382', 0))
        _fib618 = float(_fib.get('fib_618', 0))
        _tol    = 0.02
        if _fib382 > 0 and abs(close_p - _fib382) / _fib382 <= _tol:
            tags.append('🔢Fib38.2%지지')
            signals['fib_support_382'] = True
        if _fib618 > 0 and abs(close_p - _fib618) / _fib618 <= _tol:
            tags.append('🔢Fib61.8%강지지')
            signals['fib_support_618'] = True

        s_score = max(0, result['score'] - storm_count * 10)

        print(f"  🇺🇸 {ticker} 포착! 점수:{result['score']} | {result['combination']}")

        return [{
            # 기본
            '날짜':    curr_idx.strftime('%Y-%m-%d'),
            '종목명':  ticker,
            'code':    ticker,
            '시장':    '🇺🇸US',
            # 패턴
            'N등급':   f"{result['type']}{result['grade']}",
            'N조합':   result['combination'],
            'N점수':   result['score'],
            'N구분':   ' '.join(new_tags),
            '👑등급':  result['grade'],
            '📜서사히스토리': f"🇺🇸{ticker} | {result['combination']}",
            '확신점수': 0,
            # 타점 (ATR 기반)
            '🎯목표타점': round(atr_target1, 2),
            '🎯목표2차':  round(atr_target2, 2),
            '🚨손절가':   round(atr_stop, 2),
            'RR비율':     round(rr_ratio, 1),
            # 피봇
            'PP':  _pivot.get('PP', 0),
            'R1':  _pivot.get('R1', 0),
            'R2':  _pivot.get('R2', 0),
            'S1':  _pivot.get('S1', 0),
            'S2':  _pivot.get('S2', 0),
            # 피보나치
            'Fib382': _fib.get('fib_382', 0),
            'Fib618': _fib.get('fib_618', 0),
            # ATR
            'ATR값': round(atr_val, 2),
            # 기상
            '기상':    '☀️' * (2 - storm_count) + '🌪️' * storm_count,
            '안전점수': int(s_score),
            'RSI':     int(row.get('RSI', 0)),
            '점수':    int(s_score),
            '에너지':  '🔋' if row.get('MACD_Hist', 0) > 0 else '🪫',
            '현재가':  round(close_p, 2),
            '구분':    ' '.join(tags),
            '재무':    'N/A',
            '수급':    '🇺🇸거래량기반',
            '이격':    int(row.get('Disparity', 0)),
            'BB40':    f"{row.get('BB40_Width', 0):.1f}",
            'MA수렴':  f"{row.get('MA_Convergence', 0):.1f}",
            '매집':    'N/A',
            'OBV기울기': int(row.get('OBV_Slope', 0)),
            # 뉴스 (나중에 채워짐)
            'news_sentiment': '',
        }]

    except Exception as e:
        import traceback
        print(f"  ⚠️ {ticker}: {e}")
        return []


# ================================================================
# 🚀 스캔 실행
# ================================================================

def run_us_scanner(weather_data, mode: str = 'nasdaq100+sp500') -> list:
    """미국주식 전체 스캔"""
    tickers = load_us_tickers(mode=mode)
    if not tickers:
        print("❌ 티커 로드 실패")
        return []

    print(f"\n🇺🇸 미국주식 스캔 시작: {len(tickers)}개")
    all_hits = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(analyze_final_us, ticker, weather_data): ticker
            for ticker in tickers
        }
        done = 0
        for future in as_completed(future_map, timeout=SCAN_TIMEOUT):
            ticker = future_map[future]
            done  += 1
            try:
                hits = future.result(timeout=30)
                if hits:
                    all_hits.extend(hits)
            except Exception as e:
                pass
            if done % 50 == 0:
                print(f"  진행: {done}/{len(tickers)} | 포착: {len(all_hits)}개")

    all_hits.sort(key=lambda x: x['N점수'], reverse=True)
    print(f"✅ 스캔 완료: {len(all_hits)}개 포착")
    return all_hits


# ================================================================
# 📱 텔레그램 포맷
# ================================================================

def format_us_entry(item: dict) -> str:
    def sf(x, d=0.0):
        try: return float(x)
        except: return d

    pp   = sf(item.get('PP', 0))
    r1   = sf(item.get('R1', 0))
    s1   = sf(item.get('S1', 0))
    f382 = sf(item.get('Fib382', 0))
    f618 = sf(item.get('Fib618', 0))
    atr  = sf(item.get('ATR값', 0))
    tgt1 = sf(item.get('🎯목표타점', 0))
    tgt2 = sf(item.get('🎯목표2차', 0))
    stp  = sf(item.get('🚨손절가', 0))
    rr   = sf(item.get('RR비율', 1.3))

    line = (
        f"────────────────────────\n"
        f"⭐ {item.get('N등급','?')}  [{item['종목명']}]  ${sf(item['현재가']):.2f}\n"
        f"🎯 {item.get('N조합','')}\n"
        f"🏷️ {item.get('N구분','')}\n"
        f"💰 이격:{item.get('이격',0)} | RSI:{item.get('RSI',0)} | {item.get('에너지','')}\n"
        f"📊 MA수렴:{item.get('MA수렴',0)} | BB40:{item.get('BB40',0)} | OBV:{item.get('OBV기울기',0)}\n"
    )
    if tgt1 > 0:
        line += f"📌 목표1:${tgt1:.2f} → 목표2:${tgt2:.2f} | 손절:${stp:.2f} (RR {rr:.1f})\n"
    if pp > 0:
        line += f"📐 PP:${pp:.2f} | R1:${r1:.2f} | S1:${s1:.2f}\n"
    if f382 > 0:
        line += f"🔢 Fib38.2%:${f382:.2f} | Fib61.8%:${f618:.2f}\n"
    if atr > 0:
        line += f"📏 ATR:${atr:.2f}\n"

    news = item.get('news_sentiment', '').strip()
    if news:
        line += f"📰 {news[:60]}\n"

    ai_tip = item.get('ai_tip', '').strip()
    if ai_tip:
        line += f"💡 {ai_tip}\n"

    return line


# ================================================================
# 🚀 메인
# ================================================================

if __name__ == '__main__':
    print("🇺🇸 미국주식 수박지표 스캐너 v2.0 가동...")
    print(f"  API: GPT={'✅' if OPENAI_API_KEY else '❌'} "
          f"Claude={'✅' if ANTHROPIC_API_KEY else '❌'} "
          f"Gemini={'✅' if GEMINI_API_KEY else '❌'} "
          f"Groq={'✅' if GROQ_API_KEY else '❌'}")

    # 기상 데이터
    weather_data = prepare_historical_weather()

    # 스캔 실행
    # mode 옵션: 'nasdaq100' / 'sp500' / 'nasdaq100+sp500' / 'mega'
    us_hits = run_us_scanner(weather_data, mode='nasdaq100+sp500')

    if not us_hits:
        msg = f"🇺🇸 [{TODAY_STR}] 미국주식 수박신호 해당 없음"
        send_telegram_photo(msg, [])
        print("✅ 종료")
        import os; os._exit(0)

    # AI 분석 대상 (상위 15개)
    ai_candidates = pd.DataFrame(us_hits[:15])

    # 뉴스 감성 (종목당 5초 타임아웃)
    print("📰 뉴스 조회 중...")
    import signal as _sig

    for idx, row_n in ai_candidates.iterrows():
        try:
            def _th(s, f): raise TimeoutError()
            _sig.signal(_sig.SIGALRM, _th)
            _sig.alarm(5)
            news = _fetch_stock_news(str(row_n.get('code', '')), str(row_n.get('종목명', '')))
            _sig.alarm(0)
        except Exception:
            news = ''
            try: _sig.alarm(0)
            except: pass
        ai_candidates.at[idx, 'news_sentiment'] = news

    # AI 코멘트 (get_ai_summary_batch)
    print("🧠 AI 코멘트 생성 중...")
    try:
        ai_result_text = get_ai_summary_batch(ai_candidates, issues=None)

        # 종목별 파싱
        ai_map = {}
        current_key, current_lines = None, []
        for line in ai_result_text.splitlines():
            if line.startswith('[') and '(' in line and line.endswith(']'):
                if current_key and current_lines:
                    ai_map[current_key] = '\n'.join(current_lines).strip()
                current_key   = line[1:-1]
                current_lines = []
            elif current_key:
                current_lines.append(line)
        if current_key and current_lines:
            ai_map[current_key] = '\n'.join(current_lines).strip()

        for idx, item in ai_candidates.iterrows():
            key = f"{item['종목명']}({item['code']})"
            ai_candidates.at[idx, 'ai_tip'] = ai_map.get(key, '')

    except Exception as e:
        print(f"⚠️ AI 코멘트 실패: {e}")
        ai_candidates['ai_tip'] = ''

    # 텔레그램 전송 (종목 카드)
    print("📨 텔레그램 전송 중...")
    MAX_CHAR    = 3800
    top_n       = min(10, len(us_hits))
    current_msg = f"🇺🇸 [미국주식 수박신호 TOP {top_n}] {TODAY_STR}\n\n"

    for _, item in ai_candidates.head(top_n).iterrows():
        entry = format_us_entry(item.to_dict())
        if len(current_msg) + len(entry) > MAX_CHAR:
            send_telegram_photo(current_msg, [])
            current_msg = '🇺🇸 [미국주식 - 이어서]\n\n' + entry
        else:
            current_msg += entry

    send_telegram_photo(current_msg, [])

    # AI 토너먼트 (4종, 분할 전송)
    print("🏆 AI 토너먼트 실행 중...")
    print(f"  GPT={bool(OPENAI_API_KEY)} Claude={bool(ANTHROPIC_API_KEY)} "
          f"Gemini={bool(GEMINI_API_KEY)} Groq={bool(GROQ_API_KEY)}")
    try:
        tournament = run_ai_tournament(ai_candidates, issues=None)
        if tournament:
            send_tournament_results(tournament)   # AI별 분리 + 길면 자동 분할
            print("✅ 토너먼트 전송 완료")
        else:
            print("⚠️ 토너먼트 결과 없음")
    except Exception as e:
        print(f"⚠️ 토너먼트 실패: {e}")

    # 구글시트 저장
    try:
        update_google_sheet(us_hits, TODAY_STR + '_US', '')
        print(f"💾 구글시트 {len(us_hits)}개 저장 완료")
    except Exception as e:
        print(f"⚠️ 시트 저장 실패: {e}")

    print("✅ 미국주식 스캐너 종료")
    import os; os._exit(0)
