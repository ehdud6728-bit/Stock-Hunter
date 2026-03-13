# ================================================================
# us_scanner.py
# main7.py의 기존 함수들을 import해서 미국주식 스캔
# 실행: python us_scanner.py
# ================================================================

import os
import sys
import json
import pandas as pd
import numpy as np
import requests
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from Ustickerloader import load_us_tickers

# ────────────────────────────────────────────────────────────────
# ✅ main7.py에서 필요한 함수들 import
# ────────────────────────────────────────────────────────────────
from main7 import (
    # 지표 계산
    get_indicators,
    classify_style,
    calculate_combination_score,
    judge_trade_with_sequence,

    # signals 빌더
    build_default_signals,
    inject_tri_result,

    # 텔레그램
    send_telegram_photo,

    # AI 브리핑
    get_ai_summary_batch,
    run_ai_tournament,

    # 구글시트
    update_google_sheet,

    # 환경변수 / 설정값
    TELEGRAM_TOKEN,
    CHAT_ID_LIST,
    OPENAI_API_KEY,
    GROQ_API_KEY,
    TODAY_STR,
    KST,

    # 기상 데이터
    prepare_historical_weather,
)

# 삼각수렴 (별도 모듈)
from triangle_combo_analyzer import jongbe_triangle_combo_v3

# ────────────────────────────────────────────────────────────────
# ✅ 방법 2: Wikipedia 크롤링 (가장 정확한 나스닥100/S&P500)
# ────────────────────────────────────────────────────────────────
 
def get_nasdaq100_wikipedia():
    """Wikipedia에서 나스닥100 정확한 구성 종목 가져오기"""
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        tables = pd.read_html(res.text)
 
        # 티커 컬럼이 있는 테이블 찾기
        for table in tables:
            cols = [str(c).lower() for c in table.columns]
            if any('ticker' in c or 'symbol' in c for c in cols):
                col = next(c for c in table.columns if 'ticker' in str(c).lower() or 'symbol' in str(c).lower())
                tickers = table[col].dropna().tolist()
                tickers = [t.strip() for t in tickers if isinstance(t, str) and t.strip()]
                print(f"✅ Wikipedia 나스닥100: {len(tickers)}개")
                return tickers
 
        print("⚠️ Wikipedia 테이블 파싱 실패")
        return []
    except Exception as e:
        print(f"⚠️ Wikipedia 나스닥100 실패: {e}")
        return []
 
 
def get_sp500_wikipedia():
    """Wikipedia에서 S&P500 구성 종목 가져오기"""
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        df = pd.read_html(res.text)[0]
 
        sym_col = next(c for c in df.columns if 'symbol' in str(c).lower() or 'ticker' in str(c).lower())
        tickers = df[sym_col].dropna().tolist()
        tickers = [t.strip().replace('.', '-') for t in tickers]  # BRK.B → BRK-B
        print(f"✅ Wikipedia S&P500: {len(tickers)}개")
        return tickers
    except Exception as e:
        print(f"⚠️ Wikipedia S&P500 실패: {e}")
        return []
        
# ────────────────────────────────────────────────────────────────
# ✅ 미국주식 전용 설정
# ────────────────────────────────────────────────────────────────

# 나스닥100 주요 종목
NASDAQ_100 = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'TSLA',
    'AVGO', 'COST', 'NFLX', 'AMD', 'PEP', 'CSCO', 'ADBE',
    'QCOM', 'TXN', 'INTU', 'AMGN', 'CMCSA', 'AMAT', 'BKNG',
    'ISRG', 'VRTX', 'ADP', 'REGN', 'PANW', 'SBUX', 'LRCX',
    'MU', 'KLAC', 'SNPS', 'CDNS', 'MELI', 'FTNT', 'ABNB',
    'ORLY', 'CTAS', 'MNST', 'MRVL', 'PYPL', 'CRWD', 'DXCM',
    'PCAR', 'WDAY', 'ROST', 'ODFL', 'IDXX', 'FAST', 'MRNA',
    'ON', 'DDOG', 'ZS', 'TEAM', 'TTD', 'PLTR', 'ARM', 'SMCI',
    'ORCL', 'CRM', 'NOW', 'SNOW', 'UBER', 'NET', 'COIN',
]

# 달러 기준 라운드넘버
RN_LIST_US = [
    5, 10, 15, 20, 25, 30, 40, 50, 75, 100,
    125, 150, 200, 250, 300, 400, 500, 750, 1000, 1500, 2000
]

US_MIN_AMOUNT_USD = 100_000_000  # 1억달러 (거래대금 필터)


def get_target_levels_us(current_price):
    upper_rns = [rn for rn in RN_LIST_US if rn > current_price]
    lower_rns = [rn for rn in RN_LIST_US if rn <= current_price]
    return (lower_rns[-1] if lower_rns else None,
            upper_rns[0]  if upper_rns else None)


# ────────────────────────────────────────────────────────────────
# ✅ 미국주식 분석 함수
# get_indicators / build_default_signals 등 main7 함수 그대로 재사용
# ────────────────────────────────────────────────────────────────

def analyze_final_us(ticker, historical_indices):
    try:
        df = fdr.DataReader(
            ticker,
            start=(datetime.now() - timedelta(days=250)).strftime('%Y-%m-%d')
        )
        if df is None or len(df) < 100:
            return []

        # 거래대금 필터 (달러 기준)
        avg_amount_usd = (df['Close'] * df['Volume']).tail(5).mean()
        if avg_amount_usd < US_MIN_AMOUNT_USD:
            return []

        # ── 지표 계산 (main7.get_indicators 그대로)
        df = get_indicators(df)
        if df is None or df.empty:
            return []

        df = df.join(historical_indices, how='left').ffill()

        row     = df.iloc[-1]
        prev    = df.iloc[-2]
        curr_idx = df.index[-1]
        close_p  = row['Close']
        raw_idx  = len(df) - 1
        temp_df  = df.iloc[:raw_idx + 1]

        # ── signals 구성 (main7.build_default_signals 그대로)
        signals  = build_default_signals(row, close_p, prev)
        new_tags = []

        # ── 삼각수렴 주입 (main7.inject_tri_result 그대로)
        try:
            tri_result = jongbe_triangle_combo_v3(temp_df) or {}
        except:
            tri_result = {}
        signals, new_tags = inject_tri_result(signals, tri_result, new_tags)

        # ── 수박/핵심 신호 없으면 스킵
        has_signal = (
            signals['watermelon_signal'] or
            signals['watermelon_red'] or
            signals.get('dolbanzi') or
            signals.get('Real_Viper_Hook') or
            signals.get('force_pullback') or
            signals.get('bb40_reclaim_rsi_div')
        )
        if not has_signal:
            return []

        # ── 조합 점수 계산 (main7.judge_trade_with_sequence 그대로)
        result = judge_trade_with_sequence(temp_df, signals)
        new_tags.extend(result['tags'])

        # ── 스타일 / 점수
        style = classify_style(row)
        style_label = {
            "SWING": "📈스윙(5~15일)",
            "SCALP": "⚡단타(1~3일)",
            "NONE":  "➖미분류",
        }[style]

        storm_count = sum(
            1 for m_key in ['ixic', 'sp500']
            if row.get(f'{m_key}_close', 0) <= row.get(f'{m_key}_ma5', 0)
        )
        s_score = max(0, result['score'] - storm_count * 10)

        # ── 라운드넘버 (달러 기준)
        lower_rn, upper_rn = get_target_levels_us(close_p)

        # ── 태그
        tags = [style_label, '🇺🇸미국주식']
        if signals['watermelon_signal']:  tags.append('🍉수박신호')
        if signals.get('dolbanzi'):       tags.append('💍돌반지')
        if signals.get('Real_Viper_Hook'): tags.append('🐍독사훅')
        if signals.get('explosion_ready'): tags.append('💎폭발직전')
        if signals.get('force_pullback'): tags.append('🧲세력눌림')

        print(f"🇺🇸 {ticker} 포착! N점수: {result['score']}")

        return [{
            '날짜':    curr_idx.strftime('%Y-%m-%d'),
            '종목명':  ticker,
            'code':    ticker,
            '시장':    '🇺🇸US',
            'N등급':   f"{result['type']}{result['grade']}",
            'N조합':   result['combination'],
            'N점수':   result['score'],
            'N구분':   " ".join(new_tags),
            '👑등급':  result['grade'],
            '📜서사히스토리': f"나스닥 {ticker} | {result['combination']}",
            '확신점수': 0,
            '🎯목표타점': round(close_p * 1.08, 2),
            '🚨손절가':  round(close_p * 0.95, 2),
            '기상':    "☀️" * (2 - storm_count) + "🌪️" * storm_count,
            '안전점수': int(s_score),
            'RSI':     int(max(0, row['RSI'])) if not pd.isna(row['RSI']) else 0,
            '점수':    int(s_score),
            '에너지':  "🔋" if row['MACD_Hist'] > 0 else "🪫",
            '현재가':  round(close_p, 2),
            '구분':    " ".join(tags),
            '재무':    'N/A',
            '수급':    '🇺🇸거래량기반',
            '이격':    int(row['Disparity']),
            'BB40':    f"{row['BB40_Width']:.1f}",
            'MA수렴':  f"{row['MA_Convergence']:.1f}",
            '매집':    'N/A',
            'OBV기울기': int(row['OBV_Slope']),
        }]

    except Exception as e:
        import traceback
        print(f"🚨 {ticker} 오류: {traceback.format_exc()}")
        return []

# ────────────────────────────────────────────────────────────────
# ✅ 통합 로더 (우선순위 자동 fallback)
# ────────────────────────────────────────────────────────────────
 
def load_us_tickers(
    mode='nasdaq100',       # 'nasdaq100' / 'sp500' / 'all'
    min_marcap_b=10,        # 최소 시가총액 (단위: 십억달러), all 모드에서만 사용
    max_count=None,         # 최대 종목 수 제한 (None = 전체)
):
    """
    미국주식 종목 리스트 동적 로딩
    
    mode:
      'nasdaq100' → 나스닥100 구성 종목
      'sp500'     → S&P500 구성 종목
      'all'       → 나스닥+NYSE 전체 (시가총액 필터 적용)
    
    반환: 티커 리스트
    """
    tickers = []
 
    if mode == 'nasdaq100':
        # 1순위: Wikipedia
        tickers = get_nasdaq100_wikipedia()
        # 2순위: FDR
        if not tickers:
            tickers = get_nasdaq100_fdr()
        # 3순위: 하드코딩
        if not tickers:
            tickers = NASDAQ_100_FALLBACK
            print("⚠️ fallback 사용: 하드코딩 나스닥100")
 
    elif mode == 'sp500':
        # 1순위: Wikipedia
        tickers = get_sp500_wikipedia()
        # 2순위: FDR
        if not tickers:
            tickers = get_nasdaq100_fdr()
        if not tickers:
            tickers = NASDAQ_100_FALLBACK
 
    elif mode == 'all':
        min_marcap = min_marcap_b * 1_000_000_000
        tickers = get_nyse_nasdaq_all_fdr(min_marcap=min_marcap)
        if not tickers:
            tickers = NASDAQ_100_FALLBACK
 
    # ETF / 특수문자 제거
    tickers = [
        t for t in tickers
        if isinstance(t, str) and t.strip()
        and not any(c in t for c in ['^', '.', '/', ' '])
    ]
 
    # 최대 수 제한
    if max_count:
        tickers = tickers[:max_count]
 
    print(f"✅ 최종 스캔 대상: {len(tickers)}개 ({mode})")
    return tickers
    
# ────────────────────────────────────────────────────────────────
# ✅ 스캔 실행
# ────────────────────────────────────────────────────────────────

def run_us_scanner(weather_data, mode='nasdaq100', min_marcap_b=10):
    # ✅ 변경 1: 하드코딩 대신 동적 로딩
    tickers = load_us_tickers(mode=mode, min_marcap_b=min_marcap_b)
    
    print(f"🇺🇸 미국주식 스캔 시작: {len(tickers)}개 종목")
    all_hits = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_map = {
            executor.submit(analyze_final_us, ticker, weather_data): ticker
            for ticker in tickers  # ✅ 변경 2: NASDAQ_100 → tickers
        }
        done = 0
        for future in as_completed(future_map, timeout=600):
            ticker = future_map[future]
            done += 1
            try:
                result = future.result(timeout=30)
                if result:
                    all_hits.extend(result)
            except Exception as e:
                print(f"⏰ {ticker} 타임아웃: {e}")
            if done % 20 == 0:
                print(f"🇺🇸 진행: {done}/{len(tickers)}")  # ✅ 변경 3: NASDAQ_100 → tickers
    print(f"✅ 미국주식 스캔 완료: {len(all_hits)}개 포착")
    return sorted(all_hits, key=lambda x: x['N점수'], reverse=True)


# ────────────────────────────────────────────────────────────────
# ✅ 텔레그램 포맷 (달러 표시)
# ────────────────────────────────────────────────────────────────

def format_us_entry(item):
    def sf(x, d=0.0):
        try: return float(x)
        except: return d

    return (
        f"🇺🇸 {item['N등급']} | [{item['종목명']}]\n"
        f"- {item['N조합']} | {item['N구분']}\n"
        f"- {item['기상']} | {item['구분']}\n"
        f"- 현재가: ${sf(item['현재가']):.2f} | 이격: {item['이격']}\n"
        f"- BB40: {sf(item['BB40']):.1f} | MA수렴: {sf(item['MA수렴']):.1f}\n"
        f"- OBV기울기: {item['OBV기울기']} | RSI: {item['RSI']}\n"
        f"- 🎯목표: ${sf(item['🎯목표타점']):.2f} | 🚨손절: ${sf(item['🚨손절가']):.2f}\n"
        f"----------------------------\n"
    )


# ────────────────────────────────────────────────────────────────
# ✅ 메인 실행
# ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("🇺🇸 미국주식 수박지표 스캐너 가동...")

    # 기상 데이터 (main7 함수 재사용)
    weather_data = prepare_historical_weather()

    # 나스닥100만
    #us_hits = run_us_scanner(weather_data, mode='nasdaq100')
    
    # S&P500 전체
    #us_hits = run_us_scanner(weather_data, mode='sp500')
    
    # 시총 500억달러 이상 전체
    us_hits = load_us_tickers(weather_data, mode='all', min_marcap_b=50)
    
    if not us_hits:
        print("❌ 포착된 미국주식 없음")
        msg = f"🇺🇸 [{TODAY_STR}] 미국주식 수박신호 해당 없음"
        send_telegram_photo(msg, [])

    else:
        ai_candidates = pd.DataFrame(us_hits[:20])

        # AI 코멘트 (main7 함수 재사용)
        print("🧠 AI 코멘트 생성 중...")
        ai_result_text = get_ai_summary_batch(ai_candidates, issues=None)
        # ✅ 파싱 복구
        ai_map = {}
        current_key = None
        current_lines = []
        
        for line in ai_result_text.splitlines():
            # [종목명(코드)] 형식 감지
            if line.startswith("[") and "(" in line and line.endswith("]"):
                # 이전 종목 저장
                if current_key and current_lines:
                    ai_map[current_key] = "\n".join(current_lines).strip()
                current_key = line[1:-1]  # [ ] 제거
                current_lines = []
            elif current_key:
                current_lines.append(line)
        
        # 마지막 종목 저장
        if current_key and current_lines:
            ai_map[current_key] = "\n".join(current_lines).strip()
        
        # ✅ ai_tip 주입
        for idx, item in ai_candidates.iterrows():
            key = f"{item['종목명']}({item['code']})"
            ai_candidates.loc[idx, "ai_tip"] = ai_map.get(key, "브리핑 생성 실패")
    
            # 텔레그램 발송
            MAX_CHAR = 3800
            current_msg = f"🇺🇸 [미국주식 수박신호 TOP {min(10, len(us_hits))}]\n\n"
    
            for _, item in ai_candidates.head(10).iterrows():
                entry = format_us_entry(item)
                entry += f"💡 {item.get('ai_tip', '분석전')}\n----------------------------\n"
    
                if len(current_msg) + len(entry) > MAX_CHAR:
                    send_telegram_photo(current_msg, [])
                    current_msg = "🇺🇸 [미국주식 - 이어서]\n\n" + entry
                else:
                    current_msg += entry

        send_telegram_photo(current_msg, [])

        # AI 토너먼트 (main7 함수 재사용)
        print("🏆 AI 토너먼트 실행 중...")
        tournament = run_ai_tournament(ai_candidates, issues=None)
        send_telegram_photo(f"🏆 [미국주식 AI 토너먼트]\n{tournament}", [])

        # 구글시트 저장 (main7 함수 재사용)
        try:
            update_google_sheet(us_hits, TODAY_STR + "_US", tournament)
            print(f"💾 미국주식 {len(us_hits)}개 시트 저장 완료")
        except Exception as e:
            print(f"🚨 시트 저장 실패: {e}")

    print("✅ 미국주식 스캐너 종료")
    import os
    os._exit(0)
