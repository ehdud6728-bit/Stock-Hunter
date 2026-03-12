# ================================================================
# ✅ 미국주식 수박지표 스캐너 (나스닥100 기반)
# 기존 한국 스캐너와 동일한 로직, 미국주식에 맞게 조정
# 변경 포인트:
#   1. 종목 리스트: 나스닥100 / S&P500 / 임의 티커
#   2. 데이터: fdr.DataReader (미국 티커 그대로 사용)
#   3. 수급: 네이버 대신 Yahoo Finance 기반
#   4. 거래대금: 원화 → 달러 기준 (1억달러 이상)
#   5. 라운드넘버: 한국 RN_LIST → 미국용 RN_LIST_US
#   6. 결과 통합: 한국 결과와 합쳐서 텔레그램 발송
# ================================================================

import FinanceDataReader as fdr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ────────────────────────────────────────────────────────────────
# [1] 미국주식 종목 리스트
# ────────────────────────────────────────────────────────────────

# 나스닥100 티커 리스트 (2026 기준 주요 종목)
NASDAQ_100 = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'GOOG', 'TSLA',
    'AVGO', 'COST', 'NFLX', 'ASML', 'AMD', 'PEP', 'LIN', 'CSCO',
    'ADBE', 'QCOM', 'TXN', 'INTU', 'AMGN', 'CMCSA', 'HON', 'INTC',
    'AMAT', 'BKNG', 'ISRG', 'VRTX', 'ADP', 'REGN', 'PANW', 'SBUX',
    'LRCX', 'MU', 'KLAC', 'SNPS', 'CDNS', 'MELI', 'FTNT', 'ABNB',
    'ORLY', 'CTAS', 'MNST', 'MRVL', 'KDP', 'PYPL', 'CRWD', 'DXCM',
    'PCAR', 'WDAY', 'ROST', 'ODFL', 'IDXX', 'FAST', 'MRNA', 'CEG',
    'CTSH', 'GEHC', 'TTD', 'ON', 'DDOG', 'ZS', 'TEAM', 'SGEN',
    'VRSK', 'FANG', 'ANSS', 'DLTR', 'WBD', 'XEL', 'BIIB', 'ILMN',
    'GFS', 'ALGN', 'ENPH', 'LCID', 'RIVN', 'ARM', 'SMCI', 'PLTR',
]

# S&P500 추가 주요 종목 (선택적)
SP500_EXTRA = [
    'ORCL', 'CRM', 'NOW', 'SNOW', 'UBER', 'LYFT', 'RBLX', 'U',
    'NET', 'COIN', 'HOOD', 'SOFI', 'IONQ', 'QBTS', 'RGTI',
]

def get_us_ticker_list(include_sp500_extra=False):
    """스캔할 미국주식 티커 리스트 반환"""
    tickers = NASDAQ_100.copy()
    if include_sp500_extra:
        tickers += SP500_EXTRA
    return tickers


# ────────────────────────────────────────────────────────────────
# [2] 미국주식용 라운드넘버 (달러 기준)
# ────────────────────────────────────────────────────────────────

RN_LIST_US = [
    5, 10, 15, 20, 25, 30, 40, 50, 75, 100,
    125, 150, 200, 250, 300, 400, 500, 750,
    1000, 1500, 2000, 3000, 5000
]

def get_target_levels_us(current_price):
    upper_rns = [rn for rn in RN_LIST_US if rn > current_price]
    lower_rns = [rn for rn in RN_LIST_US if rn <= current_price]
    upper = upper_rns[0] if upper_rns else None
    lower = lower_rns[-1] if lower_rns else None
    return lower, upper


# ────────────────────────────────────────────────────────────────
# [3] 미국주식 거래대금 필터 (달러 기준)
# 한국: 150억원 이상 → 미국: 1억달러 이상 (약 1300억원)
# ────────────────────────────────────────────────────────────────

US_MIN_AMOUNT_USD = 100_000_000   # 1억달러


# ────────────────────────────────────────────────────────────────
# [4] 미국주식 분석 함수 (analyze_final_us)
# 기존 analyze_final과 동일 로직, 미국주식 특성에 맞게 조정
# ────────────────────────────────────────────────────────────────

def analyze_final_us(ticker, historical_indices):
    """
    미국주식 수박지표 분석
    - get_indicators()는 기존 함수 그대로 재사용 (지표 계산 동일)
    - 거래대금 필터만 달러 기준으로 변경
    - 수급(기관/외인) 없음 → 대신 거래량 분석으로 대체
    """
    try:
        df = fdr.DataReader(ticker,
            start=(datetime.now() - timedelta(days=250)).strftime('%Y-%m-%d'))

        if df is None or len(df) < 100:
            return []

        # ── 거래대금 필터 (달러 기준)
        recent_avg_amount_usd = (df['Close'] * df['Volume']).tail(5).mean()
        if recent_avg_amount_usd < US_MIN_AMOUNT_USD:
            return []

        # ── 기존 지표 계산 함수 그대로 재사용
        df = get_indicators(df)
        if df is None or df.empty:
            return []

        df = df.join(historical_indices, how='left').ffill()

        row  = df.iloc[-1]
        prev = df.iloc[-2]
        curr_idx = df.index[-1]
        close_p  = row['Close']

        # ── signals 구성 (기존 함수 재사용)
        signals = build_default_signals(row, close_p, prev)
        new_tags = []

        # ── tri_result (삼각수렴)
        try:
            raw_idx  = len(df) - 1
            temp_df  = df.iloc[:raw_idx + 1]
            tri_result = jongbe_triangle_combo_v3(temp_df) or {}
        except:
            tri_result = {}

        signals, new_tags = inject_tri_result(signals, tri_result, new_tags)

        # ── 조합 점수 계산
        result = judge_trade_with_sequence(df, signals)
        new_tags.extend(result['tags'])

        # ── 수박 신호 없으면 스킵 (미국주식은 수박 신호 기준으로만)
        if not (signals['watermelon_signal'] or
                signals['watermelon_red'] or
                signals.get('dolbanzi') or
                signals.get('Real_Viper_Hook')):
            return []

        # ── 스타일 분류
        style = classify_style(row)
        style_label = {
            "SWING": "📈스윙(5~15일)",
            "SCALP": "⚡단타(1~3일)",
            "NONE":  "➖미분류",
        }[style]

        # ── 기상 체크
        storm_count = 0
        for m_key in ['ixic', 'sp500']:
            if row.get(f'{m_key}_close', 0) <= row.get(f'{m_key}_ma5', 0):
                storm_count += 1

        s_score = int(90 + result['score'] * 0.1)
        s_score -= storm_count * 10

        # ── 라운드넘버 (달러 기준)
        lower_rn, upper_rn = get_target_levels_us(close_p)

        # ── RSI
        rsi_val = row['RSI'] if not pd.isna(row['RSI']) else 50

        tags = [style_label]
        if signals['watermelon_signal']:
            tags.append('🍉수박신호')
        if signals.get('dolbanzi'):
            tags.append('💍돌반지')
        if signals.get('Real_Viper_Hook'):
            tags.append('🐍독사훅')
        if signals.get('explosion_ready'):
            tags.append('💎폭발직전')

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
            '안전점수': int(max(0, s_score)),
            'RSI':     int(max(0, rsi_val)),
            '현재가':  round(close_p, 2),
            '구분':    " ".join(tags),
            '재무':    'N/A',
            '수급':    '🇺🇸거래량기반',
            '이격':    int(row['Disparity']),
            'BB40':    f"{row['BB40_Width']:.1f}",
            'MA수렴':  f"{row['MA_Convergence']:.1f}",
            'OBV기울기': int(row['OBV_Slope']),
            '기상':    "☀️" * (2 - storm_count) + "🌪️" * storm_count,
            '에너지':  "🔋" if row['MACD_Hist'] > 0 else "🪫",
            '매집':    'N/A',
            '📜서사히스토리': f"나스닥 {ticker} | {result['combination']}",
            '👑등급':  result['grade'],
            '확신점수': 0,
            '🎯목표타점': int(close_p * 1.08),
            '🚨손절가':  int(close_p * 0.95),
        }]

    except Exception as e:
        print(f"🚨 {ticker} 분석 오류: {e}")
        return []


# ────────────────────────────────────────────────────────────────
# [5] 미국주식 스캔 실행 함수
# ────────────────────────────────────────────────────────────────

def run_us_scanner(weather_data, include_sp500_extra=False):
    """
    미국주식 수박지표 스캔 실행
    반환: all_hits 리스트 (한국 스캔 결과와 동일 포맷)
    """
    tickers = get_us_ticker_list(include_sp500_extra)
    print(f"🇺🇸 미국주식 스캔 시작: {len(tickers)}개 종목")

    all_hits = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_map = {
            executor.submit(analyze_final_us, ticker, weather_data): ticker
            for ticker in tickers
        }

        done = 0
        for future in as_completed(future_map, timeout=300):
            ticker = future_map[future]
            done += 1
            try:
                result = future.result(timeout=20)
                if result:
                    all_hits.extend(result)
            except Exception as e:
                print(f"⏰ {ticker} 타임아웃/오류: {e}")

            if done % 20 == 0:
                print(f"🇺🇸 진행: {done}/{len(tickers)}")

    print(f"✅ 미국주식 스캔 완료: {len(all_hits)}개 포착")
    return all_hits


# ────────────────────────────────────────────────────────────────
# [6] 한국 + 미국 결과 통합
# ────────────────────────────────────────────────────────────────

def merge_kr_us_hits(kr_hits, us_hits):
    """
    한국 + 미국 결과를 N점수 기준으로 통합 정렬
    시장 구분 태그로 분리 표시 가능
    """
    all_hits = kr_hits + us_hits
    all_hits_sorted = sorted(all_hits, key=lambda x: x['N점수'], reverse=True)
    return all_hits_sorted


def format_us_telegram_entry(item):
    """미국주식 텔레그램 메시지 포맷 (달러 표시)"""
    def sf(x, d=0.0):
        try: return float(x)
        except: return d

    return (
        f"🇺🇸 {item['N등급']} | [{item['종목명']}]\n"
        f"- {item['N조합']} | {item['N구분']}\n"
        f"- {item['기상']} | {item['구분']}\n"
        f"- 현재가: ${item['현재가']:.2f} | 이격: {item['이격']}\n"
        f"- BB40: {sf(item['BB40']):.1f} | MA수렴: {sf(item['MA수렴']):.1f}\n"
        f"- OBV기울기: {item['OBV기울기']} | RSI: {item['RSI']}\n"
        f"- 목표가: ${item['🎯목표타점']:.0f} | 손절: ${item['🚨손절가']:.0f}\n"
        f"----------------------------\n"
    )


# ────────────────────────────────────────────────────────────────
# ✅ main 블록 적용 가이드
# ────────────────────────────────────────────────────────────────
"""
[1] 기존 한국 스캔 끝난 후 미국 스캔 추가:

    # 한국 스캔 (기존)
    kr_hits = run_scan_with_timeout(target_dict, weather_data, ...)

    # 미국 스캔 추가
    print("🇺🇸 미국주식 수박지표 스캔 시작...")
    us_hits = run_us_scanner(weather_data, include_sp500_extra=False)

    # 통합
    all_hits = merge_kr_us_hits(kr_hits, us_hits)

[2] 텔레그램 발송 시 시장 구분:

    for _, item in telegram_targets.iterrows():
        if item.get('시장') == '🇺🇸US':
            entry = format_us_telegram_entry(item)
        else:
            entry = format_kr_telegram_entry(item)  # 기존 포맷

[3] 미국주식만 별도 발송하고 싶을 때:

    us_df = pd.DataFrame(us_hits).sort_values('N점수', ascending=False)
    if not us_df.empty:
        us_msg = "🇺🇸 [미국주식 수박신호 TOP]\n\n"
        for _, item in us_df.head(5).iterrows():
            us_msg += format_us_telegram_entry(item)
        send_telegram_photo(us_msg, [])

[4] 구글시트도 한국/미국 구분 컬럼으로 저장:
    update_google_sheet(all_hits_sorted, TODAY_STR, tournament_report)
    → '시장' 컬럼으로 필터링 가능 (🇰🇷KR / 🇺🇸US)
"""
