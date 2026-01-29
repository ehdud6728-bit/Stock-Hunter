import FinanceDataReader as fdr
from pykrx import stock
import pandas as pd
import numpy as np
import requests
import os
import time
from datetime import datetime, timedelta

# --- [환경변수] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
MIN_BUY_AMOUNT = 50000000  # 수급 최소 금액 (5천만원)

# ---------------------------------------------------------
# [0] 텔레그램 전송 함수
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {'chat_id': TELEGRAM_CHAT_ID, 'text': message}
    try: requests.post(url, data=data)
    except: pass

# ---------------------------------------------------------
# [1] 시장 상황판 (코스피 지수 확인)
# ---------------------------------------------------------
def check_market_status():
    """코스피가 20일선 위에 있는지 확인"""
    try:
        kospi = fdr.DataReader('KS11', start=(datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d'))
        ma20 = kospi['Close'].rolling(20).mean().iloc[-1]
        current = kospi['Close'].iloc[-1]
        
        status = "📈 상승장 (공격 모드)" if current > ma20 else "📉 하락/조정장 (방어 모드)"
        return status, current, ma20
    except:
        return "판단 불가", 0, 0

# ---------------------------------------------------------
# [2] 수급 데이터 가져오기 (공통 사용)
# ---------------------------------------------------------
def get_supply_data():
    """최근 5일간 수급(5천만원 이상)이 들어온 종목 추출"""
    print("⚡ 수급 데이터 분석 중...")
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=12)).strftime("%Y%m%d")
    dates = stock.get_index_ohlcv_by_date(start_date, end_date, "1001").index
    target_dates = dates[-5:]
    
    supply_dict = {}
    
    for date in target_dates:
        ymd = date.strftime("%Y%m%d")
        try:
            df = stock.get_market_net_purchases_of_equities_by_ticker(ymd, "ALL", "value") # 금액 기준
            for ticker, row in df.iterrows():
                if ticker not in supply_dict: supply_dict[ticker] = 0
                # 외국인 + 기관 합산 순매수 금액 누적
                net_buy = row['외국인'] + row['기관합계']
                if net_buy > 0: supply_dict[ticker] += net_buy
        except: continue
        
    # 5일 누적 순매수 5천만원 이상인 종목만 필터링
    filtered_tickers = [t for t, amt in supply_dict.items() if amt >= MIN_BUY_AMOUNT]
    print(f"✅ 수급 유입 종목: {len(filtered_tickers)}개")
    return filtered_tickers

# ---------------------------------------------------------
# [3] 보조지표 계산기
# ---------------------------------------------------------
def get_indicators(df):
    close = df['Close']
    
    # 이평선
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma224 = close.rolling(224).mean() # 바닥 확인용
    
    # RSI
    delta = close.diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    # 일목균형표 (선행스팬2) - 52일 고가/저가 평균을 26일 뒤로
    high_52 = df['High'].rolling(52).max()
    low_52 = df['Low'].rolling(52).min()
    span2 = (high_52 + low_52) / 2
    # span2는 26일 앞에 그려지므로, 현재 시점의 구름대 값은 26일 전의 계산값임
    cloud_span2 = span2.shift(26) 
    
    return ma5, ma20, ma60, ma224, rsi, cloud_span2

# ---------------------------------------------------------
# [4] 전략 실행 (A: 끝판왕 / B: 바닥낚시)
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'))
        if len(df) < 230: return None # 224일선 계산 위해 넉넉히
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 기본 필터: 거래대금 20억 이상 (너무 소형주 제외)
        if (curr['Close'] * curr['Volume']) < 2000000000: return None

        ma5, ma20, ma60, ma224, rsi, cloud = get_indicators(df)
        
        # --- 🦁 전략 A: [끝판왕 튜닝] 추세 가속 ---
        # 1. 정배열 초입 (5 > 20) & 상승 추세 (현재가 > 20일선)
        # 2. 거래량 폭발 (전일 대비 150% 이상)
        # 3. RSI 50 이상 (상승 에너지)
        # 4. 수급 (이미 필터링됨)
        cond_A_trend = (curr['Close'] > ma5.iloc[-1]) and (ma5.iloc[-1] > ma20.iloc[-1])
        cond_A_vol = (curr['Volume'] >= prev['Volume'] * 1.5)
        cond_A_rsi = rsi.iloc[-1] >= 50
        
        is_endgame = cond_A_trend and cond_A_vol and cond_A_rsi

        # --- 🎣 전략 B: [바닥 낚시] 낙폭 과대 반등 ---
        # 1. 역배열 바닥 (현재가 < 224일선)
        # 2. 구름대 아래 (현재가 < 선행스팬2)
        # 3. 반등 신호 (RSI 30 이상 & 5일선 회복)
        # 4. 이격도 (20일선 근처 95~105% - 급락 멈춤)
        # 5. 거래량 실림
        cond_B_loc = (curr['Close'] < ma224.iloc[-1]) and (curr['Close'] < cloud.iloc[-1])
        cond_B_signal = (rsi.iloc[-1] >= 30) and (curr['Close'] > ma5.iloc[-1])
        disparity = (curr['Close'] / ma20.iloc[-1]) * 100
        cond_B_disparity = 95 <= disparity <= 105
        
        is_bottom = cond_B_loc and cond_B_signal and cond_B_disparity and cond_A_vol
        
        name = stock.get_market_ticker_name(ticker)
        
        if is_endgame:
            return f"🦁 [추세] {name}\n- 가격: {format(int(curr['Close']),',')}원\n- RSI: {round(rsi.iloc[-1],1)} / Vol: {int(curr['Volume']/prev['Volume']*100)}%"
        elif is_bottom:
            return f"🎣 [바닥] {name}\n- 가격: {format(int(curr['Close']),',')}원\n- 위치: 224선 아래 / RSI: {round(rsi.iloc[-1],1)}"
            
    except:
        return None
    return None

# ---------------------------------------------------------
# [5] 메인 실행
# ---------------------------------------------------------
print("🚀 통합 검색기 가동 시작")

# 1. 시장 파악
market_msg, idx_cur, idx_ma = check_market_status()
print(f"시장 상태: {market_msg}")

# 2. 수급 필터링
target_tickers = get_supply_data()

# 3. 정밀 분석
results_trend = []
results_bottom = []

print(f"⚡ {len(target_tickers)}개 종목 정밀 분석 중...")
for ticker in target_tickers:
    res = analyze_stock(ticker)
    if res:
        if "[추세]" in res: results_trend.append(res)
        if "[바닥]" in res: results_bottom.append(res)

# 4. 결과 전송
today = datetime.now().strftime('%m/%d')
header = f"📊 [거거익선 통합리포트] {today}\n시장: {market_msg}\n\n"

msg_body = ""
if results_trend:
    msg_body += f"🦁 추세 가속 (상승장 주도)\n" + "\n".join(results_trend) + "\n\n"
if results_bottom:
    msg_body += f"🎣 바닥 낚시 (반등 노림)\n" + "\n".join(results_bottom)

if not msg_body:
    msg_body = "조건을 만족하는 종목이 없습니다."
    
final_msg = header + msg_body

# 길면 나눠서 전송
if len(final_msg) > 4000:
    send_telegram(final_msg[:4000])
    send_telegram(final_msg[4000:])
else:
    send_telegram(final_msg)

print("✅ 전송 완료")
