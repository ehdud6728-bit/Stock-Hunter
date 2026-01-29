import FinanceDataReader as fdr
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta

# --- [환경변수에서 비밀키 가져오기] ---
# 코드를 공개해도 비밀키는 안전하게 보호됩니다.
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
TARGET_MARKET = 'KOSPI' 
TOP_N = 1000 # 시가총액 상위 100개 검색

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("텔레그램 설정이 없습니다.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {'chat_id': TELEGRAM_CHAT_ID, 'text': message}
    try:
        requests.post(url, data=data)
    except Exception as e:
        print(f"전송 실패: {e}")

def check_v3_condition(ticker, name):
    try:
        # 최근 200일 데이터
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d'))
        if len(df) < 120: return False 

        # 이평선
        ma5 = df['Close'].rolling(5).mean()
        ma20 = df['Close'].rolling(20).mean()
        ma60 = df['Close'].rolling(60).mean()
        ma120 = df['Close'].rolling(120).mean()

        # RSI (14)
        delta = df['Close'].diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        # --- [검색 로직 v3.0] ---
        # 1. 상승 마감 (0봉전 > 1봉전)
        cond1 = curr['Close'] >= prev['Close']
        # 2. 정배열 (5>20>60>120)
        cond2 = (ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1] > ma120.iloc[-1])
        # 3. RSI 50~80 (모멘텀)
        cond3 = 50 <= rsi.iloc[-1] <= 80
        # 4. 거래량 (최소 5만주)
        cond4 = curr['Volume'] > 50000

        if cond1 and cond2 and cond3 and cond4:
            price_str = format(int(curr['Close']), ',')
            rsi_str = round(rsi.iloc[-1], 1)
            return f"🚀 {name}({ticker})\n가격: {price_str}원 | RSI: {rsi_str}"
            
    except:
        return None
    return None

# --- 실행 ---
print("검색 시작...")
stocks = fdr.StockListing(TARGET_MARKET).head(TOP_N)
results = []

for idx, row in stocks.iterrows():
    res = check_v3_condition(row['Code'], row['Name'])
    if res:
        results.append(res)

if results:
    final_msg = f"🔔 [거거익선 v3.0] 포착 종목 ({datetime.now().strftime('%Y-%m-%d')})\n" + "\n\n".join(results)
    send_telegram(final_msg)
    print("전송 완료")
else:
    send_telegram(f"🔔 [거거익선 v3.0] 오늘 포착된 종목이 없습니다.")
    print("포착 종목 없음")
