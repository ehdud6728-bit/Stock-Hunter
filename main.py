import FinanceDataReader as fdr
from pykrx import stock
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta
import google.generativeai as genai

# --- [환경변수] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
# 여러 명의 ID를 콤마(,)로 구분해서 가져옵니다.
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
MIN_BUY_AMOUNT = 50000000

# --- [AI 설정] ---
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')

# ---------------------------------------------------------
# 📨 [수정됨] 다중 전송 함수
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    # 콤마로 쪼개진 ID 리스트를 하나씩 돌면서 전송
    for chat_id in CHAT_ID_LIST:
        chat_id = chat_id.strip() # 공백 제거
        if not chat_id: continue # 빈 문자열이면 패스
        
        data = {'chat_id': chat_id, 'text': message}
        try:
            requests.post(url, data=data)
            print(f"전송 성공: {chat_id}")
        except Exception as e:
            print(f"전송 실패 ({chat_id}): {e}")

# ---------------------------------------------------------
# 🤖 [AI 애널리스트] 종목 코멘트 생성
# ---------------------------------------------------------
def ask_gemini_analyst(ticker, name, price, status):
    if not GEMINI_API_KEY: return ""
    try:
        prompt = f"""
        당신은 월가 최고의 주식 애널리스트입니다.
        한국 주식 '{name}({ticker})'이 '{status}' 상태로 포착되었습니다.
        현재가: {price}원.
        핵심 투자 포인트 1가지와 리스크 1가지를 각 한 문장으로(50자 이내) 요약.
        형식:
        👍 호재: (내용)
        ⚠️ 주의: (내용)
        """
        response = model.generate_content(prompt)
        return "\n" + response.text.strip()
    except: return "\n(AI 분석 실패)"

# ---------------------------------------------------------
# [기존 로직] 시장/수급/차트 분석
# ---------------------------------------------------------
def check_market_status():
    try:
        kospi = fdr.DataReader('KS11', start=(datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d'))
        ma20 = kospi['Close'].rolling(20).mean().iloc[-1]
        current = kospi['Close'].iloc[-1]
        return "📈 상승장" if current > ma20 else "📉 조정장"
    except: return "판단 불가"

def get_supply_data():
    print("⚡ 수급 분석 중...")
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=12)).strftime("%Y%m%d")
    dates = stock.get_index_ohlcv_by_date(start_date, end_date, "1001").index
    target_dates = dates[-5:]
    supply_dict = {}
    for date in target_dates:
        ymd = date.strftime("%Y%m%d")
        try:
            df = stock.get_market_net_purchases_of_equities_by_ticker(ymd, "ALL", "value")
            for ticker, row in df.iterrows():
                if ticker not in supply_dict: supply_dict[ticker] = 0
                net_buy = row['외국인'] + row['기관합계']
                if net_buy > 0: supply_dict[ticker] += net_buy
        except: continue
    return [t for t, amt in supply_dict.items() if amt >= MIN_BUY_AMOUNT]

def get_indicators(df):
    close = df['Close']
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    ma224 = close.rolling(224).mean()
    delta = close.diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    high_52 = df['High'].rolling(52).max()
    low_52 = df['Low'].rolling(52).min()
    span2 = (high_52 + low_52) / 2
    cloud_span2 = span2.shift(26)
    return ma5, ma20, ma224, rsi, cloud_span2

def analyze_stock(ticker):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'))
        if len(df) < 230: return None
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        if (curr['Close'] * curr['Volume']) < 2000000000: return None

        ma5, ma20, _, ma224, rsi, cloud = get_indicators(df)
        
        # 전략 A: 추세
        cond_A = (curr['Close'] > ma5.iloc[-1]) and (ma5.iloc[-1] > ma20.iloc[-1]) and \
                 (curr['Volume'] >= prev['Volume'] * 1.5) and (rsi.iloc[-1] >= 50)

        # 전략 B: 바닥
        cond_B = (curr['Close'] < ma224.iloc[-1]) and (curr['Close'] < cloud.iloc[-1]) and \
                 (rsi.iloc[-1] >= 30) and (curr['Close'] > ma5.iloc[-1]) and \
                 (95 <= (curr['Close']/ma20.iloc[-1]*100) <= 105)

        name = stock.get_market_ticker_name(ticker)
        price_str = format(int(curr['Close']),',')
        
        if cond_A:
            ai_comment = ask_gemini_analyst(ticker, name, price_str, "상승추세/거래량폭발")
            return f"🦁 [추세] {name}\n가격: {price_str}원{ai_comment}"
        elif cond_B:
            ai_comment = ask_gemini_analyst(ticker, name, price_str, "바닥권반등/낙폭과대")
            return f"🎣 [바닥] {name}\n가격: {price_str}원{ai_comment}"
    except: return None
    return None

# ---------------------------------------------------------
# [실행]
# ---------------------------------------------------------
print("🚀 AI 자동매매 시스템 가동 (다중 전송 모드)")
market_msg = check_market_status()
target_tickers = get_supply_data()

results = []
print(f"⚡ {len(target_tickers)}개 종목 분석 중...")

for ticker in target_tickers:
    res = analyze_stock(ticker)
    if res:
        results.append(res)
        time.sleep(1)

today = datetime.now().strftime('%m/%d')
header = f"🤖 [AI 스마트 리포트] {today}\n시장: {market_msg}\n"
msg = header + "\n" + "\n\n".join(results) if results else header + "\n검색된 종목이 없습니다."

if len(msg) > 4000:
    send_telegram(msg[:4000])
    send_telegram(msg[4000:])
else:
    send_telegram(msg)
