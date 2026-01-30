import FinanceDataReader as fdr
from pykrx import stock
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta
import google.generativeai as genai
import concurrent.futures

# --- [환경변수] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

# 📉 [수정 1] 수급 최소 금액을 확 낮췄습니다 (5천만원 -> 0원)
# 즉, 외인이나 기관이 '순매수'만 했으면 다 가져옵니다.
MIN_BUY_AMOUNT = 0 

# --- [AI 설정] ---
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
    except: model = None

# ---------------------------------------------------------
# 📚 이름표 수집
# ---------------------------------------------------------
print("📚 이름표 수집 중...")
try:
    krx_stocks = fdr.StockListing('KRX')
    NAME_MAP = dict(zip(krx_stocks['Code'].astype(str), krx_stocks['Name']))
except: NAME_MAP = {}

# ---------------------------------------------------------
# 📨 전송 함수
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        if chat_id.strip():
            try: requests.post(url, data={'chat_id': chat_id, 'text': message})
            except: pass

# ---------------------------------------------------------
# 🤖 AI 애널리스트
# ---------------------------------------------------------
def ask_gemini_analyst(ticker, name, price, status):
    if not GEMINI_API_KEY or not model: return ""
    try:
        prompt = f"""
        한국 주식 '{name}({ticker})'이 '{status}' 상태. 현재가 {price}원.
        핵심 포인트 1줄, 리스크 1줄 요약.
        """
        response = model.generate_content(prompt)
        time.sleep(1)
        return "\n" + response.text.strip()
    except: return ""

# ---------------------------------------------------------
# 📅 날짜 계산
# ---------------------------------------------------------
def get_recent_biz_days(days=5):
    end = datetime.now()
    start = end - timedelta(days=30)
    try:
        return fdr.DataReader('KS11', start, end).index[-days:]
    except: return []

# ---------------------------------------------------------
# ⚡ 수급 분석
# ---------------------------------------------------------
def get_supply_data():
    print("⚡ 수급 분석 중 (조건 완화)...")
    target_dates = get_recent_biz_days(3) # 최근 3일만 봄
    if len(target_dates) == 0: return []

    supply_dict = {}
    for date in target_dates:
        ymd = date.strftime("%Y%m%d")
        try:
            df = stock.get_market_net_purchases_of_equities_by_ticker(ymd, "ALL", "value")
            for ticker, row in df.iterrows():
                if ticker not in supply_dict: supply_dict[ticker] = 0
                net_buy = row['외국인'] + row['기관합계']
                
                # 순매수면 무조건 담기 (금액 상관 X)
                if net_buy > 0: supply_dict[ticker] += net_buy
        except: continue
    
    # MIN_BUY_AMOUNT보다 큰 것만 리턴
    return [t for t, amt in supply_dict.items() if amt >= MIN_BUY_AMOUNT]

# ---------------------------------------------------------
# 🔍 종목 분석 (조건 대폭 완화)
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=365)))
        if len(df) < 60: return None # 데이터 너무 적은 건 패스
        curr = df.iloc[-1]
        
        # 📉 [수정 2] 거래대금 기준 완화 (20억 -> 5억)
        # 소형주도 걸리게 함
        if (curr['Close'] * curr['Volume']) < 500000000: return None

        ma5 = df['Close'].rolling(5).mean()
        ma20 = df['Close'].rolling(20).mean()
        ma224 = df['Close'].rolling(224).mean()
        
        # RSI 계산
        delta = df['Close'].diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + (gain / loss)))

        # 전략 A: 추세 (조건 완화)
        # 📉 [수정 3] '거래량 1.5배 폭발' 조건 삭제함.
        # 그냥 정배열이고 5일선 위에 있으면 OK.
        cond_A = (curr['Close'] > ma5.iloc[-1]) and \
                 (ma5.iloc[-1] > ma20.iloc[-1]) and \
                 (rsi.iloc[-1] >= 45) # RSI 기준도 50 -> 45로 살짝 낮춤

        # 전략 B: 바닥 (조건 유지)
        cond_B = (curr['Close'] < ma224.iloc[-1]) and \
                 (rsi.iloc[-1] >= 30) and \
                 (curr['Close'] > ma5.iloc[-1]) and \
                 (90 <= (curr['Close']/ma20.iloc[-1]*100) <= 110)

        name = NAME_MAP.get(ticker, ticker)
        price = format(int(curr['Close']),',')
        
        if cond_A:
            ai = ask_gemini_analyst(ticker, name, price, "상승추세")
            return f"🦁 [추세] {name}\n{price}원{ai}"
        elif cond_B:
            ai = ask_gemini_analyst(ticker, name, price, "바닥반등")
            return f"🎣 [바닥] {name}\n{price}원{ai}"
            
    except: return None
    return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 조건 완화 버전 가동 (Thread: 5)")
    
    # 1. 수급 종목 가져오기
    target_tickers = get_supply_data()
    
    # 너무 많으면 상위 300개만 자르기 (AI 비용 절약)
    if len(target_tickers) > 300:
        target_tickers = target_tickers[:300]
        
    results = []
    print(f"⚡ {len(target_tickers)}개 종목 분석 시작...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(analyze_stock, t): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)

    today = datetime.now().strftime('%m/%d')
    header = f"🤖 [AI 리포트] {today}\n(조건 완화 검색 결과)\n"
    msg = header + "\n" + "\n\n".join(results) if results else header + "\n여전히 종목이 없습니다 ㅠㅠ"

    if len(msg) > 4000:
        send_telegram(msg[:4000])
        send_telegram(msg[4000:])
    else:
        send_telegram(msg)