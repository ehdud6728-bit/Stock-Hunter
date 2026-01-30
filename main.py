import FinanceDataReader as fdr
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

# --- [AI 설정] ---
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
    except: model = None

# ---------------------------------------------------------
# 📚 이름표 수집 (FDR 사용)
# ---------------------------------------------------------
print("📚 전체 종목 목록 수집 중...")
try:
    # KRX 전체 종목 가져오기
    krx_stocks = fdr.StockListing('KRX')
    
    # ⚠️ 중요: 우선주, 스팩주, 리츠 등은 노이즈가 많으니 이름으로 1차 필터링
    krx_stocks = krx_stocks[~krx_stocks['Name'].str.contains('스팩|우B|우|리츠|ETN|ETF')]
    
    # 상위 500개만 샘플링 (전체 다 하면 시간이 너무 걸릴 수 있음)
    # 시가총액(Marcap) 순으로 정렬되어 있다고 가정하고 상위 종목 위주로
    # (FDR 버전에 따라 컬럼명이 다를 수 있어 단순 슬라이싱)
    krx_stocks = krx_stocks.head(600) 
    
    NAME_MAP = dict(zip(krx_stocks['Code'].astype(str), krx_stocks['Name']))
    TARGET_LIST = krx_stocks['Code'].astype(str).tolist()
    print(f"✅ 분석 대상: 우량주 위주 {len(TARGET_LIST)}개 종목 선정 완료")
    
except Exception as e:
    print(f"❌ 종목 목록 수집 실패: {e}")
    TARGET_LIST = []
    NAME_MAP = {}

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
        핵심 포인트 1줄 요약.
        """
        response = model.generate_content(prompt)
        time.sleep(1)
        return "\n" + response.text.strip()
    except: return ""

# ---------------------------------------------------------
# 🔍 종목 분석 (차트 조건만 사용)
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        # 최근 1년치 데이터
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=365)))
        
        # 데이터가 너무 적거나(신규상장), 거래 정지(거래량0) 종목 패스
        if len(df) < 120 or df['Volume'].iloc[-1] == 0: return None
        
        curr = df.iloc[-1]
        
        # 1. 거래대금 필터 (최소 3억 원 이상 터진 것만) - 너무 잡주 제외
        if (curr['Close'] * curr['Volume']) < 300000000: return None

        # 지표 계산
        ma5 = df['Close'].rolling(5).mean()
        ma20 = df['Close'].rolling(20).mean()
        ma60 = df['Close'].rolling(60).mean()
        
        # RSI
        delta = df['Close'].diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + (gain / loss)))

        # --- [전략 조건] ---
        
        # 전략 A: 정배열 추세 (상승세 타는 중)
        # 5일선 > 20일선 > 60일선 (정배열 초입 or 진행) & RSI 적당함
        cond_A = (ma5.iloc[-1] > ma20.iloc[-1]) and \
                 (ma20.iloc[-1] > ma60.iloc[-1]) and \
                 (curr['Close'] > ma5.iloc[-1]) and \
                 (rsi.iloc[-1] >= 50) 

        # 전략 B: 낙폭과대 바닥 (많이 떨어졌다 반등)
        # 20일선 아래에 있고 & RSI가 침체권(40이하) 근처
        cond_B = (curr['Close'] < ma20.iloc[-1]) and \
                 (rsi.iloc[-1] <= 40) and \
                 (curr['Close'] > ma5.iloc[-1]) # 근데 오늘 5일선은 회복함 (반등신호)

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
    print("🚀 차트 분석 전용 모드 가동 (pykrx 제거됨)")
    
    # 1. 분석 대상 리스트 확인
    if not TARGET_LIST:
        print("❌ 분석할 종목 리스트가 없습니다.")
        exit()

    print(f"⚡ 우량주 {len(TARGET_LIST)}개 집중 분석 시작...")
    results = []

    # 2. 멀티 쓰레딩으로 고속 분석
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_stock, t): t for t in TARGET_LIST}
        
        count = 0
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)
            
            count += 1
            if count % 50 == 0:
                print