import FinanceDataReader as fdr
from pykrx import stock
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta
import google.generativeai as genai
import concurrent.futures  # 🚀 병렬 처리를 위한 도구 추가

# --- [환경변수] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
MIN_BUY_AMOUNT = 50000000

# --- [AI 설정] ---
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
    except: model = None

# ---------------------------------------------------------
# 📚 [이름표 수집] FDR로 안전하게 가져오기
# ---------------------------------------------------------
print("📚 종목 이름표 수집 중... (FDR)")
try:
    krx_stocks = fdr.StockListing('KRX')
    NAME_MAP = dict(zip(krx_stocks['Code'].astype(str), krx_stocks['Name']))
    print("✅ 이름표 수집 완료")
except:
    NAME_MAP = {}
    print("⚠️ 이름표 수집 실패 (코드만 출력됩니다)")

# ---------------------------------------------------------
# 📨 전송 함수
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        chat_id = chat_id.strip()
        if not chat_id: continue
        try: requests.post(url, data={'chat_id': chat_id, 'text': message})
        except: pass

# ---------------------------------------------------------
# 🤖 AI 애널리스트
# ---------------------------------------------------------
def ask_gemini_analyst(ticker, name, price, status):
    if not GEMINI_API_KEY or not model: return ""
    try:
        # 🚀 속도를 위해 AI 답변 길이를 좀 더 짧게 제한
        prompt = f"""
        한국 주식 '{name}({ticker})'이 '{status}' 상태. 현재가 {price}원.
        핵심 투자 포인트와 리스크를 각 1줄로 요약.
        형식:
        👍 호재: ...
        ⚠️ 주의: ...
        """
        response = model.generate_content(prompt)
        time.sleep(1) # AI API 호출 제한 방지용 1초 휴식
        return "\n" + response.text.strip()
    except: return ""

# ---------------------------------------------------------
# 📅 날짜 계산 (FDR 사용 - 에러 방지)
# ---------------------------------------------------------
def get_recent_biz_days(days=5):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    try:
        kospi_idx = fdr.DataReader('KS11', start_date, end_date)
        return kospi_idx.index[-days:]
    except:
        return []

# ---------------------------------------------------------
# ⚡ 수급 분석 (pykrx 사용 - 에러 무시)
# ---------------------------------------------------------
def get_supply_data():
    print("⚡ 수급 데이터 분석 중...")
    target_dates = get_recent_biz_days(5)
    
    if len(target_dates) == 0:
        return []

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

# ---------------------------------------------------------
# 🔍 개별 종목 분석 (작업자 함수)
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        # 데이터 가져오기
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=365)))
        if len(df) < 230: return None
        curr = df.iloc[-1]
        
        # 거래대금 필터 (20억)
        if (curr['Close'] * curr['Volume']) < 2000000000: return None

        # 지표 계산
        ma5 = df['Close'].rolling(5).mean()
        ma20 = df['Close'].rolling(20).mean()
        ma224 = df['Close'].rolling(224).mean()
        
        delta = df['Close'].diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + (gain / loss)))

        high_52 = df['High'].rolling(52).max()
        low_52 = df['Low'].rolling(52).min()
        span2 = ((high_52 + low_52) / 2).shift(26)

        # 전략 조건
        cond_A = (curr['Close'] > ma5.iloc[-1]) and (ma5.iloc[-1] > ma20.iloc[-1]) and \
                 (df['Volume'].iloc[-1] >= df['Volume'].iloc[-2] * 1.5) and (rsi.iloc[-1] >= 50)

        cond_B = (curr['Close'] < ma224.iloc[-1]) and (curr['Close'] < span2.iloc[-1]) and \
                 (rsi.iloc[-1] >= 30) and (curr['Close'] > ma5.iloc[-1]) and \
                 (95 <= (curr['Close']/ma20.iloc[-1]*100) <= 105)

        name = NAME_MAP.get(ticker, ticker)
        price_str = format(int(curr['Close']),',')
        
        # 조건 만족 시 AI 호출
        if cond_A:
            ai_msg = ask_gemini_analyst(ticker, name, price_str, "상승추세/거래량급증")
            return f"🦁 [추세] {name}\n가격: {price_str}원{ai_msg}"
        elif cond_B:
            ai_msg = ask_gemini_analyst(ticker, name, price_str, "바닥권반등/낙폭과대")
            return f"🎣 [바닥] {name}\n가격: {price_str}원{ai_msg}"
            
    except: return None
    return None

# ---------------------------------------------------------
# 🚀 메인 실행 (멀티 쓰레딩 적용)
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 고속 AI 자동매매 시스템 가동 (Thread: 5)")

    # 1. 시장 상태 확인
    try:
        kospi = fdr.DataReader('KS11', start=(datetime.now() - timedelta(days=60)))
        market_msg = "📈 상승장" if kospi['Close'].iloc[-1] > kospi['Close'].rolling(20).mean().iloc[-1] else "📉 조정장"
    except:
        market_msg = "시장 데이터 조회 불가"

    # 2. 수급 상위 종목 추출
    target_tickers = get_supply_data()
    results = []
    
    print(f"⚡ {len(target_tickers)}개 종목 정밀 분석 시작 (병렬 처리)...")

    # 3. 쓰레딩으로 병렬 분석 시작
    # max_workers=5 : 직원 5명 투입 (Gemini API 제한 고려)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # 작업을 미리 다 던져놓고 결과 기다리기
        future_to_ticker = {executor.submit(analyze_stock, ticker): ticker for ticker in target_tickers}
        
        count = 0
        for future in concurrent.futures.as_completed(future_to_ticker):
            res = future.result()
            if res:
                results.append(res)
            
            # 진행 상황 표시 (선택사항)
            count += 1
            if count % 10 == 0:
                print(f"... {count}/{len(target_tickers)} 완료")

    # 4. 결과 전송
    today = datetime.now().strftime('%m/%d')
    header = f"🤖 [AI 스피드 리포트] {today}\n시장: {market_msg}\n"
    msg = header + "\n" + "\n\n".join(results) if results else header + "\n조건 만족 종목 없음"

    if len(msg) > 4000:
        send_telegram(msg[:4000])
        send_telegram(msg[4000:])
    else:
        send_telegram(msg)