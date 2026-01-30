import FinanceDataReader as fdr
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta
import concurrent.futures
from io import StringIO
import pytz
import json

# ---------------------------------------------------------
# 🌍 한국 시간(KST) 설정
# ---------------------------------------------------------
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')

# --- [환경변수 로드] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 
raw_key = os.environ.get('GEMINI_API_KEY')
GEMINI_API_KEY = raw_key.strip() if raw_key else None

# 상장 종목 리스트 가져오기
try:
    krx = fdr.StockListing('KRX')
    NAME_MAP = dict(zip(krx['Code'].astype(str), krx['Name']))
except: NAME_MAP = {}

# ---------------------------------------------------------
# 📨 텔레그램 전송 함수
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        if chat_id.strip():
            try: requests.post(url, data={'chat_id': chat_id, 'text': message})
            except: pass

# ---------------------------------------------------------
# 🤖 AI 요약 (문법오류/404오류 완벽 해결 버전)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, price, strategy):
    if not GEMINI_API_KEY: return "\n🚫 [키 오류] API Key 없음"

    # 1. 구글 최신 주소 (1.5-flash) 사용 -> 404 에러 방지
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    # 2. 안전한 삼중 따옴표 사용 -> 문법(Syntax) 에러 방지
    prompt = f"""종목: {name} ({ticker})
현재가: {price}원
포착전략: {strategy}
위 종목에 대해 딱 2줄로 요약해.
첫 줄은 '👍 호재:', 둘째 줄은 '⚠️ 주의:' 로 시작할 것."""

    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }]
    }

    try:
        # 라이브러리 없이 직접 요청
        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            try:
                text = data['candidates'][0]['content']['parts'][0]['text']
                return "\n" + text.strip()
            except:
                return "\n🚫 [응답 오류] AI 답변 해석 실패"
        else:
            return f"\n🚫 [구글 거절] 코드 {response.status_code}"
            
    except Exception as e:
        return f"\n🚫 [연결 실패] {str(e)[:20]}..."

# ---------------------------------------------------------
# ⚡ 네이버 수급 랭킹 스캔
# ---------------------------------------------------------
def get_top_buyer_stocks():
    print("⚡ 기관/외인 수급 랭킹 스캔 중...")
    # 네이버 금융에서 수급 상위 종목 긁어오기
    urls = [
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=0&investor_gubun=1000", 
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=0&investor_gubun=9000", 
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=1&investor_gubun=1000", 
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=1&investor_gubun=9000"
    ]
    headers = {'User-Agent': 'Mozilla/5.0'}
    found_tickers = set()
    for url in urls:
        try:
            res = requests.get(url, headers=headers, timeout=5)
            res.encoding = 'EUC-KR'
            dfs = pd.read_html(StringIO(res.text))
            for df in dfs:
                if '종목명' in df.columns:
                    valid_names = df['종목명'].dropna().tolist()
                    for name in valid_names:
                        code_match = krx[krx['Name'] == name]['Code']
                        if not code_match.empty:
                            found_tickers.add(str(code_match.values[0]))
        except: continue
    return list(found_tickers)

# ---------------------------------------------------------
# 🧮 보조지표 계산 (스토캐스틱)
# ---------------------------------------------------------
def get_stochastic(df, n=5, k=3, d=3):
    high = df['High'].rolling(window=n).max()
    low = df['Low'].rolling(window=n).min()
    fast_k = ((df['Close'] - low) / (high - low)) * 100
    slow_k = fast_k.rolling(window=k).mean()
    slow_d = slow_k.rolling(window=d).mean()
    return slow_k, slow_d

# ---------------------------------------------------------
# 🔍 3단 필터 (바닥/잠입/추세)
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        # 1년치 데이터 조회
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=365)).strftime('%Y-%m-%d'))
        if len(df) < 120: return None
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 거래대금 10억 미만 패스
        if (curr['Close'] * curr['Volume']) < 1000000000: return None

        # 이동평균선
        ma5 = df['Close'].rolling(5).mean()
        ma20 = df['Close'].rolling(20).mean()
        ma60 = df['Close'].rolling(60).mean()
        
        # RSI
        delta = df['Close'].diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + (gain / loss)))

        pct = curr['Change'] * 100
        name = NAME_MAP.get(ticker, ticker)
        price_str = format(int(curr['Close']),',')

        # 1. 🎣 [바닥 잡기]
        if (curr['Close'] < ma60.iloc[-1]) and (rsi.iloc[-1] <= 45) and (curr['Close'] > ma5.iloc[-1]):
            ai = get_ai_summary(ticker, name, price_str, "낙폭과대 바닥 반등")
            return f"🎣 [바닥] {name}\n가격: {price_str}원{ai}"

        # 2. 🕵️ [세력 잠입]
        elif (curr['Close'] > ma20.iloc[-1]) and (pct < 3.0 and pct > -2.0) and (rsi.iloc[-1] <= 60):
            ai = get_ai_summary(ticker, name, price_str, "이평선밀집 매집")
            return f"🕵️ [잠입] {name}\n가격: {price_str}원{ai}"

        # 3. 🦁 [급등 추세]
        else:
            is_trend = False
            if (pct >= 4.5) and (curr['Volume'] >= prev['Volume'] * 1.8):
                if (ma5.iloc[-1] > ma20.iloc[-1]) and (curr['Close'] > ma5.iloc[-1]):
                    k, d = get_stochastic(df)
                    if k.iloc[-1] > d.iloc[-1]:
                        is_trend = True
            if is_trend:
                ai = get_ai_summary(ticker, name, price_str, "거래량폭발 급등추세")
                return f"🦁 [추세] {name}\n가격: {price_str}원{ai}"
    except: return None
    return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [시스템 정상 가동] AI 주식 분석 시작")
    
    # 시작 알림
    send_telegram(f"🚀 [분석 시작] 주식 사냥을 시작합니다!\n(기준시간: {NOW.strftime('%H:%M:%S')})")

    market_msg = "분석
