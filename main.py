import FinanceDataReader as fdr
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta
import google.generativeai as genai
import concurrent.futures
from io import StringIO
import pytz

# ---------------------------------------------------------
# 🌍 한국 시간(KST) 강제 적용
# ---------------------------------------------------------
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')

# --- [환경변수] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

# ---------------------------------------------------------
# 🤖 AI 모델 설정 (gemini-1.5-flash)
# ---------------------------------------------------------
model = None
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash') 
    except Exception as e:
        model = None

try:
    krx = fdr.StockListing('KRX')
    NAME_MAP = dict(zip(krx['Code'].astype(str), krx['Name']))
except: NAME_MAP = {}

# ---------------------------------------------------------
# 📨 텔레그램 전송
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        if chat_id.strip():
            try: requests.post(url, data={'chat_id': chat_id, 'text': message})
            except: pass

# ---------------------------------------------------------
# 🤖 AI 요약 (문법 오류 방지 수정됨)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, price, strategy):
    if not GEMINI_API_KEY: return "\n🚫 [키 없음] API Key 설정 필요"
    if not model: return "\n🚫 [모델 오류] 라이브러리 업데이트 필요"

    try:
        # ⚠️ [수정] 따옴표 에러 방지를 위해 괄호()로 감싸는 안전한 방식 사용
        prompt = (
            f"종목: {name} ({ticker})\n"
            f"현재가: {price}원\n"
            f"포착전략: {strategy}\n"
            "위 종목에 대해 딱 2줄로 요약해.\n"
            "첫 줄은 '👍 호재:', 둘째 줄은 '⚠️ 주의:' 로 시작할 것."
        )
        
        response = model.generate_content(prompt)
        time.sleep(1)
        return "\n" + response.text.strip()
    except Exception as e:
        err = str(e)
        if "404" in err: return "\n🚫 [모델 없음] 모델명이 변경되었습니다."
        if "429" in err: return "\n🚫 [과부하] 잠시 후 다시 시도합니다."
        return f"\n🚫 [오류] {err[:20]}..."

# ---------------------------------------------------------
# ⚡ 네이버 수급 랭킹 스캔
# ---------------------------------------------------------
def get_top_buyer_stocks():
    print("⚡ 기관/외인 수급 랭킹 스캔 중...")
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
            res = requests.get(url, headers=headers)
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
# 🧮 스토캐스틱 계산
# ---------------------------------------------------------
def get_stochastic(df, n=5, k=3, d=3):
    high = df['High'].rolling(window=n).max()
    low = df['Low'].rolling(window=n).min()
    fast_k = ((df['Close'] - low) / (high - low)) * 100
    slow_k = fast_k.rolling(window=k).mean()
    slow_d = slow_k.rolling(window=d).mean()
    return slow_k, slow_d

# ---------------------------------------------------------
# 🔍 3단 필터 (오차보정 포함)
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=365)).strftime('%Y-%m-%d'))
        if len(df) < 120: return None
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 거래대금 10억
        if (curr['Close'] * curr['Volume']) < 1000000000: return None

        ma5 = df['Close'].rolling(5).mean()
        ma20 = df['Close'].rolling(20).mean()
        ma60 = df['Close'].rolling(60).mean()
        
        delta = df['Close'].diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + (gain / loss)))

        pct = curr['Change'] * 100
        name = NAME_MAP.get(ticker, ticker)
        price_str = format(int(curr['Close']),',')

        # 1. 🎣 [바닥]
        if (curr['Close'] < ma60.iloc[-1]) and (rsi.iloc[-1] <= 45) and (curr['Close'] > ma5.iloc[-1]):
            ai = get_ai_summary(ticker, name, price_str, "낙폭과대 바닥 반등")
            return f"🎣 [바닥] {name}\n가격: {price_str}원{ai}"

        # 2. 🕵️ [잠입]
        elif (curr['Close'] > ma20.iloc[-1]) and (pct < 3.0 and pct > -2.0) and (rsi.iloc[-1] <= 60):
            ai = get_ai_summary(ticker, name, price_str, "이평선밀집 매집")
            return f"🕵️ [잠입] {name}\n가격: {price_str}원{ai}"

        # 3. 🦁 [추세]
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
    print(f"🚀 [SyntaxError 수정완료] 시스템 재가동...")
    
    market_msg = "분석 중..."
    try:
        kospi = fdr.DataReader('KS11', start=(NOW - timedelta(days=60)).strftime('%Y-%m-%d'))
        curr_k = kospi['Close'].iloc[-1]
        ma20_k = kospi['Close'].rolling(20).mean().iloc[-1]
        market_msg = "📈 상승장" if curr_k > ma20_k else "📉 조정장"
    except: pass

    target_tickers = get_top_buyer_stocks()
