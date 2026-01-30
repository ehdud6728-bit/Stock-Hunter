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
# 🌍 한국 시간(KST) 강제 적용 (HTS와 싱크 맞추기)
# ---------------------------------------------------------
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')

# --- [환경변수] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

# ---------------------------------------------------------
# 🤖 [최신형] AI 모델 설정 (gemini-1.5-flash)
# ---------------------------------------------------------
model = None
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        # ⚠️ 최신 라이브러리가 설치되었으므로, 최신 모델 사용 가능!
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
# 🤖 AI 요약 (에러 추적 기능)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, price, strategy):
    if not GEMINI_API_KEY: return "\n🚫 [키 없음] API Key 설정 필요"
    if not model: return "\n🚫 [모델 오류] 라이브러리 업데이트 필요"

    try:
        prompt = f"""
        종목: {name} ({ticker})
        현재가: {price}원
        포착전략: {strategy}
        위 종목에 대해 딱 2줄로 요약해.
        첫 줄은 '👍 호재:', 둘째 줄은 '⚠️ 주의:' 로 시작할 것.
        """
        response = model.generate_content(prompt)
        time.sleep(1)
        return "\n" + response.text.strip()
    except Exception as e:
        # 에러 발생 시 구체적인 이유 리턴
        err = str(e)
        if "404" in err: return "\n🚫 [모델 없음] 모델명이 변경되었습니다."
        if "429" in err: return "\n🚫 [과부하] 잠시 후 다시 시도합니다."
        return f"\n🚫 [오류] {err[:30]}..."

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
    try
