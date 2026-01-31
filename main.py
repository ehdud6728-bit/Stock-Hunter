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
# 🌍 한국 시간(KST)
# ---------------------------------------------------------
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')

# --- [환경변수 로드] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 

# Groq 키 공백 제거 안전장치
raw_groq_key = os.environ.get('GROQ_API_KEY', '')
GROQ_API_KEY = raw_groq_key.strip() 

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
# 🤖 AI 요약 (Groq: 최신형 Llama-3.3)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, price, strategy):
    if not GROQ_API_KEY: return "\n🚫 [키 없음] GitHub Secrets 확인 필요"

    url = "https://api.groq.com/openai/v1/chat/completions"
    
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    
    prompt = f"""종목: {name} ({ticker})
현재가: {price}원
포착전략: {strategy}
위 종목에 대해 딱 2줄로 요약해.
첫 줄은 '👍 호재:', 둘째 줄은 '⚠️ 주의:' 로 시작할 것."""

    payload = {
        # ⚠️ [수정] 폐기된 구형 모델 대신 최신형 모델 사용!
        "model": "llama-3.3-70b-versatile", 
        "messages": [
            {"role": "system", "content": "너는 한국 주식 전문가야. 반드시 한국어로 답변해."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.5
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return "\n" + data['choices'][0]['message']['content'].strip()
        else:
            return f"\n🚫 [Groq 거절] {response.status_code}\n(메시지: {response.text[:30]}...)"
            
    except Exception as e:
        return f"\n🚫 [연결 실패] {str(e)[:20]}..."

# ---------------------------------------------------------
# ⚡ 네이버 수급 랭킹
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
            res = requests.get(url, headers=headers, timeout=5)
            res.encoding = 'EUC-KR'
            dfs = pd.read_html(StringIO(res.text))
            for df in dfs:
                if '종목명' in df.columns:
                    valid_names = df['종목명'].dropna().tolist()
                    for name in valid_names:
                        code_match = krx[krx['Name'] == name]['Code']
                        if not code_match.empty:
                            found_tickers.add(str(code_match.values
