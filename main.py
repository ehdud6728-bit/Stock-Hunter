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

# --- [환경변수] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 
# 혹시 모를 공백 제거를 위해 strip() 추가
raw_key = os.environ.get('GEMINI_API_KEY')
GEMINI_API_KEY = raw_key.strip() if raw_key else None

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
# 🤖 AI 요약 (gemini-1.5-flash 직접 연결)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, price, strategy):
    if not GEMINI_API_KEY: return "\n🚫 [키 오류] API Key 없음"

    # ⚠️ [수정] 여기가 핵심! 구글이 현재 열어둔 최신 주소입니다.
    # gemini-pro (X) -> gemini-1.5-flash (O)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    prompt = (
        f"종목: {name} ({ticker})\n"
        f"현재가: {price}원\n"
        f"포착전략: {strategy}\n"
        "위 종목에 대해 딱 2줄로 요약해.\n"
        "첫 줄은 '👍 호재:', 둘째 줄은 '⚠️ 주의:' 로 시작할 것."
    )

    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }]
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            try:
                text = data['candidates'][0]['content']['parts'][0]['text']
                return "\n" + text.strip()
            except:
                return "\n🚫 [응답 오류] AI 답변 해석 실패"
        else:
            # 404가 뜨면 주소 문제, 400이면 키 문제
            return f"\n🚫 [구글 거절] {response.status_code} (오류내용: {response.text[:20]}...)"
            
    except Exception as e:
        return f"\n🚫 [연결 실패] {str(e)[:30]}..."

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
            res
