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
# 🌍 한국 시간(KST)
# ---------------------------------------------------------
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')

# --- [환경변수] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

# ---------------------------------------------------------
# 🤖 AI 모델 설정 (안전한 'gemini-pro' 사용)
# ---------------------------------------------------------
model = None
model_error = "초기화 전"

if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        # ⚠️ [수정] 최신형(flash) 대신 호환성 좋은 'gemini-pro' 사용
        model = genai.GenerativeModel('gemini-pro')
        print("✅ AI 모델(gemini-pro) 로드 성공")
    except Exception as e:
        model = None
        model_error = str(e)
        print(f"❌ AI 모델 로드 실패: {e}")
else:
    model_error = "API Key 없음"

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
# 🤖 AI 요약
# ---------------------------------------------------------
def get_ai_summary(ticker, name, price, strategy):
    if not GEMINI_API_KEY: return "\n🚫 [키 오류] API Key 없음"
    if not model: return f"\n🚫 [오류] 모델 로드 실패 ({model_error})"

    try:
        # 안전한 문장 만들기
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
        error_msg = str(e)
        print(f"❌ 분석 에러: {error_msg}")
        
        # 에러 발생
