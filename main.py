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
# 🤖 AI 요약 (업그레이드: 수급/시장 전문 분석가 모드)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, price, strategy):
    if not GROQ_API_KEY: return "\n🚫 [키 없음] GitHub Secrets 확인 필요"

    url = "https://api.groq.com/openai/v1/chat/completions"
    
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # ⚡ [핵심] 프롬프트를 아주 구체적으로 변경했습니다.
    system_role = "너는 여의도에서 20년 경력의 주식 트레이더야. 초보자가 아니라 고수에게 브리핑하듯이 전문 용어(수급, 매물대, 투심 등)를 섞어서 날카롭게 분석해."
    
    user_msg = f"""
    [종목 정보]
    종목명: {name} ({ticker})
    현재가: {price}원
    포착된 패턴: {strategy}

    [분석 요청]
    위 종목이 이 패턴에 포착된 이유를 '수급(기관/외인 유입 가능성)'과 '시장 심리' 관점에서 분석해.
    뻔한 주의사항(투자는 본인 몫 등)은 절대 쓰지 마.

    [출력 양식]
    👍 호재: (수급 유입 배경, 돌파 매매 관점, 섹터 분위기 등을 포함해 1문장)
    ⚠️ 주의: (차트상 저항선, 단기 이격도 과열, 매물대 부담 등을 포함해 1문장)
    """

    payload = {
        "model": "llama-3.3-70b-versatile", 
        "messages": [
            {"role": "system", "content": system_role},
            {"role": "user", "content": user_msg}
        ],
        "temperature": 0.7 # 창의성 약간 높임 (더 다양한 표현 위해)
    }

    try:
