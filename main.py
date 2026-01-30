import FinanceDataReader as fdr
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta
import google.generativeai as genai
import concurrent.futures
from io import StringIO

# --- [환경변수] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
    except: model = None

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
# 🧠 [핵심] AI 검증 및 정밀 분석 함수
# ---------------------------------------------------------
def get_ai_verification(ticker, name, price, strategy, technical_data):
    if not GEMINI_API_KEY or not model: return "\n(AI 분석 불가)"
    try:
        # AI에게 건네줄 상세 데이터표
        prompt = f"""
        역할: 당신은 냉철한 주식 펀드매니저입니다.
        종목: {name} ({ticker})
        현재가: {price}원
        포착전략: {strategy}
        
        [기술적 지표 데이터]
        {technical_data}

        위 데이터를 바탕으로 정밀 검증 리포트를 작성하세요.
        반드시 아래 3가지 항목만 짧고 굵게 출력하세요. (군더더기 금지)
        
        1. 📊 검증 점수: (0~100점, 80점이상이면 매수 추천)
        2. 💡 핵심 이유: (왜 떴는지, 속임수 가능성은 없는지 1줄 요약)
        3. 🎯 대응 전략: (손절가는 -3%~-5% 수준에서 구체적 가격 제시)
        """
        response = model.generate_content(prompt)
        time.sleep(1) # API 과부하 방지
        return "\n" + response.text.strip()
    except: return "\n(AI 응답 시간초과)"

# ---------------------------------------------------------
# ⚡ 네이버 수급 랭킹 스캔
# ---------------------------------------------------------
def get_top_buyer_stocks():
    print("⚡ 기관/외인 수급 랭킹 스캔 중...")
    urls = [
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=0&investor_gubun=1000", 
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=0&investor_gubun=9000", 
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=1&investor_gubun=1000", 
        "
