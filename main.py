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
raw_groq_key = os.environ.get('GROQ_API_KEY', '')
GROQ_API_KEY = raw_groq_key.strip() 

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
    if not GROQ_API_KEY: return ""

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    
    prompt = f"""
    종목: {name} ({ticker})
    현재가: {price}원
    패턴: {strategy}
    
    이 종목을 '이격도(가격부담)'와 '수급' 관점에서 2줄 요약해.
    👍 호재: (초입 구간 메리트)
    ⚠️ 주의: (단기 매물대)
    """

    payload = {
        "model": "llama-3.3-70b-versatile", 
        "messages": [
            {"role": "system", "content": "너는 주식 분석가야. 한국어로 짧고 명확하게 답해."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.5
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=5)
        if response.status_code == 200:
            return "\n" + response.json()['choices'][0]['message']['content'].strip()
        return ""
    except: return ""

# ---------------------------------------------------------
# ⚡ [Top 1000] 데이터 수집
# ---------------------------------------------------------
def get_market_leaders():
    print("⚡ 시장 데이터 수집 (Top 1000)...")
    try:
        df_krx = fdr.StockListing('KRX')
        df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(1000)
        target_dict = dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
        return target_dict
    except: return {}

# ---------------------------------------------------------
# 🧮 지표 계산 (이격도 추가)
# ---------------------------------------------------------
def get_indicators(df):
    ma5 = df['Close'].rolling(5).mean()
    ma20 = df['Close'].rolling(20).mean()
    ma60 = df['Close'].rolling(60).mean()
    
    # 이격도(Disparity) 계산: 현재가 / 20일선 * 100
    # (100이면 20일선에 딱 붙어있는 것, 110이면 10% 떠있는 것)
    disparity = (df['Close'] / ma20) * 100
    
    # OBV
    direction = df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    obv = (direction * df['Volume']).cumsum()
    obv_rising = obv.iloc[-1] > obv.iloc[-2]
    
    return ma5, ma20, ma60, disparity, obv_rising

# ---------------------------------------------------------
# 🔍 분석 로직 (이격도 110% 제한 -> 초입 포착)
# ---------------------------------------------------------
def analyze_stock(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=200)).strftime('%Y-%m-%d'))
        if len(df) < 60: return None
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        if curr['Close'] < 1000: return None
        
        # 지표 로드
        ma5, ma20, ma60, disparity, obv_rising = get_indicators(df)
        
        pct = curr['Change'] * 100
        vol_ratio = curr['Volume'] / prev['Volume'] if prev['Volume'] > 0 else 0
        price_str = format(int(curr['Close']),',')
        
        curr_disp = disparity.iloc[-1] # 오늘의 이격도

        # -----------------------------------------------------------
        # 🦁 [1] 추세 초입 (Start-Up Trend)
        # 조건: 정배열 + OBV 상승 + ⭐이격도 110% 이하 (안 비쌈!)
        # -----------------------------------------------------------
        if (ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1]) and (curr['Close'] > ma20.iloc[-1]):
            # 1. 상승 중인가? (1% 이상)
            # 2. 거래량 1.2배 or OBV 상승 (수급 확인)
            # 3. ⭐핵심: 이격도가 110 이하여야 함 (20일선 근처)
            if (pct >= 1.0) and (curr_disp <= 110) and obv_rising:
                
                ai = get_ai_summary(ticker, name, price_str, f"추세초입(이격도{int(curr_disp)}%)")
                return f"🦁 [추세초입] {name}\n등락: +{pct:.2f}% (이격도 {int(curr_disp)}%)\n특징: 20일선 근처 정배열 출발!{ai}"

        # -----------------------------------------------------------
        # 🕵️ [2] 잠입/매집 (눌림목)
        # 조건: 주가 하락 + OBV 상승 + ⭐이격도 105% 이하 (완전 바닥권)
        # -----------------------------------------------------------
        elif (curr['Close'] > ma20.iloc[-1]) and (-3.0 < pct < 1.0):
            # 주가는 쉬는데 OBV는 오름 + 이격도가 낮음(안전)
            if (vol_ratio < 1.0) and obv_rising and (curr_disp <= 105):
                ai = get_ai_summary(ticker, name, price_str, "눌림목 매집")
                return f"🕵️ [잠입] {name}\n등락: {pct:.2f}% (이격도 {int(curr_disp)}%)\n특징: OBV 상승 + 20일선 지지{ai}"

        # -----------------------------------------------------------
        # 🚀 [3] 급등 (이격도 무시) - 거래량 200% 터지면 그냥 잡음
        # (이건 힘이 너무 좋아서 이격도 무시하고 따라붙는 영역)
        # -----------------------------------------------------------
        elif (vol_ratio >= 2.0) and (pct >= 3.0) and (curr['Close'] > ma20.iloc[-1]):
             # 너무 높은 건 위험하니까 120%까지만 허용
             if curr_disp <= 120:
                ai = get_ai_summary(ticker, name, price_str, f"거래량폭발")
                return f"🚀 [급등] {name}\n등락: +{pct:.2f}%\n특징: 거래량 {int(vol_ratio*100)}% 폭발{ai}"

    except: return None
    return None

# ---------------------------------------------------------
# 🚨 비상용
# ---------------------------------------------------------
def get_fallback_stocks(target_dict):
    print("🚨 조건 만족 종목 없음 -> 단순 급등주 추출")
    results = []
    tickers = list(target_dict.keys())[:50] 
    for t in tickers:
        try:
            df = fdr.DataReader(t, start=(NOW - timedelta(days=5)).strftime('%Y-%m-%d'))
            pct = df.iloc[-1]['Change'] * 100
            if pct > 4.0:
                name = target_dict[t]
                results.append(f"🔥 [단순급등] {name} (+{pct:.2f}%)")
        except: pass
    return results

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [시스템 가동] 이격도 필터(110%) 적용")
    send_telegram(f"🚀 [전략 업데이트] '이미 오른 놈'은 버리고 '이제 시작하는 놈(초입)'만 잡습니다!\n(기준: 이격도 110% 이하)")

    target_dict = get_market_leaders()
    target_tickers = list(target_dict.keys())

    print(f"⚡ {len(target_tickers)}개 종목 분석 중...")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        futures = {executor.submit(analyze_stock, t, target_dict[t]): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)

    if not results:
        results = get_fallback_stocks(target_dict)

    header = f"🤖 [AI 스마트 리포트] {TODAY_STR}\n(추세 초입 발굴 / 이격도 필터)\n"
    
    if results:
        # 우선순위: 추세초입(🦁) > 매집(🕵️) > 급등(🚀)
        def sort_priority(msg):
            if "🦁" in msg: return 1 # 오늘은 '초입'이 주인공
            if "🕵️" in msg: return 2
            return 3
        results.sort(key=sort_priority)
        
        final_list = results[:30]
        msg = header + "\n" + "\n\n".join(final_list)
        
        if len(results) > 30: msg += f"\n\n🔥 ...외 {len(results)-30}개 더 있음"
        
        if len(msg) > 4000:
            send_telegram(msg[:4000])
            send_telegram(msg[4000:])
        else:
            send_telegram(msg)
    else:
        send_telegram("💤 조건에 맞는 종목이 없습니다.")
