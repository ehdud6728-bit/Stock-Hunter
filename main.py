import FinanceDataReader as fdr
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta
import concurrent.futures
import pytz
import numpy as np

# ---------------------------------------------------------
# 🌍 설정
# ---------------------------------------------------------
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '').strip() 

# 📊 전역 변수
FUNDAMENTALS = {} 

# ---------------------------------------------------------
# 📨 텔레그램
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
    for chat_id in CHAT_ID_LIST:
        if chat_id.strip():
            for chunk in chunks:
                try: 
                    requests.post(url, data={'chat_id': chat_id, 'text': chunk})
                    time.sleep(0.5) 
                except: pass

# ---------------------------------------------------------
# 🤖 AI 코멘트
# ---------------------------------------------------------
def get_ai_summary(ticker, name, score, reason):
    if not GROQ_API_KEY: return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    prompt = f"종목: {name}\n점수: {score}\n특징: {reason}\n이 종목의 '숨고르기(눌림목)' 패턴에 대해 1줄로 조언해줘."
    payload = {
        "model": "llama-3.3-70b-versatile", 
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    }
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=5)
        return "\n💡 AI: " + res.json()['choices'][0]['message']['content'].strip()
    except: return ""

# ---------------------------------------------------------
# ⚡ 데이터 수집
# ---------------------------------------------------------
def get_market_data():
    print("⚡ 시장 데이터 수집 중...")
    try:
        df_krx = fdr.StockListing('KRX')
        global FUNDAMENTALS
        FUNDAMENTALS = df_krx.set_index('Code')[['Name', 'PER', 'PBR', 'Amount']].to_dict('index')
        # 거래대금 상위 1000개
        df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(1000)
        return dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    except: return {}

# ---------------------------------------------------------
# 💯 점수 계산 (숨고르기 로직 추가)
# ---------------------------------------------------------
def calculate_score(ticker, pct, vol_ratio, disparity, obv_rising, is_flag_pattern):
    score = 0
    reasons = []
    
    # 1. 재무 (30점)
    info = FUNDAMENTALS.get(ticker, {})
    if 0 < info.get('PBR', 0) < 1.0: score += 10; reasons.append("저PBR")
    if 0 < info.get('PER', 0) < 10: score += 10; reasons.append("저PER")
    score += 10

    # 2. 기술적 (40점)
    if is_flag_pattern: # ⭐ 숨고르기 패턴 발견 시 가산점 폭발
        score += 30
        reasons.append("🚩숨고르기(단봉)")
    elif vol_ratio >= 2.0: 
        score += 15
        reasons.append("거래량폭발")
    
    if obv_rising: score += 10; reasons.append("OBV상승")

    # 3. 타이밍 (30점)
    if 100 <= disparity <= 105: score += 20; reasons.append("이격도좁음")
    elif 105 < disparity <= 110: score += 10
    
    return score, ", ".join(reasons)

# ---------------------------------------------------------
# 🔍 정밀 분석
# ---------------------------------------------------------
def analyze_stock(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=200)).strftime('%Y-%m-%d'))
        if len(df) < 60: return None
        
        curr = df.iloc[-1]   # 오늘
        prev = df.iloc[-2]   # 어제 (D-1)
        prev2 = df.iloc[-3]  # 그제 (D-2) -> 가끔 어제가 아니라 그제 장대양봉일수도 있으니
        
        if curr['Close'] < 1000: return None
        
        # 지표
        ma5 = df['Close'].rolling(5).mean()
        ma20 = df['Close'].rolling(20).mean()
        disparity = (curr['Close'] / ma20.iloc[-1]) * 100
        
        # OBV
        direction = df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        obv = (direction * df['Volume']).cumsum()
        obv_rising = obv.iloc[-1] > obv.iloc[-2]

        pct = curr['Change'] * 100
        vol_ratio = curr['Volume'] / prev['Volume'] if prev['Volume'] > 0 else 0
        
        # -----------------------------------------------------------
        # ⭐ [NEW] 숨고르기(Flag) 패턴 감지 로직
        # -----------------------------------------------------------
        is_flag = False
        
        # 조건 1. 어제(prev) 장대양봉인가? (10% 이상 상승 + 거래량 빵빵)
        prev_is_long = (prev['Change'] >= 0.10) 
        
        # 조건 2. 오늘(curr) 거래량이 죽었는가? (어제의 70% 이하)
        curr_vol_drop = (curr['Volume'] < prev['Volume'] * 0.7)
        
        # 조건 3. 오늘 몸통이 짧은가? (등락률 -3% ~ +3% 사이)
        curr_is_short = (-3.0 <= pct <= 3.0)
        
        # 조건 4. 가격이 어제 종가 근처에서 버티는가? (5일선 위)
        curr_support = (curr['Close'] > ma5.iloc[-1])

        if prev_is_long and curr_vol_drop and curr_is_short and curr_support:
            is_flag = True

        # -----------------------------------------------------------
        # 전략 분류
        # -----------------------------------------------------------
        strategy = ""
        if is_flag: strategy = "🏳️ 숨고르기 (강력추천)"  # 이게 1순위
        elif (vol_ratio >= 2.0) and (pct >= 3.0): strategy = "🚀 급등"
        elif (ma5.iloc[-1] > ma20.iloc[-1]) and obv_rising: strategy = "🦁 추세"
        elif (-3.0 < pct < 1.0) and obv_rising: strategy = "🕵️ 잠입"
        
        if strategy:
            score, reason = calculate_score(ticker, pct, vol_ratio, disparity, obv_rising, is_flag)
            
            if score < 60: return None
            
            rank = "🥉"
            if score >= 80: rank = "🏆 SS급"
            elif score >= 70: rank = "🥇 S급"
            elif score >= 65: rank = "🥈 A급"

            ai_comment = ""
            if score >= 70: ai_comment = get_ai_summary(ticker, name, score, reason)

            amt_billion = int(FUNDAMENTALS.get(ticker, {}).get('Amount', 0) / 100000000)
            price_str = format(int(curr['Close']),',')

            return {
                "score": score,
                "msg": f"{rank} {name} ({score}점)\n"
                       f"💵 현재가: {price_str}원 ({pct:+.2f}%)\n"
                       f"💰 거래대금: {amt_billion}억\n"
                       f"📊 특징: {reason}\n"
                       f"👉 패턴: {strategy}{ai_comment}"
            }
    except: return None
    return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [시스템 가동] '장대양봉 후 단봉(숨고르기)' 추적 가동")
    send_telegram(f"🚀 [전략 추가] 어제 급등하고 오늘 쉬어가는 '숨고르기(Flag)' 종목을 1순위로 찾습니다!")

    target_dict = get_market_data()
    target_tickers = list(target_dict.keys())
    print(f"⚡ {len(target_tickers)}개 종목 분석 중...")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        futures = {executor.submit(analyze_stock, t, target_dict[t]): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)

    if results:
        results.sort(key=lambda x: x['score'], reverse=True)
        final_msgs = [r['msg'] for r in results]
        full_text = f"🤖 [오늘의 추천주 점수표] {TODAY_STR}\n(총 {len(results)}개 포착)\n\n" + "\n\n".join(final_msgs)
        send_telegram(full_text)
    else:
        send_telegram("💤 조건에 맞는 종목이 없습니다.")