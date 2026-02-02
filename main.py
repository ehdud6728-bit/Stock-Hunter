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
# 🤖 AI 요약 (Groq Llama 3.3)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, price, strategy):
    if not GROQ_API_KEY: return ""

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    
    prompt = f"""
    종목: {name} ({ticker})
    현재가: {price}원
    포착전략: {strategy}
    
    위 종목을 'OBV(거래량 매집)'와 '기술적 위치' 관점에서 분석해.
    반드시 아래 두 줄 양식으로 요약해.
    
    👍 핵심: (매집 여부, 상승 여력)
    ⚠️ 주의: (매물대 저항, 손절가)
    """

    payload = {
        "model": "llama-3.3-70b-versatile", 
        "messages": [
            {"role": "system", "content": "너는 주식 차트 분석가야. 한국어로 짧고 명확하게 답해."},
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
# ⚡ [광대역 스캔] 거래대금 상위 1000개
# ---------------------------------------------------------
def get_market_leaders():
    print("⚡ 시장 데이터 수집 중... (Top 1,000)")
    try:
        df_krx = fdr.StockListing('KRX')
        df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(1000)
        target_dict = dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
        return target_dict
    except Exception as e:
        print(f"❌ 목록 수집 실패: {e}")
        return {}

# ---------------------------------------------------------
# 🧮 보조지표 계산 (OBV 추가됨!)
# ---------------------------------------------------------
def get_indicators(df):
    # 1. 이동평균
    ma5 = df['Close'].rolling(5).mean()
    ma20 = df['Close'].rolling(20).mean()
    ma60 = df['Close'].rolling(60).mean()
    
    # 2. RSI
    delta = df['Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rsi = 100 - (100 / (1 + (gain / loss)))
    
    # 3. Stochastic (Slow)
    high = df['High'].rolling(9).max()
    low = df['Low'].rolling(9).min()
    fast_k = ((df['Close'] - low) / (high - low)) * 100
    slow_k = fast_k.rolling(3).mean()
    slow_d = slow_k.rolling(3).mean()

    # 4. 🌊 OBV (On-Balance Volume) 계산
    # (주가가 오르면 거래량을 더하고, 내리면 뺌)
    # -----------------------------------------------------
    change = df['Close'].diff()
    # 방향: 오르면 1, 내리면 -1, 같으면 0
    direction = change.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    # 누적 합계 (OBV)
    obv = (direction * df['Volume']).cumsum()
    # OBV 이동평균 (추세 확인용)
    obv_ma20 = obv.rolling(20).mean()
    
    return ma5, ma20, ma60, rsi, slow_k, slow_d, obv, obv_ma20

# ---------------------------------------------------------
# 🔍 3단 필터 (OBV 적용 완료)
# ---------------------------------------------------------
def analyze_stock(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=200)).strftime('%Y-%m-%d'))
        if len(df) < 60: return None
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        if curr['Close'] < 1000: return None
        
        # 지표 가져오기 (OBV 포함)
        ma5, ma20, ma60, rsi, k, d, obv, obv_ma = get_indicators(df)
        
        pct = curr['Change'] * 100
        vol_ratio = curr['Volume'] / prev['Volume'] if prev['Volume'] > 0 else 0
        price_str = format(int(curr['Close']),',')

        # -----------------------------------------------------------
        # 🦁 [1] 추세 (Trend)
        # 조건: 정배열 + 거래량 1.5배 + ⭐OBV가 이평선 위에 있음 (힘이 좋음)
        # -----------------------------------------------------------
        if (ma5.iloc[-1] > ma20.iloc[-1]) and (curr['Close'] > ma20.iloc[-1]):
            if (pct >= 2.0) and (vol_ratio >= 1.5):
                # OBV 확인: 거래량이 뒷받침되는 진짜 상승인가?
                if obv.iloc[-1] > obv_ma.iloc[-1]: 
                    ai = get_ai_summary(ticker, name, price_str, "정배열+OBV상승")
                    return f"🦁 [추세] {name}\n현재가: {price_str}원 (+{pct:.2f}%)\n특징: 거래량 실린 진짜 상승 (OBV 양호){ai}"

        # -----------------------------------------------------------
        # 🎣 [2] 바닥 (Bottom)
        # 조건: 역배열 과매도 + ⭐OBV가 주가보다 먼저 고개를 듦 (다이버전스)
        # -----------------------------------------------------------
        elif (curr['Close'] < ma60.iloc[-1]) and (curr['Close'] > ma5.iloc[-1]):
            if rsi.iloc[-1] <= 55:
                ai = get_ai_summary(ticker, name, price_str, "바닥 반등")
                return f"🎣 [바닥] {name}\n현재가: {price_str}원 (+{pct:.2f}%)\n특징: 과매도 구간 탈출 시도{ai}"

        # -----------------------------------------------------------
        # 🕵️ [3] 잠입 (Infiltration)
        # 조건: 눌림목 + ⭐주가는 빠져도 OBV는 안 빠짐 (매집 의심)
        # -----------------------------------------------------------
        elif (curr['Close'] > ma20.iloc[-1]) and (-3.0 < pct < 5.0):
            if vol_ratio < 1.0:
                # 주가는 20일선 근처인데, OBV는 20일 평균보다 위에 있다? => 누군가 꽉 쥐고 있음
                if (k.iloc[-1] <= 80) and (obv.iloc[-1] >= obv_ma.iloc[-1]):
                    ai = get_ai_summary(ticker, name, price_str, "눌림목 매집형")
                    return f"🕵️ [잠입] {name}\n현재가: {price_str}원 (+{pct:.2f}%)\n특징: 주가 눌려도 물량 안 나옴 (OBV 견고){ai}"

    except: return None
    return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [시스템 업그레이드] OBV 보조지표 장착 완료")
    send_telegram(f"🚀 [시스템 업데이트] 이제 'OBV(세력의 흔적)'까지 추적합니다!\n(대상: Top 1,000 / 시간: {NOW.strftime('%H:%M:%S')})")

    target_dict = get_market_leaders()
    target_tickers = list(target_dict.keys())

    print(f"⚡ {len(target_tickers)}개 종목 정밀 분석 중...")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        futures = {executor.submit(analyze_stock, t, target_dict[t]): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)

    header = f"🤖 [AI 스마트 리포트] {TODAY_STR}\n(OBV 매집 패턴 분석 적용)\n"
    
    if results:
        def sort_priority(msg):
            if "🦁" in msg: return 1
            if "🕵️" in msg: return 2
            return 3
        results.sort(key=sort_priority)
        
        final_list = results[:30]
        msg = header + "\n" + "\n\n".join(final_list)
        
        if len(results) > 30:
            msg += f"\n\n🔥 ...외 {len(results)-30}개 종목 더 있음"
    else:
        msg = header + "\n조건에 맞는 종목이 없습니다."

    if len(msg) > 4000:
        send_telegram(msg[:4000])
        send_telegram(msg[4000:])
    else:
        send_telegram(msg)