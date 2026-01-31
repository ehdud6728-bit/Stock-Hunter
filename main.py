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
        # ⚠️ [수정] 최신형 모델 (은퇴한 모델 X)
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
                            found_tickers.add(str(code_match.values[0]))
        except: continue
    return list(found_tickers)

# ---------------------------------------------------------
# 🧮 스토캐스틱
# ---------------------------------------------------------
def get_stochastic(df, n=5, k=3, d=3):
    high = df['High'].rolling(window=n).max()
    low = df['Low'].rolling(window=n).min()
    fast_k = ((df['Close'] - low) / (high - low)) * 100
    slow_k = fast_k.rolling(window=k).mean()
    slow_d = slow_k.rolling(window=d).mean()
    return slow_k, slow_d

# ---------------------------------------------------------
# 🔍 3단 필터
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=365)).strftime('%Y-%m-%d'))
        if len(df) < 120: return None
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
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
    print(f"🚀 [Groq] 시스템 재가동 (모델 업데이트 완료)")
    send_telegram(f"🚀 [시스템 가동] AI 모델 최신화(Llama3.3) 완료!\n(시간: {NOW.strftime('%H:%M:%S')})")

    market_msg = "분석 중..."
    try:
        kospi = fdr.DataReader('KS11', start=(NOW - timedelta(days=60)).strftime('%Y-%m-%d'))
        curr_k = kospi['Close'].iloc[-1]
        ma20_k = kospi['Close'].rolling(20).mean().iloc[-1]
        market_msg = "📈 상승장" if curr_k > ma20_k else "📉 조정장"
    except: pass

    target_tickers = get_top_buyer_stocks()
    if not target_tickers:
        print("⚠️ 수급 데이터 실패 -> 시총 상위 대체")
        target_tickers = krx.sort_values(by='Marcap', ascending=False).head(100)['Code'].astype(str).tolist()

    print(f"⚡ {len(target_tickers)}개 종목 분석 중...")
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_stock, t): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)

    header = f"🤖 [AI 스마트 리포트] {TODAY_STR}\n시장: {market_msg}\n"
    
    if results:
        def sort_priority(msg):
            if "🦁" in msg: return 1
            if "🕵️" in msg: return 2
            return 3
        results.sort(key=sort_priority)
        msg = header + "\n" + "\n\n".join(results)
    else:
        msg = header + "\n조건 만족 종목 없음"

    if len(msg) > 4000:
        send_telegram(msg[:4000])
        send_telegram(msg[4000:])
    else:
        send_telegram(msg)
