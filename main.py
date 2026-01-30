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
# 🤖 AI 요약 (호재/주의 포맷)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, price, strategy):
    if not GEMINI_API_KEY or not model: return ""
    try:
        # 선생님이 원하시는 포맷대로 요청
        prompt = f"""
        주식: {name} ({ticker})
        현재가: {price}원
        포착된이유: {strategy}
        
        위 종목에 대해 딱 2줄로 요약해.
        첫 줄은 '👍 호재:', 둘째 줄은 '⚠️ 주의:' 로 시작할 것.
        (잡담 금지, 오직 결과만 출력)
        """
        response = model.generate_content(prompt)
        time.sleep(1)
        return "\n" + response.text.strip()
    except: return "\n(AI 분석 불가)"

# ---------------------------------------------------------
# 📈 시장 상황 파악 (코스피)
# ---------------------------------------------------------
def get_market_status():
    try:
        kospi = fdr.DataReader('KS11', start=(datetime.now() - timedelta(days=60)))
        curr = kospi['Close'].iloc[-1]
        ma20 = kospi['Close'].rolling(20).mean().iloc[-1]
        
        if curr > ma20:
            return "📈 상승장 (20일선 위)"
        else:
            return "📉 조정장 (20일선 아래)"
    except:
        return "❓ 시장 데이터 확인 불가"

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
# 🔍 3단 필터 (바닥/잠입/추세)
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=365)))
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

        # -----------------------------------------------------
        # 1. 🎣 [바닥]
        # -----------------------------------------------------
        if (curr['Close'] < ma60.iloc[-1]) and \
           (rsi.iloc[-1] <= 45) and \
           (curr['Close'] > ma5.iloc[-1]):
            
            ai = get_ai_summary(ticker, name, price_str, "낙폭과대 바닥 반등")
            return f"🎣 [바닥] {name}\n가격: {price_str}원{ai}"

        # -----------------------------------------------------
        # 2. 🕵️ [잠입]
        # -----------------------------------------------------
        elif (curr['Close'] > ma20.iloc[-1]) and \
             (pct < 3.0 and pct > -2.0) and \
             (rsi.iloc[-1] <= 60):
             
            ai = get_ai_summary(ticker, name, price_str, "이평선밀집 매집")
            return f"🕵️ [잠입] {name}\n가격: {price_str}원{ai}"

        # -----------------------------------------------------
        # 3. 🦁 [추세] (선생님 오리지널)
        # -----------------------------------------------------
        else:
            is_trend = False
            if (pct >= 5.0) and (curr['Volume'] >= prev['Volume'] * 2.0):
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
    print("🚀 [최종 복구] 호재/주의 포맷으로 실행...")
    
    # 1. 시장 상황 체크
    market_msg = get_market_status()
    
    # 2. 종목 스캔
    target_tickers = get_top_buyer_stocks()
    if not target_tickers:
        print("❌ 수급 데이터 확보 실패. 시총 상위로 대체.")
        target_tickers = krx.sort_values(by='Marcap', ascending=False).head(100)['Code'].astype(str).tolist()

    print(f"⚡ 수급주 {len(target_tickers)}개 분석 중...")
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_stock, t): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)

    # 3. 결과 전송
    today = datetime.now().strftime('%m/%d')
    header = f"🤖 [AI 스마트 리포트] {today}\n시장: {market_msg}\n"
    
    if results:
        # 정렬: 추세 -> 잠입 -> 바닥
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
