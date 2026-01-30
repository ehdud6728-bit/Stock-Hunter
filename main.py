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

# --- [AI 설정] ---
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
    except: model = None

# --- [이름표 준비] ---
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
# 🤖 AI 애널리스트
# ---------------------------------------------------------
def ask_gemini_analyst(ticker, name, price, status):
    if not GEMINI_API_KEY or not model: return ""
    try:
        prompt = f"한국 주식 {name}({ticker})이 '{status}' 상태로 포착됨. 현재 {price}원. 1줄 코멘트."
        response = model.generate_content(prompt)
        time.sleep(1)
        return "\n" + response.text.strip()
    except: return ""

# ---------------------------------------------------------
# ⚡ 네이버 수급 랭킹 가져오기
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
# 🔍 [핵심] 하이브리드 분석기 (잠입 OR 급등)
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=365)))
        if len(df) < 60: return None
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 기본 필터: 거래대금 10억 이상 (너무 죽은 건 패스)
        if (curr['Close'] * curr['Volume']) < 1000000000: return None

        # 지표 계산
        ma5 = df['Close'].rolling(5).mean()
        ma20 = df['Close'].rolling(20).mean()
        ma60 = df['Close'].rolling(60).mean()
        
        # RSI
        delta = df['Close'].diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + (gain / loss)))

        daily_change_pct = curr['Change'] * 100
        name = NAME_MAP.get(ticker, ticker)
        price = format(int(curr['Close']),',')
        change_str = f"{daily_change_pct:.2f}%"

        # -------------------------------------------------
        # 🕵️ 전략 1: 조용한 잠입 (Stealth)
        # -------------------------------------------------
        # 조건: 3% 미만 상승 & 정배열 초입 or 바닥권 & RSI 안정적
        is_stealth = False
        if daily_change_pct < 3.0 and daily_change_pct > -2.0: # 조용함
            if (curr['Close'] > ma20.iloc[-1]) and (rsi.iloc[-1] <= 60): # 정배열 매집
                is_stealth = True
            elif (curr['Close'] < ma60.iloc[-1]) and (rsi.iloc[-1] <= 40): # 바닥권 줍줍
                is_stealth = True
        
        if is_stealth:
            ai = ask_gemini_analyst(ticker, name, price, "수급유입/주가횡보")
            return f"🕵️ [잠입] {name}\n등락: {change_str} / 가: {price}원{ai}"

        # -------------------------------------------------
        # 🚀 전략 2: 화끈한 급등 (Rocket)
        # -------------------------------------------------
        # 조건: 5% 이상 상승 & 거래량 폭발 & 신고가 or 정배열 돌파
        is_rocket = False
        if daily_change_pct >= 5.0: # 화끈함
            # 거래량이 전일 대비 150% 이상 터졌거나, RSI가 강세(60이상)일 때
            if (curr['Volume'] >= prev['Volume'] * 1.5) or (rsi.iloc[-1] >= 60):
                is_rocket = True
        
        if is_rocket:
            ai = ask_gemini_analyst(ticker, name, price, "거래량폭발/급등")
            return f"🚀 [급등] {name}\n등락: {change_str} / 가: {price}원{ai}"

    except: return None
    return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 하이브리드(잠입+급등) 탐색 시작...")
    
    target_tickers = get_top_buyer_stocks()
    
    if not target_tickers:
        print("❌ 수급 데이터 확보 실패. 시총 상위로 대체.")
        target_tickers = krx.sort_values(by='Marcap', ascending=False).head(100)['Code'].astype(str).tolist()

    print(f"⚡ 수급주 {len(target_tickers)}개 정밀 분석 (Thread: 10)")
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_stock, t): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)

    today = datetime.now().strftime('%m/%d')
    header = f"🤖 [AI 수급 리포트] {today}\n(🕵️잠입 vs 🚀급등)\n"
    msg = header + "\n" + "\n\n".join(results) if results else header + "\n조건 만족 종목 없음"

    if len(msg) > 4000:
        send_telegram(msg[:4000])
        send_telegram(msg[4000:])
    else:
        send_telegram(msg)
