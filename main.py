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
# 🧠 [핵심] AI 검증 및 정밀 분석 함수 (에러 수정됨)
# ---------------------------------------------------------
def get_ai_verification(ticker, name, price, strategy, technical_data):
    if not GEMINI_API_KEY or not model: return "\n(AI 분석 불가)"
    try:
        # ⚠️ 수정: 따옴표 에러 방지를 위해 안전한 방식(textwrap) 사용 안 함
        # f-string의 삼중 따옴표(""")를 정확히 사용
        prompt = f"""
        역할: 냉철한 주식 펀드매니저
        종목: {name} ({ticker})
        현재가: {price}원
        전략: {strategy}
        
        [데이터]
        {technical_data}

        위 데이터를 바탕으로 정밀 검증 리포트를 작성하라.
        반드시 아래 3가지 항목만 짧게 출력할 것.
        
        1. 📊 검증 점수: (0~100점)
        2. 💡 핵심 이유: (1줄 요약)
        3. 🎯 대응 전략: (손절가 제시)
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
# 🔍 [종목 분석] 필터링 -> AI 검증
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=365)))
        if len(df) < 120: return None
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 기본 거래대금 필터 (10억)
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

        pct = curr['Change'] * 100
        name = NAME_MAP.get(ticker, ticker)
        price = format(int(curr['Close']),',')
        change_str = f"{pct:.2f}%"

        # AI에게 넘길 기술적 데이터 요약
        tech_data = f"등락:{pct:.2f}%, 현재가:{price}, 거래량:{int(curr['Volume']/prev['Volume']*100)}%, RSI:{rsi.iloc[-1]:.2f}, 이평선(5/20/60):{int(ma5.iloc[-1])}/{int(ma20.iloc[-1])}/{int(ma60.iloc[-1])}"

        # -----------------------------------------------------
        # 1. 🎣 [바닥] Bottom
        # -----------------------------------------------------
        if (curr['Close'] < ma60.iloc[-1]) and \
           (rsi.iloc[-1] <= 45) and \
           (curr['Close'] > ma5.iloc[-1]):
            
            # AI 검증 요청
            ai_report = get_ai_verification(ticker, name, price, "낙폭과대 바닥 반등", tech_data)
            return f"🎣 [바닥] {name}\n등락: {change_str} / RSI: {int(rsi.iloc[-1])}\n{ai_report}"

        # -----------------------------------------------------
        # 2. 🕵️ [잠입] Stealth
        # -----------------------------------------------------
        elif (curr['Close'] > ma20.iloc[-1]) and \
             (pct < 3.0 and pct > -2.0) and \
             (rsi.iloc[-1] <= 60):
             
            ai_report = get_ai_verification(ticker, name, price, "이평선밀집 매집(횡보)", tech_data)
            return f"🕵️ [잠입] {name}\n등락: {change_str} / 20일선 안착\n{ai_report}"

        # -----------------------------------------------------
        # 3. 🚀 [추세] Trend (선생님 조건)
        # -----------------------------------------------------
        else:
            is_trend = False
            # 조건: 5% 상승 & 거래량 2배 & 정배열 & 스토캐스틱
            if (pct >= 5.0) and (curr['Volume'] >= prev['Volume'] * 2.0):
                if (ma5.iloc[-1] > ma20.iloc[-1]) and (curr['Close'] > ma5.iloc[-1]):
                    k, d = get_stochastic(df)
                    if k.iloc[-1] > d.iloc[-1]:
                        is_trend = True
            
            if is_trend:
                ai_report = get_ai_verification(ticker, name, price, "거래량폭발 급등추세", tech_data)
                return f"🚀 [추세] {name}\n등락: {change_str} / 거래량 2배↑\n{ai_report}"

    except: return None
    return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 [3단 기어 + AI 정밀검증] 시스템 재가동...")
    
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
    header = f"🤖 [AI 정밀 분석 리포트] {today}\n(검증 점수 및 대응 전략 포함)\n"
    
    if results:
        # 정렬: 추세 -> 잠입 -> 바닥
        def sort_priority(msg):
            if "🚀" in msg: return 1
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
