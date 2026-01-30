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
        prompt = f"한국 주식 {name}({ticker}) {status} 상태. 현재 {price}원. 투자포인트 1줄 요약."
        response = model.generate_content(prompt)
        time.sleep(1)
        return "\n" + response.text.strip()
    except: return ""

# ---------------------------------------------------------
# 🕵️‍♂️ [핵심] 네이버 '순매수 상위' 랭킹 훔쳐오기
# ---------------------------------------------------------
def get_top_buyer_stocks():
    print("⚡ 네이버 금융 '수급 랭킹' 데이터 가져오는 중...")
    
    # 네이버 금융: 투자자별 매매동향 상위 (기관/외국인)
    # sosok=0 (코스피), sosok=1 (코스닥)
    urls = [
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=0&investor_gubun=1000", # 코스피 기관
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=0&investor_gubun=9000", # 코스피 외국인
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=1&investor_gubun=1000", # 코스닥 기관
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=1&investor_gubun=9000"  # 코스닥 외국인
    ]
    
    # 봇 차단 방지용 헤더 (나는 크롬 브라우저다!)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    found_tickers = set() # 중복 제거용 집합
    
    for url in urls:
        try:
            res = requests.get(url, headers=headers)
            # 인코딩 문제 해결 (네이버는 옛날 방식인 EUC-KR을 씀)
            res.encoding = 'EUC-KR'
            
            # 테이블 읽기 (pd.read_html)
            dfs = pd.read_html(StringIO(res.text))
            
            # 보통 랭킹 테이블은 2번째나 3번째에 있음
            for df in dfs:
                # '종목명'이라는 컬럼이 있는 테이블만 찾음
                if '종목명' in df.columns:
                    # 종목명이 있는 행만 남기기
                    valid_names = df['종목명'].dropna().tolist()
                    
                    # 이름을 코드로 변환 (NAME_MAP 역이용)
                    # (이름 -> 코드 찾기가 느리므로, 미리 뒤집어둔 맵 필요)
                    # 여기서는 그냥 KRX 리스트에서 찾음
                    for name in valid_names:
                        # 종목명으로 코드 찾기
                        code_match = krx[krx['Name'] == name]['Code']
                        if not code_match.empty:
                            found_tickers.add(str(code_match.values[0]))
        except Exception as e:
            print(f"⚠️ 랭킹 크롤링 중 에러 (무시하고 진행): {e}")
            continue
            
    # set을 리스트로 변환
    result_list = list(found_tickers)
    print(f"✅ 수급 주도주 {len(result_list)}개 확보 완료!")
    return result_list

# ---------------------------------------------------------
# 🔍 종목 분석 (차트 조건)
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=365)))
        if len(df) < 60: return None
        curr = df.iloc[-1]
        
        # 거래대금 50억 이상 (수급주니까 거래량은 좀 관대하게)
        if (curr['Close'] * curr['Volume']) < 5000000000: return None

        ma5 = df['Close'].rolling(5).mean()
        ma20 = df['Close'].rolling(20).mean()
        ma60 = df['Close'].rolling(60).mean()
        
        # RSI
        delta = df['Close'].diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + (gain / loss)))

        # 전략 A: 수급 붙은 정배열 추세
        cond_A = (curr['Close'] > ma5.iloc[-1]) and \
                 (ma5.iloc[-1] > ma20.iloc[-1]) and \
                 (rsi.iloc[-1] >= 50) and \
                 (curr['Close'] > df.iloc[-2]['Close']) # 오늘 양봉

        # 전략 B: 수급 들어온 바닥 반등
        cond_B = (curr['Close'] < ma60.iloc[-1]) and \
                 (rsi.iloc[-1] <= 45) and \
                 (curr['Close'] > ma5.iloc[-1])

        name = NAME_MAP.get(ticker, ticker)
        price = format(int(curr['Close']),',')
        
        if cond_A:
            ai = ask_gemini_analyst(ticker, name, price, "쌍끌이매수/상승추세")
            return f"🦁 [수급+추세] {name}\n{price}원{ai}"
        elif cond_B:
            ai = ask_gemini_analyst(ticker, name, price, "기관매집/바닥반등")
            return f"🎣 [수급+바닥] {name}\n{price}원{ai}"
            
    except: return None
    return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 네이버 수급 랭킹 기반 분석 시작...")
    
    # 1. 랭킹 페이지에서 종목 긁어오기 (Request 4번이면 끝)
    target_tickers = get_top_buyer_stocks()
    
    if not target_tickers:
        print("❌ 수급 종목을 못 가져왔습니다. (네이버 접속 실패)")
        # 실패 시 비상용으로 시총 상위 50개만 분석
        target_tickers = krx.sort_values(by='Marcap', ascending=False).head(50)['Code'].astype(str).tolist()
        print("⚠️ 비상 모드: 시총 상위 50개로 대체합니다.")

    print(f"⚡ 엄선된 수급주 {len(target_tickers)}개 정밀 분석 (Thread: 10)")
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_stock, t): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)

    today = datetime.now().strftime('%m/%d')
    header = f"🤖 [AI 수급 리포트] {today}\n(네이버 기관/외인 순매수 상위)\n"
    msg = header + "\n" + "\n\n".join(results) if results else header + "\n조건 만족 종목 없음"

    if len(msg) > 4000:
        send_telegram(msg[:4000])
        send_telegram(msg[4000:])
    else:
        send_telegram(msg)
