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
        prompt = f"한국 주식 {name}({ticker}) {status} 상태. 현재 {price}원. 매집 의심 이유 1줄 요약."
        response = model.generate_content(prompt)
        time.sleep(1)
        return "\n" + response.text.strip()
    except: return ""

# ---------------------------------------------------------
# 🕵️‍♂️ 네이버 수급 랭킹 '은밀하게' 훔쳐오기
# ---------------------------------------------------------
def get_top_buyer_stocks():
    print("⚡ 네이버 금융 '수급 랭킹' 스캔 중...")
    
    # 코스피/코스닥 + 기관/외국인 순매수 상위
    urls = [
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=0&investor_gubun=1000", # 코스피 기관
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=0&investor_gubun=9000", # 코스피 외인
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=1&investor_gubun=1000", # 코스닥 기관
        "https://finance.naver.com/sise/sise_deal_rank.naver?sosok=1&investor_gubun=9000"  # 코스닥 외인
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
            
    result_list = list(found_tickers)
    print(f"✅ 수급 포착 종목 {len(result_list)}개 확보")
    return result_list

# ---------------------------------------------------------
# 🔍 [핵심] 잠입 매집주 판독기 (Stealth Filter)
# ---------------------------------------------------------
def analyze_stock(ticker):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=365)))
        if len(df) < 60: return None
        curr = df.iloc[-1]
        
        # 1. 🤫 [스텔스 필터] 오늘 급등한 건 버린다!
        # 등락률이 3% 이상이면 이미 들킨 종목 -> 탈락
        # -2% ~ +3% 사이인 '조용한' 종목만 통과
        daily_change_pct = curr['Change'] * 100
        if daily_change_pct > 3.0 or daily_change_pct < -2.0:
            return None

        # 2. 거래대금 최소 컷 (그래도 10억은 터져야 함, 너무 죽은 종목 제외)
        if (curr['Close'] * curr['Volume']) < 1000000000: return None

        ma5 = df['Close'].rolling(5).mean()
        ma20 = df['Close'].rolling(20).mean()
        
        # RSI (과열 여부 체크)
        delta = df['Close'].diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + (gain / loss)))

        # 전략: "수급은 들어왔는데(리스트 포함), 차트는 바닥이거나 정배열 초입"
        
        # Case A: 정배열 초입 매집 (20일선 지지)
        cond_A = (curr['Close'] > ma20.iloc[-1]) and \
                 (ma5.iloc[-1] > ma20.iloc[-1]) and \
                 (rsi.iloc[-1] <= 60) # RSI가 너무 높지 않아야 함 (아직 안 터짐)

        # Case B: 바닥권 매집 (20일선 아래서 꿈틀)
        cond_B = (curr['Close'] < ma20.iloc[-1]) and \
                 (curr['Close'] > ma5.iloc[-1]) and \
                 (rsi.iloc[-1] <= 45) # 바닥권

        name = NAME_MAP.get(ticker, ticker)
        price = format(int(curr['Close']),',')
        change_str = f"{daily_change_pct:.2f}%"
        
        if cond_A:
            ai = ask_gemini_analyst(ticker, name, price, "수급유입/주가횡보")
            return f"🕵️ [잠입매집] {name}\n등락: {change_str} / 가: {price}원{ai}"
        elif cond_B:
            ai = ask_gemini_analyst(ticker, name, price, "바닥매집/저점다지기")
            return f"🛒 [바닥줍줍] {name}\n등락: {change_str} / 가: {price}원{ai}"
            
    except: return None
    return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 '잠입 매집주(Stealth)' 탐색 시작...")
    
    # 1. 수급 상위 긁어오기
    target_tickers = get_top_buyer_stocks()
    
    if not target_tickers:
        print("❌ 수급 데이터를 못 가져왔습니다. 비상 모드 가동.")
        target_tickers = krx.sort_values(by='Marcap', ascending=False).head(50)['Code'].astype(str).tolist()

    print(f"⚡ 후보군 {len(target_tickers)}개 중 '안 오른' 종목 선별 (Thread: 10)")
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_stock, t): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)

    today = datetime.now().strftime('%m/%d')
    header = f"🤖 [AI 스텔스 리포트] {today}\n(수급상위 + 3%미만 상승)\n"
    msg = header + "\n" + "\n\n".join(results) if results else header + "\n오늘은 살금살금 사는 종목이 없네요."

    if len(msg) > 4000:
        send_telegram(msg[:4000])
        send_telegram(msg[4000:])
    else:
        send_telegram(msg)
