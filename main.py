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
    prompt = f"종목: {name}\n점수: {score}\n특징: {reason}\n이 종목의 매수 타이밍을 1줄로 조언해줘."
    payload = {
        "model": "llama-3.3-70b-versatile", 
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    }
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=3)
        return "\n💡 " + res.json()['choices'][0]['message']['content'].strip()
    except: return ""

# ---------------------------------------------------------
# ⚡ 데이터 수집
# ---------------------------------------------------------
def get_market_data():
    print("⚡ 시장 데이터 수집 중...")
    try:
        df_krx = fdr.StockListing('KRX')
        global FUNDAMENTALS
        try:
            FUNDAMENTALS = df_krx.set_index('Code')[['Name', 'PER', 'PBR', 'Amount']].to_dict('index')
        except: FUNDAMENTALS = {}
        
        # 거래대금 상위 1000개
        df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(1000)
        return dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    except: return {}

# ---------------------------------------------------------
# 🏢 네이버 재무 크롤링
# ---------------------------------------------------------
def get_naver_financials(code):
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        dfs = pd.read_html(url, encoding='euc-kr', header=0)
        for df in dfs:
            if '최근 연간 실적' in str(df.columns) or '주요재무제표' in str(df.columns):
                if '주요재무제표' in df.columns[0]: df = df.set_index(df.columns[0])
                if '영업이익' in df.index:
                    op_profit = df.loc['영업이익']
                    valid_data = []
                    for val in op_profit.values:
                        try:
                            v = float(str(val).replace(',', '').strip())
                            if not np.isnan(v): valid_data.append(v)
                        except: pass
                    
                    if len(valid_data) >= 2:
                        last = valid_data[-1]
                        prev = valid_data[-2]
                        if prev < 0 and last > 0: return "🐢흑자전환"
                        if last > prev * 1.3: return "📈이익급증"
                        if last > prev: return "🔺이익증가"
                        if last < prev: return "📉이익감소"
        return "보통"
    except: return "확인불가"

# ---------------------------------------------------------
# ⚖️ 재무 등급 판독기
# ---------------------------------------------------------
def get_financial_badge(ticker):
    info = FUNDAMENTALS.get(ticker, {})
    per = info.get('PER', 0)
    pbr = info.get('PBR', 0)
    
    roe = 0
    if per > 0 and pbr > 0: roe = (pbr / per) * 100
        
    badge = "⚖️ 보통"
    if per <= 0: badge = "⚠️ 적자기업 (주의)"
    elif (0 < per < 10) and (pbr < 1.2): badge = "💎 저평가 우량주"
    elif (roe > 15): badge = "💰 고수익 성장주"
    elif (pbr < 0.6): badge = "🧱 헐값 자산주"
        
    return badge, roe

# ---------------------------------------------------------
# 🧮 [6대 보조지표]
# ---------------------------------------------------------
def get_indicators(df):
    ma5 = df['Close'].rolling(5).mean()
    ma20 = df['Close'].rolling(20).mean()
    ma60 = df['Close'].rolling(60).mean()
    disparity = (df['Close'] / ma20) * 100
    
    delta = df['Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rsi = 100 - (100 / (1 + (gain / loss)))

    high = df['High'].rolling(9).max()
    low = df['Low'].rolling(9).min()
    fast_k = ((df['Close'] - low) / (high - low)) * 100
    slow_k = fast_k.rolling(3).mean()
    slow_d = slow_k.rolling(3).mean()

    direction = df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    obv = (direction * df['Volume']).cumsum()
    obv_rising = obv.iloc[-1] > obv.iloc[-2]
    
    return ma5, ma20, ma60, disparity, rsi, slow_k, slow_d, obv_rising

# ---------------------------------------------------------
# ⚔️ [NEW] 3대 공통 필터 (여기 통과 못하면 무조건 탈락)
# ---------------------------------------------------------
def check_common_conditions(obv_rising, rsi, k, d):
    # 1. OBV: 돈이 들어오고 있는가? (상승)
    if not obv_rising: return False
    
    # 2. RSI: 너무 과열되거나(75이상) 너무 죽어있는가(30이하)?
    # -> 30 ~ 75 사이가 매매하기 제일 좋은 구간
    if not (30 <= rsi <= 75): return False
    
    # 3. 스토캐스틱: 골든크로스(K>D) 상태인가?
    if k < d: return False
    
    return True

# ---------------------------------------------------------
# 💯 점수 계산
# ---------------------------------------------------------
def calculate_score(ticker, pct, vol_ratio, disparity, is_flag, badge):
    score = 50 # 기본점수 상향 (공통필터 통과했으므로)
    reasons = []
    
    # 재무 가산점
    if "💎" in badge: score += 10; reasons.append("재무우수")
    if "💰" in badge: score += 10; reasons.append("고수익")
    if "⚠️" in badge: score -= 10 # 적자는 감점

    # 패턴 점수
    if is_flag: 
        score += 30; reasons.append("🚩숨고르기(강력)")
    elif vol_ratio >= 1.5: 
        score += 15; reasons.append("수급폭발")
    
    # 타이밍 점수
    if 100 <= disparity <= 105: score += 20; reasons.append("이격도최상")
    elif disparity <= 110: score += 10; reasons.append("이격도양호")
    
    return score, ", ".join(reasons)

# ---------------------------------------------------------
# 🔍 통합 분석
# ---------------------------------------------------------
def analyze_stock(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=200)).strftime('%Y-%m-%d'))
        if len(df) < 60: return None
        curr = df.iloc[-1]   
        prev = df.iloc[-2]   
        if curr['Close'] < 1000: return None
        
        # 지표 계산
        ma5, ma20, ma60, disparity, rsi, k, d, obv_rising = get_indicators(df)
        pct = curr['Change'] * 100
        vol_ratio = curr['Volume'] / prev['Volume'] if prev['Volume'] > 0 else 0
        
        curr_rsi = rsi.iloc[-1]
        curr_k = k.iloc[-1]
        curr_d = d.iloc[-1]
        curr_disp = disparity.iloc[-1]

        # -------------------------------------------------------
        # 🛑 [1차 관문] 3대 공통 필터 (OBV, RSI, Stoch)
        # -------------------------------------------------------
        # 하나라도 기준 미달이면 바로 탈락 (단, 숨고르기는 예외적으로 Stoch 무시 가능)
        pass_common = check_common_conditions(obv_rising, curr_rsi, curr_k, curr_d)
        
        # -------------------------------------------------------
        # 🏳️ 전략 1: 숨고르기 (Flag) - 공통필터 일부 예외 허용
        # -------------------------------------------------------
        # 조건: 10%장대양봉 -> 거래량 50%미만 급감 -> 주가 -2%~+2%
        is_flag = False
        strategy = ""
        
        if (prev['Change'] >= 0.10) and (curr['Volume'] < prev['Volume'] * 0.5) and (-2.0 <= pct <= 2.0):
            # 숨고르기 때는 스토캐스틱이 살짝 꺾일 수 있어서 OBV/RSI만 체크
            if obv_rising and (30 <= curr_rsi <= 75):
                is_flag = True
                strategy = "🏳️ 숨고르기"

        # -------------------------------------------------------
        # 🦁 전략 2: 상승 초입 (통합됨: 추세/잠입/바닥)
        # -------------------------------------------------------
        # 공통 필터를 반드시 통과해야 함
        elif pass_common and (curr_disp <= 110):
            # 세부 스타일 분류 (리포트용)
            if (vol_ratio >= 1.5) and (pct >= 1.0):
                strategy = "🦁 상승초입 (돌파형)" # 거래량 실린 상승
            elif (-3.0 <= pct <= 1.0) and (curr_disp <= 105):
                strategy = "🦁 상승초입 (눌림목)" # 살짝 눌렸는데 지표가 좋음
            elif (curr['Close'] < ma60.iloc[-1]) and (curr['Close'] > ma5.iloc[-1]):
                strategy = "🦁 상승초입 (바닥턴)" # 바닥에서 고개 듬

        if strategy:
            badge, roe = get_financial_badge(ticker)
            score, reason = calculate_score(ticker, pct, vol_ratio, curr_disp, is_flag, badge)
            
            # 커트라인 60점 (공통필터가 빡빡해서 점수는 좀 높게 잡음)
            if score < 60: return None
            
            # 네이버 재무
            fin_trend = get_naver_financials(ticker)

            rank = "🥉 B급"
            if score >= 90: rank = "🏆 SS급"
            elif score >= 80: rank = "🥇 S급"
            elif score >= 70: rank = "🥈 A급"

            ai_comment = ""
            if score >= 70: ai_comment = get_ai_summary(ticker, name, score, reason)

            amt_billion = int(FUNDAMENTALS.get(ticker, {}).get('Amount', 0) / 100000000)
            price_str = format(int(curr['Close']),',')

            return {
                "score": score,
                "msg": f"{rank} {name} ({score}점)\n"
                       f"💵 {price_str}원 ({pct:+.2f}%)\n"
                       f"🏢 재무: {badge} (ROE {roe:.1f}%)\n"
                       f"📈 실적: {fin_trend} (영업이익)\n"
                       f"📊 특징: {reason}\n"
                       f"👉 패턴: {strategy}{ai_comment}"
            }
    except: return None
    return None

# ---------------------------------------------------------
# 🚨 비상용
# ---------------------------------------------------------
def get_fallback_stocks(target_dict):
    print("🚨 [비상] 결과 없음 -> 단순 상승주 추출")
    results = []
    top_tickers = list(target_dict.keys())[:50]
    for t in top_tickers:
        try:
            df = fdr.DataReader(t, start=(NOW - timedelta(days=5)).strftime('%Y-%m-%d'))
            curr = df.iloc[-1]
            pct = curr['Change'] * 100
            if pct > 0:
                name = target_dict[t]
                price_str = format(int(curr['Close']),',')
                msg = f"🆘 [비상] {name}\n💵 {price_str}원 (+{pct:.2f}%)\n👉 거래대금 상위 상승주"
                results.append({"score": pct, "msg": msg}) 
        except: pass
    return sorted(results, key=lambda x: x['score'], reverse=True)[:10]

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [시스템 가동] 3대 공통필터(OBV,RSI,Stoch) + 통합전략")
    send_telegram(f"🚀 [완전체 가동] 잡주는 가라! '3대 필수지표'를 통과한 '숨고르기 & 상승초입' 종목만 엄선합니다.")

    target_dict = get_market_data()
    target_tickers = list(target_dict.keys())
    print(f"⚡ {len(target_tickers)}개 종목 정밀 분석 중...")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(analyze_stock, t, target_dict[t]): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)

    if not results:
        results = get_fallback_stocks(target_dict)

    if results:
        results.sort(key=lambda x: x['score'], reverse=True)
        final_msgs = [r['msg'] for r in results[:30]]
        full_text = f"🤖 [오늘의 추천주] {TODAY_STR}\n(총 {len(results)}개 포착)\n\n" + "\n\n".join(final_msgs)
        send_telegram(full_text)
    else:
        send_telegram("💀 시장 관망 필요.")