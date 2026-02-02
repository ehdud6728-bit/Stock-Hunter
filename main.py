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
    prompt = f"종목: {name}\n점수: {score}\n특징: {reason}\n이 종목의 매력을 1줄로 요약해."
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
# 🏢 네이버 재무 크롤링 (영업이익 추세)
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
                        if prev < 0 and last > 0: return "🐢 흑자전환"
                        if last > prev * 1.3: return "📈 이익급증"
                        if last > prev: return "🔺 이익증가"
                        if last < prev: return "📉 이익감소"
        return "보통"
    except: return "확인불가"

# ---------------------------------------------------------
# ⚖️ 재무 등급 판독기 (Badge)
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
# 🧮 [6대 보조지표] 전부 계산
# ---------------------------------------------------------
def get_indicators(df):
    # 1. 이동평균 (MA5, 20, 60)
    ma5 = df['Close'].rolling(5).mean()
    ma20 = df['Close'].rolling(20).mean()
    ma60 = df['Close'].rolling(60).mean() # 복구됨
    
    # 2. 이격도
    disparity = (df['Close'] / ma20) * 100
    
    # 3. RSI
    delta = df['Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rsi = 100 - (100 / (1 + (gain / loss)))

    # 4. Stochastic (Fast K -> Slow K -> Slow D)
    high = df['High'].rolling(9).max()
    low = df['Low'].rolling(9).min()
    fast_k = ((df['Close'] - low) / (high - low)) * 100
    slow_k = fast_k.rolling(3).mean()
    slow_d = slow_k.rolling(3).mean()

    # 5. OBV
    direction = df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    obv = (direction * df['Volume']).cumsum()
    obv_rising = obv.iloc[-1] > obv.iloc[-2]
    
    return ma5, ma20, ma60, disparity, rsi, slow_k, slow_d, obv_rising

# ---------------------------------------------------------
# 💯 점수 계산 (보조지표 반영)
# ---------------------------------------------------------
def calculate_score(ticker, pct, vol_ratio, disparity, obv_rising, is_flag, rsi, k, d):
    score = 40 
    reasons = []
    
    # 재무 배지
    badge, roe = get_financial_badge(ticker)
    if "💎" in badge: score += 15; reasons.append("재무우수")
    if "💰" in badge: score += 10; reasons.append("고수익")
    if "⚠️" in badge: score -= 5

    # 패턴 & 거래량
    if is_flag: score += 30; reasons.append("🚩숨고르기")
    elif vol_ratio >= 1.5: score += 15; reasons.append("수급유입")
    
    if obv_rising: score += 10; reasons.append("OBV상승")

    # [보조지표 점수]
    if 40 <= rsi <= 65: score += 10
    elif rsi <= 40: score += 15; reasons.append("바닥권(RSI)")
    
    if k > d: score += 10; reasons.append("스토캐스틱GC") # 골든크로스

    # 타이밍 (이격도)
    if 95 <= disparity <= 110: score += 20; reasons.append("이격도안정")
    
    return score, ", ".join(reasons), badge, roe

# ---------------------------------------------------------
# 🔍 통합 분석 (5대 전략)
# ---------------------------------------------------------
def analyze_stock(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=200)).strftime('%Y-%m-%d'))
        if len(df) < 60: return None
        curr = df.iloc[-1]   
        prev = df.iloc[-2]   
        if curr['Close'] < 1000: return None
        
        # 6대 지표 모두 가져오기
        ma5, ma20, ma60, disparity, rsi, k, d, obv_rising = get_indicators(df)
        pct = curr['Change'] * 100
        vol_ratio = curr['Volume'] / prev['Volume'] if prev['Volume'] > 0 else 0
        
        # 전략 분류
        strategy = ""
        is_flag = False
        
        # 1. 숨고르기
        if (prev['Change'] >= 0.10) and (curr['Volume'] < prev['Volume'] * 0.8) and (-4.0 <= pct <= 4.0):
            is_flag = True; strategy = "🏳️ 숨고르기"
        
        # 2. 바닥 반등 (RSI & MA60 활용)
        elif (curr['Close'] < ma60.iloc[-1]) and (curr['Close'] > ma5.iloc[-1]) and (rsi.iloc[-1] <= 55):
            strategy = "🎣 바닥반등"
        
        # 3. 급등
        elif (vol_ratio >= 1.8) and (pct >= 2.0): strategy = "🚀 급등"
        
        # 4. 추세
        elif (ma5.iloc[-1] > ma20.iloc[-1]): strategy = "🦁 추세"
        
        # 5. 잠입 (OBV & 눌림목)
        elif (-3.0 < pct < 2.0) and obv_rising and (disparity.iloc[-1] <= 105): strategy = "🕵️ 잠입"
        
        if strategy:
            # 점수 산출
            score, reason, badge, roe = calculate_score(ticker, pct, vol_ratio, disparity.iloc[-1], obv_rising, is_flag, rsi.iloc[-1], k.iloc[-1], d.iloc[-1])
            
            if score < 50: return None
            
            # 네이버 재무 크롤링
            fin_trend = get_naver_financials(ticker)

            rank = "🥉 B급"
            if score >= 80: rank = "🏆 SS급"
            elif score >= 70: rank = "🥇 S급"
            elif score >= 60: rank = "🥈 A급"

            ai_comment = ""
            if score >= 60: ai_comment = get_ai_summary(ticker, name, score, reason)

            amt_billion = int(FUNDAMENTALS.get(ticker, {}).get('Amount', 0) / 100000000)
            price_str = format(int(curr['Close']),',')

            return {
                "score": score,
                "msg": f"{rank} {name} ({score}점)\n"
                       f"💵 {price_str}원 ({pct:+.2f}%)\n"
                       f"🏢 재무: {badge} (ROE {roe:.1f}%)\n"
                       f"📈 추세: {fin_trend} (영업이익)\n"
                       f"📊 특징: {reason}\n"
                       f"👉 패턴: {strategy}{ai_comment}"
            }
    except: return None
    return None

# ---------------------------------------------------------
# 🚨 비상용
# ---------------------------------------------------------
def get_fallback_stocks(target_dict):
    print("🚨 [비상] 결과 없음 -> 단순 급등주 추출")
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
    print(f"🚀 [시스템 가동] 6대 보조지표 + 재무 배지(Badge) + 영업이익 추세")
    send_telegram(f"🚀 [최종 완성] 보조지표 6종 + 재무등급 + 영업이익 추세가 모두 적용되었습니다!\n(커트라인 50점 / Top 1000)")

    target_dict = get_market_data()
    target_tickers = list(target_dict.keys())
    print(f"⚡ {len(target_tickers)}개 종목 분석 중...")
    
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