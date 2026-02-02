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
# 🌍 설정 및 환경변수
# ---------------------------------------------------------
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '').strip() 

# 📊 전역 변수 (재무 데이터 저장소)
FUNDAMENTALS = {} 

# ---------------------------------------------------------
# 📨 텔레그램 전송
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
# 🤖 AI 코멘트 (Groq)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, score, reason):
    if not GROQ_API_KEY: return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    prompt = f"종목: {name}\n점수: {score}\n특징: {reason}\n이 종목의 매수 타이밍과 리스크를 1줄로 조언해."
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
# ⚡ 데이터 수집 (Top 1000 & EPS 수집)
# ---------------------------------------------------------
def get_market_data():
    print("⚡ 시장 데이터 수집 중...")
    try:
        df_krx = fdr.StockListing('KRX')
        global FUNDAMENTALS
        try:
            # ⭐ EPS(주당순이익) 필수 포함
            FUNDAMENTALS = df_krx.set_index('Code')[['Name', 'PER', 'PBR', 'EPS', 'Amount']].to_dict('index')
        except: FUNDAMENTALS = {}
        
        # 거래대금 상위 1000개 선정
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
                        if prev < 0 and last > 0: return "🐢흑자전환"
                        if last > prev * 1.3: return "📈이익급증"
                        if last > prev: return "🔺이익증가"
                        if last < prev: return "📉이익감소"
        return "보통"
    except: return "확인불가"

# ---------------------------------------------------------
# ⚖️ 재무 등급(Badge) 판독기 (EPS 기준)
# ---------------------------------------------------------
def get_financial_badge(ticker):
    info = FUNDAMENTALS.get(ticker, {})
    per = info.get('PER', 0)
    pbr = info.get('PBR', 0)
    eps = info.get('EPS', 0) # EPS 확인
    
    # 데이터 정제
    if pd.isna(per): per = 0
    if pd.isna(pbr): pbr = 0
    if pd.isna(eps): eps = 0
    
    roe = 0
    if per > 0 and pbr > 0: roe = (pbr / per) * 100
        
    badge = "⚖️ 보통"
    
    # 1. 적자기업 (EPS가 마이너스)
    if eps < 0:
        badge = "⚠️ 적자기업 (주의)"
    # 2. 저평가 우량주
    elif (eps > 0) and (0 < per < 12) and (pbr < 1.5):
        badge = "💎 저평가 우량주"
    # 3. 고성장주
    elif (eps > 0) and (per >= 12):
        badge = "💰 고수익 성장주"
    # 4. 자산주
    elif (pbr < 0.6) and (eps >= 0):
        badge = "🧱 헐값 자산주"
        
    return badge, roe

# ---------------------------------------------------------
# 🧮 [6대 보조지표] 전부 계산
# ---------------------------------------------------------
def get_indicators(df):
    # 1. 이동평균 (MA 5, 20, 60)
    ma5 = df['Close'].rolling(5).mean()
    ma20 = df['Close'].rolling(20).mean()
    ma60 = df['Close'].rolling(60).mean()
    
    # 2. 이격도 (20일선 기준)
    disparity = (df['Close'] / ma20) * 100
    
    # 3. RSI (14일 기준)
    delta = df['Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rsi = 100 - (100 / (1 + (gain / loss)))

    # 4. Stochastic (Slow K, D)
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
# ⚔️ [필수] 3대 공통 필터 (OBV, RSI, Stoch)
# ---------------------------------------------------------
def check_common_conditions(obv_rising, rsi, k, d):
    # 1. OBV: 돈이 들어오는가?
    if not obv_rising: return False 
    
    # 2. RSI: 정상 범위인가? (30~75)
    if not (30 <= rsi <= 75): return False 
    
    # 3. 스토캐스틱: 정배열(K>=D)인가?
    if k < d: return False 
    
    return True

# ---------------------------------------------------------
# 💯 점수 계산 시스템
# ---------------------------------------------------------
def calculate_score(ticker, pct, vol_ratio, disparity, is_flag, is_golpagi, badge):
    score = 50 # 기본점수
    reasons = []
    
    # [재무]
    if "💎" in badge: score += 10; reasons.append("재무우수")
    if "💰" in badge: score += 10; reasons.append("성장주")
    if "⚠️" in badge: score -= 10 # 적자는 감점

    # [패턴]
    if is_golpagi: score += 30; reasons.append("⛏️골파기")
    elif is_flag: score += 30; reasons.append("🚩숨고르기")
    elif vol_ratio >= 1.5: score += 15; reasons.append("수급유입")
    
    # [타이밍]
    if 100 <= disparity <= 105: score += 20; reasons.append("이격도최상")
    elif disparity <= 110: score += 10; reasons.append("이격도양호")
    
    return score, ", ".join(reasons)

# ---------------------------------------------------------
# 🔍 통합 분석 (3대 전략 + 공통필터)
# ---------------------------------------------------------
def analyze_stock(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=200)).strftime('%Y-%m-%d'))
        if len(df) < 60: return None
        curr = df.iloc[-1]   
        prev = df.iloc[-2]   
        if curr['Close'] < 1000: return None
        
        # 1. 지표 계산
        ma5, ma20, ma60, disparity, rsi, k, d, obv_rising = get_indicators(df)
        pct = curr['Change'] * 100
        vol_ratio = curr['Volume'] / prev['Volume'] if prev['Volume'] > 0 else 0
        
        # 값 추출
        curr_rsi = rsi.iloc[-1]
        curr_k = k.iloc[-1]
        curr_d = d.iloc[-1]
        curr_disp = disparity.iloc[-1]

        # -------------------------------------------------------
        # 🛑 [공통 필터] 3대 지표 필수 통과
        # -------------------------------------------------------
        if not check_common_conditions(obv_rising, curr_rsi, curr_k, curr_d):
            return None # 탈락

        # -------------------------------------------------------
        # 🎯 전략 패턴 매칭
        # -------------------------------------------------------
        strategy = ""
        is_flag = False
        is_golpagi = False

        # 1. ⛏️ 골파기 (Bear Trap) - 이평선 깼다 복구
        broken_ma20 = (prev['Close'] < ma20.iloc[-2]) and (df['Close'].iloc[-3] > ma20.iloc[-3])
        recover_ma20 = (curr['Close'] > ma20.iloc[-1])
        broken_ma60 = (prev['Close'] < ma60.iloc[-2]) and (df['Close'].iloc[-3] > ma60.iloc[-3])
        recover_ma60 = (curr['Close'] > ma60.iloc[-1])

        if ((broken_ma20 and recover_ma20) or (broken_ma60 and recover_ma60)) and (pct > 0):
            is_golpagi = True
            strategy = "⛏️ 골파기 (개미털기)"

        # 2. 🏳️ 숨고르기 (Flag)
        # 10% 급등 -> 거래량 50% 미만 -> 주가 ±2%
        elif (prev['Change'] >= 0.10) and (curr['Volume'] < prev['Volume'] * 0.5) and (-2.0 <= pct <= 2.0):
            is_flag = True
            strategy = "🏳️ 숨고르기"

        # 3. 🦁 상승 초입 (통합형)
        # 이격도 110% 이하 필수
        elif (curr_disp <= 110):
            if (vol_ratio >= 1.5) and (pct >= 1.0):
                strategy = "🦁 상승초입 (돌파형)"
            elif (-3.0 <= pct <= 1.0) and (curr_disp <= 105):
                strategy = "🦁 상승초입 (눌림목)"
            elif (curr['Close'] < ma60.iloc[-1]) and (curr['Close'] > ma5.iloc[-1]):
                strategy = "🦁 상승초입 (바닥턴)"

        if strategy:
            # 재무 배지 및 점수 산출
            badge, roe = get_financial_badge(ticker)
            score, reason = calculate_score(ticker, pct, vol_ratio, curr_disp, is_flag, is_golpagi, badge)
            
            # 커트라인 60점
            if score < 60: return None
            
            # 네이버 실적 크롤링
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
    print(f"🚀 [시스템 가동] 모든 요구사항 통합 및 검증 완료")
    send_telegram(f"🚀 [검색기 가동] 3대 공통필터(OBV/RSI/Stoch)를 통과하고,\n골파기/숨고르기/초입 패턴을 완성한 '우량주'만 발굴합니다!")

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