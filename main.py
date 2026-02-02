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

    print(f"📨 전송 시작... 대상: {len(CHAT_ID_LIST)}명")

    # 1. 혹시 뭉쳐있는 아이디가 있다면 콤마(,)로 쪼개서 리스트를 다시 만듭니다.
    real_id_list = []
    if isinstance(CHAT_ID_LIST, list):
        for item in CHAT_ID_LIST:
            # 콤마로 쪼개고, 공백 제거해서 하나씩 추가
            real_id_list.extend([x.strip() for x in item.split(',') if x.strip()])
    else:
        # 리스트가 아니라 문자열로 넣었을 경우 대비
        real_id_list = [x.strip() for x in str(CHAT_ID_LIST).split(',') if x.strip()]

    # 2. 정리된 리스트로 전송 시작
    for chat_id in real_id_list:
        if chat_id.strip():
            print(f"✅ 전송 ! ({chat_id})")
            for chunk in chunks:
                try: 
                    response = requests.post(url, data={'chat_id': chat_id, 'text': chunk})

                    # 결과 확인
                    if response.status_code == 200:
                        print(f"✅ 전송 성공! ({user_id})")
                    else:
                        print(f"❌ 전송 실패 ({user_id}): {response.text}")
                        
                    time.sleep(0.5) 
                except Exception as e:
                    print(f"🚨 에러 발생 ({user_id}): {e}")
                time.sleep(0.5)
# ---------------------------------------------------------
# 🤖 AI 코멘트
# ---------------------------------------------------------
def get_ai_summary(ticker, name, score, reason):
    if not GROQ_API_KEY: return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    prompt = f"종목: {name}\n점수: {score}\n특징: {reason}\n이 종목의 수급과 차트 흐름을 1줄로 분석해."
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
# ⚡ 시장 데이터 (기본)
# ---------------------------------------------------------
def get_market_data():
    print("⚡ 시장 데이터 수집 중...")
    try:
        df_krx = fdr.StockListing('KRX')
        global FUNDAMENTALS
        try:
            FUNDAMENTALS = df_krx.set_index('Code')[['Name', 'PER', 'PBR', 'EPS', 'Amount']].to_dict('index')
        except: FUNDAMENTALS = {}
        
        df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(1000)
        return dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    except: return {}

# ---------------------------------------------------------
# 🕵️ [NEW] 외인/기관 수급 크롤링 (네이버 금융)
# ---------------------------------------------------------
def get_investor_trend(code):
    """
    네이버 금융 '매매동향' 탭에서 외국인/기관 순매수량을 가져옴
    """
    try:
        # 네이버 금융 > 투자자별 매매동향 페이지
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        
        # 테이블 읽기
        dfs = pd.read_html(url, encoding='euc-kr', header=0)
        
        # 보통 2번째 테이블이 일별 매매동향임
        for df in dfs:
            if '날짜' in df.columns and '외국인' in df.columns and '기관' in df.columns:
                # 데이터 정제 (결측치 제거)
                df = df.dropna()
                if len(df) < 1: return False, False, "수급정보없음"
                
                # 가장 최근 날짜(맨 윗줄) 데이터 가져오기
                latest = df.iloc[0]
                
                # 수량 or 금액 (네이버는 보통 수량)
                foreigner = int(str(latest['외국인']).replace(',', ''))
                institution = int(str(latest['기관']).replace(',', ''))
                
                # 순매수 여부 판단
                is_for_buy = foreigner > 0
                is_ins_buy = institution > 0
                
                trend_str = ""
                if is_for_buy and is_ins_buy: trend_str = "🚀쌍끌이매수"
                elif is_for_buy: trend_str = "👨🏼‍🦰외인매수"
                elif is_ins_buy: trend_str = "🏢기관매수"
                else: trend_str = "💧개인매수(양매도)"
                
                return is_for_buy, is_ins_buy, trend_str
                
        return False, False, "확인불가"
    except:
        return False, False, "크롤링실패"

# ---------------------------------------------------------
# 🏢 재무 크롤링 (실적 추세)
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
                        last = valid_data[-1]; prev = valid_data[-2]
                        if prev < 0 and last > 0: return "🐢흑자전환"
                        if last > prev * 1.3: return "📈이익급증"
                        if last > prev: return "🔺이익증가"
                        if last < prev: return "📉이익감소"
        return "보통"
    except: return "확인불가"

# ---------------------------------------------------------
# ⚖️ 재무 등급(Badge) 판독기
# ---------------------------------------------------------
def get_financial_badge(ticker):
    info = FUNDAMENTALS.get(ticker, {})
    per = info.get('PER', 0); pbr = info.get('PBR', 0); eps = info.get('EPS', 0)
    
    if pd.isna(per): per = 0
    if pd.isna(pbr): pbr = 0
    if pd.isna(eps): eps = 0
    
    roe = 0
    if per > 0 and pbr > 0: roe = (pbr / per) * 100
        
    badge = "⚖️ 보통"
    if eps < 0: badge = "⚠️ 적자기업 (주의)"
    elif (eps > 0) and (0 < per < 12) and (pbr < 1.5): badge = "💎 저평가 우량주"
    elif (eps > 0) and (per >= 12): badge = "💰 고수익 성장주"
    elif (pbr < 0.6) and (eps >= 0): badge = "🧱 헐값 자산주"
        
    return badge, roe

# ---------------------------------------------------------
# 🧮 6대 지표
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
# 💯 점수 계산 (수급 포함!)
# ---------------------------------------------------------
def calculate_score(ticker, pct, vol_ratio, disparity, is_flag, is_golpagi, badge, is_for_buy, is_ins_buy):
    score = 50 
    reasons = []
    
    # [수급 점수] ⭐ 여기가 핵심!
    if is_for_buy and is_ins_buy:
        score += 30; reasons.append("쌍끌이매수") # 둘 다 사면 대박
    elif is_for_buy:
        score += 10; reasons.append("외인매수")
    elif is_ins_buy:
        score += 10; reasons.append("기관매수")

    # [재무]
    if "💎" in badge: score += 10; reasons.append("재무우수")
    if "💰" in badge: score += 10; reasons.append("성장주")
    if "⚠️" in badge: score -= 10

    # [패턴]
    if is_golpagi: score += 30; reasons.append("⛏️골파기")
    elif is_flag: score += 30; reasons.append("🚩숨고르기")
    elif vol_ratio >= 1.5: score += 15; reasons.append("수급폭발")
    
    # [타이밍]
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
        
        # 지표
        ma5, ma20, ma60, disparity, rsi, k, d, obv_rising = get_indicators(df)
        curr_rsi = rsi.iloc[-1]
        curr_k = k.iloc[-1]
        curr_d = d.iloc[-1]

        # 🛑 공통 필터 (OBV, RSI, Stoch)
        if not (obv_rising and (30 <= curr_rsi <= 75) and (curr_k >= curr_d)):
            return None

        # 🕵️ [NEW] 수급 분석 (합격권 애들만 조회해서 속도 방어)
        is_for_buy, is_ins_buy, trend_str = get_investor_trend(ticker)

        # 🎯 전략 패턴
        pct = curr['Change'] * 100
        vol_ratio = curr['Volume'] / prev['Volume'] if prev['Volume'] > 0 else 0
        strategy = ""
        is_flag = False; is_golpagi = False

        # 1. 골파기
        if ((prev['Close'] < ma20.iloc[-2] and df['Close'].iloc[-3] > ma20.iloc[-3]) and curr['Close'] > ma20.iloc[-1]) and pct > 0:
            is_golpagi = True; strategy = "⛏️ 골파기 (개미털기)"
        # 2. 숨고르기
        elif (prev['Change'] >= 0.10) and (curr['Volume'] < prev['Volume'] * 0.5) and (-2.0 <= pct <= 2.0):
            is_flag = True; strategy = "🏳️ 숨고르기"
        # 3. 상승 초입
        elif (disparity.iloc[-1] <= 110):
            if (vol_ratio >= 1.5) and (pct >= 1.0): strategy = "🦁 상승초입 (돌파형)"
            elif (-3.0 <= pct <= 1.0) and (disparity.iloc[-1] <= 105): strategy = "🦁 상승초입 (눌림목)"
            elif (curr['Close'] < ma60.iloc[-1]) and (curr['Close'] > ma5.iloc[-1]): strategy = "🦁 상승초입 (바닥턴)"

        if strategy:
            badge, roe = get_financial_badge(ticker)
            # 점수 계산에 수급 정보 전달!
            score, reason = calculate_score(ticker, pct, vol_ratio, disparity.iloc[-1], is_flag, is_golpagi, badge, is_for_buy, is_ins_buy)
            
            if score < 60: return None
            
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
                       f"🛒 수급: {trend_str}\n"  # 수급 정보 표시!
                       f"🏢 재무: {badge}\n"
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
                results.append({"score": pct, "msg": f"🆘 [비상] {name} (+{pct:.2f}%)"}) 
        except: pass
    return sorted(results, key=lambda x: x['score'], reverse=True)[:10]

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [시스템 가동] 외인/기관 수급 분석 기능 추가")
    send_telegram(f"🚀 [기능 탑재] 이제 '외인/기관'이 샀는지까지 확인합니다!\n'🚀쌍끌이매수' 종목을 주목하세요.")

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
