import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
from datetime import datetime, timedelta
import concurrent.futures
import pytz
from io import StringIO

# ---------------------------------------------------------
# 🌍 설정 (시간 역행 & 환경변수)
# ---------------------------------------------------------
# 👇 [시간 역행 마법] 야간/새벽에 실행 시 '어제' 날짜로 인식
current_time = datetime.now()
if current_time.hour < 8:
    NOW = current_time - timedelta(days=1)
    print(f"🌙 야간 모드 발동! 분석 기준일을 {NOW.strftime('%Y-%m-%d')}로 설정합니다.")
else:
    NOW = current_time

TODAY_STR = NOW.strftime('%Y-%m-%d')
TOP_N = 100  # 분석 대상 상위 N개

# 환경변수 (GitHub Secrets)
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '').strip()

# 전역 변수 (기본 정보 캐싱용)
FUNDAMENTALS = {}

# ---------------------------------------------------------
# 📨 텔레그램 전송
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST:
        print("❌ 텔레그램 토큰 없음 (화면 출력만 함)")
        print(message)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    # ID 리스트 정리
    real_id_list = []
    for item in CHAT_ID_LIST:
        real_id_list.extend([x.strip() for x in item.split(',') if x.strip()])

    # 메시지 청크 나누기 (4000자 제한)
    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]

    print(f"📨 텔레그램 전송 시작 ({len(real_id_list)}명)...")
    
    for chat_id in real_id_list:
        if not chat_id: continue
        for chunk in chunks:
            try:
                data = {'chat_id': chat_id, 'text': chunk}
                requests.post(url, data=data)
                time.sleep(0.5)
            except Exception as e:
                print(f"🚨 전송 실패 ({chat_id}): {e}")

# ---------------------------------------------------------
# 🤖 AI 코멘트 (Groq)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, score, details, risk):
    if not GROQ_API_KEY: return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    
    prompt = (f"종목명: {name}\n점수: {score}\n상세: {details}\n위험요소: {risk}\n"
              f"이 종목의 매력과 주의할 점을 1줄로 요약해줘. (한국어)")
    
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    }
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=5)
        return "\n💡 " + res.json()['choices'][0]['message']['content'].strip()
    except: return ""

# ---------------------------------------------------------
# ⚡ 시장 데이터 (종목 리스트)
# ---------------------------------------------------------
def get_market_data():
    print("⚡ KRX 전 종목 리스트 확보 중...")
    try:
        df_krx = fdr.StockListing('KRX')
        # 펀더멘털 정보 캐싱
        global FUNDAMENTALS
        try:
            FUNDAMENTALS = df_krx.set_index('Code')[['Name']].to_dict('index')
        except: FUNDAMENTALS = {}
        
        # 거래대금 상위 N개 선정
        df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
        return dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    except: return {}

# ---------------------------------------------------------
# 🕵️ [스텔스] 수급 데이터 (네이버)
# ---------------------------------------------------------
def get_investor_trend(code):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        resp = requests.get(url, headers=headers)
        
        # 테이블 파싱 (결측치 제거 및 필터링)
        dfs = pd.read_html(StringIO(resp.text), attrs={'class': 'type2'}, header=0)
        target_df = dfs[2] # 보통 3번째 테이블이 수급
        
        # "날짜" 헤더가 중간에 또 들어가는 경우 제거 (선생님 소스 반영)
        target_df = target_df.dropna()
        target_df = target_df[target_df['날짜'].str.contains('날짜') == False]
        
        if len(target_df) < 1: return False, False, "데이터없음"
        
        latest = target_df.iloc[0]
        
        # 콤마 제거 및 정수 변환
        foreigner = int(str(latest['외국인']).replace(',', ''))
        institution = int(str(latest['기관']).replace(',', ''))
        
        is_for_buy = foreigner > 0
        is_ins_buy = institution > 0
        
        trend_str = ""
        if is_for_buy and is_ins_buy: trend_str = "🚀쌍끌이"
        elif is_for_buy: trend_str = "👨🏼‍🦰외인"
        elif is_ins_buy: trend_str = "🏢기관"
        else: trend_str = "💧개인"
        
        return is_for_buy, is_ins_buy, trend_str
    except:
        return False, False, "분석불가"

# ---------------------------------------------------------
# 🏢 [재무] 실적 및 뱃지 분석
# ---------------------------------------------------------
def get_financial_info(code):
    result = {"trend": "", "badge": "⚖️보통"}
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers)
        
        dfs = pd.read_html(StringIO(resp.text), header=0)
        fin_df = None
        for df in dfs:
            if '최근 연간 실적' in str(df.columns) or '주요재무제표' in str(df.columns):
                fin_df = df; break
        
        if fin_df is None: return result
        if len(fin_df.columns) > 0: fin_df = fin_df.set_index(fin_df.columns[0])

        # 1. 이익 추이
        if '영업이익' in fin_df.index:
            vals = []
            for v in fin_df.loc['영업이익'].values:
                try: vals.append(float(str(v).replace(',', '')))
                except: pass
            if len(vals) >= 2:
                prev, last = vals[-2], vals[-1]
                if prev < 0 and last > 0: result['trend'] = "🐢흑자전환"
                elif last > prev * 1.3: result['trend'] = "📈이익급증"
                elif last > prev: result['trend'] = "🔺이익증가"
                elif last < prev: result['trend'] = "📉이익감소"

        # 2. 뱃지 (PER, PBR, EPS)
        per, pbr, eps = 0, 0, 0
        
        def get_val(idx):
            if idx in fin_df.index:
                for v in reversed(fin_df.loc[idx].values):
                    try: return float(str(v).replace(',', ''))
                    except: pass
            return 0
            
        per = get_val('PER(배)')
        pbr = get_val('PBR(배)')
        eps = get_val('EPS(원)') if 'EPS(원)' in fin_df.index else get_val('주당순이익')

        if eps < 0: result['badge'] = "⚠️적자"
        elif (eps > 0) and (0 < per < 12) and (pbr < 1.5): result['badge'] = "💎저평가"
        elif (eps > 0) and (per >= 12): result['badge'] = "💰성장주"
        elif (pbr < 0.6) and (eps >= 0): result['badge'] = "🧱자산주"
        
    except: pass
    return result

# ---------------------------------------------------------
# 📊 [지표] 공구리/기울기/보조지표 산출 (핵심 로직)
# ---------------------------------------------------------
def add_indicators(df):
    # 이평선
    df['MA5'] = df['Close'].rolling(5).mean()
    df['MA10'] = df['Close'].rolling(10).mean()
    df['MA20'] = df['Close'].rolling(20).mean()
    
    # ⭐️ 기울기(Slope) 계산
    df['MA5_Slope'] = df['MA5'].diff()
    df['MA5_Slope_Prev'] = df['MA5_Slope'].shift(1)
    
    df['MA10_Slope'] = df['MA10'].diff()
    df['MA10_Slope_Prev'] = df['MA10_Slope'].shift(1)
    
    df['MA20_Slope'] = df['MA20'].diff()
    df['MA20_Slope_Prev'] = df['MA20_Slope'].shift(1)
    
    # 전일 값 (추세 필터용)
    df['MA5_Prev'] = df['MA5'].shift(1)
    df['MA10_Prev'] = df['MA10'].shift(1)
    df['MA20_Prev'] = df['MA20'].shift(1)
    df['MA20_Prev2'] = df['MA20'].shift(2)
    
    # 이격도 & RSI
    df['Disp'] = (df['Close'] / df['MA20']) * 100
    delta = df['Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / loss)))

    # 스토캐스틱 & 기울기
    high = df['High'].rolling(9).max()
    low = df['Low'].rolling(9).min()
    fast_k = ((df['Close'] - low) / (high - low)) * 100
    df['Stoch_K'] = fast_k.rolling(3).mean()
    df['Stoch_D'] = df['Stoch_K'].rolling(3).mean()
    df['Stoch_Slope'] = df['Stoch_K'].diff() 
    
    # OBV & 기울기
    direction = df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    df['OBV'] = (direction * df['Volume']).cumsum()
    df['OBV_Rising'] = df['OBV'] > df['OBV'].shift(1)
    df['OBV_Slope'] = df['OBV'].diff() 
    
    # 기타 데이터
    df['Prev_Close'] = df['Close'].shift(1)
    df['Prev_Vol'] = df['Volume'].shift(1)
    df['Pct'] = df['Change'] * 100
    df['Vol_Ratio'] = np.where(df['Prev_Vol'] > 0, df['Volume'] / df['Prev_Vol'], 1.0)
    df['Prev_Change'] = df['Change'].shift(1)
    
    # 🍉 수박 지표 (엔벨로프 하단)
    df['Env_Lower'] = df['MA20'] * 0.85 
    
    return df

# ---------------------------------------------------------
# 💯 [점수] 최종 점수 계산기 (재무 + 수급 + 패턴 + 차트)
# ---------------------------------------------------------
def calculate_score(row, pattern_name, is_for_buy, is_ins_buy, fin_info):
    score = 50 
    details = [] 
    
    # 1. 재무 점수 (Trend + Badge)
    trend = fin_info.get('trend', '')
    badge = fin_info.get('badge', '⚖️보통')
    
    if "흑자" in trend: score += 15; details.append("흑자(15)")
    elif "급증" in trend: score += 10; details.append("급증(10)")
    elif "증가" in trend: score += 5; details.append("증가(5)")
    
    if "저평가" in badge: score += 15; details.append("저평가(15)")
    elif "성장" in badge: score += 10; details.append("성장(10)")
    elif "적자" in badge: score -= 15; details.append("적자(-15)")
    
    # 2. 수급 (30점)
    if is_for_buy and is_ins_buy: score += 30; details.append("쌍끌이(30)")
    elif is_for_buy or is_ins_buy: score += 10; details.append("수급(10)")
    else: details.append("수급X(0)")

    # 3. 패턴 (50점 만점)
    if "황금수박" in pattern_name: score += 50; details.append("👑황금(50)")
    elif "공구리" in pattern_name: score += 40; details.append("🔨공구리(40)")
    elif "골파기" in pattern_name: score += 30; details.append("⛏️골파기(30)")
    elif "숨고르기" in pattern_name: score += 30; details.append("🏳️숨고르기(30)")
    elif "돌파" in pattern_name: score += 15; details.append("🦁돌파(15)")
    
    # 4. 차트 (40점)
    if "수박" in pattern_name:
        if row['RSI'] <= 30: score += 30; details.append("과매도(30)")
        elif row['RSI'] <= 40: score += 20; details.append("과매도(20)")
    else:
        if 100 <= row['Disp'] <= 105: score += 20; details.append("이격(20)")
        elif row['Disp'] <= 110: score += 10; details.append("이격(10)")
        if row['Stoch_K'] > row['Stoch_D']: score += 5; details.append("Stoch(5)")

    # 📉 [감점 및 위험신호]
    warnings = []
    if row['OBV_Slope'] < 0: 
        score -= 10; warnings.append("⚠️돈이탈")
    if row['Stoch_Slope'] < 0:
        score -= 5; warnings.append("⚠️힘빠짐")
    if "수박" not in pattern_name and row['MA10'] < row['MA10_Prev']: 
        score -= 5; warnings.append("⚠️단기저항")

    risk_label = " ".join(warnings) if warnings else "✅깨끗함"
    
    return score, ", ".join(details), risk_label

# ---------------------------------------------------------
# 🔍 [분석 엔진] 통합 로직
# ---------------------------------------------------------
def analyze_stock(ticker, name):
    try:
        # 데이터 가져오기 (200일치)
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=200)).strftime('%Y-%m-%d'))
        if len(df) < 60: return None
        
        df = add_indicators(df)
        
        row = df.iloc[-1]
        prev_row = df.iloc[-2]
        prev2_row = df.iloc[-3]
        
        # 동전주 제외
        if row['Close'] < 1000: return None
        
        # 🛑 [Global Filter] 5일 & 10일 동시 하락? (단기 급락)
        is_crash = (row['MA5'] < row['MA5_Prev']) and (row['MA10'] < row['MA10_Prev'])
        if is_crash: return None 

        # 🛑 [Trend Filter] 20일선 하락? (일반 패턴용)
        is_downtrend = row['MA20'] < row['MA20_Prev']
        
        signal = None
        
        # 1. 🍉 수박 Check (공구리 조건)
        if row['Low'] <= row['Env_Lower']:
            gongguri_5 = row['MA5_Slope'] > row['MA5_Slope_Prev']
            gongguri_10 = row['MA10_Slope'] > row['MA10_Slope_Prev']
            
            if gongguri_5 and gongguri_10:
                if (row['MA20_Slope'] < 0) and (row['MA20_Slope'] > row['MA20_Slope_Prev']):
                    signal = "👑황금수박" 
                else:
                    signal = "🍉공구리수박"
        
        # 2. 일반 패턴
        else:
            if is_downtrend: return None # 20일선 하락 시 탈락

            pass_filter = True
            if not row['OBV_Rising']: pass_filter = False
            if not (30 <= row['RSI'] <= 75): pass_filter = False
            if row['Stoch_K'] < row['Stoch_D']: pass_filter = False
            
            if pass_filter:
                if ((prev_row['Close'] < prev_row['MA20']) and (prev2_row['Close'] > prev2_row['MA20']) and (row['Close'] > row['MA20']) and (row['Pct'] > 0)):
                    signal = "⛏️골파기"
                elif (prev_row['Change'] >= 0.10) and (row['Volume'] < prev_row['Volume'] * 0.5) and (-2.0 <= row['Pct'] <= 2.0):
                    if (row['OBV_Slope'] >= 0) and (row['Stoch_Slope'] > -5):
                        signal = "🏳️숨고르기"
                elif (row['Disp'] <= 110):
                    if (row['Vol_Ratio'] >= 1.5) and (row['Pct'] >= 1.0): signal = "🦁돌파"
                    elif (-3.0 <= row['Pct'] <= 1.0) and (row['Disp'] <= 105): signal = "🦁눌림"
        
        if signal:
            # 수급 및 재무 확인
            is_for_buy, is_ins_buy, trend_str = get_investor_trend(ticker)
            fin_info = get_financial_info(ticker)
            
            # 점수 계산
            score, details, risk = calculate_score(row, signal, is_for_buy, is_ins_buy, fin_info)
            
            if score < 50: return None # 과락
            
            # AI 분석 (80점 이상만)
            ai_comment = ""
            if score >= 80:
                ai_comment = get_ai_summary(ticker, name, score, details, risk)
            
            return {
                "score": score,
                "msg": f"[{signal}] {name}\n"
                       f"📊 {score}점 ({fin_info['badge']})\n"
                       f"💰 {trend_str} / {risk}\n"
                       f"📝 {details}\n"
                       f"💵 {int(row['Close']):,}원 ({row['Pct']:+.2f}%){ai_comment}"
            }
            
    except: return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"📡 [The Final Bot] {TODAY_STR} 분석 시작...")
    
    target_dict = get_market_data()
    target_tickers = list(target_dict.keys())
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(analyze_stock, t, target_dict[t]): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)
            
    if results:
        results.sort(key=lambda x: x['score'], reverse=True)
        
        final_msgs = []
        for r in results[:15]: # 상위 15개만
            final_msgs.append(r['msg'])
            
        full_report = f"🦁 [오늘의 추천] {len(results)}개 포착\n\n" + "\n\n".join(final_msgs)
        
        print(full_report)
        send_telegram(full_report)
    else:
        msg = "❌ 오늘 조건에 맞는 종목이 없습니다. (시장 관망)"
        print(msg)
        send_telegram(msg)