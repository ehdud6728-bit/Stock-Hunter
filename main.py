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
from google_sheet_manager import update_google_sheet
from concurrent.futures import ThreadPoolExecutor

# ---------------------------------------------------------
# 🌍 [시간 설정] 무조건 한국 시간(KST) 기준!
# ---------------------------------------------------------
# 1. 한국 표준시(KST) 설정
KST = pytz.timezone('Asia/Seoul')
current_time = datetime.now(KST) # 👈 서버 시간이 아니라 한국 시간을 가져옴

# 2. 자정(00시) ~ 아침 8시 사이라면?
if current_time.hour < 8:
    # "야, 지금 새벽이야. 어제 장 끝난 거 분석해." -> 하루 뺌
    NOW = current_time - timedelta(days=1)
    print(f"🌙 야간 모드(00~08시): {NOW.strftime('%Y-%m-%d')} 기준 분석")
else:
    # 아침 8시 지났으면 오늘 날짜
    NOW = current_time
    print(f"☀️ 주간 모드: {NOW.strftime('%Y-%m-%d')} 기준 분석")

TODAY_STR = NOW.strftime('%Y-%m-%d')
TODAY_STR = NOW.strftime('%Y-%m-%d')
TOP_N = 250  # 거래대금 상위 100개만 (속도 최적화)

# GitHub Secrets 환경변수
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '').strip()

# ---------------------------------------------------------
# 📨 텔레그램 전송
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST:
        print("❌ 텔레그램 토큰 없음")
        print(message)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    real_id_list = []
    for item in CHAT_ID_LIST:
        real_id_list.extend([x.strip() for x in item.split(',') if x.strip()])

    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
    
    print(f"📨 전송 시작 ({len(real_id_list)}명)...")
    for chat_id in real_id_list:
        if not chat_id: continue
        for chunk in chunks:
            try:
                requests.post(url, data={'chat_id': chat_id, 'text': chunk})
                time.sleep(0.5)
            except: pass

# ---------------------------------------------------------
# 🤖 AI 요약
# ---------------------------------------------------------
def get_ai_summary(ticker, name, score, details, risk):
    if not GROQ_API_KEY: return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    prompt = (f"종목: {name}\n점수: {score}\n패턴: {details}\n위험: {risk}\n"
              f"이 종목의 매매 포인트를 한 줄로 요약해줘. (한국어)")
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
# ⚡ 시장 데이터
# ---------------------------------------------------------
def get_market_data():
    print("⚡ 종목 리스트 확보 중...")
    try:
        df_krx = fdr.StockListing('KRX')
        df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
        return dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    except: return {}

# ---------------------------------------------------------
# 🕵️ [수급] 네이버 크롤링 (스마트 탐색)
# ---------------------------------------------------------
def get_investor_trend(code):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        resp = requests.get(url, headers=headers, timeout=5)
        
        # '날짜'가 들어간 테이블만 찾기
        dfs = pd.read_html(StringIO(resp.text), match='날짜', header=0)
        target_df = None
        for df in dfs:
            if '외국인' in df.columns and '기관' in df.columns:
                target_df = df; break
        
        if target_df is None: return False, False, "분석불가"
        
        target_df = target_df.dropna()
        target_df = target_df[target_df['날짜'].str.contains('날짜') == False]
        if len(target_df) < 1: return False, False, "데이터없음"
        
        latest = target_df.iloc[0]
        foreigner = int(str(latest['외국인']).replace(',', ''))
        institution = int(str(latest['기관']).replace(',', ''))
        
        is_for_buy = foreigner > 0
        is_ins_buy = institution > 0
        
        trend = "🚀쌍끌이" if (is_for_buy and is_ins_buy) else \
                "👨🏼‍🦰외인" if is_for_buy else \
                "🏢기관" if is_ins_buy else "💧개인"
        return is_for_buy, is_ins_buy, trend
    except: return False, False, "크롤링실패"

# ---------------------------------------------------------
# 🏢 [재무] 실적
# ---------------------------------------------------------
def get_financial_info(code):
    res = {"trend": "", "badge": "⚖️보통"}
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        dfs = pd.read_html(StringIO(resp.text), header=0)
        
        fin_df = None
        for df in dfs:
            if '최근 연간 실적' in str(df.columns) or '주요재무제표' in str(df.columns):
                fin_df = df; break
        
        if fin_df is not None:
            if len(fin_df.columns) > 0: fin_df = fin_df.set_index(fin_df.columns[0])
            
            # 이익 추이
            if '영업이익' in fin_df.index:
                vals = [float(str(v).replace(',', '')) for v in fin_df.loc['영업이익'].values if str(v).replace(',', '').replace('.','').replace('-','').isdigit()]
                if len(vals) >= 2:
                    if vals[-2] < 0 and vals[-1] > 0: res['trend'] = "🐢흑자전환"
                    elif vals[-1] > vals[-2] * 1.3: res['trend'] = "📈이익급증"
            
            # 뱃지
            def get_v(k): return float(str(fin_df.loc[k].values[-1]).replace(',', '')) if k in fin_df.index else 0
            per, pbr, eps = get_v('PER(배)'), get_v('PBR(배)'), get_v('EPS(원)')
            
            if eps < 0: res['badge'] = "⚠️적자"
            elif eps > 0 and per < 10 and pbr < 1.0: res['badge'] = "💎저평가"
            elif eps > 0 and per >= 15: res['badge'] = "💰성장주"
    except: pass
    return res

# ---------------------------------------------------------
# 📊 [지표] 공구리 & 기울기 계산
# ---------------------------------------------------------
def add_indicators(df):
    df['MA5'] = df['Close'].rolling(5).mean()
    df['MA10'] = df['Close'].rolling(10).mean()
    df['MA20'] = df['Close'].rolling(20).mean()
    
    # ⭐️ 기울기(Slope): 오늘값 - 어제값
    df['MA5_Slope'] = df['MA5'].diff()
    df['MA5_Slope_Prev'] = df['MA5_Slope'].shift(1)
    df['MA10_Slope'] = df['MA10'].diff()
    df['MA10_Slope_Prev'] = df['MA10_Slope'].shift(1)
    df['MA20_Slope'] = df['MA20'].diff()
    df['MA20_Slope_Prev'] = df['MA20_Slope'].shift(1)
    
    # 전일값 (추세 필터용)
    df['MA5_Prev'] = df['MA5'].shift(1)
    df['MA10_Prev'] = df['MA10'].shift(1)
    df['MA20_Prev'] = df['MA20'].shift(1)
    
    # 수박 지표 (엔벨로프 하단)
    df['Env_Lower'] = df['MA20'] * 0.85 
    
    # 보조지표
    df['Disp'] = (df['Close'] / df['MA20']) * 100
    
    delta = df['Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / loss)))

    high = df['High'].rolling(9).max()
    low = df['Low'].rolling(9).min()
    fast_k = ((df['Close'] - low) / (high - low)) * 100
    df['Stoch_K'] = fast_k.rolling(3).mean()
    df['Stoch_D'] = df['Stoch_K'].rolling(3).mean()
    df['Stoch_Slope'] = df['Stoch_K'].diff()
    
    direction = df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    df['OBV'] = (direction * df['Volume']).cumsum()
    df['OBV_Rising'] = df['OBV'] > df['OBV'].shift(1)
    df['OBV_Slope'] = df['OBV'].diff()
    
    # 과거 데이터
    df['Prev_Close'] = df['Close'].shift(1)
    df['Prev_Vol'] = df['Volume'].shift(1)
    df['Pct'] = df['Change'] * 100
    df['Vol_Ratio'] = np.where(df['Prev_Vol'] > 0, df['Volume'] / df['Prev_Vol'], 1.0)
    df['Prev_Change'] = df['Change'].shift(1)
    df['MA20_Prev2'] = df['MA20'].shift(2)
    
    return df

# ---------------------------------------------------------
# 💯 [점수] 잠입(35점) & 수박(40점) & 황금수박(50점)
# ---------------------------------------------------------
def calculate_score(row, pattern, is_buy, is_ins, fin):
    score = 50
    details = []
    
    # 1. 재무
    if "흑자" in fin['trend']: score += 15; details.append("흑자(15)")
    elif "급증" in fin['trend']: score += 10; details.append("급증(10)")
    if "저평가" in fin['badge']: score += 15; details.append("저평가(15)")
    elif "성장" in fin['badge']: score += 10; details.append("성장(10)")
    
    # 2. 수급
    if is_buy and is_ins: score += 30; details.append("쌍끌이(30)")
    elif is_buy or is_ins: score += 10; details.append("수급(10)")
    
    # 3. 패턴 (여기가 핵심!)
    if "황금수박" in pattern: score += 50; details.append("👑황금(50)")
    elif "공구리" in pattern: score += 40; details.append("🍉공구리(40)") # 공구리된 수박
    elif "잠입" in pattern: score += 35; details.append("🥷잠입(35)")     # NEW!
    elif "골파기" in pattern: score += 30; details.append("⛏️골파기(30)")
    elif "숨고르기" in pattern: score += 30; details.append("🏳️숨고르기(30)")
    elif "돌파" in pattern: score += 15; details.append("🦁돌파(15)")
    
    # 4. 차트
    if "수박" in pattern: # 수박류는 과매도여야 좋음
        if row['RSI'] <= 30: score += 30; details.append("과매도(30)")
        elif row['RSI'] <= 40: score += 20; details.append("과매도(20)")
    else: # 일반 패턴
        if 100 <= row['Disp'] <= 105: score += 20; details.append("이격(20)")
        if row['Stoch_K'] > row['Stoch_D']: score += 5; details.append("Stoch(5)")

    # 📉 감점 (위험요소)
    warns = []
    if row['OBV_Slope'] < 0: score -= 10; warns.append("⚠️돈이탈")
    if row['Stoch_Slope'] < 0: score -= 5; warns.append("⚠️힘빠짐")
    # 수박 아닌데 10일선 꺾이면 감점
    if "수박" not in pattern and row['MA10'] < row['MA10_Prev']: 
        score -= 5; warns.append("⚠️단기저항")

    risk = " ".join(warns) if warns else "✅깨끗함"
    return score, ", ".join(details), risk

# ---------------------------------------------------------
# 🔍 [분석 엔진] 잠입 + 수박 + 공구리 + 추세필터
# ---------------------------------------------------------
def analyze_stock(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=200)).strftime('%Y-%m-%d'))
        if len(df) < 60: return None
        
        df = add_indicators(df)
        row = df.iloc[-1]
        prev = df.iloc[-2]
        
        if row['Close'] < 1000: return None
        
        # 🛑 [Global Filter] 5일 & 10일 동시 급락은 무조건 제외 (수박이라도 안 됨)
        is_crash = (row['MA5'] < row['MA5_Prev']) and (row['MA10'] < row['MA10_Prev'])
        if is_crash: return None 

        signal = None
        
        # 1. 🍉 수박 Check (공구리 필수)
        if row['Low'] <= row['Env_Lower']:
            # 공구리: 하락 각도가 완만해져야 함 (Slope 증가)
            gongguri_5 = row['MA5_Slope'] > row['MA5_Slope_Prev']
            gongguri_10 = row['MA10_Slope'] > row['MA10_Slope_Prev']
            
            if gongguri_5 and gongguri_10:
                if (row['MA20_Slope'] < 0) and (row['MA20_Slope'] > row['MA20_Slope_Prev']):
                    signal = "👑황금수박" # 20일선까지 공구리
                else:
                    signal = "🍉공구리수박" # 5/10일선 공구리
        
        # 2. 일반 패턴 (추세 필터 적용)
        else:
            # 20일선 하락 중이면 일반 패턴은 탈락
            if row['MA20'] < row['MA20_Prev']: return None

            pass_filter = True
            if not row['OBV_Rising']: pass_filter = False
            if not (30 <= row['RSI'] <= 75): pass_filter = False
            
            if pass_filter:
                                # ⭐️ [A] 골파기 (Deep Dip & Recovery) - N일간의 하락 후 복귀
                # 로직: "최근 5일 안에 20일선 붕괴가 있었고, 오늘 드디어 회복했다."
                
                is_gold_digger = False
                
                # 1. 오늘은 무조건 20일선 위에 있어야 함 (회복)
                if row['Close'] > row['MA20']:
                    
                    # 2. 어제는 20일선 밑이었어야 함 (어제까진 공포)
                    if prev['Close'] < prev['MA20']:
                        
                        # 3. 최근 5일간의 데이터를 봅니다.
                        # "멀쩡하다가 툭 떨어진 지점"이 있었는지 확인
                        # (즉, 2~5일 전에는 20일선 위에 있었던 적이 있어야 함)
                        was_above = False
                        for k in range(2, 6): # 2일전 ~ 5일전
                            if df.iloc[-k]['Close'] > df.iloc[-k]['MA20']:
                                was_above = True
                                break
                        
                        # 4. 깊이 확인: 골 파는 동안 20일선보다 최소 2% 이상은 빠졌어야 함 (겁을 줬어야 함)
                        # (최근 5일간 최저가가 20일선보다 2% 밑)
                        min_low_5days = df['Low'].tail(5).min()
                        current_ma20 = row['MA20']
                        dip_depth = ((current_ma20 - min_low_5days) / current_ma20) * 100
                        
                        if was_above and (dip_depth >= 2.0):
                            is_gold_digger = True

                # 신호 확정
                if is_gold_digger:
                    # 5. 수급 확인 (필수): 오늘 양봉이면서 거래량이 터져줘야 신뢰도 상승
                    if (row['Pct'] >= 1.0) and (row['Volume'] > prev['Volume']):
                         signal = "⛏️골파기"
                
                # [B] 🥷 잠입 (선생님 요청 부활!)
                # 조건: 거래량 40% 미만 급감 + 캔들 몸통 작음 + 20일선 위 + 지표 살아있음
                elif (row['Volume'] < prev['Volume'] * 0.4) and (abs(row['Pct']) < 1.5) and (row['Close'] > row['MA20']):
                    if (row['OBV_Slope'] >= 0) and (row['Stoch_Slope'] > -5):
                        signal = "🥷잠입"

                # [C] 숨고르기
                elif (prev['Change'] >= 0.10) and (row['Volume'] < prev['Volume'] * 0.6) and (-2.0 <= row['Pct'] <= 2.0):
                    if (row['OBV_Slope'] >= 0) and (row['Stoch_Slope'] > -5):
                        signal = "🏳️숨고르기"
                
                # [D] 돌파
                elif (row['Disp'] <= 110):
                    if (row['Vol_Ratio'] >= 1.5) and (row['Pct'] >= 1.0): signal = "🦁돌파"

        if signal:
            is_buy, is_ins, trend = get_investor_trend(ticker)
            fin = get_financial_info(ticker)
            
            score, detail, risk = calculate_score(row, signal, is_buy, is_ins, fin)
            if score < 50: return None
            
            ai_cmt = ""
            if score >= 80: ai_cmt = get_ai_summary(ticker, name, score, detail, risk)
            
            return {
                "score": score,
                "msg": f"[{signal}] {name}\n"
                       f"📊 {score}점 ({fin['badge']})\n"
                       f"💰 {trend} / {risk}\n"
                       f"📝 {detail}\n"
                       f"💵 {int(row['Close']):,}원 ({row['Pct']:+.2f}%){ai_cmt}"
            }
    except: return None

# ---------------------------------------------------------
# 🚀 메인 실행 (수정된 부분)
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"📡 [The Ultimate Bot] {TODAY_STR} 분석 시작 (수박/잠입/공구리)")
    print(f"📄 구글 시트 연동 모드 활성화")
    
    # 1. 데이터 수집
    targets = get_market_data() # get_market_data 함수가 위에 정의되어 있어야 함
    results = []
    
    # 2. 병렬 분석
    with ThreadPoolExecutor(max_workers=30) as executor:
        # analyze_stock 함수도 위에 정의되어 있어야 함 (mode='realtime')
        futures = {executor.submit(analyze_stock, t, n, 'realtime'): t for t, n in targets.items()}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)
            
    # 3. 결과 처리
    if results:
        # 총점순 정렬
        results.sort(key=lambda x: x['총점'], reverse=True)
        final_msgs = [r['msg'] for r in results[:15]] # 상위 15개 텔레그램 전송
        
        # 텔레그램 리포트 작성
        report = f"🦁 [오늘의 추천] {len(results)}개 발견\n\n" + "\n\n".join(final_msgs)
        print(report)
        send_telegram(report) # send_telegram 함수가 위에 정의되어 있어야 함
        
        # 👇👇👇 [여기가 추가된 핵심!] 구글 시트로 데이터 전송 👇👇👇
        print("-" * 50)
        update_google_sheet(results, TODAY_STR)
        print("-" * 50)

    else:
        msg = "❌ 조건에 맞는 종목이 없습니다. (시장 관망)"
        print(msg)
        send_telegram(msg)
        
        # 👇 종목이 없어도 '기존 보유 종목 수익률'은 업데이트해야 함!
        print("-" * 50)
        update_google_sheet([], TODAY_STR)
        print("-" * 50)
