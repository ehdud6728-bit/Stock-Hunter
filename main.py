# ------------------------------------------------------------------
!pip install finance-datareader requests lxml beautifulsoup4 gspread oauth2client pytz
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
from datetime import datetime, timedelta
from io import StringIO
from concurrent.futures import ThreadPoolExecutor # 👈 멀티태스킹 필수
import pytz # 👈 한국 시간 필수

# 👇 구글 시트 매니저 불러오기
from google_sheet_manager import update_google_sheet

# =================================================
# ⚙️ 설정
# =================================================
TOP_N = 300           
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '').strip()

# 🌍 [시간 설정] 한국 시간(KST) 기준
KST = pytz.timezone('Asia/Seoul')
current_time = datetime.now(KST)

if current_time.hour < 8:
    NOW = current_time - timedelta(days=1)
    print(f"🌙 야간 모드(00~08시): {NOW.strftime('%Y-%m-%d')} 기준 분석")
else:
    NOW = current_time
    print(f"☀️ 주간 모드: {NOW.strftime('%Y-%m-%d')} 기준 분석")

TODAY_STR = NOW.strftime('%Y-%m-%d')

# ---------------------------------------------------------
# 📨 텔레그램
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    real_id_list = []
    for item in CHAT_ID_LIST:
        real_id_list.extend([x.strip() for x in item.split(',') if x.strip()])
    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
    for chat_id in real_id_list:
        if not chat_id: continue
        for chunk in chunks:
            try: requests.post(url, data={'chat_id': chat_id, 'text': chunk})
            except: pass

# ---------------------------------------------------------
# 🤖 AI 요약
# ---------------------------------------------------------
def get_ai_summary(ticker, name, score, details, risk):
    if not GROQ_API_KEY: return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    prompt = f"종목: {name}\n점수: {score}\n특징: {details}\n위험: {risk}\n한줄 매매 전략 요약 (한국어)"
    payload = {"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}]}
    try: return "\n💡 " + requests.post(url, json=payload, headers=headers, timeout=5).json()['choices'][0]['message']['content'].strip()
    except: return ""

# ---------------------------------------------------------
# ⚡ 데이터 수집
# ---------------------------------------------------------
def get_market_data():
    print(f"⚡ 거래대금 상위 {TOP_N}개 스캔 중...")
    try:
        df_krx = fdr.StockListing('KRX')
        df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
        return dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    except: return {}

def get_investor_trend(code):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        resp = requests.get(url, headers=headers, timeout=5)
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
        
        is_buy = foreigner > 0; is_ins = institution > 0
        trend = "🚀쌍끌이" if (is_buy and is_ins) else ("👨🏼‍🦰외인" if is_buy else ("🏢기관" if is_ins else "💧개인"))
        return is_buy, is_ins, trend
    except: return False, False, "분석불가"

def get_financial_info(code):
    res = {"trend": "", "badge": "⚖️보통"}
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        dfs = pd.read_html(StringIO(resp.text), header=0)
        fin_df = None
        for df in dfs:
            if '최근 연간 실적' in str(df.columns) or '주요재무제표' in str(df.columns): fin_df = df; break
        if fin_df is not None:
            if len(fin_df.columns) > 0: fin_df = fin_df.set_index(fin_df.columns[0])
            if '영업이익' in fin_df.index:
                vals = [float(str(v).replace(',', '')) for v in fin_df.loc['영업이익'].values if str(v).replace(',', '').replace('-','').isdigit()]
                if len(vals) >= 2 and vals[-2] < 0 and vals[-1] > 0: res['trend'] = "🐢흑자전환"
            
            def get_v(k): return float(str(fin_df.loc[k].values[-1]).replace(',', '')) if k in fin_df.index else 0
            per, pbr, eps = get_v('PER(배)'), get_v('PBR(배)'), get_v('EPS(원)')
            if eps < 0: res['badge'] = "⚠️적자"
            elif eps > 0 and per < 10 and pbr < 1.0: res['badge'] = "💎저평가"
            elif eps > 0 and per >= 15: res['badge'] = "💰성장주"
    except: pass
    return res

# ---------------------------------------------------------
# 📊 지표 계산
# ---------------------------------------------------------
def add_indicators(df):
    df['MA5'] = df['Close'].rolling(5).mean()
    df['MA10'] = df['Close'].rolling(10).mean()
    df['MA20'] = df['Close'].rolling(20).mean()
    
    df['MA5_Slope'] = df['MA5'].diff()
    df['MA5_Slope_Prev'] = df['MA5_Slope'].shift(1)
    df['MA10_Slope'] = df['MA10'].diff()
    df['MA10_Slope_Prev'] = df['MA10_Slope'].shift(1)
    df['MA20_Slope'] = df['MA20'].diff()
    df['MA20_Slope_Prev'] = df['MA20_Slope'].shift(1)
    
    df['MA5_Prev'] = df['MA5'].shift(1)
    df['MA10_Prev'] = df['MA10'].shift(1)
    df['MA20_Prev'] = df['MA20'].shift(1)
    
    df['Env_Lower'] = df['MA20'] * 0.85 
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
    
    df['Prev_Close'] = df['Close'].shift(1)
    df['Prev_Vol'] = df['Volume'].shift(1)
    df['Pct'] = df['Change'] * 100
    df['Vol_Ratio'] = np.where(df['Prev_Vol'] > 0, df['Volume'] / df['Prev_Vol'], 1.0)
    
    return df

# ---------------------------------------------------------
# 💯 점수 계산 (4가지 점수 반환 수정됨)
# ---------------------------------------------------------
def calculate_score(row, pattern, is_buy, is_ins, fin):
    score = 50; details = []
    
    # 1. 재무
    if "흑자" in fin['trend']: score += 15; details.append("흑자(15)")
    if "저평가" in fin['badge']: score += 15; details.append("저평가(15)")
    elif "성장" in fin['badge']: score += 10; details.append("성장(10)")
    
    # 2. 수급
    s_score = 0
    if is_buy and is_ins: s_score = 30; score += 30; details.append("쌍끌이(30)")
    elif is_buy or is_ins: s_score = 10; score += 10; details.append("수급(10)")
    
    # 3. 패턴
    p_score = 0
    if "황금수박" in pattern: p_score = 50; score += 50; details.append("👑황금(50)")
    elif "공구리" in pattern: p_score = 40; score += 40; details.append("🔨공구리(40)")
    elif "잠입" in pattern: p_score = 35; score += 35; details.append("🥷잠입(35)")
    elif "골파기" in pattern: p_score = 30; score += 30; details.append("⛏️골파기(30)")
    elif "숨고르기" in pattern: p_score = 30; score += 30; details.append("🏳️숨고르기(30)")
    elif "돌파" in pattern: p_score = 15; score += 15; details.append("🦁돌파(15)")
    
    # 4. 차트
    c_score = 0
    if "수박" in pattern: 
        if row['RSI'] <= 30: c_score = 30; score += 30; details.append("과매도(30)")
    else:
        if 100 <= row['Disp'] <= 105: c_score = 20; score += 20; details.append("이격(20)")
        if row['Stoch_K'] > row['Stoch_D']: c_score += 5; score += 5; details.append("Stoch(5)")

    warns = []
    if row['OBV_Slope'] < 0: score -= 10; warns.append("⚠️돈이탈")
    if row['Stoch_Slope'] < 0: score -= 5; warns.append("⚠️힘빠짐")
    if "수박" not in pattern and row['MA10'] < row['MA10_Prev']: score -= 5; warns.append("⚠️단기저항")

    risk = " ".join(warns) if warns else "✅깨끗함"
    
    # 구글 시트에 넣기 위해 세부 점수도 다 리턴합니다!
    return score, s_score, p_score, c_score, risk, ", ".join(details)

# ---------------------------------------------------------
# 🔍 [수정됨] 분석 엔진 (에러 해결 & 골파기 강화)
# ---------------------------------------------------------
def analyze_stock(ticker, name, mode='realtime'): # 👈 3번째 인자 추가 완료!
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=200)).strftime('%Y-%m-%d'))
        if len(df) < 60: return None
        df = add_indicators(df)
        row = df.iloc[-1]; prev = df.iloc[-2]
        
        if row['Close'] < 1000: return None
        # 급락 제외
        if (row['MA5'] < row['MA5_Prev']) and (row['MA10'] < row['MA10_Prev']): return None 

        signal = None
        
        # 1. 🍉 수박
        if row['Low'] <= row['Env_Lower']:
            if (row['MA5_Slope'] > row['MA5_Slope_Prev']) and (row['MA10_Slope'] > row['MA10_Slope_Prev']):
                signal = "👑황금수박" if (row['MA20_Slope'] < 0 and row['MA20_Slope'] > row['MA20_Slope_Prev']) else "🍉공구리수박"
        
        # 2. 일반
        else:
            if row['MA20'] < row['MA20_Prev']: return None # 20일선 하락 제외
            if not row['OBV_Rising']: return None
            if not (30 <= row['RSI'] <= 75): return None
            
            # ⭐️ [골파기] 심화 로직 (Deep Dip)
            # 최근 5일 내에 20일선 붕괴 -> 2% 이상 깊이 -> 오늘 회복
            if row['Close'] > row['MA20'] and prev['Close'] < prev['MA20']:
                 min_low = df['Low'].iloc[-5:-1].min() # 최근 5일 저가
                 dip = ((row['MA20'] - min_low) / row['MA20']) * 100
                 if dip >= 2.0 and row['Pct'] >= 1.0: # 깊이 2% 이상 + 오늘 1% 이상 상승
                     signal = "⛏️골파기"
            
            # [잠입] 거래량 급감
            elif (row['Volume'] < prev['Volume'] * 0.4) and (abs(row['Pct']) < 1.5) and (row['Close'] > row['MA20']):
                if (row['OBV_Slope'] >= 0) and (row['Stoch_Slope'] > -5): signal = "🥷잠입"
            
            # [숨고르기]
            elif (prev['Change'] >= 0.10) and (row['Volume'] < prev['Volume'] * 0.6) and (-2.0 <= row['Pct'] <= 2.0):
                if (row['OBV_Slope'] >= 0) and (row['Stoch_Slope'] > -5): signal = "🏳️숨고르기"
            
            # [돌파]
            elif (row['Disp'] <= 110) and (row['Vol_Ratio'] >= 1.5) and (row['Pct'] >= 1.0): signal = "🦁돌파"

        if signal:
            is_buy, is_ins, trend = get_investor_trend(ticker)
            fin = get_financial_info(ticker)
            
            # 점수 계산 (6개 값 받아옴)
            score, s_p, p_p, c_p, risk, detail = calculate_score(row, signal, is_buy, is_ins, fin)
            
            if score < 50: return None
            
            supply_status = trend
            ai_cmt = ""
            if score >= 80: ai_cmt = get_ai_summary(ticker, name, score, detail, risk)
            
            # 구글 시트에 넣을 데이터 구조
            return {
                'code': ticker,
                '종목명': name, '현재가': int(row['Close']), '등락률': f"{row['Pct']:.2f}%",
                '신호': signal, '총점': score, '수급점수': s_p, '패턴점수': p_p, '차트점수': c_p,
                '수급현황': supply_status, 'Risk': risk,
                'msg': f"[{signal}] {name}\n📊 {score}점 ({fin['badge']})\n💰 {supply_status} / {risk}\n📝 {detail}\n💵 {int(row['Close']):,}원 ({row['Pct']:+.2f}%){ai_cmt}"
            }
            
    except: return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"📡 [The Ultimate Bot] {TODAY_STR} 분석 시작")
    print(f"📄 구글 시트 연동 활성화")
    
    targets = get_market_data()
    results = []
    
    with ThreadPoolExecutor(max_workers=30) as executor:
        # ⭐️ 3번째 인자 'realtime'이 자동으로 전달됨 (에러 해결!)
        futures = {executor.submit(analyze_stock, t, n, 'realtime'): t for t, n in targets.items()}
        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
                if res: results.append(res)
            except Exception as e:
                pass
            
    if results:
        results.sort(key=lambda x: x['총점'], reverse=True)
        final_msgs = [r['msg'] for r in results[:15]]
        
        report = f"🦁 [오늘의 추천] {len(results)}개 발견\n\n" + "\n\n".join(final_msgs)
        print(report)
        send_telegram(report)
        
        print("-" * 50)
        # 구글 시트에 기록
        update_google_sheet(results, TODAY_STR)
        print("-" * 50)
    else:
        msg = "❌ 조건에 맞는 종목이 없습니다. (시장 관망)"
        print(msg)
        send_telegram(msg)
        print("-" * 50)
        # 종목 없어도 기존 종목 업데이트는 실행
        update_google_sheet([], TODAY_STR)
        print("-" * 50)
