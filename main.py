# ------------------------------------------------------------------
# 💎 [Ultimate Masterpiece] 전천후 AI 전략 사령부 (All-In-One 통합판)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import mplfinance as mpf
import matplotlib.pyplot as plt
import os, re, time, pytz
from bs4 import BeautifulSoup
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from bs4 import BeautifulSoup 
import pytz

# 👇 OpenAI 연결
try: from openai import OpenAI
except: OpenAI = None

# 👇 구글 시트
from google_sheet_manager import update_google_sheet
import io # 상단에 추가
import warnings
warnings.filterwarnings('ignore', category=FutureWarning) # 경고 끄기

# =================================================
# ⚙️ [1. 필수 설정] API 키 및 텔레그램 정보
# =================================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') 
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')     

TEST_MODE = True  

KST = pytz.timezone('Asia/Seoul')
current_time = datetime.now(KST)
NOW = current_time - timedelta(days=1) if current_time.hour < 8 else current_time
TODAY_STR = NOW.strftime('%Y-%m-%d')

REAL_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Referer': 'https://finance.naver.com/',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
}

# 스캔 설정
SCAN_DAYS, TOP_N = 1, 50
MIN_MARCAP = 10000000000 
STOP_LOSS_PCT = -5.0
WHALE_THRESHOLD = 50 

# ---------------------------------------------------------
# 🏥 [2] 재무 건전성 분석 (건강검진)
# ---------------------------------------------------------
def get_financial_health(code):
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = requests.get(url, headers=REAL_HEADERS, timeout=5)
        dfs = pd.read_html(res.text)
        df_fin = dfs[3]; df_fin.columns = df_fin.columns.get_level_values(1)
        # 영업이익과 부채비율 (N/A 처리)
        profit = str(df_fin.iloc[1, -2]).replace(',', '')
        debt = str(df_fin.iloc[6, -2]).replace(',', '')
        p_val = float(profit) if profit != 'nan' else 0
        d_val = float(debt) if debt != 'nan' else 999
        
        f_score = (1 if p_val > 0 else 0) + (1 if d_val < 150 else 0)
        tag = "S(우량)" if f_score == 2 else ("A(양호)" if f_score == 1 else "C(주의)")
        return tag, f_score
    except: return "N(미비)", 0

# ---------------------------------------------------------
# 🐳 [3] 수급 및 고래 베팅액 분석
# ---------------------------------------------------------
def get_supply_and_money(code, price):
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        res = requests.get(url, headers=REAL_HEADERS, timeout=5); res.encoding = 'euc-kr'
        df = pd.read_html(res.text, match='날짜')[0].dropna().head(10)
        new_cols = ['_'.join(col) if isinstance(col, tuple) else col for col in df.columns]; df.columns = new_cols
        inst_col = next((c for c in df.columns if '기관' in c and '순매매' in c), None)
        frgn_col = next((c for c in df.columns if '외국인' in c and '순매매' in c), None)
        inst_qty = [int(float(str(v).replace(',', ''))) for v in df[inst_col].values]
        frgn_qty = [int(float(str(v).replace(',', ''))) for v in df[frgn_col].values]
        
        def get_streak(data):
            c = 0
            for v in data:
                if v > 0: c += 1
                else: break
            return c
        i_s, f_s = get_streak(inst_qty), get_streak(frgn_qty)
        inst_m = round((inst_qty[0] * price) / 10000000); frgn_m = round((frgn_qty[0] * price) / 10000000)
        total_m = abs(inst_m) + abs(frgn_m)
        leader = "🤝쌍끌" if inst_m > 0 and frgn_m > 0 else ("🔴기관" if inst_m > frgn_m else "🔵외인")
        
        whale_streak = 0
        for k in range(len(df)):
            if (abs(inst_qty[k]) + abs(frgn_qty[k])) * price / 10000000 >= WHALE_THRESHOLD: whale_streak += 1
            else: break
        
        w_score = (total_m // 50) + (3 if whale_streak >= 3 else 0)
        return f"{leader}({i_s}/{f_s})", total_m, whale_streak, w_score
    except: return "⚠️오류", 0, 0, 0

# ---------------------------------------------------------
# 📈 [4] 기술적 분석 지표 (OBV, Double-GC 등)
# ---------------------------------------------------------
def get_indicators(df):
    for n in [5, 10, 20, 60, 120]: df[f'MA{n}'] = df['Close'].rolling(n).mean()
    for n in [5, 20]: df[f'VMA{n}'] = df['Volume'].rolling(n).mean()
    # OBV
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
    df['OBV_MA20'] = df['OBV'].rolling(20).mean()
    # Stochastic
    l, h = df['Low'].rolling(5).min(), df['High'].rolling(5).max()
    df['Slow_K'] = ((df['Close'] - l) / (h - l)).rolling(3).mean() * 100
    df['Slow_D'] = df['Slow_K'].rolling(3).mean()
    # BB & RSI
    df['BB_Up'] = df['Close'].rolling(20).mean() + (2 * df['Close'].rolling(20).std())
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / loss)))
    return df

# ---------------------------------------------------------
# 📸 [5] 시각화 및 텔레그램 전송 함수 (선생님 요청 통합)
# ---------------------------------------------------------
def create_index_chart(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=100)))
        mc = mpf.make_marketcolors(up='r', down='b', inherit=True)
        s  = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=False)
        fname = f"{name}.png"
        mpf.plot(df, type='candle', style=s, title=f"\n{name} Index", savefig=fname, figsize=(8, 4))
        return fname
    except: return None

def send_telegram_photo(message, image_paths=[]):
    if TEST_MODE: print(f"📝 [TEST] {message}"); return
    url_p = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    url_t = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        if message: requests.post(url_t, data={'chat_id': chat_id, 'text': message[:4000]})
        for img in image_paths:
            if img and os.path.exists(img):
                with open(img, 'rb') as f: requests.post(url_p, data={'chat_id': chat_id}, files={'photo': f})
                os.remove(img)

# ---------------------------------------------------------
# 🧠 [6] AI 브리핑 및 토너먼트 (GPT + Groq)
# ---------------------------------------------------------
def get_hot_themes():
    try:
        res = requests.get("https://finance.naver.com/sise/theme.naver", headers=REAL_HEADERS)
        soup = BeautifulSoup(res.text, 'html.parser')
        themes = [t.text.strip() for t in soup.select('table.type_1 td.col_type1')[:3]]
        return ", ".join(themes)
    except: return "테마수집불가"

def get_market_briefing():
    try:
        theme_info = get_hot_themes()
        prompt = f"오늘 코스피/나스닥 흐름과 {theme_info} 테마를 바탕으로 개장전/마감 전략 3줄 요약해줘(반말)."
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"user", "content":prompt}])
        return f"🌇 [시황 브리핑]\n{res.choices[0].message.content.strip()}"
    except: return "브리핑 생성 실패"

def run_ai_tournament(candidate_list):
    if not candidate_list: return "후보 없음"
    candidate_list = sorted(candidate_list, key=lambda x: x['점수'], reverse=True)[:15]
    prompt_data = "\n".join([f"- {c['종목명']}({c['code']}): {c['구분']}, 수급:{c['수급']}, 재무:{c['재무']}" for c in candidate_list])
    
    sys_prompt = "너는 전설적인 투자자야. 절대 돈을 잃으면 안되는 상화이야. 타율이 높은 종목으로 꼭 골라줘. 단타 종목 1위와 스윙 종목 1위를 각각 선정하고 짧은 이유를 말해줘."
    
    # GPT 심사
    client = OpenAI(api_key=OPENAI_API_KEY)
    res_gpt = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"system", "content":sys_prompt}, {"role":"user", "content":prompt_data}])
    # Groq 심사 (Llama)
    res_groq = requests.post("https://api.groq.com/openai/v1/chat/completions", 
                             json={"model":"llama-3.3-70b-versatile", "messages":[{"role":"system", "content":sys_prompt}, {"role":"user", "content":prompt_data}]},
                             headers={"Authorization": f"Bearer {GROQ_API_KEY}"})
    
    groq_text = res_groq.json()['choices'][0]['message']['content'] if res_groq.status_code == 200 else "Groq 연결 실패"
    return f"🏆 [AI 토너먼트 결승]\n\n🧠 [GPT]: {res_gpt.choices[0].message.content}\n\n⚡ [Groq]: {groq_text}"

def get_ai_summary(ticker, name, tags):
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"user", "content":f"{name}({ticker}) 주식 최고 트레이더 입장에서 종목의 최근 핵심 테마와 특징을 한줄로 요약해(반말)."}])
        return res.choices[0].message.content.strip()
    except: return "분석 불가"

# ---------------------------------------------------------
# 🕵️‍♂️ [7] 분석 엔진 (당일 집중형 - 중복 방지)
# ---------------------------------------------------------
def analyze_final(ticker, name):
    try:
        # 1. 지표 계산을 위해 과거 데이터를 충분히 가져옵니다.
        df = fdr.DataReader(ticker, start=(datetime.now()-timedelta(days=250)))
        if len(df) < 100: return []
        
        # 2. 보조지표 계산 (MA, OBV, Stochastic 등)
        df = get_indicators(df)
        
        # 3. 💡 반복문 제거! 마지막(오늘) 데이터와 그 직전(어제) 데이터만 딱 집습니다.
        # iloc[-1]은 가장 최신 날짜, iloc[-2]는 바로 전날입니다.
        row = df.iloc[-1]
        prev = df.iloc[-2]
        curr_idx = df.index[-1] # 오늘 날짜
        
        score, tags = 0, []
        
        # --- [전략 1: Double GC] ---
        # 오늘 골든크로스가 발생했는지 확인
        is_p_gc = prev['MA5'] <= prev['MA20'] and row['MA5'] > row['MA20']
        is_v_gc = prev['VMA5'] <= prev['VMA20'] and row['VMA5'] > row['VMA20']
        if is_p_gc and is_v_gc: 
            tags.append("✨Double-GC"); score += 5
        
        # --- [전략 2: OBV 매집 & 공구리] ---
        if row['OBV'] > row['OBV_MA20']: 
            tags.append("🌊OBV매집"); score += 2
            
        # 💡 공구리: 오늘 종가가 지난 25일간의 고점을 돌파했는지 확인
        box_h = df['High'].iloc[-26:-1].max() 
        if row['Close'] > box_h: 
            tags.append("🔨공구리"); score += 3
        
        # --- [전략 3: 수박(Stochastic)] ---
        if prev['Slow_K'] <= prev['Slow_D'] and row['Slow_K'] > row['Slow_D'] and row['Slow_K'] < 75:
            tags.append("🍉수박"); score += 2

        # 4. 아무런 신호가 없다면 즉시 종료
        if not tags: return []

        # 5. 수급 및 재무 데이터 가져오기 (신호가 뜬 종목만 정밀 분석)
        s_tag, total_m, w_streak, whale_score = get_supply_and_money(ticker, row['Close'])
        f_tag, f_score = get_financial_health(ticker)
        score += (whale_score + f_score)

        # 6. 결과 리턴 (리스트 안에 딕셔너리 딱 1개만 담깁니다)
        return [{
            '날짜': curr_idx.strftime('%m-%d'), 
            '점수': score, 
            '종목명': name, 
            'code': ticker,
            '구분': " ".join(tags), 
            '재무': f_tag, 
            '수급': s_tag, 
            '베팅액': total_m, 
            '진단': "✅양호"
        }]
    except: 
        return []

# ---------------------------------------------------------
# 🚀 [8] 메인 실행 (전략 사령부 가동)
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 전략 사령부 가동 시작...")
    # 1. 시황 및 차트 준비
    imgs = [create_index_chart('KS11', 'KOSPI'), create_index_chart('IXIC', 'NASDAQ')]
    briefing = get_market_briefing()
    
    # 2. 전 종목 스캔
    df_krx = fdr.StockListing('KRX')
    target_dict = dict(zip(df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)['Code'], df_krx['Name']))
    sector_dict = {} # (필요시 추가)
    
    all_hits = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(analyze_final, t, n) for t, n in target_dict.items()]
        for f in futures: 
            res = f.result()
            if res: all_hits.extend(res)
        
if all_hits:
    # 3. 데이터 정렬 및 전송 준비
    # 3-1. 리스트를 데이터프레임으로 변환
    df_res = pd.DataFrame(all_hits)
    # 2. 종목코드를 기준으로 가장 최신 날짜(혹은 높은 점수)만 남기고 중복 제거
    df_res = df_res.sort_values(by=['code', '날짜', '점수'], ascending=[True, False, False])
    df_res = df_res.drop_duplicates(subset=['code'], keep='first')
    # 3. 다시 리스트로 변환
    all_hits = df_res.to_dict('records')
    tournament_report = run_ai_tournament(all_hits)
        
    MAX_CHAR = 3800  # 여유 있게 3,800자로 설정
    current_msg = f"{briefing}\n\n📢 [오늘의 추천주]\n\n"
        
    # 4. 종목별 본문 구성 및 실시간 분할
    for item in sorted_hits:
        ai_tip = get_ai_summary(item['code'], item['종목명'], item['구분'])
    # 종목별 엔트리 생성 (구분선 포함)
        entry = (f"⭐{item['점수']}점 [{item['종목명']}] {item['구분']}\n"
                f"- 재무: {item['재무']} | 수급: {item['수급']}\n"
                f"💡 {ai_tip}\n"
                f"----------------------------\n")
        # 길이 체크: 현재 메시지에 이번 종목을 더했을 때 한도를 넘는지 확인
        if len(current_msg) + len(entry) > MAX_CHAR:
            # 한도를 넘으면 지금까지 만든 메시지를 사진과 함께(첫 전송일 때만) 발송
            send_telegram_photo(current_msg, imgs if imgs else [])
            imgs = [] # 사진은 한 번만 보내면 되므로 비움

            print(current_msg)
            # 새 메시지 시작
            current_msg = "📢 [오늘의 추천주 - 이어서]\n\n" + entry
        else:
            current_msg += entry

    # 5. AI 토너먼트 결과 추가
    final_block = f"\n{tournament_report}"
    
    if len(current_msg) + len(final_block) > MAX_CHAR:
        # 토너먼트 리포트가 들어가기에 자리가 부족하면 나눠서 전송
        send_telegram_photo(current_msg, imgs if imgs else [])
        current_msg = "🏆 [AI 토너먼트 최종 결과]\n" + final_block
    else:
        current_msg += final_block

    # 6. 최종 남은 메시지 전송
    send_telegram_photo(current_msg, imgs if imgs else [])

    # 7. 구글 시트 업데이트 (별도 관리)
    try:
        update_google_sheet(all_hits, TODAY_STR)
    except:
        pass
        
    print(current_msg)            
    print("✅ 모든 리포트가 전송되었습니다!")
