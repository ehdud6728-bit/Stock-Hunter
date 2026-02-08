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

TEST_MODE = TRUE  

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
SCAN_DAYS, TOP_N = 1, 400
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
    df = df.copy()
    for n in [5, 20, 60]:
        df[f'MA{n}'] = df['Close'].rolling(n).mean()
        df[f'VMA{n}'] = df['Volume'].rolling(n).mean()
        df[f'Slope{n}'] = (df[f'MA{n}'] - df[f'MA{n}'].shift(3)) / df[f'MA{n}'].shift(3) * 100
    
    df['Disparity'] = (df['Close'] / df['MA20']) * 100
    std = df['Close'].rolling(20).std()
    df['BB_Upper'] = df['MA20'] + (std * 2)
    df['BB_Width'] = (df['BB_Upper'] - (df['MA20'] - (std * 2))) / df['MA20'] * 100
    df['BB40_Upper'] = df['Close'].rolling(window=40).mean() + (df['Close'].rolling(window=40).std() * 2)
    
    # 💡 [스토캐스틱 슬로우 12-5-5]
    l_min, h_max = df['Low'].rolling(12).min(), df['High'].rolling(12).max()
    df['Sto_K'] = ((df['Close'] - l_min) / (h_max - l_min)) * 100
    df['Sto_D'] = df['Sto_K'].rolling(5).mean()
    df['Sto_SD'] = df['Sto_D'].rolling(5).mean()
    
    # DMI/ADX
    high, low, close = df['High'], df['Low'], df['Close']
    tr = pd.concat([high - low, abs(high - close.shift(1)), abs(low - close.shift(1))], axis=1).max(axis=1)
    df['pDI'] = (pd.Series(np.where((high-high.shift(1) > low.shift(1)-low), (high-high.shift(1)).clip(lower=0), 0)).rolling(14).sum().values / tr.rolling(14).sum().values) * 100
    df['mDI'] = (pd.Series(np.where((low.shift(1)-low > high-high.shift(1)), (low.shift(1)-low).clip(lower=0), 0)).rolling(14).sum().values / tr.rolling(14).sum().values) * 100
    df['ADX'] = ((abs(df['pDI'] - df['mDI']) / (df['pDI'] + df['mDI'])) * 100).rolling(14).mean()
    
    df['MACD_Hist'] = (df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()) - (df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()).ewm(span=9).mean()
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
    df['OBV_Slope'] = (df['OBV'] - df['OBV'].shift(5)) / df['OBV'].shift(5).abs() * 100
    df['Base_Line'] = df['Close'].rolling(20).min().shift(5)
    return df
    
# ---------------------------------------------------------
# 🏛️ [4-1] 역사적 지수 데이터 통합 로직
# ---------------------------------------------------------
def prepare_historical_weather():
    start_point = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
    
    # 3대 지수 호출
    ndx = fdr.DataReader('^IXIC', start=start_point)[['Close']]
    sp5 = fdr.DataReader('^GSPC', start=start_point)[['Close']]
    vix = fdr.DataReader('^VIX', start=start_point)[['Close']]
    
    # 각 지수별 MA5 계산
    ndx['ixic_ma5'] = ndx['Close'].rolling(5).mean()
    sp5['sp500_ma5'] = sp5['Close'].rolling(5).mean()
    vix['vix_ma5'] = vix['Close'].rolling(5).mean()
    
    # 컬럼명 변경 후 결합
    weather_df = pd.concat([
        ndx.rename(columns={'Close': 'ixic_close'}),
        sp5.rename(columns={'Close': 'sp500_close'}),
        vix.rename(columns={'Close': 'vix_close'})
    ], axis=1).fillna(method='ffill')
    
    return weather_df

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
        prompt = f"당신은 전세계 최고의 퀀트 분석가 및 월가 최고 수준의 리서치 애널리스트 입니다. 오늘 장 준비 전 코스피/나스닥 흐름과 {theme_info} 테마를 바탕으로 개장전/마감 전략 3줄 요약해줘(반말)."
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"user", "content":prompt}])
        return f"🌇 [시황 브리핑]\n{res.choices[0].message.content.strip()}"
    except: return "브리핑 생성 실패"

def run_ai_tournament(candidate_list):
    if not candidate_list: return "후보 없음"
    candidate_list = sorted(candidate_list, key=lambda x: x['점수'], reverse=True)[:15]
    prompt_data = "\n".join([f"- {c['종목명']}({c['code']}): {c['구분']}, 수급:{c['수급']}, 재무:{c['재무']}" for c in candidate_list])
    
    sys_prompt = (
        "당신은 대한민국 '역매공파(역배열바닥, 매집, 공구리돌파, 파동시작)' 매매법의 권위자이자 퀀트 분석가입니다. 절대 돈을 잃으면 안되는 상황이야."
        "제공된 기술적 데이터를 분석하여 2024년 12월 24일 '재영솔루텍'과 같은 "
        "역배열 바닥 매집형 급등 패턴인지 엄격하게 심사하십시오."
        "단타 종목 1위와 스윙 종목 1위를 선정하고 각각 5백만달러 수준의 리포트 브리핑을 간략하게 알려줘 "
    )
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
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"user", "content":f"{name}({ticker}) 주식 최고 트레이더 입장에서 매매에 꼭 필요한 종목의 최근 핵심 테마와 특징을 한줄로 요약해(반말) 뻔한 이야기는 금지."}])
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
        storm_count = 0
        weather_icons = []

        # 수급 및 재무 데이터 가져오기 (신호가 뜬 종목만 정밀 분석)
        s_tag, total_m, w_streak, whale_score = get_supply_and_money(ticker, row['Close'])
        f_tag, f_score = get_financial_health(ticker)
        score += (whale_score + f_score)
        
        # --- [A] 기술적 신호 판정 ---
        is_sto_gc = prev['Sto_D'] <= prev['Sto_SD'] and row['Sto_D'] > row['Sto_SD']
        is_vma_gc = prev['VMA5'] <= prev['VMA20'] and row['VMA5'] > row['VMA20']
        is_bb_brk = prev['Close'] <= prev['BB_Upper'] and row['Close'] > row['BB_Upper']
        is_melon = twin_b and row['OBV_Slope'] > 0 and row['ADX'] > 20 and row['MACD_Hist'] > 0
        is_nova = is_sto_gc and is_vma_gc and is_bb_brk and is_melon
        is_bb40_brk = prev['Close'] <= prev['BB40_Upper'] and row['Close'] > row['BB40_Upper']

        # --- [B-1] 🎯 재영솔루텍 패턴 매칭 (Legend Filter) --- 역매공파
        # 1. 이격도가 바닥권인가? (98~104)
        is_bottom = 98 <= row['Disparity'] <= 104
        # 2. 거래량이 실리며 에너지가 도는가?
        is_energy = row['OBV_Slope'] > 0 and row['MACD_Hist'] > 0
        # 3. 고래가 입질을 시작했는가?
        is_whale = whale_score > 5
        
        # 레전드 점수 계산 (재영솔루텍 조건 충족 시 폭등)
        legend_score = 0
        if is_bottom and is_energy and is_vma_gc:
            legend_score = 50 # 🏆 레전드 패턴 가산점

        # 1. 나스닥 판정
        if row['ixic_close'] > row['ixic_ma5']: weather_icons.append("☀️")
        else: weather_icons.append("🌪️"); storm_count += 1
        
        # 2. S&P500 판정
        if row['sp500_close'] > row['sp500_ma5']: weather_icons.append("☀️")
        else: weather_icons.append("🌪️"); storm_count += 1
        
        # 3. VIX 판정 (VIX는 낮을 때가 맑음)
        if row['vix_close'] < row['vix_ma5']: weather_icons.append("☀️")
        else: weather_icons.append("🌪️"); storm_count += 1
        
        # --- [C] 점수 산출 (당시 기상도 반영) ---
        s_score = int(90 + (30 if is_nova else 15 if is_melon else 0))
        s_score -= (storm_count * 10) # 🌪️ 1개당 10점 감점

        if row['OBV_Slope'] < 0: s_score -= 20
        s_score -= max(0, int((row['Disparity']-105)*4))

        # 꼬리% 계산
        t_pct = int((row['High']-max(row['Open'],row['Close']))/(row['High']-row['Low'])*100) if row['High']!=row['Low'] else 0
        if t_pct > 40: s_score -= 15

        # 4. 볼린저밴드(40,2) 돌파했는가?
        if is_bb40_brk:
            s_score += 40  # 장기 추세 돌파는 매우 강력한 가점 대상!

        # 태그 생성
        tags = [t for t, c in zip(["🚀슈퍼타점","🍉수박","Sto-GC","VMA-GC","BB-Break","5일선","🏆LEGEND","🚨장기돌파" ], 
                                  [is_nova, is_melon, is_sto_gc, is_vma_gc, is_bb_brk, row['Close']>row['MA5'], legend_score >= 50, is_bb40_brk]) if c]
        if not tags: continue

        # --- [전략 1: Double GC] --- > 기존 전략 그래도 놔둔다.
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

        # 6. 결과 리턴 (리스트 안에 딕셔너리 딱 1개만 담깁니다)
        return [{
            '날짜': curr_idx.strftime('%Y-%m-%d'),
            '기상': "".join(weather_icons), # 💡 기상도 컬럼 추가
            '안전': int(max(0, s_score)), 
            '점수': score, 
            '에너지': "🔋" if row['MACD_Hist']>0 else "🪫",
            'OBV기울기': int(row['OBV_Slope']),
            '종목명': name, 
            'code': ticker,
            '꼬리%': t_pct, 
            '이격': int(row['Disparity']),
            '재무': f_tag, 
            '수급': s_tag, 
            '베팅액': total_m, 
            '구분': " ".join(tags),
            '진단': "✅양호"
        }]
    except: 
        return []

# ---------------------------------------------------------
# 🚀 [8] 메인 실행 (전략 사령부 가동)
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 전략 사령부 가동 시작...")
    m_ndx = get_safe_macro('^IXIC', '나스닥')
    m_sp5 = get_safe_macro('^GSPC', 'S&P500')
    m_vix = get_safe_macro('^VIX', 'VIX공포')
    m_fx  = get_safe_macro('USD/KRW', '달러환율')
    macro_status = {'nasdaq': m_ndx, 'sp500': m_sp5, 'vix': m_vix, 'fx': m_fx , 'kospi': {get_index_investor_data('KOSPI')}}

    print("\n" + "🌍 " * 5 + "[ 글로벌 사령부 통합 관제 센터 ]" + " 🌍" * 5)
    print(f"🇺🇸 {m_ndx['text']} | {m_sp5['text']} | ⚠️ {m_vix['text']}")
    print(f"💵 {m_fx['text']} | 🇰🇷 KOSPI 수급: {get_index_investor_data('KOSPI')}")
    print("=" * 115)
    
    # 1. 시황 및 차트 준비
    imgs = [create_index_chart('KS11', 'KOSPI'), create_index_chart('IXIC', 'NASDAQ')]
    briefing = get_market_briefing()
    
    # 2. 전 종목 스캔
    df_krx = fdr.StockListing('KRX')
    target_dict = dict(zip(df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)['Code'], df_krx['Name']))
    weather_data = prepare_historical_weather()
    sector_dict = {} # (필요시 추가)
    
    all_hits = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(analyze_final, t, n) for t, n in target_dict.items()]
        for f in futures: 
            res = f.result()
            if res: all_hits.extend(res)
        
if all_hits:
    # 3. 데이터 정렬 및 전송 준비
    # 점수 순 정렬 (중복은 analyze_final에서 이미 차단됨)
    sorted_hits = sorted(all_hits, key=lambda x: x['점수'], reverse=True)[:15]
    tournament_report = run_ai_tournament(all_hits)
        
    MAX_CHAR = 3800  # 여유 있게 3,800자로 설정
    current_msg = f"{briefing}\n\n📢 [오늘의 추천주]\n\n"
        
    # 4. 종목별 본문 구성 및 실시간 분할
    for item in sorted_hits:
        ai_tip = get_ai_summary(item['code'], item['종목명'], item['구분'])
        # 종목별 엔트리 생성 (구분선 포함)
        entry = (f"⭐{item['점수']}점 {item['안전']}점 [{item['종목명']}]}\n"
                f"- {item['구분']}\n"
                f"- 재무: {item['재무']} | 수급: {item['수급']}\n"
                f"💡 {ai_tip}\n"
                f"----------------------------\n")
        # 길이 체크: 현재 메시지에 이번 종목을 더했을 때 한도를 넘는지 확인
        if len(current_msg) + len(entry) > MAX_CHAR:
            # 한도를 넘으면 지금까지 만든 메시지를 사진과 함께(첫 전송일 때만) 발송
            send_telegram_photo(current_msg, imgs if imgs else [])
            imgs = [] # 사진은 한 번만 보내면 되므로 비움
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
