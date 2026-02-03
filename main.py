
!pip install finance-datareader requests lxml beautifulsoup4
# ------------------------------------------------------------------
# 1️⃣ [필수 로딩] 라이브러리 임포트 (이게 빠져서 죄송했습니다!)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
from datetime import datetime, timedelta
from io import StringIO
from concurrent.futures import ThreadPoolExecutor

# =================================================
# ⚙️ [설정] 백테스트 & 실전 통합 설정
# =================================================
TEST_DAYS = 90        # 백테스트 기간 (최근 3달)
HOLDING_DAYS = 15     # 보유 기간
TOP_N = 300           # 검색 대상 (300개로 확장 - 수박/잠입 포착용)
# =================================================

# ---------------------------------------------------------
# 🌍 [시간 설정] 야간/새벽에는 '어제' 기준으로 분석
# ---------------------------------------------------------
current_time = datetime.now()
if current_time.hour < 8:
    NOW = current_time - timedelta(days=1)
    print(f"🌙 야간 모드: {NOW.strftime('%Y-%m-%d')} 기준 분석")
else:
    NOW = current_time
TODAY_STR = NOW.strftime('%Y-%m-%d')

# 환경변수 (GitHub용)
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '').strip()

# ---------------------------------------------------------
# 📨 텔레그램 전송 함수
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST:
        # 토큰 없으면 화면에만 출력하고 끝
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    real_id_list = []
    for item in CHAT_ID_LIST:
        real_id_list.extend([x.strip() for x in item.split(',') if x.strip()])

    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
    for chat_id in real_id_list:
        if not chat_id: continue
        for chunk in chunks:
            try:
                requests.post(url, data={'chat_id': chat_id, 'text': chunk})
                time.sleep(0.5)
            except: pass

# ---------------------------------------------------------
# 🤖 AI 요약 (옵션)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, score, details, risk):
    if not GROQ_API_KEY: return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    prompt = (f"종목: {name}\n점수: {score}\n패턴: {details}\n위험: {risk}\n"
              f"이 종목의 핵심 매매 전략 1줄 요약 (한국어)")
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
# ⚡ 시장 데이터 확보
# ---------------------------------------------------------
def get_market_data():
    print(f"⚡ [데이터 수집] 거래대금 상위 {TOP_N}개 종목 스캔 중...")
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
        
        # '날짜'가 포함된 테이블만 추출
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
                "👨🏼‍🦰외인" if (is_for_buy) else \
                "🏢기관" if (is_ins_buy) else "💧개인"
        return is_for_buy, is_ins_buy, trend
    except: return False, False, "크롤링실패"

# ---------------------------------------------------------
# 🏢 [재무] 실적 데이터
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
# 📊 [지표] 공구리/기울기/보조지표
# ---------------------------------------------------------
def add_indicators(df):
    df['MA5'] = df['Close'].rolling(5).mean()
    df['MA10'] = df['Close'].rolling(10).mean()
    df['MA20'] = df['Close'].rolling(20).mean()
    
    # 기울기 계산
    df['MA5_Slope'] = df['MA5'].diff()
    df['MA5_Slope_Prev'] = df['MA5_Slope'].shift(1)
    df['MA10_Slope'] = df['MA10'].diff()
    df['MA10_Slope_Prev'] = df['MA10_Slope'].shift(1)
    df['MA20_Slope'] = df['MA20'].diff()
    df['MA20_Slope_Prev'] = df['MA20_Slope'].shift(1)
    
    # 전일값 (필터용)
    df['MA5_Prev'] = df['MA5'].shift(1)
    df['MA10_Prev'] = df['MA10'].shift(1)
    df['MA20_Prev'] = df['MA20'].shift(1)
    
    # 수박 지표 (엔벨로프 하단)
    df['Env_Lower'] = df['MA20'] * 0.85 
    
    # 이격도 & RSI & 스토캐스틱 & OBV
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
    df['Prev_Change'] = df['Change'].shift(1)
    
    return df

# ---------------------------------------------------------
# 💯 [점수] 점수 상세 계산
# ---------------------------------------------------------
def calculate_score(row, pattern, is_buy, is_ins, fin):
    score = 50
    details = []
    
    # [1] 재무
    if "흑자" in fin['trend']: score += 15; details.append("흑자(15)")
    elif "급증" in fin['trend']: score += 10; details.append("급증(10)")
    if "저평가" in fin['badge']: score += 15; details.append("저평가(15)")
    elif "성장" in fin['badge']: score += 10; details.append("성장(10)")
    
    # [2] 수급
    s_score = 0
    if is_buy and is_ins: s_score = 30; score += 30; details.append("쌍끌이(30)")
    elif is_buy or is_ins: s_score = 10; score += 10; details.append("수급(10)")
    
    # [3] 패턴
    p_score = 0
    if "황금수박" in pattern: p_score = 50; score += 50; details.append("👑황금(50)")
    elif "공구리" in pattern: p_score = 40; score += 40; details.append("🔨공구리(40)")
    elif "잠입" in pattern: p_score = 35; score += 35; details.append("🥷잠입(35)")
    elif "골파기" in pattern: p_score = 30; score += 30; details.append("⛏️골파기(30)")
    elif "숨고르기" in pattern: p_score = 30; score += 30; details.append("🏳️숨고르기(30)")
    elif "돌파" in pattern: p_score = 15; score += 15; details.append("🦁돌파(15)")
    
    # [4] 차트
    c_score = 0
    if "수박" in pattern: 
        if row['RSI'] <= 30: c_score += 30; score += 30; details.append("과매도(30)")
        elif row['RSI'] <= 40: c_score += 20; score += 20; details.append("과매도(20)")
    else:
        if 100 <= row['Disp'] <= 105: c_score += 20; score += 20; details.append("이격(20)")
        if row['Stoch_K'] > row['Stoch_D']: c_score += 5; score += 5; details.append("Stoch(5)")

    # [감점]
    warns = []
    if row['OBV_Slope'] < 0: score -= 10; warns.append("⚠️돈이탈")
    if row['Stoch_Slope'] < 0: score -= 5; warns.append("⚠️힘빠짐")
    if "수박" not in pattern and row['MA10'] < row['MA10_Prev']: 
        score -= 5; warns.append("⚠️단기저항")

    risk = " ".join(warns) if warns else "✅깨끗함"
    return score, s_score, p_score, c_score, risk, ", ".join(details)

# ---------------------------------------------------------
# 🩺 [부검] 실패 원인 진단
# ---------------------------------------------------------
def diagnose_failure(future_data, supply_df, buy_date):
    reasons = []
    try:
        target = supply_df.loc[buy_date:].head(5)
        if not target.empty:
            if target['외국인'].sum() < 0: reasons.append("💧외인이탈")
    except: pass
    
    broken = False
    for i in range(min(3, len(future_data))):
        if future_data.iloc[i]['Close'] < future_data.iloc[i]['MA20']: broken = True
    if broken: reasons.append("📉추세붕괴")
    if not reasons: reasons.append("❓시장하락")
    return ", ".join(reasons)

# ---------------------------------------------------------
# 🔍 [분석 엔진] 통합 (백테스트 + 실전)
# ---------------------------------------------------------
def analyze_stock(ticker, name, mode='backtest'):
    try:
        df = fdr.DataReader(ticker)
        if len(df) < 60: return [] if mode == 'backtest' else None
        df = add_indicators(df)
        supply_df = get_supply_data(ticker) if mode == 'backtest' else pd.DataFrame() # 백테스트용 수급
        
        results = []
        start_idx = len(df) - TEST_DAYS if mode == 'backtest' else len(df) - 1
        if start_idx < 60: start_idx = 60
        end_idx = len(df)

        for i in range(start_idx, end_idx):
            row = df.iloc[i]
            prev = df.iloc[i-1]
            
            if row['Close'] < 1000: continue

            # 🛑 [Global Filter] 5/10일 동시 급락 무조건 제외
            is_crash = (row['MA5'] < row['MA5_Prev']) and (row['MA10'] < row['MA10_Prev'])
            if is_crash: continue 

            signal = None
            
            # [A] 수박 (공구리)
            if row['Low'] <= row['Env_Lower']:
                gongguri_5 = row['MA5_Slope'] > row['MA5_Slope_Prev']
                gongguri_10 = row['MA10_Slope'] > row['MA10_Slope_Prev']
                if gongguri_5 and gongguri_10:
                    if (row['MA20_Slope'] < 0) and (row['MA20_Slope'] > row['MA20_Slope_Prev']):
                        signal = "👑황금수박" 
                    else:
                        signal = "🍉공구리수박"
            
            # [B] 일반 패턴
            else:
                if row['MA20'] < row['MA20_Prev']: continue # 추세 하락 제외
                pass_filter = True
                if not row['OBV_Rising']: pass_filter = False
                if not (30 <= row['RSI'] <= 75): pass_filter = False
                
                if pass_filter:
                    # 골파기 (깊이 조건)
                    if row['Close'] > row['MA20'] and prev['Close'] < prev['MA20']:
                         min_low = df['Low'].iloc[i-5:i].min()
                         dip = ((row['MA20'] - min_low) / row['MA20']) * 100
                         if dip >= 2.0 and row['Pct'] >= 1.0:
                             signal = "⛏️골파기"
                    
                    # 🥷 잠입 (거래량 급감)
                    elif (row['Volume'] < prev['Volume'] * 0.4) and (abs(row['Pct']) < 1.5) and (row['Close'] > row['MA20']):
                        if (row['OBV_Slope'] >= 0) and (row['Stoch_Slope'] > -5):
                            signal = "🥷잠입"
                    
                    # 숨고르기
                    elif (prev['Change'] >= 0.10) and (row['Volume'] < prev['Volume'] * 0.6) and (-2.0 <= row['Pct'] <= 2.0):
                        if (row['OBV_Slope'] >= 0) and (row['Stoch_Slope'] > -5):
                            signal = "🏳️숨고르기"
                    
                    # 돌파
                    elif (row['Disp'] <= 110):
                        if (row['Vol_Ratio'] >= 1.5) and (row['Pct'] >= 1.0): signal = "🦁돌파"

            if signal:
                is_buy = False; is_ins = False
                date_str = df.index[i].strftime('%Y-%m-%d')
                
                # 실전 모드면 실시간 수급 확인
                if mode == 'realtime':
                    is_buy, is_ins, trend = get_investor_trend(ticker)
                
                # 백테스트 모드면 과거 수급 데이터 확인
                elif mode == 'backtest' and not supply_df.empty and date_str in supply_df.index:
                    s_row = supply_df.loc[date_str]
                    if isinstance(s_row, pd.DataFrame): s_row = s_row.iloc[0]
                    if s_row['외국인'] > 0: is_buy = True
                    if s_row['기관'] > 0: is_ins = True

                fin = {"trend": "", "badge": ""} 
                if mode == 'realtime': fin = get_financial_info(ticker)

                # 점수 계산
                score, s_p, p_p, c_p, risk, detail = calculate_score(row, signal, is_buy, is_ins, fin)
                
                # 실전/백테 분기
                if mode == 'backtest':
                    buy_price = row['Close']
                    future = df.iloc[i+1 : i+1+HOLDING_DAYS]
                    if len(future) > 0:
                        max_p = ((future['High'].max() - buy_price) / buy_price) * 100
                        min_p = ((future['Low'].min() - buy_price) / buy_price) * 100
                        final = ((future.iloc[-1]['Close'] - buy_price) / buy_price) * 100
                        
                        diag = "✅성공"
                        if max_p < 2.0 or final < 0:
                            diag = diagnose_failure(future, supply_df, date_str)
                            
                        results.append({
                            'Date': date_str, 'Name': name, 'Signal': signal, 
                            'Score': score, 'S': s_p, 'P': p_p, 'C': c_p,
                            'Max': max_p, 'Min': min_p, 'Final': final,
                            'Diag': diag, 'Risk': risk
                        })
                else: 
                    # 실전에선 50점 미만 과락
                    if score < 50: return None
                    
                    supply_str = '🚀쌍끌이' if (is_buy and is_ins) else ('👨🏼‍🦰외인' if is_buy else ('🏢기관' if is_ins else '💧개인'))
                    ai_cmt = ""
                    if score >= 80: ai_cmt = get_ai_summary(ticker, name, score, detail, risk)
                    
                    return {
                        '종목명': name, '현재가': int(row['Close']), '등락률': f"{row['Pct']:.2f}%",
                        '신호': signal, '총점': score, 
                        '수급점수': s_p, '패턴점수': p_p, '차트점수': c_p,
                        '수급현황': supply_str, 'Risk': risk,
                        'msg': f"[{signal}] {name}\n"
                               f"📊 {score}점 ({fin['badge']})\n"
                               f"💰 {supply_str} / {risk}\n"
                               f"📝 {detail}\n"
                               f"💵 {int(row['Close']):,}원 ({row['Pct']:+.2f}%){ai_cmt}"
                    }
        return results if mode == 'backtest' else None
    except: return [] if mode == 'backtest' else None

# ---------------------------------------------------------
# 🚀 메인 실행 (백테스트 + 실전 통합)
# ---------------------------------------------------------
if __name__ == "__main__":
    target_dict = get_market_data()
    
    print("\n" + "=" * 100)
    print(f"📡 [The Ultimate Bot] 1.검증 ➡️ 2.추천")
    print(f"🍉공구리 / 🥷잠입 / ⛏️골파기 / 🦁돌파 / High&Low 검증")
    print("=" * 100)

    # 1. 백테스트 (과거 검증)
    print(f"⏳ 1단계: 검증 시작 ({TEST_DAYS}일)")
    backtest_trades = []
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(analyze_stock, t, n, 'backtest') for t, n in target_dict.items()]
        for future in futures:
            res = future.result()
            backtest_trades.extend(res)
            
    if backtest_trades:
        df_bt = pd.DataFrame(backtest_trades).sort_values(by='Max', ascending=False)
        
        print("\n" + "=" * 130)
        print(f"📜 [백테스트 성적표] High(최고) / Low(최저) 포함")
        print("-" * 130)
        print(f"{'날짜':<12} {'종목명':<8} {'신호':<8} {'총점':<4} {'수급':<4} {'패턴':<4} {'차트':<4} {'최고(High)':<12} {'최저(Low)':<12} {'진단'}")
        
        for _, row in df_bt.head(15).iterrows():
            print(f"{row['Date']:<12} {row['Name']:<8} {row['Signal']:<8} "
                  f"{row['Score']:<4} {row['S']:<4} {row['P']:<4} {row['C']:<4} "
                  f"🔺{row['Max']:6.2f}%   "
                  f"💧{row['Min']:6.2f}%   "
                  f"{row['Diag']}")
        
        print("-" * 130)
        # 오답노트
        fails = df_bt[df_bt['Diag'] != '✅성공'].sort_values(by='Final', ascending=True).head(5)
        if not fails.empty:
            print("\n💀 [오답 노트] 실패 케이스 분석")
            for _, row in fails.iterrows():
                print(f"{row['Date']:<12} {row['Name']:<8} {row['Signal']:<8} "
                      f"{row['Score']:<4} {row['S']:<4} {row['P']:<4} {row['C']:<4} "
                      f"🔺{row['Max']:6.2f}%   "
                      f"💧{row['Min']:6.2f}%   "
                      f"{row['Diag']} ({row['Risk']})")
    else:
        print("\n❌ 검증 데이터 없음")

    # 2. 실전 추천 (오늘의 종목)
    print("\n" + "=" * 130)
    print(f"📡 2단계: 오늘({TODAY_STR}) 실전 추천")
    
    realtime_results = []
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(analyze_stock, t, n, 'realtime') for t, n in target_dict.items()]
        for future in futures:
            res = future.result()
            if res: realtime_results.append(res)
            
    if realtime_results:
        realtime_results.sort(key=lambda x: x['총점'], reverse=True)
        final_msgs = [r['msg'] for r in realtime_results[:20]]
        
        report = f"🦁 [오늘의 추천] {len(realtime_results)}개 발견\n\n" + "\n\n".join(final_msgs)
        print(report)
        send_telegram(report)
    else:
        msg = "❌ 추천 종목 없음 (시장 관망)"
        print(msg)
        send_telegram(msg)