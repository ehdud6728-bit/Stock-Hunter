# ------------------------------------------------------------------
# 👑 [The Ultimate Bot] 네이버 차단 우회 & 풀옵션 통합본
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
import mplfinance as mpf
from datetime import datetime, timedelta
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
import pytz

# 👇 OpenAI (필수)
try:
    from openai import OpenAI
except ImportError:
    OpenAI = None
    print("❌ [오류] requirements.txt에 'openai'를 추가해주세요!")

# 👇 구글 시트 매니저
from google_sheet_manager import update_google_sheet

# =================================================
# ⚙️ 설정
# =================================================
TOP_N = 500            
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') 
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')     

# 🌍 시간 설정
KST = pytz.timezone('Asia/Seoul')
current_time = datetime.now(KST)
NOW = current_time - timedelta(days=1) if current_time.hour < 8 else current_time
TODAY_STR = NOW.strftime('%Y-%m-%d')

# 🛡️ [핵심] 네이버가 사람으로 착각하게 만드는 '진짜 헤더'
REAL_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Referer': 'https://finance.naver.com/',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Connection': 'keep-alive'
}

# ---------------------------------------------------------
# 📸 [기능 1] 지수 차트
# ---------------------------------------------------------
def create_index_chart(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=180)))
        mc = mpf.make_marketcolors(up='r', down='b', inherit=True)
        s  = mpf.make_mpf_style(marketcolors=mc)
        apds = [
            mpf.make_addplot(df['Close'].rolling(20).mean(), color='orange', width=1),
            mpf.make_addplot(df['Close'].rolling(60).mean(), color='purple', width=1)
        ]
        filename = f"{name}.png"
        mpf.plot(df, type='candle', style=s, addplot=apds, title=f"{name}", volume=False, savefig=filename, figscale=1.0, figratio=(10, 5))
        return filename
    except: return None

def send_telegram_photo(message, image_paths=[]):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    url_photo = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    url_text = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    real_id_list = []
    for item in CHAT_ID_LIST:
        real_id_list.extend([x.strip() for x in item.split(',') if x.strip()])
    
    for chat_id in real_id_list:
        if not chat_id: continue
        if message: requests.post(url_text, data={'chat_id': chat_id, 'text': message})
        if image_paths:
            for img_path in image_paths:
                if img_path and os.path.exists(img_path):
                    try:
                        with open(img_path, 'rb') as f:
                            requests.post(url_photo, data={'chat_id': chat_id}, files={'photo': f})
                    except: pass
    for img_path in image_paths:
        if img_path and os.path.exists(img_path): os.remove(img_path)

# ---------------------------------------------------------
# 📢 [기능 2] 시황 브리핑
# ---------------------------------------------------------
def get_market_briefing():
    if not OPENAI_API_KEY: return None
    try:
        kospi = fdr.DataReader('KS11', start=datetime.now() - timedelta(days=5))
        kosdaq = fdr.DataReader('KQ11', start=datetime.now() - timedelta(days=5))
        nasdaq = fdr.DataReader('IXIC', start=datetime.now() - timedelta(days=5))
        
        def get_change(df):
            if len(df) < 2: return "0.00"
            curr = df['Close'].iloc[-1]; prev = df['Close'].iloc[-2]
            return f"{(curr - prev) / prev * 100:+.2f}%"

        data = f"나스닥:{get_change(nasdaq)}, 코스피:{get_change(kospi)}, 코스닥:{get_change(kosdaq)}"
        prompt = f"데이터: {data}. 주식 트레이더들에게 '오늘의 시황'을 3줄로 반말 요약해줘."
        
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"user", "content":prompt}])
        return f"📢 [오늘의 시황]\n{res.choices[0].message.content.strip()}"
    except: return None

# ---------------------------------------------------------
# 🧠 [기능 3] AI 종목 분석
# ---------------------------------------------------------
# 👇 디버깅용 get_ai_summary (에러 원인을 출력해줌)
def get_ai_summary(ticker, name, category, reasons):
    print(f"🔍 [AI 분석 시도] {name} 분석 시작...") # 로그 추가

    prompt = (f"종목: {name} ({ticker})\n"
              f"포착 결과: {category}\n"
              f"특징: {', '.join(reasons)}\n\n"
              f"1. [테마/업종]을 1단어로 정의 (예: [반도체]).\n"
              f"2. 매력적인 이유를 한 줄 요약.\n"
              f"(반말 모드)")

    final_comment = ""
    
    # 1. GPT 시도
    if OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            res = client.chat.completions.create(
                model="gpt-4o-mini", 
                messages=[{"role":"user", "content":prompt}], 
                max_tokens=150
            )
            final_comment += f"\n🧠 [GPT]: {res.choices[0].message.content.strip()}"
            print("✅ GPT 응답 성공")
        except Exception as e:
            print(f"❌ [GPT 에러] {e}") # 에러 메시지 출력!!
    else:
        print("⚠️ OpenAI API 키가 없어서 건너뜀")

    # 2. Groq 시도
    if GROQ_API_KEY:
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
            payload = {"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}]}
            res = requests.post(url, json=payload, headers=headers, timeout=2)
            if res.status_code == 200:
                final_comment += f"\n⚡ [Groq]: {res.json()['choices'][0]['message']['content'].strip()}"
                print("✅ Groq 응답 성공")
            else:
                print(f"❌ [Groq 에러] 상태코드: {res.status_code}")
        except Exception as e:
            print(f"❌ [Groq 에러] {e}")

    return final_comment

# ---------------------------------------------------------
# 📊 [기능 4] 공통 데이터 (수급/재무) - ⚠️ 수정완료
# ---------------------------------------------------------
def get_common_data(code):
    trend = "정보없음"; badge = "⚖️보통"
    
    # 1. 수급 (네이버 차단 우회 적용)
    try: 
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        # 👈 선생님 말씀대로 '진짜 사람 헤더'를 넣었습니다!
        resp = requests.get(url, headers=REAL_HEADERS, timeout=3)
        
        dfs = pd.read_html(StringIO(resp.text), match='날짜')
        if dfs:
            target_df = dfs[0].dropna()
            # 날짜 열이 있는 헤더가 중간에 껴있는 경우 제거
            target_df = target_df[target_df['날짜'].astype(str).str.contains('날짜') == False]
            
            if len(target_df) > 0:
                latest = target_df.iloc[0]
                # 천단위 콤마 제거 후 정수 변환
                foreigner = int(str(latest['외국인']).replace(',', ''))
                institution = int(str(latest['기관']).replace(',', ''))
                
                buy = foreigner > 0
                ins = institution > 0
                trend = "🚀쌍끌이" if (buy and ins) else ("👨🏼‍🦰외인" if buy else ("🏢기관" if ins else "💧개인"))
    except Exception as e:
        # print(f"수급 에러({code}): {e}") # 디버깅용
        pass

    # 2. 재무 (네이버 차단 우회 적용)
    try: 
        url2 = f"https://finance.naver.com/item/main.naver?code={code}"
        resp2 = requests.get(url2, headers=REAL_HEADERS, timeout=3)
        dfs2 = pd.read_html(StringIO(resp2.text))
        for df in dfs2:
            if '최근 연간 실적' in str(df.columns) or '주요재무제표' in str(df.columns):
                # 컬럼 정리
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(1) # 하단 컬럼만 사용
                    
                fin = df.set_index(df.columns[0])
                # EPS 확인
                target_key = next((k for k in fin.index if 'EPS' in str(k)), None)
                if target_key:
                    # 최근 값 가져오기 (NaN 제외)
                    vals = fin.loc[target_key].values
                    last_val = 0
                    for v in vals:
                        v_str = str(v).replace(',', '')
                        if v_str.replace('.', '', 1).replace('-', '', 1).isdigit():
                            last_val = float(v_str)
                    
                    if last_val < 0: badge = "⚠️적자"
                    elif last_val > 0: badge = "💎흑자"
                break
    except Exception as e:
        pass
        
    return trend, badge

# ---------------------------------------------------------
# ⚔️ [기능 5] 듀얼 엔진
# ---------------------------------------------------------
def check_trend_strategy(df, row):
    ma5 = df['Close'].rolling(5).mean().iloc[-1]
    ma20 = df['Close'].rolling(20).mean().iloc[-1]
    prev_ma5 = df['Close'].rolling(5).mean().iloc[-2]
    prev_ma20 = df['Close'].rolling(20).mean().iloc[-2]
    score = 0; reasons = []
    
    if prev_ma5 <= prev_ma20 and ma5 > ma20: score += 40; reasons.append("✨골든크로스")
    if row['Volume'] > df['Volume'].iloc[-20:].mean() * 2.0: score += 30; reasons.append("💥거래량폭발")
    if row['Close'] > ma20 and df['Close'].iloc[-2] < df['Close'].rolling(20).mean().iloc[-2]: score += 30; reasons.append("⛏️골파기/복귀")
    if score >= 50: return True, score, reasons
    return False, 0, []

def check_dante_strategy(df, row):
    ma112 = df['Close'].rolling(112).mean().iloc[-1]
    ma224 = df['Close'].rolling(224).mean().iloc[-1]
    past_high = df['High'].iloc[:-120].max()
    score = 0; reasons = []
    
    if row['Close'] > past_high * 0.85: return False, 0, []
    dist_112 = (row['Close'] - ma112) / ma112
    if -0.10 <= dist_112 <= 0.10: score += 40; reasons.append("🎯112선지지")
    if row['Close'] > ma224: score += 30; reasons.append("🔥224돌파")
    elif (ma224 - row['Close']) / row['Close'] < 0.05: score += 20; reasons.append("🔨224도전")
    if (df['Close'].iloc[-5:].std() / df['Close'].iloc[-5:].mean()) < 0.02: score += 20; reasons.append("🛡️공구리")

    if score >= 60: return True, score, reasons
    return False, 0, []

def analyze_stock_dual(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=730)).strftime('%Y-%m-%d'))
        if len(df) < 225: return None
        row = df.iloc[-1]
        if row['Close'] < 1000 or row['Volume'] == 0: return None

        is_trend, s_trend, r_trend = check_trend_strategy(df, row)
        is_dante, s_dante, r_dante = check_dante_strategy(df, row)
        if not is_trend and not is_dante: return None

        category = ""; final_score = 0; final_reasons = []
        if is_trend and is_dante:
            category = "👑 [강력추천/겹침]"; final_score = s_trend + s_dante
            final_reasons = list(set(r_trend + r_dante))
        elif is_trend:
            category = "🦁 [추세 Pick]"; final_score = s_trend; final_reasons = r_trend
        elif is_dante:
            category = "🥣 [단테 Pick]"; final_score = s_dante; final_reasons = r_dante

        trend, badge = get_common_data(ticker)
        ai_msg = ""
        if final_score >= 60: ai_msg = get_ai_summary(ticker, name, category, final_reasons)

        return {
            'code': ticker, '종목명': name, '현재가': int(row['Close']),
            '신호': " ".join(final_reasons), '총점': final_score,
            '수급현황': trend, 'Risk': badge,
            'msg': f"{category} {name} ({final_score}점)\n👉 신호: {' '.join(final_reasons)}\n💰 현재가: {int(row['Close']):,}원\n📊 {trend} / {badge}\n{ai_msg}"
        }
    except: return None

# ---------------------------------------------------------
# 🚀 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [Ultimate Bot] {TODAY_STR} 시작 (네이버 차단 우회 적용)")
    
    # 1. 시황
    print("📊 지수 차트 생성 중...")
    charts = [create_index_chart('IXIC','NASDAQ'), create_index_chart('KS11','KOSPI'), create_index_chart('KQ11','KOSDAQ')]
    brief = get_market_briefing()
    #if brief: send_telegram_photo(brief, charts)
    
    # 2. 스캔
    print("🔍 종목 스캔 중...")
    df_krx = fdr.StockListing('KRX')
    df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    target_dict = dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    
    force_list = {'008350':'남선알미늄', '294630':'서남', '005930':'삼성전자'}
    for k, v in force_list.items():
        if k not in target_dict: target_dict[k] = v

    results = []
    with ThreadPoolExecutor(max_workers=20) as executor: # 네이버 차단 방지 위해 속도 조금 조절
        futures = [executor.submit(analyze_stock_dual, t, n) for t, n in target_dict.items()]
        for future in futures:
            res = future.result()
            if res: results.append(res)
            
    if results:
        results.sort(key=lambda x: x['총점'], reverse=True)
        final_msgs = [r['msg'] for r in results[:15]]
        report = f"💎 [오늘의 발굴] {len(results)}개 완료\n\n" + "\n\n".join(final_msgs)
        print(report)
        #send_telegram_photo(report, []) 
        try: update_google_sheet(results, TODAY_STR)
        except: pass
    else: print("❌ 발견된 종목 없음")
