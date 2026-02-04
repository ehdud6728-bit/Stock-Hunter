# ------------------------------------------------------------------
# 👑 [The Ultimate Bot] Final (AI 줄바꿈 제거 + 업종분석 강화)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
import mplfinance as mpf
import matplotlib.pyplot as plt # 👈 차트 텍스트 박스용
from datetime import datetime, timedelta
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
import pytz

# 👇 OpenAI 연결
try: from openai import OpenAI
except: OpenAI = None

# 👇 구글 시트
from google_sheet_manager import update_google_sheet

# =================================================
# ⚙️ 설정
# =================================================
TOP_N = 300            
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') 
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')     

KST = pytz.timezone('Asia/Seoul')
current_time = datetime.now(KST)
NOW = current_time - timedelta(days=1) if current_time.hour < 8 else current_time
TODAY_STR = NOW.strftime('%Y-%m-%d')

REAL_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Referer': 'https://finance.naver.com/',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
}

# ---------------------------------------------------------
# 📸 [기능 1] 지수 차트 (텍스트 박스 포함)
# ---------------------------------------------------------
def create_index_chart(ticker, name):
    print(f"🎨 {name} 차트 생성 중...")
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=120) 
        df = fdr.DataReader(ticker, start=start_date, end=end_date)
        if len(df) < 2: return None

        latest = df['Close'].iloc[-1]; prev = df['Close'].iloc[-2]
        pct = (latest - prev) / prev * 100
        sign = "+" if pct > 0 else ""
        
        info_text = f"{name}\n{latest:,.2f} ({sign}{pct:.2f}%)"
        text_color = 'red' if pct > 0 else ('blue' if pct < 0 else 'black')

        mc = mpf.make_marketcolors(up='r', down='b', inherit=True)
        s  = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=False)
        apds = [
            mpf.make_addplot(df['Close'].rolling(20).mean(), color='orange', width=1),
            mpf.make_addplot(df['Close'].rolling(60).mean(), color='purple', width=1)
        ]

        fig, axlist = mpf.plot(df, type='candle', style=s, addplot=apds, title="", volume=False, returnfig=True, figscale=1.0)
        axlist[0].text(0.03, 0.95, info_text, transform=axlist[0].transAxes, fontsize=14, fontweight='bold', color=text_color,
                       bbox=dict(facecolor='white', alpha=0.8, edgecolor='gray', boxstyle='round,pad=0.5'))
        
        fname = f"{name}.png"
        fig.savefig(fname, bbox_inches='tight')
        plt.close(fig)
        return fname
    except: return None

def send_telegram_photo(message, image_paths=[]):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    url_p = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    url_t = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    real_id_list = [x.strip() for item in CHAT_ID_LIST for x in item.split(',') if x.strip()]
    for chat_id in real_id_list:
        if message: requests.post(url_t, data={'chat_id': chat_id, 'text': message})
        for img in image_paths:
            if img and os.path.exists(img):
                try:
                    with open(img, 'rb') as f: requests.post(url_p, data={'chat_id': chat_id}, files={'photo': f})
                except: pass
    for img in image_paths:
        if img and os.path.exists(img): os.remove(img)

# ---------------------------------------------------------
# 📢 [기능 2] 시황 브리핑 (테마+업종)
# ---------------------------------------------------------
def get_hot_themes():
    hot_info = []
    try:
        url_t = "https://finance.naver.com/sise/theme.naver"
        df_t = pd.read_html(requests.get(url_t, headers=REAL_HEADERS).text)[0].dropna().head(5)
        hot_info.append(f"🔥강세테마: {', '.join(df_t['테마명'].tolist())}")
        
        url_u = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
        df_u = pd.read_html(requests.get(url_u, headers=REAL_HEADERS).text)[0].dropna().head(5)
        hot_info.append(f"📈강세업종: {', '.join(df_u['업종명'].tolist())}")
        return "\n".join(hot_info)
    except: return "테마 정보 수집 실패"

def get_market_briefing():
    if not OPENAI_API_KEY: return None
    try:
        kospi = fdr.DataReader('KS11', start=datetime.now()-timedelta(days=5))
        nasdaq = fdr.DataReader('IXIC', start=datetime.now()-timedelta(days=5))
        theme = get_hot_themes()
        def rate(df): return f"{(df['Close'].iloc[-1]-df['Close'].iloc[-2])/df['Close'].iloc[-2]*100:+.2f}%"
        data = f"나스닥:{rate(nasdaq)}, 코스피:{rate(kospi)}\n{theme}"
        
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user", "content":f"데이터:\n{data}\n\n위 데이터를 바탕으로 '오늘의 시장 흐름'을 3줄로 요약해줘(반말). 지수 등락과 주도 테마를 연결지어 분석해."}]
        )
        return f"📢 [오늘의 시황]\n{res.choices[0].message.content.strip()}"
    except: return None

# ---------------------------------------------------------
# 🧠 [기능 3] AI 종목 분석 (🔥 선생님이 주신 코드 적용 완료!)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, category, reasons):
    # 🔥 선생님 요청사항: 차트보다는 사업내용, 테마, 시황, 그리고 줄바꿈 금지!
    prompt = (f"종목: {name} ({ticker})\n"
              f"포착: {category}\n"
              f"특징: {', '.join(reasons)}\n\n"
              f"위 신호는 참고만 하고, 이 회사의 '사업 내용'에 집중해.\n"
              f"1. 이 회사의 핵심 [테마/섹터]가 뭐야? (예: [반도체], [2차전지], [로봇], [제약바이오])\n"
              f"2. 현재날짜 기준으로 주식 전문가 입장에서 시황, 기술적 차트 분석 등 여러가지를 분석해서 간략하게 알려줘.\n\n"
              f"3. 답변은 줄바꿈 없이 한 줄로 이어서 작성.\n"
              f"🚨 중요: 답변은 무조건 아래 형식으로만 해.\n"
              f"형식: [테마명] 분석 내용 (반말 모드)")

    final_comment = ""

    # 1. GPT
    if OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            res = client.chat.completions.create(
                model="gpt-4o-mini", 
                messages=[{"role":"user", "content":prompt}], 
                max_tokens=200
            )
            # 👇 핵심 수정: 줄바꿈(\n)을 공백으로 치환해서 빈 줄 삭제
            content = res.choices[0].message.content.strip().replace('\n', ' ')
            final_comment += f"\n\n🧠 [GPT]: {content}"
        except: pass

    # 2. Groq
    if GROQ_API_KEY:
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
            payload = {"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}]}
            res = requests.post(url, json=payload, headers=headers, timeout=2)
            if res.status_code == 200:
                # 👇 핵심 수정: 줄바꿈 제거
                content = res.json()['choices'][0]['message']['content'].strip().replace('\n', ' ')
                final_comment += f"\n\n⚡ [Groq]: {content}"
        except: pass

    return final_comment

# ---------------------------------------------------------
# 🏟️ [기능 4] AI 토너먼트
# ---------------------------------------------------------
def run_ai_tournament(candidate_list):
    if not candidate_list: return ""
    
    prompt_data = ""
    for item in candidate_list[:50]:
        prompt_data += f"- {item['종목명']} ({item['총점']}점): {item['신호']} / {item['Risk']}\n"

    print(f"🏟️ AI 토너먼트 개최! (후보 {len(candidate_list[:50])}개)")

    system_prompt = (
        "너는 최고의 주식 트레이더야. 제공된 '유망 종목 리스트'를 분석해서 'Top 3 종목'을 추천해줘.\n"
        "형식:\n🥇 [1위 종목명]\n- 이유: (한 줄 요약)\n🥈 [2위 종목명]\n- 이유: (한 줄 요약)\n🥉 [3위 종목명]\n- 이유: (한 줄 요약)\n(반말)"
    )

    final_report = "\n🏆 [AI 토너먼트 결승전]\n"

    if OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            res = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"system", "content":system_prompt}, {"role":"user", "content":f"List:\n{prompt_data}"}]
            )
            final_report += f"\n🧠 [GPT Pick]\n{res.choices[0].message.content.strip()}\n"
        except Exception as e: final_report += f"\n🧠 GPT 오류: {e}\n"

    final_report += "\n" + "-"*30 + "\n"

    if GROQ_API_KEY:
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
            payload = {"model": "llama-3.3-70b-versatile", "messages": [{"role":"system", "content":system_prompt}, {"role":"user", "content":f"List:\n{prompt_data}"}]}
            res = requests.post(url, json=payload, headers=headers, timeout=5)
            if res.status_code == 200:
                final_report += f"\n⚡ [Groq Pick]\n{res.json()['choices'][0]['message']['content'].strip()}\n"
        except: pass

    return final_report

# ---------------------------------------------------------
# 📊 [기능 5] 공통 데이터 & 분석 엔진
# ---------------------------------------------------------
def get_common_data(code):
    trend = "정보없음"; badge = "⚖️보통"
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        df = pd.read_html(requests.get(url, headers=REAL_HEADERS).text, match='날짜')[0].dropna().iloc[0]
        buy = int(str(df['외국인']).replace(',', '')) > 0
        ins = int(str(df['기관']).replace(',', '')) > 0
        trend = "🚀쌍끌이" if (buy and ins) else ("👨🏼‍🦰외인" if buy else ("🏢기관" if ins else "💧개인"))
    except: pass
    try:
        url2 = f"https://finance.naver.com/item/main.naver?code={code}"
        df2 = pd.read_html(requests.get(url2, headers=REAL_HEADERS).text)
        for d in df2:
            if '최근 연간 실적' in str(d.columns):
                fin = d.set_index(d.columns[0])
                if 'EPS(원)' in fin.index:
                    eps = float(str(fin.loc['EPS(원)'].values[-1]).replace(',',''))
                    badge = "💎흑자" if eps > 0 else "⚠️적자"
                break
    except: pass
    return trend, badge

# ---------------------------------------------------------
# ⚔️ [기능 5] 듀얼 엔진 (변수명 호환성 완벽 수정)
# ---------------------------------------------------------

# 1. 추세 전략 (Trend)
def check_trend_strategy(df, row):
    # 👇 여기서 필요한 이평선 데이터를 row에서 가져옵니다
    ma5 = row['Close_MA5']
    ma20 = row['Close_MA20']
    
    # 전일 데이터는 df에서 직접 조회
    prev_ma5 = df['Close_MA5'].iloc[-2]
    prev_ma20 = df['Close_MA20'].iloc[-2]
    
    score = 0; reasons = []
    
    # 골든크로스
    if prev_ma5 <= prev_ma20 and ma5 > ma20: 
        score += 40; reasons.append("✨골든크로스")
    
    # 거래량 폭발
    if row['Volume'] > df['Volume'].iloc[-20:].mean() * 2.0: 
        score += 30; reasons.append("💥거래량폭발")
    
    # 골파기 (20일선 이탈 후 복귀)
    if row['Close'] > ma20 and df['Close'].iloc[-2] < prev_ma20: 
        score += 30; reasons.append("⛏️골파기")

    # 합격 기준: 30점 이상 (하나라도 걸리면)
    if score >= 30: return True, score, reasons
    return False, 0, []

# 2. 단테 전략 (Dante)
def check_dante_strategy(df, row):
    ma112 = row['Close_MA112']
    ma224 = row['Close_MA224']
    past_high = df['High'].iloc[:-120].max() # 과거 고점
    
    score = 0; reasons = []
    
    # 고점 대비 너무 높으면 탈락
    if row['Close'] > past_high * 0.85: return False, 0, []
    
    # 112일선 지지
    dist_112 = (row['Close'] - ma112) / ma112
    if -0.05 <= dist_112 <= 0.05: 
        score += 40; reasons.append("🎯112선지지")
    
    # 224일선 돌파/도전
    if row['Close'] > ma224: 
        score += 30; reasons.append("🔥224돌파")
    elif (ma224 - row['Close']) / row['Close'] < 0.05: 
        score += 20; reasons.append("🔨224도전")
    
    # 공구리 (변동성 축소)
    if (df['Close'].iloc[-5:].std() / df['Close'].iloc[-5:].mean()) < 0.02: 
        score += 20; reasons.append("🛡️공구리")

    if score >= 30: return True, score, reasons
    return False, 0, []

# 3. 통합 분석 엔진 (MA 계산 기능 탑재!)
def analyze_stock(ticker, name):
    try:
        # 1. 데이터 가져오기
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=730)).strftime('%Y-%m-%d'))
        if len(df) < 225: return None
        
        # 🔥 [핵심 수정] 이평선 미리 계산 (이게 빠져서 에러가 났던 겁니다!)
        for n in [5, 20, 112, 224]: 
            df[f'Close_MA{n}'] = df['Close'].rolling(n).mean()
            
        row = df.iloc[-1]
        if row['Close'] < 1000 or row['Volume'] == 0: return None

        # 2. 전략 실행
        is_trend, s_trend, r_trend = check_trend_strategy(df, row)
        is_dante, s_dante, r_dante = check_dante_strategy(df, row)
        
        # 둘 다 아니면 탈락
        if not is_trend and not is_dante: return None

        # 3. 등급 산정
        category = ""; final_score = 0; final_reasons = []
        if is_trend and is_dante:
            category = "👑 [강력추천/겹침]"
            final_score = s_trend + s_dante
            final_reasons = list(set(r_trend + r_dante))
        elif is_trend:
            category = "🦁 [추세 Pick]"
            final_score = s_trend
            final_reasons = r_trend
        elif is_dante:
            category = "🥣 [단테 Pick]"
            final_score = s_dante
            final_reasons = r_dante

        # 4. 공통 데이터
        trend, badge = get_common_data(ticker)
        
        # 5. AI 요약 (0점 이상 호출)
        ai_msg = ""
        if final_score >= 0:
            ai_msg = get_ai_summary(ticker, name, category, final_reasons)

        # 6. 결과 반환 (구분선 및 줄바꿈 완벽 적용)
        return {
            'code': ticker, '종목명': name, '현재가': int(row['Close']),
            '신호': " ".join(final_reasons), '총점': final_score,
            '수급현황': trend, 'Risk': badge,
            'msg': f"{category} {name} ({final_score}점)\n"
                   f"👉 신호: {' '.join(final_reasons)}\n"
                   f"💰 현재가: {int(row['Close']):,}원\n"
                   f"📊 {trend} / {badge}"
                   f"{ai_msg}\n\n"
                   f"➖➖➖➖➖➖➖➖➖➖➖➖\n"
        }
    except Exception as e:
        # 에러 확인용 (나중엔 주석 처리 하셔도 됩니다)
        # print(f"❌ 에러 발생 ({name}): {e}") 
        return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [Ultimate Bot] {TODAY_STR} 시작")
    
    # 1. 📸 차트 & 시황
    print("📸 차트 및 시황 생성 중...")
    charts = [create_index_chart('IXIC','NASDAQ'), create_index_chart('KS11','KOSPI'), create_index_chart('KQ11','KOSDAQ')]
    brief = get_market_briefing()
    if brief: send_telegram_photo(brief, charts)
    
    # 2. 🔍 종목 스캔
    print("🔍 종목 스캔 중...")
    df_krx = fdr.StockListing('KRX')
    df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    target_dict = dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    
    force_list = {'008350':'남선알미늄', '294630':'서남', '005930':'삼성전자'}
    for k, v in force_list.items():
        if k not in target_dict: target_dict[k] = v

    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(analyze_stock, t, n) for t, n in target_dict.items()]
        for future in futures:
            res = future.result()
            if res: results.append(res)
            
    if results:
        results.sort(key=lambda x: x['총점'], reverse=True)
        top_50 = results[:50]
        
        # 3. 🏟️ AI 토너먼트
        print("🏟️ AI 토너먼트 시작...")
        tournament_result = run_ai_tournament(top_50)
        print(tournament_result)
        send_telegram_photo(tournament_result)
        
        # 4. 전체 리스트 전송
        final_msgs = [r['msg'] for r in results[:15]]
        report = f"💎 [예선 통과 상위 15개]\n\n" + "".join(final_msgs)
        send_telegram_photo(report)
        
        try: update_google_sheet(results, TODAY_STR)
        except: pass
    else:
        print("❌ 발견된 종목 없음")
