# ------------------------------------------------------------------
# 👑 [The Ultimate Bot] Final (이격도 밀집 기능 추가 버전)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
import re
import mplfinance as mpf
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup 
import pytz

# 👇 OpenAI
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
# 📸 [기능 1] 지수 차트
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

# ---------------------------------------------------------
# 📨 [기능 2] 텔레그램 전송
# ---------------------------------------------------------
def send_telegram_photo(message, image_paths=[]):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    url_p = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    url_t = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    real_id_list = [x.strip() for item in CHAT_ID_LIST for x in item.split(',') if x.strip()]
    for chat_id in real_id_list:
        if message:
            if len(message) > 4000:
                chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
                for chunk in chunks:
                    try: requests.post(url_t, data={'chat_id': chat_id, 'text': chunk}); time.sleep(0.3)
                    except: pass
            else:
                try: requests.post(url_t, data={'chat_id': chat_id, 'text': message})
                except: pass
        for img in image_paths:
            if img and os.path.exists(img):
                try:
                    with open(img, 'rb') as f: requests.post(url_p, data={'chat_id': chat_id}, files={'photo': f})
                except: pass
    for img in image_paths:
        if img and os.path.exists(img): try: os.remove(img)
        except: pass

# ---------------------------------------------------------
# 📢 [기능 3] 시황 브리핑
# ---------------------------------------------------------
def get_hot_themes():
    hot_info = []
    print("🕵️ 테마 & 대장주 추적 중...")
    try:
        url = "https://finance.naver.com/sise/theme.naver"
        res = requests.get(url, headers=REAL_HEADERS)
        soup = BeautifulSoup(res.text, 'html.parser')
        count = 0
        for row in soup.select('table.type_1 tr'):
            if count >= 3: break
            cols = row.select('td')
            if len(cols) < 2: continue
            theme = cols[0].text.strip()
            link = cols[0].select_one('a')
            if link:
                sub_res = requests.get("https://finance.naver.com" + link['href'], headers=REAL_HEADERS)
                sub_soup = BeautifulSoup(sub_res.text, 'html.parser')
                leader = sub_soup.select_one('div.name_area')
                leader_name = leader.text.strip().replace('*','') if leader else "확인불가"
                hot_info.append(f"🔥{theme}(대장:{leader_name})")
            else: hot_info.append(f"🔥{theme}")
            count += 1; time.sleep(0.1)
        return ", ".join(hot_info)
    except: return "테마 정보 수집 실패"

def get_market_briefing():
    if not OPENAI_API_KEY: return None
    try:
        kospi = fdr.DataReader('KS11', start=datetime.now()-timedelta(days=5))
        nasdaq = fdr.DataReader('IXIC', start=datetime.now()-timedelta(days=5))
        theme_data = get_hot_themes()
        def rate(df): return f"{(df['Close'].iloc[-1]-df['Close'].iloc[-2])/df['Close'].iloc[-2]*100:+.2f}%"
        data = f"나스닥:{rate(nasdaq)}, 코스피:{rate(kospi)}\n주도테마:{theme_data}"
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = (f"시장데이터:\n{data}\n\n'오늘의 시장 흐름'을 3줄로 요약해(반말). 주도 테마와 대장주를 꼭 언급해.")
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"user", "content":prompt}])
        return f"📢 [오늘의 시황]\n{res.choices[0].message.content.strip()}"
    except: return None

# ---------------------------------------------------------
# 🧠 [기능 4] AI 종목 분석
# ---------------------------------------------------------
def get_ai_summary(ticker, name, category, reasons):
    prompt = (f"종목: {name} ({ticker})\n"
              f"포착: {category}\n"
              f"특징: {', '.join(reasons)}\n\n"
              f"이 회사의 '사업 내용'과 '테마'에 집중해.\n"
              f"1. 핵심 [테마/섹터]가 뭐야?\n"
              f"2. 전문가 입장에서 시황, 차트, 재료 분석 요약.\n"
              f"3. 답변은 줄바꿈 없이 한 줄로.\n"
              f"형식: [테마명] 분석 내용 (반말)")

    final_comment = ""
    if OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"user", "content":prompt}], max_tokens=200)
            content = res.choices[0].message.content.strip().replace('\n', ' ')
            final_comment += f"\n\n🧠 [GPT]: {content}"
        except: pass
    if GROQ_API_KEY:
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
            res = requests.post(url, json={"model":"llama-3.3-70b-versatile", "messages":[{"role":"user", "content":prompt}]}, headers=headers, timeout=2)
            if res.status_code == 200:
                content = res.json()['choices'][0]['message']['content'].strip().replace('\n', ' ')
                final_comment += f"\n\n⚡ [Groq]: {content}"
        except: pass
    return final_comment

# ---------------------------------------------------------
# 🏟️ [기능 5] AI 토너먼트
# ---------------------------------------------------------
def run_ai_tournament(candidate_list):
    if not candidate_list: return "", {}
    prompt_data = ""
    for item in candidate_list[:50]:
        prompt_data += f"- {item['종목명']}({item['code']}) 점수:{item['총점']} 신호:{item['신호']}\n"
    
    print(f"🏟️ AI 토너먼트 개최! (후보 {len(candidate_list[:50])}개)")
    system_prompt = (
        "너는 최고의 주식 트레이더야. 'Top 3 종목'을 추천해줘.\n"
        "🚨 중요: 종목명 뒤에 반드시 (종목코드)를 적어. 예: [삼성전자](005930)\n"
        "형식:\n🥇 [1위 종목명](코드)\n- 이유: (한 줄 요약)\n🥈 [2위 종목명](코드)\n- 이유: ...\n🥉 [3위 종목명](코드)\n- 이유: ..."
    )
    final_report = "\n🏆 [AI 토너먼트 결승전]\n"; ai_picks = {}
    
    if OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"system", "content":system_prompt}, {"role":"user", "content":f"List:\n{prompt_data}"}])
            content = res.choices[0].message.content.strip()
            final_report += f"\n🧠 [GPT Pick]\n{content}\n"
            matches = re.findall(r'([🥇🥈🥉])\s*(?:\[)?.*?(?:\])?\s*\((\d{6})\)', content)
            for rank, code in matches:
                label = f"{rank}GPT{rank.replace('🥇','1').replace('🥈','2').replace('🥉','3')}"
                ai_picks[code] = ai_picks.get(code, "") + label + " "
        except Exception as e: final_report += f"\n🧠 GPT 오류: {e}\n"

    final_report += "\n" + "-"*30 + "\n"

    if GROQ_API_KEY:
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
            res = requests.post(url, json={"model":"llama-3.3-70b-versatile", "messages":[{"role":"system", "content":system_prompt}, {"role":"user", "content":f"List:\n{prompt_data}"}]}, headers=headers, timeout=5)
            if res.status_code == 200:
                content = res.json()['choices'][0]['message']['content'].strip()
                final_report += f"\n⚡ [Groq Pick]\n{content}\n"
                matches = re.findall(r'([🥇🥈🥉])\s*(?:\[)?.*?(?:\])?\s*\((\d{6})\)', content)
                for rank, code in matches:
                    label = f"{rank}Groq{rank.replace('🥇','1').replace('🥈','2').replace('🥉','3')}"
                    ai_picks[code] = ai_picks.get(code, "") + label + " "
        except: pass
    return final_report, ai_picks

# ---------------------------------------------------------
# 📊 [기능 6] 수급 및 재무 데이터
# ---------------------------------------------------------
def get_stock_data_extras(code):
    trend = "정보없음"; badge = "⚖️보통"
    is_for_3days = False; is_ins_3days = False
    
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        df = pd.read_html(requests.get(url, headers=REAL_HEADERS).text, match='날짜')[0].dropna()
        recent = df.head(5)
        f_cnt = 0; i_cnt = 0
        for _, r in recent.iterrows():
            if int(str(r['외국인']).replace(',', '')) > 0: f_cnt += 1
            if int(str(r['기관']).replace(',', '')) > 0: i_cnt += 1
        if f_cnt >= 3: is_for_3days = True
        if i_cnt >= 3: is_ins_3days = True
        
        if is_for_3days and is_ins_3days: trend = "🚀쌍끌이(5일)"
        elif is_for_3days: trend = "👨🏼‍🦰외인매집"
        elif is_ins_3days: trend = "🏢기관매집"
        else: trend = "💧개인/관망"
    except: pass
    
    try:
        url2 = f"https://finance.naver.com/item/main.naver?code={code}"
        for d in pd.read_html(requests.get(url2, headers=REAL_HEADERS).text):
            if '최근 연간 실적' in str(d.columns):
                fin = d.set_index(d.columns[0])
                if 'EPS(원)' in fin.index:
                    badge = "💎흑자" if float(str(fin.loc['EPS(원)'].values[-1]).replace(',','')) > 0 else "⚠️적자"
                break
    except: pass
    return trend, badge, is_for_3days, is_ins_3days

# ---------------------------------------------------------
# ⚔️ [기능 7] 듀얼 엔진 (단테 유지 + 엑셀 추세 + 이격도 밀집 추가)
# ---------------------------------------------------------

# 🦁 [1] 추세 전략 (엑셀 검색식 기반 + 이격도 밀집)
def check_trend_strategy_excel(df, row, is_for_3days, is_ins_3days):
    score = 0; reasons = []
    
    # [조건 G, H] 정배열 우상향
    ma60_up = df['Close_MA60'].iloc[-1] > df['Close_MA60'].iloc[-2]
    ma120_up = df['Close_MA120'].iloc[-1] > df['Close_MA120'].iloc[-2]
    
    # [조건 O, P] 수급
    has_supply = is_for_3days or is_ins_3days
    
    # [조건 D] 거래량 급증
    vol_spike = row['Volume'] >= df['Volume'].iloc[-2] * 2.0
    
    # [조건 F] 골든크로스
    ma5 = row['Close_MA5']; ma20 = row['Close_MA20']
    golden = (df['Close_MA5'].iloc[-2] <= df['Close_MA20'].iloc[-2]) and (ma5 > ma20)

    # 🔥 [New] 이격도 밀집 (5, 10, 20, 60, 112)
    try:
        mas = [row['Close_MA5'], row['Close_MA10'], row['Close_MA20'], row['Close_MA60'], row['Close_MA112']]
        if all(not np.isnan(m) for m in mas):
            min_ma = min(mas); max_ma = max(mas)
            # 5% 이내로 모여있으면 밀집
            if (max_ma - min_ma) / min_ma <= 0.05:
                score += 30
                reasons.append("🌀이격도밀집")
    except: pass
    
    # 추세 점수
    if ma60_up and ma120_up: score += 30; reasons.append("📈정배열우상향")
    if has_supply: score += 30; reasons.append("💰메이저수급")
    if vol_spike: score += 20; reasons.append("💥거래량폭발")
    if golden: score += 20; reasons.append("✨골든크로스")

    if score >= 60: return True, score, reasons
    return False, 0, []

# 🥣 [2] 단테 전략 (보존)
def check_dante_strategy_original(df, row):
    ma112 = row['Close_MA112']; ma224 = row['Close_MA224']
    score = 0; reasons = []
    
    dist = (row['Close'] - ma112) / ma112
    if -0.05 <= dist <= 0.05: score += 40; reasons.append("🎯112선지지")
    if row['Close'] > ma224: score += 30; reasons.append("🔥224돌파")
    elif (ma224 - row['Close']) / row['Close'] < 0.05: score += 20; reasons.append("🔨224도전")
    
    if (df['Close'].iloc[-5:].std() / df['Close'].iloc[-5:].mean()) < 0.02: 
        score += 20; reasons.append("🛡️공구리")
    
    ma20 = row['Close_MA20']
    if row['Close'] > ma20 and df['Close'].iloc[-2] < df['Close_MA20'].iloc[-2]:
        score += 20; reasons.append("⛏️골파기")

    if score >= 40: return True, score, reasons
    return False, 0, []

# 🏭 통합 분석 함수
def analyze_stock(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=730)).strftime('%Y-%m-%d'))
        if len(df) < 225: return None
        
        # 🔥 이평선 계산 (10일선 추가!)
        # [5, 10, 20, 60, 112, 120, 224]
        for n in [5, 10, 20, 60, 112, 120, 224]: 
            df[f'Close_MA{n}'] = df['Close'].rolling(n).mean()
        row = df.iloc[-1]
        
        if row['Close'] < 1000 or row['Volume'] == 0: return None
        
        # 거래대금 10억 이상
        amount = (row['Close'] * row['Volume']) 
        if amount < 1000000000: return None

        # 재무/수급
        trend, badge, is_for_3, is_ins_3 = get_stock_data_extras(ticker)
        if "적자" in badge: return None

        # 전략 실행
        is_trend, s_trend, r_trend = check_trend_strategy_excel(df, row, is_for_3, is_ins_3)
        is_dante, s_dante, r_dante = check_dante_strategy_original(df, row)
        
        if not is_trend and not is_dante: return None
        
        category = "🦁추세Pick" if s_trend > s_dante else "🥣단테Pick"
        if is_trend and is_dante: category = "👑강력추천"
        
        total = s_trend + s_dante
        reasons = list(set(r_trend + r_dante))
        
        ai_msg = get_ai_summary(ticker, name, category, reasons)

        return {
            'code': ticker, '종목명': name, '현재가': int(row['Close']),
            '신호': " ".join(reasons), '총점': total,
            '수급현황': trend, 'Risk': badge,
            'AI_Pick': "",
            'msg': f"{category} {name} ({total}점)\n👉 신호: {' '.join(reasons)}\n💰 현재가: {int(row['Close']):,}원\n📊 {trend} / {badge}\n{ai_msg}\n➖➖➖➖➖➖➖➖➖➖➖➖\n"
        }
    except: return None

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [Ultimate Bot] {TODAY_STR} 시작")
    print("📸 차트 및 시황 생성 중...")
    charts = [create_index_chart('IXIC','NASDAQ'), create_index_chart('KS11','KOSPI'), create_index_chart('KQ11','KOSDAQ')]
    brief = get_market_briefing()
    if brief: send_telegram_photo(brief, charts)
    
    print("🔍 종목 스캔 중...")
    df_krx = fdr.StockListing('KRX')
    # 시총 500억 이상 (빠른 필터)
    df_leaders = df_krx[df_krx['Marcap'] >= 50000000000].sort_values(by='Amount', ascending=False).head(TOP_N)
    target_dict = dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    force = {'008350':'남선알미늄', '294630':'서남', '005930':'삼성전자'}
    for k, v in force.items(): 
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
        
        print("🏟️ AI 토너먼트 시작...")
        tournament_report, ai_picks_map = run_ai_tournament(top_50)
        
        for r in results:
            if r['code'] in ai_picks_map: r['AI_Pick'] = ai_picks_map[r['code']]
        
        print("📨 리포트 전송 중...")
        send_telegram_photo(tournament_report)
        
        final_msgs = [r['msg'] for r in results[:15]]
        header = f"💎 [예선 통과 상위 15개]\n(총 {len(results)}개 중 엄선)\n\n"
        chunk = header
        for msg in final_msgs:
            if len(chunk) + len(msg) > 4000:
                send_telegram_photo(chunk)
                chunk = "💎 [이어서] 다음 리스트\n\n" + msg
            else: chunk += msg
        if chunk: send_telegram_photo(chunk)
        
        try: update_google_sheet(results, TODAY_STR)
        except: pass
    else:
        print("❌ 발견된 종목 없음")
        send_telegram_photo("❌ 오늘 조건에 맞는 종목이 없습니다.")
