# ------------------------------------------------------------------
# 👑 [The Ultimate Bot] 완벽 통합본 (시황+차트+듀얼엔진+AI분석)
# ------------------------------------------------------------------
import matplotlib.pyplot as plt
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
import mplfinance as mpf  # 📸 차트 기능 필수
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
TOP_N = 300            
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')

# 🔑 API 키
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') 
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')     

# 🌍 시간 설정
KST = pytz.timezone('Asia/Seoul')
current_time = datetime.now(KST)
NOW = current_time - timedelta(days=1) if current_time.hour < 8 else current_time
TODAY_STR = NOW.strftime('%Y-%m-%d')

# 🛡️ 네이버 차단 우회 헤더
REAL_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Referer': 'https://finance.naver.com/',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
}

# ---------------------------------------------------------
# 📸 [기능 1] 지수 차트 그리기 (텍스트 정보 추가 버전)
# ---------------------------------------------------------
def create_index_chart(ticker, name):
    print(f"🎨 {name} 차트 그리는 중...")
    try:
        # 1. 최근 데이터 가져오기
        end_date = datetime.now()
        start_date = end_date - timedelta(days=120) # 6개월은 너무 기니 4개월로 조정
        df = fdr.DataReader(ticker, start=start_date, end=end_date)
        
        if len(df) < 2: return None

        # 2. 등락률 및 현재가 계산
        latest_close = df['Close'].iloc[-1]
        prev_close = df['Close'].iloc[-2]
        change = latest_close - prev_close
        change_pct = (change / prev_close) * 100

        # 3. 텍스트 정보 만들기 (예: NASDAQ: 12,345.67 (+1.23%))
        sign = "+" if change_pct > 0 else ""
        info_text = f"{name}\n{latest_close:,.2f} ({sign}{change_pct:.2f}%)"
        text_color = 'red' if change_pct > 0 else ('blue' if change_pct < 0 else 'black')

        # 4. 차트 스타일 설정
        mc = mpf.make_marketcolors(up='r', down='b', inherit=True)
        s  = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=False)

        # 이평선 추가
        apds = [
            mpf.make_addplot(df['Close'].rolling(20).mean(), color='orange', width=1.5),
            mpf.make_addplot(df['Close'].rolling(60).mean(), color='purple', width=1.5)
        ]

        # 5. 차트 생성 (중요: returnfig=True로 객체를 받아옴)
        fig, axlist = mpf.plot(df, type='candle', style=s, addplot=apds,
                               title=f"", # 제목은 텍스트 박스로 대체
                               volume=False,
                               returnfig=True, # 👈 핵심! 그림 객체를 받아옵니다.
                               figscale=1.2, figratio=(10, 6),
                               datetime_format='%m-%d', xrotation=0)

        # 6. 차트 위에 텍스트 박스 추가 (왼쪽 상단)
        # axlist[0]이 메인 차트 영역입니다.
        axlist[0].text(0.03, 0.95, info_text, 
                       transform=axlist[0].transAxes, # 좌표 기준을 축(0~1)으로 설정
                       fontsize=16, fontweight='bold', color=text_color,
                       bbox=dict(facecolor='white', alpha=0.8, edgecolor='gray', boxstyle='round,pad=0.5'))

        # 7. 파일 저장
        filename = f"{name}_chart.png"
        fig.savefig(filename, bbox_inches='tight', pad_inches=0.1)
        plt.close(fig) # 메모리 해제

        return filename

    except Exception as e:
        print(f"⚠️ 차트 생성 실패({name}): {e}")
        return None

# 📸 사진 전송 함수 (텍스트 + 사진 묶음 전송)
def send_telegram_photo(message, image_paths=[]):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    
    url_photo = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    url_text = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    real_id_list = []
    for item in CHAT_ID_LIST:
        real_id_list.extend([x.strip() for x in item.split(',') if x.strip()])
    
    for chat_id in real_id_list:
        if not chat_id: continue
        
        # 1. 텍스트 먼저 전송 (시황 브리핑 등)
        if message:
            requests.post(url_text, data={'chat_id': chat_id, 'text': message})
            
        # 2. 이미지가 있으면 전송
        if image_paths:
            for img_path in image_paths:
                if img_path and os.path.exists(img_path):
                    try:
                        with open(img_path, 'rb') as f:
                            requests.post(url_photo, data={'chat_id': chat_id}, files={'photo': f})
                    except: pass
                    
    # 3. 전송 후 이미지 삭제 (청소)
    for img_path in image_paths:
        if img_path and os.path.exists(img_path): os.remove(img_path)

# ---------------------------------------------------------
# 🕵️ [New] 실시간 주도 테마/업종 긁어오기 (네이버 크롤링)
# ---------------------------------------------------------
def get_hot_themes():
    """
    네이버 증권에서 '테마별 시세'와 '업종별 시세' 상위권을 긁어옵니다.
    이게 있어야 GPT가 "로봇주가 강세다" 같은 말을 할 수 있습니다.
    """
    hot_info = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36'}
    
    try:
        # 1. 상승 테마 TOP 5
        url_theme = "https://finance.naver.com/sise/theme.naver"
        df_theme = pd.read_html(requests.get(url_theme, headers=headers).text)[0]
        df_theme = df_theme.dropna().head(5) # 상위 5개
        themes = df_theme['테마명'].tolist()
        hot_info.append(f"🔥강세테마: {', '.join(themes)}")

        # 2. 상승 업종 TOP 5
        url_up = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
        df_up = pd.read_html(requests.get(url_up, headers=headers).text)[0]
        df_up = df_up.dropna().head(5)
        sectors = df_up['업종명'].tolist()
        hot_info.append(f"📈강세업종: {', '.join(sectors)}")
        
        return "\n".join(hot_info)

    except Exception as e:
        return "테마 정보 수집 실패"

# ---------------------------------------------------------
# 📢 [기능 2] 시황 브리핑 (전문가 모드)
# ---------------------------------------------------------
def get_market_briefing():
    if not OPENAI_API_KEY: 
        print("⚠️ OpenAI 키 없음: 시황 브리핑 스킵")
        return None
        
    print("🌍 실시간 테마 및 지수 데이터 수집 중...")
    try:
        # 1. 지수 데이터 (숫자)
        kospi = fdr.DataReader('KS11', start=datetime.now() - timedelta(days=5))
        kosdaq = fdr.DataReader('KQ11', start=datetime.now() - timedelta(days=5))
        nasdaq = fdr.DataReader('IXIC', start=datetime.now() - timedelta(days=5))
        
        def get_change(df):
            if len(df) < 2: return "0.00"
            curr = df['Close'].iloc[-1]; prev = df['Close'].iloc[-2]
            return f"{(curr - prev) / prev * 100:+.2f}%"

        index_data = f"나스닥:{get_change(nasdaq)}, 코스피:{get_change(kospi)}, 코스닥:{get_change(kosdaq)}"
        
        # 2. 🔥 주도 테마 데이터 (여기가 핵심!)
        theme_data = get_hot_themes()
        
        # 3. GPT에게 명령 (프롬프트 강화)
        prompt = (f"시장 데이터: {index_data}\n"
                  f"주도 섹터: {theme_data}\n\n"
                  f"위 데이터를 바탕으로 주식 트레이더에게 '오늘의 시장 흐름'을 브리핑해줘.\n"
                  f"단순히 지수가 올랐다는 말 말고, '미장은 빠졌는데 국장은 특정 테마(로봇, 반도체 등) 중심으로 버티고 있다'는 식으로 섹터와 연관 지어 분석해.\n"
                  f"말투: 통찰력 있는 전문가의 반말 (3줄 요약).")
        
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"user", "content":prompt}])
        return f"📢 [오늘의 시황]\n{res.choices[0].message.content.strip()}"

    except Exception as e: 
        print(f"⚠️ 시황 에러: {e}")
        return None

# ---------------------------------------------------------
# 🧠 [기능 3] AI 종목 분석 (줄바꿈 제거 + 가독성 향상)
# ---------------------------------------------------------
def get_ai_summary(ticker, name, category, reasons):
    # 🔥 프롬프트 대폭 수정: "차트 얘기 금지, 회사 업종만 말해!"
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
# 📊 [기능 4] 공통 데이터 (수급/재무 - 네이버 차단 우회)
# ---------------------------------------------------------
def get_common_data(code):
    trend = "정보없음"; badge = "⚖️보통"
    try: # 수급
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        resp = requests.get(url, headers=REAL_HEADERS, timeout=3)
        dfs = pd.read_html(StringIO(resp.text), match='날짜')
        if dfs:
            target_df = dfs[0].dropna()
            target_df = target_df[target_df['날짜'].astype(str).str.contains('날짜') == False]
            if len(target_df) > 0:
                latest = target_df.iloc[0]
                buy = int(str(latest['외국인']).replace(',', '')) > 0
                ins = int(str(latest['기관']).replace(',', '')) > 0
                trend = "🚀쌍끌이" if (buy and ins) else ("👨🏼‍🦰외인" if buy else ("🏢기관" if ins else "💧개인"))
    except: pass
    try: # 재무
        url2 = f"https://finance.naver.com/item/main.naver?code={code}"
        resp2 = requests.get(url2, headers=REAL_HEADERS, timeout=3)
        dfs2 = pd.read_html(StringIO(resp2.text))
        for df in dfs2:
            if '최근 연간 실적' in str(df.columns) or '주요재무제표' in str(df.columns):
                if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(1)
                fin = df.set_index(df.columns[0])
                target_key = next((k for k in fin.index if 'EPS' in str(k)), None)
                if target_key:
                    vals = fin.loc[target_key].values
                    last_val = 0
                    for v in vals:
                        v_str = str(v).replace(',', '')
                        if v_str.replace('.', '', 1).replace('-', '', 1).isdigit(): last_val = float(v_str)
                    if last_val < 0: badge = "⚠️적자"
                    elif last_val > 0: badge = "💎흑자"
                break
    except: pass
    return trend, badge

# ---------------------------------------------------------
# ⚔️ [기능 5] 듀얼 엔진 (추세 + 단테)
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

# ---------------------------------------------------------
# 🕵️‍♂️ 통합 분석 엔진 (가독성 패치 완료)
# ---------------------------------------------------------
def analyze_stock(ticker, name, mode='realtime'): 
    try:
        # 1. 데이터 가져오기
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=730)).strftime('%Y-%m-%d'))
        if len(df) < 225: return None
        row = df.iloc[-1]
        if row['Close'] < 1000 or row['Volume'] == 0: return None

        # 2. 전략 체크
        is_trend, s_trend, r_trend = check_trend_strategy(df, row)
        is_dante, s_dante, r_dante = check_dante_strategy(df, row)
        if not is_trend and not is_dante: return None

        # 3. 등급 산정
        category = ""; final_score = 0; final_reasons = []
        if is_trend and is_dante:
            category = "👑 [강력추천/겹침]"; final_score = s_trend + s_dante
            final_reasons = list(set(r_trend + r_dante))
        elif is_trend:
            category = "🦁 [추세 Pick]"; final_score = s_trend; final_reasons = r_trend
        elif is_dante:
            category = "🥣 [단테 Pick]"; final_score = s_dante; final_reasons = r_dante

        # 4. 데이터 조회
        trend, badge = get_common_data(ticker)
        
        # 5. AI 요약
        ai_msg = ""
        # ⚠️ 점수 0점 이상이면 무조건 AI 호출 (테스트용)
        if final_score >= 0: 
            ai_msg = get_ai_summary(ticker, name, category, final_reasons)

        # 6. 메시지 생성 (줄바꿈 \n 확실하게 추가!)
        return {
            'code': ticker, '종목명': name, '현재가': int(row['Close']),
            '신호': " ".join(final_reasons), '총점': final_score,
            '수급현황': trend, 'Risk': badge,
            'msg': f"{category} {name} ({final_score}점)\n"
                   f"👉 신호: {' '.join(final_reasons)}\n"
                   f"💰 현재가: {int(row['Close']):,}원\n"
                   f"📊 {trend} / {badge}"
                   f"{ai_msg}\n\n"               # 👈 AI 멘트 끝나고 두 줄 띄움
                   f"➖➖➖➖➖➖➖➖➖➖➖➖\n" # 👈 구분선 뒤에도 줄바꿈 추가
        }
    except: return None

# ---------------------------------------------------------
# 🚀 메인 실행 (이 부분이 가장 중요합니다!!)
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [Ultimate Bot] {TODAY_STR} 시작")
    
    # 1. 📊 차트 생성 (나스닥/코스피/코스닥)
    print("📸 지수 차트 3장 생성 중...")
    charts = [
        create_index_chart('IXIC', 'NASDAQ'),
        create_index_chart('KS11', 'KOSPI'),
        create_index_chart('KQ11', 'KOSDAQ')
    ]
    
    # 2. 📢 시황 브리핑 생성
    print("🌍 시황 브리핑 작성 중...")
    brief = get_market_briefing()
    
    # 3. 📨 [중요] 브리핑 + 차트 먼저 전송!
    if brief:
        print("📨 시황 텔레그램 전송 중...")
        send_telegram_photo(brief, charts)
    else:
        print("⚠️ 시황 브리핑 생성 실패 (API 키 확인 필요)")
    
    # 4. 🔍 종목 스캔 시작
    print("🔍 종목 스캔 중... (잠시만 기다려주세요)")
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
        final_msgs = [r['msg'] for r in results[:15]]
        
        report = f"💎 [오늘의 발굴] {len(results)}개 완료\n\n" + "\n\n".join(final_msgs)
        print(report)
        send_telegram_photo(report, []) # 종목 리스트는 텍스트로만 전송
        try: update_google_sheet(results, TODAY_STR)
        except: pass
    else:
        print("❌ 발견된 종목 없음")
