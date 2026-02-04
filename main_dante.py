# ------------------------------------------------------------------
# 🥣 [단테 전용] main_dante.py (손절가 = 112일선 변경)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import google.generativeai as genai 

# 기존 시트 매니저 활용
from google_sheet_manager import update_google_sheet

# =================================================
# ⚙️ [설정] 단테 기법 파라미터
# =================================================
TOP_N = 2500          # 검색 대상 2000개
DROP_RATE = 0.25      # 고점 대비 25% 이상 하락
STOP_LOSS_RANGE = 40  # (참고용 변수)

# 텔레그램 & API 설정
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')

# =================================================

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
def get_dante_summary(ticker, name, signal, stop_loss, ma_status):
    prompt = (f"나는 주식 유튜버 '단테'의 기법(밥그릇 패턴, 이평선 돌파)으로 종목을 분석 중이다.\n"
              f"종목: {name} ({ticker})\n"
              f"신호: {signal}\n"
              f"손절가: {stop_loss}원 (112일 이동평균선 지지라인)\n"
              f"이평선 상태: {ma_status}\n"
              f"위 정보를 바탕으로 '왜 이 자리가 중요한지'와 '손절 원칙'을 강조해서 1줄로 조언해줘. (한국어)")

    if GOOGLE_API_KEY:
        try:
            genai.configure(api_key=GOOGLE_API_KEY)
            model = genai.GenerativeModel('gemini-1.5-flash')
            res = model.generate_content(prompt)
            return f"\n🥣 {res.text.strip()} (Gemini)"
        except: pass
    
    if GROQ_API_KEY:
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
            payload = {"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}]}
            res = requests.post(url, json=payload, headers=headers, timeout=5)
            return f"\n🥣 {res.json()['choices'][0]['message']['content'].strip()} (Groq)"
        except: pass
        
    return ""

# ---------------------------------------------------------
# 🔍 [핵심] 단테 알고리즘 분석기
# ---------------------------------------------------------
def analyze_dante_stock(ticker, name):
    try:
        # 밥그릇 패턴을 보려면 최소 2년치 데이터 필요
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d'))
        if len(df) < 250: return None
        
        row = df.iloc[-1]
        
        if row['Close'] < 1000 or row['Volume'] == 0: return None

        # -----------------------------------------------------
        # 1. 이평선 계산
        # -----------------------------------------------------
        ma112 = df['Close'].rolling(112).mean().iloc[-1]
        ma224 = df['Close'].rolling(224).mean().iloc[-1]
            
        # -----------------------------------------------------
        # 2. 밥그릇 1번 체크 (고점 대비 하락폭)
        # -----------------------------------------------------
        past_high = df['High'].iloc[:-120].max() 
        current_price = row['Close']
        
        if current_price > past_high * (1 - DROP_RATE): 
            return None 

        # -----------------------------------------------------
        # 3. 밥그릇 3번 체크 (이평선 도전/지지)
        # -----------------------------------------------------
        # 범위 0.85 ~ 1.15
        is_near_112 = (ma112 * 0.85 <= current_price <= ma112 * 1.15)
        is_near_224 = (ma224 * 0.85 <= current_price <= ma224 * 1.15)
        
        if not (is_near_112 or is_near_224):
            return None 

        # -----------------------------------------------------
        # 4. 🔨 공구리 (손절가 = 112일선)
        # -----------------------------------------------------
        # ⚠️ [변경] 사용자 요청: 손절가를 112일 이평선 가격으로 설정
        stop_loss_price = int(ma112)
        
        # 현재가와 112일선(손절가)의 거리 계산
        # (만약 112일선 아래에 있다면 마이너스가 나올 수 있음 -> 즉시 손절 혹은 돌파 대기)
        risk_pct = (current_price - stop_loss_price) / current_price * 100
        
        # 112일선보다 너무 높게 떠있으면(30% 이상) 먹을 게 없으므로 패스
        if risk_pct > 30.0: return None 

        # -----------------------------------------------------
        # 5. 점수 및 신호 부여
        # -----------------------------------------------------
        score = 70
        signal = "🥣밥그릇_준비"
        ma_status = f"112선({int(ma112):,})"
        
        # 224일선 돌파하면 대박 (+20점)
										if row['Close'] > ma224:
    									score += 20
    									signal = "🔥224일선_돌파"
									# 뚫지는 못했지만 5% 이내로 바짝 붙어서 도전 중이면 우수 (+15점) 👈 추가!
									elif abs(row['Close'] - ma224) / ma224 < 0.05:
    										score += 15
    										signal = "🔨224일선_도전(공구리)"
        elif row['Close'] > ma112:
            score += 10
            signal = "🌊112일선_지지"
            ma_status = f"112선({int(ma112):,}) 지지"

        if df['Close'].iloc[-10] < df['Close'].iloc[-1]:
            score += 5

        ai_msg = get_dante_summary(ticker, name, signal, stop_loss_price, ma_status)
        
        return {
            'code': ticker,
            '종목명': name,
            '현재가': int(current_price),
            '신호': signal,
            '총점': score,
            '수급점수': 0, '패턴점수': score, '차트점수': int(100 - abs(risk_pct)),
            'msg': f"[{signal}] {name}\n"
                   f"💰 현재가: {int(current_price):,}원\n"
                   f"🛡️ 손절가: {stop_loss_price:,}원 (112일선)\n"
                   f"📉 고점대비: -{((past_high - current_price)/past_high*100):.1f}%\n"
                   f"📊 {ma_status} (이격: {risk_pct:.1f}%)\n"
                   f"{ai_msg}"
        }

    except Exception as e:
        return None

# ---------------------------------------------------------
# 🚀 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🥣 [단테의 밥그릇 봇] {datetime.now().strftime('%Y-%m-%d')} 분석 시작")
    print(f"📉 손절 기준: 112일 이동평균선")
    
    df_krx = fdr.StockListing('KRX')
    df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    target_dict = dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    
    results = []
    
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(analyze_dante_stock, t, n) for t, n in target_dict.items()]
        for future in futures:
            res = future.result()
            if res: results.append(res)
            
    if results:
        results.sort(key=lambda x: x['총점'], reverse=True)
        final_msgs = [r['msg'] for r in results[:10]]
        
        report = f"🥣 [단테 Pick] {len(results)}개 포착\n\n" + "\n\n".join(final_msgs)
        print(report)
        send_telegram(report)
        
        try:
            update_google_sheet(results, datetime.now().strftime('%Y-%m-%d'))
            print("💾 구글 시트 저장 완료")
        except Exception as e:
            print(f"❌ 시트 저장 실패: {e}")
            
    else:
        print("❌ 조건에 맞는 밥그릇 종목이 없습니다.")