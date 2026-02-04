# ------------------------------------------------------------------
# 🥣 [단테 전용] main_dante.py (바닥권 분출 가산점 강화 Ver)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# Gemini 라이브러리
try:
    import google.generativeai as genai
except ImportError:
    genai = None

# 기존 시트 매니저 활용
from google_sheet_manager import update_google_sheet

# =================================================
# ⚙️ [설정] 단테 기법 파라미터
# =================================================
TOP_N = 2500          # 전체 종목 검색
DROP_RATE = 0.25      # 고점 대비 25% 이상 하락
STOP_LOSS_BUFFER = 0.95  # 112일선 -5% 여유

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
              f"손절가: {stop_loss}원 (112일선 -5% 구간)\n"
              f"이평선 상태: {ma_status}\n"
              f"위 정보를 바탕으로 '왜 이 자리가 중요한지'와 '손절 원칙'을 강조해서 1줄로 조언해줘. (한국어)")

    if GOOGLE_API_KEY and genai:
        try:
            genai.configure(api_key=GOOGLE_API_KEY)
            model = genai.GenerativeModel('gemini-1.5-flash')
            res = model.generate_content(prompt)
            return f"\n🥣 {res.text.strip()} (Gemini)"
        except Exception: pass
    
    if GROQ_API_KEY:
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
            payload = {"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}]}
            res = requests.post(url, json=payload, headers=headers, timeout=5)
            response_json = res.json()
            if 'choices' in response_json:
                return f"\n🥣 {response_json['choices'][0]['message']['content'].strip()} (Groq)"
        except: pass
        
    return ""

# ---------------------------------------------------------
# 🔍 [핵심] 단테 알고리즘 분석기
# ---------------------------------------------------------
def analyze_dante_stock(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d'))
        if len(df) < 250: return None
        
        row = df.iloc[-1]
        prev = df.iloc[-2]
        if row['Close'] < 1000 or row['Volume'] == 0: return None

        # 1. 이평선 계산
        ma112 = df['Close'].rolling(112).mean().iloc[-1]
        ma224 = df['Close'].rolling(224).mean().iloc[-1]
        
        # 거래량 이동평균 (20일) - 거래량 터진거 확인용
        vol_ma20 = df['Volume'].rolling(20).mean().iloc[-1]
            
        # 2. 밥그릇 1번 (하락폭)
        past_high = df['High'].iloc[:-120].max() 
        current_price = row['Close']
        if current_price > past_high * (1 - DROP_RATE): return None 

        # 3. 밥그릇 3번 (이평선 근처)
        is_near_112 = (ma112 * 0.85 <= current_price <= ma112 * 1.15)
        is_near_224 = (ma224 * 0.85 <= current_price <= ma224 * 1.15)
        if not (is_near_112 or is_near_224): return None 

        # 4. 🔨 공구리 (손절가 = 112일선 - 5% 버퍼)
        stop_loss_price = int(ma112 * STOP_LOSS_BUFFER)
        risk_pct = (current_price - stop_loss_price) / current_price * 100
        if risk_pct > 35.0: return None 

        # -----------------------------------------------------
        # 5. 🔥 점수 계산 (여기가 핵심!)
        # -----------------------------------------------------
        score = 60 # 기본점수 시작
        signal = "🥣밥그릇_준비"
        ma_status = f"112선({int(ma112):,})"
        
        # [A] 위치 점수 (Position)
        if row['Close'] > ma224:
            score += 15
            signal = "🔥224일선_돌파"
            ma_status = f"224선({int(ma224):,}) 돌파"
        elif row['Close'] > ma112:
            score += 10
            signal = "🌊112일선_지지"
            ma_status = f"112선({int(ma112):,}) 지지"
        
        # [B] 도전 점수 (Challenge) - 뚫기 직전이면 점수 팍팍!
        dist_224 = abs(row['Close'] - ma224) / ma224
        if row['Close'] < ma224 and dist_224 < 0.05: # 5% 이내로 근접
            score += 20 # 뚫은 놈보다 더 줌 (기대감)
            signal = "🔨224일선_도전(강력)"

        # [C] 🌋 마그마 점수 (Energy) - 바닥에서 거래량 터지면 가산점
        # 평소 거래량의 200% 이상 터짐 + 양봉
        if row['Volume'] > vol_ma20 * 2.0 and row['Close'] > row['Open']:
            score += 20
            signal = f"🌋바닥_거래폭발+{signal}"
        
        # [D] 기세 점수 (Momentum) - 오늘 3% 이상 상승 중
        if row['Pct'] >= 3.0:
            score += 10
            
        # V자 반등 (최근 10일 상승세)
        if df['Close'].iloc[-10] < df['Close'].iloc[-1]:
            score += 5

        # AI 요약 호출
        ai_msg = get_dante_summary(ticker, name, signal, stop_loss_price, ma_status)
        
        return {
            'code': ticker,
            '종목명': name,
            '현재가': int(current_price),
            '신호': signal,
            '총점': score,
            '수급점수': 0, '패턴점수': score, '차트점수': int(100 - abs(risk_pct)),
            'msg': f"[{signal}] {name}\n"
                   f"📊 점수: {score}점 (바닥 에너지⚡)\n"
                   f"💰 현재가: {int(current_price):,}원 ({row['Pct']:+.2f}%)\n"
                   f"🛡️ 손절가: {stop_loss_price:,}원 (112선 -5%)\n"
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
    print(f"🥣 [단테 봇] {datetime.now().strftime('%Y-%m-%d')} Gemini 모드 분석 시작")
    print("📉 전략: 바닥권 거래량 폭발 & 112일선 지지 (분출 대기 종목 가산점)")

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
        
        report = f"🥣 [단테 Pick] {len(results)}개 포착 (바닥 에너지⚡)\n\n" + "\n\n".join(final_msgs)
        print(report)
        send_telegram(report)
        
        try:
            update_google_sheet(results, datetime.now().strftime('%Y-%m-%d'))
        except: pass
            
    else:
        print("❌ 조건에 맞는 종목이 없습니다.")