# ------------------------------------------------------------------
# 🥣 [단테 전용] main_dante.py
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

# 기존 시트 매니저 활용 (기록은 한 곳에 모으는 게 좋습니다)
from google_sheet_manager import update_google_sheet

# =================================================
# ⚙️ [설정] 단테 기법 파라미터
# =================================================
TOP_N = 500           # 검색 대상 (코스피/코스닥 상위 500개)
DROP_RATE = 0.30      # 고점 대비 하락폭 (최소 30% 이상 빠진 놈만)
STOP_LOSS_RANGE = 40  # 손절가 산정 기준 (최근 40일 최저가)

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
# 🤖 AI 요약 (단테 스타일로 프롬프트 변경)
# ---------------------------------------------------------
def get_dante_summary(ticker, name, signal, stop_loss, ma_status):
    prompt = (f"나는 주식 유튜버 '단테'의 기법(밥그릇 패턴, 이평선 돌파)으로 종목을 분석 중이다.\n"
              f"종목: {name} ({ticker})\n"
              f"신호: {signal}\n"
              f"손절가: {stop_loss}원 (지지라인)\n"
              f"이평선 상태: {ma_status}\n"
              f"위 정보를 바탕으로 '왜 이 자리가 중요한지'와 '손절 원칙'을 강조해서 1줄로 조언해줘. (한국어)")

    # 1. Gemini
    if GOOGLE_API_KEY:
        try:
            genai.configure(api_key=GOOGLE_API_KEY)
            model = genai.GenerativeModel('gemini-1.5-flash')
            res = model.generate_content(prompt)
            return f"\n🥣 {res.text.strip()} (Gemini)"
        except: pass
    
    # 2. Groq
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
        # 밥그릇 패턴을 보려면 최소 2년치 데이터 필요 (224일선, 448일선 계산)
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d'))
        if len(df) < 250: return None
        
        row = df.iloc[-1]
        
        # 기본 필터: 동전주 제외, 거래 정지 제외
        if row['Close'] < 1000 or row['Volume'] == 0: return None

        # -----------------------------------------------------
        # 1. 이평선 계산 (112일, 224일, 448일)
        # -----------------------------------------------------
        ma112 = df['Close'].rolling(112).mean().iloc[-1]
        ma224 = df['Close'].rolling(224).mean().iloc[-1]
        # ma448은 데이터 부족할 수도 있으니 예외처리
        ma448 = 0
        if len(df) >= 448:
            ma448 = df['Close'].rolling(448).mean().iloc[-1]
            
        # -----------------------------------------------------
        # 2. 밥그릇 1번 체크 (고점 대비 하락폭)
        # -----------------------------------------------------
        # 2년 전 ~ 6개월 전 사이의 '최고가'를 찾음
        past_high = df['High'].iloc[:-120].max() 
        current_price = row['Close']
        
        # 고점 대비 -30% 이상 빠져 있어야 함 (가격 조정 완료)
        if current_price > past_high * (1 - DROP_RATE): 
            return None # 아직 덜 빠짐 (밥그릇 1번 미완성)

        # -----------------------------------------------------
        # 3. 밥그릇 3번 체크 (이평선 도전/지지)
        # -----------------------------------------------------
        # 현재가가 112일선 혹은 224일선 근처(-5% ~ +10%)에 있어야 함
        is_near_112 = (ma112 * 0.95 <= current_price <= ma112 * 1.10)
        is_near_224 = (ma224 * 0.95 <= current_price <= ma224 * 1.10)
        
        if not (is_near_112 or is_near_224):
            return None # 이평선이랑 상관없는 자리는 패스

        # -----------------------------------------------------
        # 4. 🔨 공구리 (손절가 자동 계산)
        # -----------------------------------------------------
        # 최근 40일(약 2달) 간의 최저가를 '세력의 지지 라인'으로 봄
        
# [수정 후] 손절가를 40일 최저가가 아니라 '112일 이평선' 가격으로 변경
recent_low = df['Low'].iloc[-STOP_LOSS_RANGE:].min()
ma112 = df['Close'].rolling(112).mean().iloc[-1]

# "최저가"와 "112일선" 중 더 높은 가격을 손절가로 잡음 (손절폭을 줄이기 위해)
stop_loss_price = int(max(recent_low, ma112 * 0.95)) # 112일선 살짝 아래

        # 현재가가 손절가랑 너무 멀면 안 됨 (손익비 꽝) -> 15% 이내여야 함
        risk_pct = (current_price - stop_loss_price) / current_price * 100
        if risk_pct > 15.0: return None 

        # -----------------------------------------------------
        # 5. 점수 및 신호 부여
        # -----------------------------------------------------
        score = 70
        signal = "🥣밥그릇_준비"
        ma_status = f"112선({int(ma112):,})"
        
        # 224일선(검은선) 돌파는 강력한 신호 (+20점)
        if row['Close'] > ma224:
            score += 20
            signal = "🔥224일선_돌파"
            ma_status = f"224선({int(ma224):,}) 돌파"
        # 112일선(파란선) 돌파 (+10점)
        elif row['Close'] > ma112:
            score += 10
            signal = "🌊112일선_지지"
            ma_status = f"112선({int(ma112):,}) 지지"

        # 골파기 후 V자 반등 체크 (최근 10일 상승세)
        if df['Close'].iloc[-10] < df['Close'].iloc[-1]:
            score += 5

        ai_msg = get_dante_summary(ticker, name, signal, stop_loss_price, ma_status)
        
        return {
            'code': ticker,
            '종목명': name,
            '현재가': int(current_price),
            '신호': signal,
            '총점': score,
            '수급점수': 0, '패턴점수': score, '차트점수': int(100 - risk_pct), # 시트 호환용
            'msg': f"[{signal}] {name}\n"
                   f"💰 현재가: {int(current_price):,}원\n"
                   f"🛡️ 손절가: {stop_loss_price:,}원 (Risk: -{risk_pct:.1f}%)\n"
                   f"📉 고점대비: -{((past_high - current_price)/past_high*100):.1f}%\n"
                   f"📊 {ma_status}\n"
                   f"{ai_msg}"
        }

    except Exception as e:
        return None

# ---------------------------------------------------------
# 🚀 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🥣 [단테의 밥그릇 봇] {datetime.now().strftime('%Y-%m-%d')} 분석 시작")
    print(f"📉 기준: 고점 대비 30% 하락 & 112/224일선 공략")
    
    # KRX 상위 종목 수집
    df_krx = fdr.StockListing('KRX')
    # 거래대금 상위 500개 (너무 잡주는 제외)
    df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    target_dict = dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    
    results = []
    
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(analyze_dante_stock, t, n) for t, n in target_dict.items()]
        for future in futures:
            res = future.result()
            if res: results.append(res)
            
    if results:
        # 점수순 정렬
        results.sort(key=lambda x: x['총점'], reverse=True)
        final_msgs = [r['msg'] for r in results[:10]] # 상위 10개만
        
        report = f"🥣 [단테 Pick] {len(results)}개 포착\n\n" + "\n\n".join(final_msgs)
        print(report)
        send_telegram(report)
        
        # 구글 시트에 저장 (기존 함수 재사용)
        # '단테'라고 따로 표시되도록 리스트 전달
        try:
            update_google_sheet(results, datetime.now().strftime('%Y-%m-%d'))
            print("💾 구글 시트 저장 완료")
        except Exception as e:
            print(f"❌ 시트 저장 실패: {e}")
            
    else:
        print("❌ 조건에 맞는 밥그릇 종목이 없습니다.")