# ------------------------------------------------------------------
# 🥣 [단테 전용] main_dante.py (황금 타점 가산점 Ver)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# Gemini (없으면 패스)
try:
    import google.generativeai as genai
except ImportError:
    genai = None

# 시트 매니저
from google_sheet_manager import update_google_sheet

# =================================================
# ⚙️ [설정] 파라미터
# =================================================
TOP_N = 2500            # 전 종목 검색
DROP_RATE = 0.15        # 고점대비 하락 기준 (완화)
STOP_LOSS_BUFFER = 0.95 # 112일선 -5% 여유

# 텔레그램 설정
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')

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
# 🤖 AI 한줄평
# ---------------------------------------------------------
def get_dante_summary(ticker, name, signal, stop_loss):
    prompt = (f"단테 기법으로 '{name}' 종목을 포착했다. 신호: {signal}. "
              f"손절가는 {stop_loss}원이다. 매력도와 주의사항을 딱 1줄로 요약해줘.")
    
    if GOOGLE_API_KEY and genai:
        try:
            genai.configure(api_key=GOOGLE_API_KEY)
            model = genai.GenerativeModel('gemini-1.5-flash')
            res = model.generate_content(prompt)
            return f"\n🤖 {res.text.strip()}"
        except: pass
    return ""

# ---------------------------------------------------------
# 🔍 [핵심] 점수 산정 로직 (대폭 수정됨)
# ---------------------------------------------------------
def analyze_dante_stock(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d'))
        if len(df) < 225: return None
        
        row = df.iloc[-1]
        current_price = row['Close']
        
        # 기본 필터 (동전주, 거래정지 제외)
        if current_price < 500 or row['Volume'] == 0: return None

        # 1. 이평선 & 고점 계산
        ma112 = df['Close'].rolling(112).mean().iloc[-1]
        ma224 = df['Close'].rolling(224).mean().iloc[-1]
        past_high = df['High'].iloc[:-120].max() 
        
        # 2. 밥그릇 1번 (하락폭 체크) - 기준 완화
        if current_price > past_high * (1 - DROP_RATE): return None 

        # 3. 밥그릇 3번 구간인가? (112일선 위 or 근처)
        # 112일선보다 -10% ~ +30% 구간에 있으면 일단 후보 등록
        dist_112 = (current_price - ma112) / ma112
        if not (-0.10 <= dist_112 <= 0.30): return None
        
        # -----------------------------------------------------
        # 🏆 점수 채점 (여기가 핵심!)
        # -----------------------------------------------------
        score = 50 # 기본점수
        signal_list = []
        
        # [1] 🎯 황금 타점 (+30점)
        # 112일선(파란선)을 깔고 앉아있는 자리 (이격도 0% ~ 5%)
        # 여기가 손절은 짧고 먹을 건 많은 최고의 자리!
        if 0 <= dist_112 <= 0.05:
            score += 30
            signal_list.append("🎯맥점(손익비Good)")
        
        # [2] 🔥 224일선 도전/돌파 (+20점)
        if row['Close'] > ma224:
            score += 20
            signal_list.append("🔥224돌파")
        elif (ma224 - current_price) / current_price < 0.05:
            score += 15
            signal_list.append("🔨224도전")
            
        # [3] 🛡️ 공구리 (주가 관리) (+15점)
        # 최근 5일간 변동폭이 작음 (누군가 가격 관리 중)
        recent_volatility = df['Close'].iloc[-5:].std() / df['Close'].iloc[-5:].mean()
        if recent_volatility < 0.02: # 2% 이내 변동
            score += 15
            signal_list.append("🛡️공구리(횡보)")
            
        # [4] 🤫 매집봉 발견 (+15점)
        # 최근 20일 내에 거래량 2배 터진 양봉이 있는데, 가격은 제자리임
        vol_avg = df['Volume'].iloc[-20:].mean()
        has_volume_spike = any((df['Volume'].iloc[-20:] > vol_avg * 2) & (df['Close'].iloc[-20:] > df['Open'].iloc[-20:]))
        if has_volume_spike and dist_112 < 0.1: # 가격은 안 떴는데 거래량만 터짐
            score += 15
            signal_list.append("🤫매집의심")

        # 손절가 설정 (112일선 -5%)
        stop_loss_price = int(ma112 * STOP_LOSS_BUFFER)
        
        # 최종 신호 문자열
        signal = " / ".join(signal_list) if signal_list else "밥그릇_관심"
        
        # 점수 미달 탈락 (70점 미만 잡)
        if score < 70: return None

        ai_msg = get_dante_summary(ticker, name, signal, stop_loss_price)
        
        return {
            'code': ticker,
            '종목명': name,
            '현재가': int(current_price),
            '신호': signal,
            '총점': score,
            'msg': f"🥣 [단테 Pick] {name} ({score}점)\n"
                   f"👉 {signal}\n"
                   f"💰 현재가: {int(current_price):,}원\n"
                   f"🛡️ 손절가: {stop_loss_price:,}원 (이탈시 컷)\n"
                   f"📊 이격도: 112선과 {dist_112*100:.1f}% 차이\n"
                   f"{ai_msg}"
        }

    except Exception as e:
        return None

# ---------------------------------------------------------
# 🚀 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🥣 [단테 봇] 바닥주/매집주 집중 발굴 시작...")
    
    df_krx = fdr.StockListing('KRX')
    df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    target_dict = dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    
    # 🕵️‍♂️ (테스트용) 단테 추천주가 리스트에 없으면 강제 추가해서 검증
    force_list = {'008350':'남선알미늄', '294630':'서남'}
    for k, v in force_list.items():
        if k not in target_dict: target_dict[k] = v

    results = []
    
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(analyze_dante_stock, t, n) for t, n in target_dict.items()]
        for future in futures:
            res = future.result()
            if res: results.append(res)
            
    if results:
        # 점수순 정렬
        results.sort(key=lambda x: x['총점'], reverse=True)
        final_msgs = [r['msg'] for r in results[:15]]
        
        report = f"🥣 [오늘의 단테 픽] {len(results)}개 발견\n\n" + "\n\n".join(final_msgs)
        print(report)
        send_telegram(report)
        
        try:
            update_google_sheet(results, datetime.now().strftime('%Y-%m-%d'))
        except: pass
    else:
        print("❌ 검색된 종목이 없습니다.")