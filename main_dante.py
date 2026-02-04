# ------------------------------------------------------------------
# 🥣 [단테 봇] main_dante.py (긴급 디버깅 모드)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# 기존 시트 매니저
from google_sheet_manager import update_google_sheet

# =================================================
# ⚙️ [긴급 설정] 거름망 대폭 완화
# =================================================
TOP_N = 2500            # 검색 대상 (2500개)
DROP_RATE = 0.10        # 📉 고점대비 하락 (기존 0.25 -> 0.10 로 대폭 완화)
MA_MARGIN = 0.30        # 📊 이평선 거리 (기존 0.15 -> 0.30 로 대폭 완화)
STOP_LOSS_BUFFER = 0.95 # 손절가 (112일선 -5%)

# 🚨 AI 잠시 끄기 (오류 방지)
USE_AI = False 

# 🕵️‍♂️ [수사반장] 얘네들은 탈락해도 이유를 꼬치꼬치 캐묻는다!
DEBUG_TARGETS = ['서남', '남선알미늄', '테라뷰', 'SK이터닉스']

# 텔레그램 설정
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')

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
# 🔍 [핵심] 단테 알고리즘 (디버깅 강화)
# ---------------------------------------------------------
def analyze_dante_stock(ticker, name):
    try:
        # 데이터 가져오기
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d'))
        
        # 1. 데이터 부족 체크
        if len(df) < 225: 
            if name in DEBUG_TARGETS:
                print(f"🕵️‍♂️ [추적] {name}: ❌ 데이터 부족 (상장 {len(df)}일차 -> 225일 필요)")
            return None
        
        row = df.iloc[-1]
        current_price = row['Close']
        
        # 2. 동전주/거래정지 체크
        if current_price < 500 or row['Volume'] == 0: return None

        # -----------------------------------------------------
        # 지표 계산
        # -----------------------------------------------------
        ma112 = df['Close'].rolling(112).mean().iloc[-1]
        ma224 = df['Close'].rolling(224).mean().iloc[-1]
        past_high = df['High'].iloc[:-120].max() # 6개월 전 고점
        
        # -----------------------------------------------------
        # 🔍 조건 체크 (하나라도 걸리면 탈락)
        # -----------------------------------------------------
        
        # [조건 A] 고점 대비 하락했는가?
        drop_pct = (past_high - current_price) / past_high
        if drop_pct < DROP_RATE: 
            if name in DEBUG_TARGETS:
                print(f"🕵️‍♂️ [추적] {name}: ❌ 하락폭 부족 (현재 -{drop_pct*100:.1f}% < 기준 {DROP_RATE*100}%)")
            return None 

        # [조건 B] 이평선 근처인가? (기준: MA_MARGIN = 30%)
        # 112일선 근처 or 224일선 근처
        is_near_112 = abs(current_price - ma112) / ma112 <= MA_MARGIN
        is_near_224 = abs(current_price - ma224) / ma224 <= MA_MARGIN
        
        if not (is_near_112 or is_near_224):
            if name in DEBUG_TARGETS:
                dist112 = abs(current_price - ma112) / ma112 * 100
                dist224 = abs(current_price - ma224) / ma224 * 100
                print(f"🕵️‍♂️ [추적] {name}: ❌ 이평선과 너무 멉니다 (112선과 {dist112:.1f}%, 224선과 {dist224:.1f}%)")
            return None 

        # [조건 C] 손절선 이격도 (손익비)
        stop_loss_price = int(ma112 * STOP_LOSS_BUFFER)
        risk_pct = (current_price - stop_loss_price) / current_price * 100
        
        # 위험도가 50% 넘어가면 컷 (아주 널널하게 잡음)
        if risk_pct > 50.0: 
            if name in DEBUG_TARGETS:
                print(f"🕵️‍♂️ [추적] {name}: ❌ 손절가 너무 멉니다 (-{risk_pct:.1f}%)")
            return None 

        # -----------------------------------------------------
        # 🏆 합격! 점수 계산
        # -----------------------------------------------------
        score = 60
        signal = "🥣밥그릇_준비"
        ma_status = f"112선({int(ma112):,})"
        
        if row['Close'] > ma224:
            score += 15
            signal = "🔥224일선_돌파"
        elif row['Close'] > ma112:
            score += 10
            signal = "🌊112일선_지지"
        
        # 수사반장 타겟이면 합격 소식도 출력
        if name in DEBUG_TARGETS:
            print(f"🕵️‍♂️ [추적] {name}: 🎉 조건 통과! (점수: {score})")

        ai_msg = "" # AI 끔

        return {
            'code': ticker,
            '종목명': name,
            '현재가': int(current_price),
            '신호': signal,
            '총점': score,
            '수급점수': 0, '패턴점수': score, '차트점수': int(100 - abs(risk_pct)),
            'msg': f"[{signal}] {name}\n"
                   f"💰 현재가: {int(current_price):,}원\n"
                   f"🛡️ 손절가: {stop_loss_price:,}원 (112선 -5%)\n"
                   f"📉 고점대비: -{drop_pct*100:.1f}%\n"
                   f"📊 점수: {score}점"
        }

    except Exception as e:
        if name in DEBUG_TARGETS:
            print(f"🕵️‍♂️ [추적] {name}: 🚨 에러 발생 ({e})")
        return None

# ---------------------------------------------------------
# 🚀 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🥣 [단테 봇] 긴급 점검 모드 시작 (AI Off)")
    print(f"🕵️‍♂️ 추적 대상: {DEBUG_TARGETS}")
    
    df_krx = fdr.StockListing('KRX')
    
    # 거래대금 상위 2500개
    df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    
    # 딕셔너리 변환
    target_dict = dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    
    # ⚠️ 혹시 목록에 없으면 강제 추가 (검사하기 위해)
    # 테라뷰, 서남, 남선알미늄 코드가 2500등 안에 없어도 강제로 검사시킴
    force_targets = {
        '008350': '남선알미늄', '294630': '서남', '475150': '테라뷰', '458730': 'SK이터닉스'
    }
    for code, name in force_targets.items():
        target_dict[code] = name # 강제 추가

    results = []
    
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(analyze_dante_stock, t, n) for t, n in target_dict.items()]
        for future in futures:
            res = future.result()
            if res: results.append(res)
            
    if results:
        results.sort(key=lambda x: x['총점'], reverse=True)
        final_msgs = [r['msg'] for r in results[:15]] # 15개만
        
        report = f"🥣 [단테 Pick] {len(results)}개 포착 (조건완화)\n\n" + "\n\n".join(final_msgs)
        print(report)
        send_telegram(report)
        
        try:
            update_google_sheet(results, datetime.now().strftime('%Y-%m-%d'))
        except: pass
            
    else:
        print("❌ 조건 완화에도 불구하고 검색된 종목이 없습니다.")