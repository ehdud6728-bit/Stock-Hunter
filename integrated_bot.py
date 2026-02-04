import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import time
import requests
import os

# 우리가 만든 모듈들 임포트
from main import analyze_stock as analyze_main          # 기존 봇
from main_dante import analyze_dante_stock             # 단테 봇
from google_sheet_manager import update_google_sheet   # 시트 매니저

# =================================================
# ⚙️ 설정
# =================================================
TOP_N = 600  # 깃허브는 힘이 좋으니 600개까지 늘려서 샅샅이 뒤집니다!
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')

# ---------------------------------------------------------
# 📨 텔레그램 전송
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    real_id_list = [x.strip() for item in CHAT_ID_LIST for x in item.split(',') if x.strip()]
    
    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
    for chat_id in real_id_list:
        if not chat_id: continue
        for chunk in chunks:
            try: requests.post(url, data={'chat_id': chat_id, 'text': chunk})
            except: pass

# ---------------------------------------------------------
# 🚀 메인 로직
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🤖 [Integrated Bot] 통합 분석 시작... (Target: Top {TOP_N})")
    
    # 1. 시장 데이터 확보 (한 번만 해서 공유)
    try:
        df_krx = fdr.StockListing('KRX')
        df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
        target_dict = dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    except Exception as e:
        print(f"❌ 데이터 수집 실패: {e}")
        exit()

    # 2. 병렬 처리로 두 가지 로직 동시 실행
    # (결과를 담을 딕셔너리: code -> data)
    results_map = {} 

    print("⚡ 1단계: 🍉단기스윙 & 🥣단테기법 동시 스캔 중...")
    
    with ThreadPoolExecutor(max_workers=30) as executor:
        # A팀: 메인 봇 (Future 객체 리스트)
        futures_main = {executor.submit(analyze_main, t, n, 'realtime'): (t, n) for t, n in target_dict.items()}
        # B팀: 단테 봇
        futures_dante = {executor.submit(analyze_dante_stock, t, n): (t, n) for t, n in target_dict.items()}
        
        # --- A팀 결과 수집 ---
        for future in futures_main:
            try:
                res = future.result()
                if res:
                    code = res['code']  # main.py에서 code 리턴하는지 확인 필요 (없으면 res['code'] = ticker 추가)
                    res['source'] = '🍉Main'
                    results_map[code] = res
            except: pass
            
        # --- B팀 결과 수집 (중복 체크 핵심 로직) ---
        for future in futures_dante:
            try:
                res = future.result()
                if res:
                    code = res['code']
                    
                    if code in results_map:
                        # 👑 대박! 이미 Main 봇이 찾았는데 단테 봇도 찾음!
                        existing = results_map[code]
                        
                        # 점수 합산 (보너스 점수)
                        existing['총점'] += 50 
                        existing['source'] = '👑BOTH' # 출처 변경
                        existing['신호'] = f"👑{existing['신호']}+{res['신호']}" # 신호 합체
                        
                        # 메시지도 합체
                        existing['msg'] = (
                            f"👑 [강력추천] {existing['종목명']} (Double Pick!)\n"
                            f"------------------------------\n"
                            f"1️⃣ {existing['msg']}\n\n"
                            f"2️⃣ {res['msg']}\n"
                            f"------------------------------\n"
                            f"💡 결론: 수급과 바닥이 동시에 확인됨!"
                        )
                        results_map[code] = existing
                        
                    else:
                        # 단테 봇만 찾음
                        res['source'] = '🥣Dante'
                        results_map[code] = res
            except: pass

    # 3. 결과 정리 및 전송
    final_results = list(results_map.values())
    
    if final_results:
        # 점수순 정렬 (Both가 점수가 높아서 맨 위로 올라옴)
        final_results.sort(key=lambda x: x['총점'], reverse=True)
        
        # 텔레그램 메시지 구성
        msgs = []
        for r in final_results[:15]: # 상위 15개
            # 출처 표기 강화
            src_icon = r.get('source', '')
            header = f"[{src_icon}] {r['종목명']} ({r['총점']}점)"
            
            # 메시지 내용이 너무 길면 요약
            body = r['msg']
            if r['source'] != '👑BOTH': # Double Pick이 아니면 헤더 좀 다듬기
                 body = r['msg'].replace(f"[{r['신호']}] {r['종목명']}", header)
            
            msgs.append(body)

        full_report = f"📊 [오늘의 통합 분석] {len(final_results)}개 발견\n\n" + "\n\n".join(msgs)
        print(full_report)
        send_telegram(full_report)
        
        # 4. 구글 시트 저장
        # 시트 매니저에게 넘기기 전에 '신호' 컬럼에 출처를 같이 적어주면 시트에서도 보임
        for r in final_results:
            # 예: [Main] 🥷잠입 / [Dante] 🔥224일선 / [Both] 👑...
            r['신호'] = f"[{r['source']}] {r['신호']}"
            
        update_google_sheet(final_results, datetime.now().strftime('%Y-%m-%d'))
        print("💾 통합 데이터 저장 완료")
        
    else:
        print("❌ 검색된 종목이 없습니다.")