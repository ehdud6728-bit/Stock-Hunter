import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import os
import FinanceDataReader as fdr

# ---------------------------------------------------------
# 📊 [구글 시트 비서] 별도 모듈
# ---------------------------------------------------------
def update_google_sheet(new_picks, today_str):
    """
    new_picks: 오늘 추천된 종목 리스트
    today_str: 기록할 기준 날짜 (YYYY-MM-DD)
    """
    
    # 1. 인증 (키 파일 or 깃허브 시크릿)
    json_key_path = 'stock-key.json' 
    
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        if os.path.exists(json_key_path):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        else:
            print("❌ [Google] 인증 키(JSON)를 찾을 수 없습니다. (기록 건너뜀)")
            return

        client = gspread.authorize(creds)
        
        # 2. 시트 연결
        sheet_name = "주식자동매매일지" 
        try:
            doc = client.open(sheet_name)
            worksheet = doc.sheet1
        except:
            print(f"❌ [Google] '{sheet_name}' 시트를 찾을 수 없습니다.")
            return

        # 3. 기존 데이터 로딩
        existing_data = worksheet.get_all_records()
        df_log = pd.DataFrame(existing_data)
        
        if df_log.empty:
            df_log = pd.DataFrame(columns=[
                '추천일', '종목명', '종목코드', '신호', '점수', '매수가', 
                '현재가', '최고수익', '최저수익', '현재수익', '상태'
            ])
        else:
            df_log['추천일'] = df_log['추천일'].astype(str)

        print(f"☁️ [Google] 시트 로딩 완료 (기록 {len(df_log)}건)")

        # -----------------------------------------------------
        # 4. [기록] 신규 종목 추가
        # -----------------------------------------------------
        if new_picks:
            added = 0
            for pick in new_picks:
                name = pick['종목명']
                # 중복 체크 (오늘 날짜 + 종목명)
                is_dup = not df_log[
                    (df_log['추천일'] == today_str) & 
                    (df_log['종목명'] == name)
                ].empty
                
                if is_dup: continue
                
                price = int(str(pick['현재가']).replace(',', ''))
                code = pick.get('code', '') # main.py에서 code 넘겨줘야 함
                
                new_row = {
                    '추천일': today_str,
                    '종목명': name,
                    '종목코드': str(code),
                    '신호': pick['신호'],
                    '점수': pick['총점'],
                    '매수가': price,
                    '현재가': price,
                    '최고수익': 0.0,
                    '최저수익': 0.0,
                    '현재수익': 0.0,
                    '상태': '진행중'
                }
                df_log = pd.concat([df_log, pd.DataFrame([new_row])], ignore_index=True)
                added += 1
            
            if added > 0: print(f"📝 [Google] 신규 종목 {added}개 추가")

        # -----------------------------------------------------
        # 5. [추적] 수익률 업데이트
        # -----------------------------------------------------
        print("🔄 [Google] 수익률 동기화 중...")
        
        for idx, row in df_log.iterrows():
            if row['상태'] == '완료': continue
            if str(row['추천일']) == today_str: continue # 오늘은 패스

            try:
                code = str(row['종목코드']).zfill(6)
                if not code or code == 'nan': continue

                rec_date = pd.to_datetime(row['추천일'])
                # 추천일 ~ 오늘 데이터
                df_curr = fdr.DataReader(code, start=rec_date)
                
                if len(df_curr) > 0:
                    buy_price = float(row['매수가'])
                    high = float(df_curr['High'].max())
                    low = float(df_curr['Low'].min())
                    curr = float(df_curr['Close'].iloc[-1])
                    
                    df_log.at[idx, '현재가'] = curr
                    df_log.at[idx, '최고수익'] = round(((high - buy_price)/buy_price)*100, 2)
                    df_log.at[idx, '최저수익'] = round(((low - buy_price)/buy_price)*100, 2)
                    df_log.at[idx, '현재수익'] = round(((curr - buy_price)/buy_price)*100, 2)
            except: pass

        # 6. 저장
        worksheet.clear()
        worksheet.update([df_log.columns.values.tolist()] + df_log.values.tolist())
        print("💾 [Google] 저장 완료!")

    except Exception as e:
        print(f"🚨 [Google] 연동 실패: {e}")
