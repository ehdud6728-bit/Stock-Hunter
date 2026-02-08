import os
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import FinanceDataReader as fdr

def update_google_sheet(new_picks, today_str):
    json_key_path = 'stock-key.json'
    sheet_name = "주식자동매매일지"
    
    # 컬럼 정의 (이 순서대로 시트에 기록됩니다)
    # 💡 컬럼 추가: 'AI한줄평', 'AI토너먼트'
    cols = [
        '추천일', '기상', '종목명', '종목코드', '에너지', '안전', '점수', '매수가', 
        '현재가', '최고수익', '현재수익', '구분', '이격', '수급', 'AI한줄평', 'AI토너먼트', '상태'
    ]

    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 1. 인증 로직
        if os.path.exists(json_key_path):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        else:
            print("❌ [Google] 인증 키를 찾을 수 없습니다.")
            return

        client = gspread.authorize(creds)
        doc = client.open(sheet_name)
        worksheet = doc.sheet1

        # 2. 기존 데이터 로드 및 전처리
        existing_data = worksheet.get_all_records()
        df_log = pd.DataFrame(existing_data)

        if df_log.empty:
            df_log = pd.DataFrame(columns=cols)
        else:
            # 기존 데이터의 컬럼 순서 및 타입 보정
            df_log = df_log.reindex(columns=cols)
            df_log['추천일'] = df_log['추천일'].astype(str)
            df_log['종목코드'] = df_log['종목코드'].astype(str).apply(lambda x: x.zfill(6))

        # 3. 신규 종목 추가 (중복 체크 포함)
        if new_picks:
            new_rows = []
            for pick in new_picks:
                new_row = {
                    '추천일': today_str,
                    '기상': pick.get('기상', '☀️'),
                    '종목명': pick.get('종목명', 'N/A'),
                    '종목코드': f"'{pick.get('code', '000000')}", 
                    '에너지': pick.get('에너지', '🔋'),
                    '안전': pick.get('안전', 0),
                    '점수': pick.get('점수', 0),
                    '매수가': pick.get('현재가', 0),
                    '현재가': pick.get('현재가', 0),
                    '현재수익': 0.0,
                    '구분': pick.get('구분', ''),
                    '이격': pick.get('이격', 0),
                    '수급': pick.get('수급', ''),
                    # 💡 AI 분석 결과 매핑
                    'AI한줄평': pick.get('ai_tip', '분석전'), 
                    'AI토너먼트': pick.get('ai_tournament', '해당없음'),
                    '상태': '진행중'
                }
                new_rows.append(new_row)
            
            if new_rows:
                df_log = pd.concat([df_log, pd.DataFrame(new_rows)], ignore_index=True)
                print(f"📝 [Google] 신규 {len(new_rows)}개 종목 리스트 추가")

        # 4. 수익률 자동 업데이트
        print("🔄 [Google] 과거 추천주 수익률 동기화 중...")
        for idx, row in df_log.iterrows():
            if str(row['상태']) == '완료' or str(row['추천일']) == today_str:
                continue

            try:
                code = str(row['종목코드']).replace("'", "").zfill(6)
                rec_date = pd.to_datetime(row['추천일'])
                
                # FDR 데이터 호출 (안정성을 위해 최근 데이터 확보)
                df_curr = fdr.DataReader(code, start=rec_date)
                
                if not df_curr.empty:
                    buy_price = float(row['매수가'])
                    if buy_price == 0: continue
                    
                    high_p = float(df_curr['High'].max())
                    low_p  = float(df_curr['Low'].min())
                    curr_p = float(df_curr['Close'].iloc[-1])
                    
                    df_log.at[idx, '현재가'] = int(curr_p)
                    df_log.at[idx, '최고수익'] = round(((high_p - buy_price) / buy_price) * 100, 2)
                    df_log.at[idx, '최저수익'] = round(((low_p - buy_price) / buy_price) * 100, 2)
                    df_log.at[idx, '현재수익'] = round(((curr_p - buy_price) / buy_price) * 100, 2)
            except Exception:
                continue

        # 5. 시트 반영 (Overwrite)
        df_log = df_log.fillna('')
        data_to_upload = [df_log.columns.values.tolist()] + df_log.values.tolist()
        
        worksheet.clear()
        worksheet.update('A1', data_to_upload) # 💡 최신 gspread 규격 적용
        print("💾 [Google] 시트 저장 및 동기화 완료!")

    except Exception as e:
        print(f"🚨 [Google] 시트 연동 중 치명적 오류: {e}")
