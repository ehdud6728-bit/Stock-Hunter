import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import os
import FinanceDataReader as fdr
import time

# ---------------------------------------------------------
# 📊 [구글 시트 비서] 통합 관리 모듈
# ---------------------------------------------------------
def update_google_sheet(new_picks, today_str):
    """
    new_picks: 오늘 추천된 종목 리스트 (딕셔너리 리스트)
    today_str: 기록할 기준 날짜 (YYYY-MM-DD)
    """
    
    # 1. 인증 및 연결
    json_key_path = 'stock-key.json' # ⚠️ 키 파일 이름 확인
    sheet_name = "주식자동매매일지"    # ⚠️ 시트 이름 확인
    
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 키 파일 우선, 없으면 환경변수 사용
        if os.path.exists(json_key_path):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        else:
            print("❌ [Google] 인증 키(JSON)를 찾을 수 없습니다. (기록 건너뜀)")
            return

        client = gspread.authorize(creds)
        
        try:
            doc = client.open(sheet_name)
            worksheet = doc.sheet1 # 첫 번째 탭 사용
        except Exception as e:
            print(f"❌ [Google] '{sheet_name}' 시트를 열 수 없습니다: {e}")
            return

        # 2. 기존 데이터 가져오기
        existing_data = worksheet.get_all_records()
        df_log = pd.DataFrame(existing_data)
        
        # 컬럼 순서 강제 설정 (기존 양식 유지)
        cols = [
            '추천일', '기상', '종목명', '종목코드', '에너지', '안전', '점수', '매수가', 
            '현재가', '최고수익', '최저수익', '현재수익', '구분', '꼬리%', '이격', '수급', 'OBV', '상태'
        ]
        
        # 데이터가 없으면 빈 프레임 생성
        if df_log.empty:
            df_log = pd.DataFrame(columns=cols)
        else:
            # 날짜 등 문자열로 변환하여 에러 방지
            df_log['추천일'] = df_log['추천일'].astype(str)
            df_log['종목코드'] = df_log['종목코드'].astype(str).apply(lambda x: x.zfill(6)) # 000123 유지

        print(f"☁️ [Google] 시트 로딩 완료 (기록 {len(df_log)}건)")

        # -----------------------------------------------------
        # 3. [기록] 오늘 추천된 신규 종목 추가
        # -----------------------------------------------------
        if new_picks:
            added_count = 0
            new_rows = []
            
            for pick in new_picks:
                name = pick['종목명']
                
                # 중복 방지: 같은 날짜 + 같은 종목명이면 패스
                if not df_log.empty:
                    is_dup = not df_log[
                        (df_log['추천일'] == today_str) & 
                        (df_log['종목명'] == name)
                    ].empty
                    if is_dup: continue
                
                # 가격 정보 정리
                price = int(str(pick['현재가']).replace(',', ''))
                code = str(pick.get('code', '')).zfill(6)

                '추천일', '기상', '종목명', '종목코드', '에너지', '안전', '점수', '매수가', 
            '현재가', '최고수익', '최저수익', '현재수익', '구분', '꼬리%', '이격', '수급', 'OBV', '상태'

                
                # 신규 데이터 행 생성
                new_row = {
                    '추천일': today_str,
                    '기상': pick['기상'],
                    '종목명': name,
                    '종목코드': code,
                    '에너지': pick['에너지'],
                    '안전': pick['안전'],
                    '점수': pick['총점'],
                    '매수가': price,
                    '현재가': price,    # 초기엔 매수가와 동일
                    '최고수익': 0.0,
                    '최저수익': 0.0,
                    '현재수익': 0.0,
                    '구분': pick['구분'],
                    '꼬리%': pick['꼬리%'],
                    '이격': pick['이격'],
                    '수급': pick['수급'],
                    'OBV': pick['OBV기울기'],
                    '상태': '진행중'
                }
                new_rows.append(new_row)
                added_count += 1
            
            # DataFrame에 추가
            if new_rows:
                df_log = pd.concat([df_log, pd.DataFrame(new_rows)], ignore_index=True)
                print(f"📝 [Google] 신규 종목 {added_count}개 리스트 추가")

        # -----------------------------------------------------
        # 4. [추적] 과거 추천주 수익률 자동 업데이트 (핵심!)
        # -----------------------------------------------------
        print("🔄 [Google] 수익률 자동 계산 중...")
        
        # 날짜 형식 처리 (YYYY-MM-DD)
        today_date = pd.to_datetime(today_str)
        
        for idx, row in df_log.iterrows():
            # 이미 끝난 종목('완료')이나 오늘 추천된 종목은 계산 건너뜀
            if str(row['상태']) == '완료': continue
            if str(row['추천일']) == today_str: continue 

            try:
                code = str(row['종목코드']).zfill(6)
                if not code or code == 'nan': continue

                rec_date = pd.to_datetime(row['추천일'])
                
                # FDR로 추천일부터 오늘까지의 데이터 조회
                df_curr = fdr.DataReader(code, start=rec_date)
                
                if len(df_curr) > 0:
                    buy_price = float(row['매수가'])
                    
                    # 기간 내 최고가 / 최저가 / 현재가 찾기
                    high_price = float(df_curr['High'].max())
                    low_price  = float(df_curr['Low'].min())
                    curr_price = float(df_curr['Close'].iloc[-1])
                    
                    # 수익률 계산 (%)
                    pct_high = round(((high_price - buy_price) / buy_price) * 100, 2)
                    pct_low  = round(((low_price - buy_price) / buy_price) * 100, 2)
                    pct_curr = round(((curr_price - buy_price) / buy_price) * 100, 2)
                    
                    # DataFrame 업데이트 (at 사용)
                    df_log.at[idx, '현재가'] = int(curr_price)
                    df_log.at[idx, '최고수익'] = pct_high
                    df_log.at[idx, '최저수익'] = pct_low
                    df_log.at[idx, '현재수익'] = pct_curr
                    
            except Exception as e:
                # 에러 나면 로그만 찍고 다음 종목으로 넘어감 (멈추지 않음)
                # print(f"⚠️ {row['종목명']} 계산 패스: {e}")
                pass

        # -----------------------------------------------------
        # 5. [저장] 구글 시트에 반영
        # -----------------------------------------------------
        # NaN 값(빈 값)이 있으면 구글 시트 오류 나므로 빈 문자열로 대체
        df_log = df_log.fillna('')
        
        # 전체 덮어쓰기 (가장 확실한 방법)
        worksheet.clear()
        
        # 헤더 + 데이터 업데이트
        # 주의: gspread 업데이트 시 numpy 자료형(int64 등)은 에러나므로 변환 필요할 수 있음
        # 여기서는 pandas가 기본적으로 처리해주지만, 안전하게 list로 변환
        data_to_upload = [df_log.columns.values.tolist()] + df_log.values.tolist()
        worksheet.update(data_to_upload)
        
        print("💾 [Google] 시트 저장 및 동기화 완료!")

    except Exception as e:
        print(f"🚨 [Google] 시트 연동 중 치명적 오류: {e}")
