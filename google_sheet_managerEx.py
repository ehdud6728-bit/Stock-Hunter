
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import os
import FinanceDataReader as fdr
import time

import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials

def update_google_sheet_with_format(df, sheet_name):
    try:
    # 1. 인증 및 연결
    json_key_path = 'stock-key.json' # ⚠️ 키 파일 이름 확인

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
        
        doc = client.open(sheet_name)
        sheet = doc.get_worksheet(0)
        
        # 2. 데이터 업로드 (기존 데이터 초기화 후 업로드)
        sheet.clear()
        data = [df.columns.values.tolist()] + df.values.tolist()
        sheet.update(data)
        
        # 3. 🎨 자동 채색 로직 (gspread-formatting 사용)
        print("🎨 상황판 채색 중...")
        
        # 전체 데이터 범위 설정 (헤더 제외 2행부터 마지막 행까지)
        num_rows = len(data)
        num_cols = len(df.columns)
        body_range = f"A2:{chr(64 + num_cols)}{num_rows}"
        
        # 💡 [조건 1] 현재 수익률이 0% 이상일 때 (연한 빨간색)
        # '현재' 열이 11번째(K열)라고 가정할 때의 예시입니다.
        rule_red = ConditionalFormatRule(
            ranges=[GridRange.from_a1_range(body_range, sheet)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('NUMBER_GREATER_THAN', ['0']),
                format=CellFormat(backgroundColor=Color(1, 0.9, 0.9)) # 연한 빨강
            )
        )

        # 💡 [조건 2] 최고 수익률이 0% 미만(배신자)일 때 (연한 파란색)
        # '🔺최고' 열을 기준으로 필터링
        rule_blue = ConditionalFormatRule(
            ranges=[GridRange.from_a1_range(body_range, sheet)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('NUMBER_LESS_THAN', ['0']),
                format=CellFormat(backgroundColor=Color(0.9, 0.9, 1)) # 연한 파랑
            )
        )

        # 서식 적용 (기존 서식 삭제 후 적용)
        rules = get_conditional_format_rules(sheet)
        rules.clear()
        rules.append(rule_red)
        rules.append(rule_blue)
        rules.save()

        print(f"✅ 구글 시트 '{sheet_name}' 업데이트 및 자동 채색 완료!")
        
    except Exception as e:
        print(f"❌ 구글 시트 작업 중 오류 발생: {e}")
        
# ---------------------------------------------------------
# 📊 [구글 시트 비서] 통합 관리 모듈
# ---------------------------------------------------------
def update_google_sheet(new_picks, sheet_name):
    """
    new_picks: 오늘 추천된 종목 리스트 (딕셔너리 리스트)
    today_str: 기록할 기준 날짜 (YYYY-MM-DD)
    """
    #sheet_name = "주식자동매매일지"    # ⚠️ 시트 이름 확인
  
    # 1. 인증 및 연결
    json_key_path = 'stock-key.json' # ⚠️ 키 파일 이름 확인

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
        cols = ['날짜', '안전', '종목', '외인', '기관', '쌍끌이', '에너지', 'OBV기울기', '🔺최고', '💧최저', '현재', '꼬리%', '이격', '구분']
        
        # 데이터가 없으면 빈 프레임 생성
        if df_log.empty:
            df_log = pd.DataFrame(columns=cols)
        else:
            # 날짜 등 문자열로 변환하여 에러 방지
            df_log['날짜'] = df_log['날짜'].astype(str)
            df_log['안전'] = df_log['안전'].astype(str)
            #df_log['종목'] = df_log['종목'].astype(str).apply(lambda x: x.zfill(6)) # 000123 유지

        print(f"☁️ [Google] 시트 로딩 완료 (기록 {len(df_log)}건)")

        # -----------------------------------------------------
        # 3. [기록] 오늘 추천된 신규 종목 추가
        # -----------------------------------------------------
        if new_picks:
            added_count = 0
            new_rows = []
            
            for pick in new_picks:
                name = pick['종목']
                buydate = pick['날짜']
                # 중복 방지: 같은 날짜 + 같은 종목명이면 패스
                if not df_log.empty:
                    is_dup = not df_log[
                        (df_log['날짜'] == buydate) & 
                        (df_log['종목'] == name)
                    ].empty
                    if is_dup: continue
                
                # 가격 정보 정리
                price = int(str(pick['현재가']).replace(',', ''))
                code = str(pick.get('code', '')).zfill(6)
                
                # 신규 데이터 행 생성
                new_row = {
                    '날짜': pick['날짜'], 
                    '안전': pick['안전'],
                    '종목': pick['종목'],
                    '외인': pick['외인'],
                    '기관': pick['기관'],
                    '쌍끌이': pick['쌍끌이'],
                    '에너지': pick['에너지'],
                    'OBV기울기': pick['OBV기울기'],
                    '🔺최고': pick['🔺최고'],
                    '💧최저': pick['💧최저'],
                    '현재': pick['현재'],
                    '꼬리%': pick['꼬리%'],
                    '이격': pick['이격'],
                    '구분': pick['구분']
                }
                new_rows.append(new_row)
                added_count += 1
            
            # DataFrame에 추가
            if new_rows:
                df_log = pd.concat([df_log, pd.DataFrame(new_rows)], ignore_index=True)
                print(f"📝 [Google] 신규 종목 {added_count}개 리스트 추가")
        
        # 헤더 + 데이터 업데이트
        # 주의: gspread 업데이트 시 numpy 자료형(int64 등)은 에러나므로 변환 필요할 수 있음
        # 여기서는 pandas가 기본적으로 처리해주지만, 안전하게 list로 변환
        data_to_upload = [df_log.columns.values.tolist()] + df_log.values.tolist()
        worksheet.update(data_to_upload)
        
        print("💾 [Google] 시트 저장 및 동기화 완료!")

    except Exception as e:
        print(f"🚨 [Google] 시트 연동 중 치명적 오류: {e}")
