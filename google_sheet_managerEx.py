
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import os
import FinanceDataReader as fdr
import time
from datetime import datetime, timedelta  # 💡 datetime 오류 해결 핵심 라인

import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials

def update_commander_dashboard(df, macro_data, sheet_name):
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
        sheet.clear() # 기존 데이터 완전 초기화

        # 2. [상단] 글로벌 지표 대시보드 작성 (1~5행)
        # ✅ 수리된 매크로 리스트 구조
        macro_list = [
            ["🌐 글로벌 관제 센터 실시간 상황판", "", ""], # 1행: 제목 (3칸 맞춤)
            [f"📅 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", ""], # 2행: 시간
            [macro_data['nasdaq']['text'], "", ""],
            [macro_data['sp500']['text'], "", ""],
            [macro_data['vix']['text'], "", ""], # 3행: 미국 지수
            [f"💵 달러환율: {macro_data['fx']['text']}", "", ""],
            [f"🇰🇷 KOSPI 수급: {macro_data['kospi']['text']}", "", ""], # 4행: 환율 및 수급
            ["", "", ""] # 8행: 공백 (가독성용)
        ]
        sheet.update('A1', macro_list)
        # 상단 제목 강조 (Bold)
        format_cell_range(sheet, 'A1:C1', cellFormat(textFormat=textFormat(bold=True, fontSize=12)))
        format_cell_range(sheet, 'A9:Q9', cellFormat(textFormat=textFormat(bold=True, fontSize=12)))
        
        # 3. [하단] 종목 리포트 작성 (7행부터)
        # 💡 금색 별(★) 추가 로직: 안전 점수 110점 이상
        df['종목'] = df.apply(lambda x: f"★ {x['종목']}" if x['안전'] >= 110 else x['종목'], axis=1)
        
        stock_data = [df.columns.values.tolist()] + df.values.tolist()
        sheet.update('A9', stock_data)

        # 4. 🎨 조건부 서식 (채색 프로토콜)
        num_rows = len(stock_data) + 10
        num_cols = len(df.columns)
        last_col_letter = chr(64 + num_cols)
        data_range = f"A10:{last_col_letter}{num_rows}" # 헤더 제외 데이터 범위

        rules = get_conditional_format_rules(sheet)
        rules.clear()

        # 💡 규칙 1: '★' 포함된 행은 금색(노란색) 배경
        rule_star = ConditionalFormatRule(
            ranges=[GridRange.from_a1_range(data_range, sheet)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('TEXT_CONTAINS', ['★']),
                format=CellFormat(backgroundColor=Color(1, 0.95, 0.8), textFormat=textFormat(bold=True))
            )
        )
        
        # 💡 규칙 2: 현재 수익률이 0% 초과일 때 (연한 빨강)
        # '현재' 열 위치를 찾아 자동 적용 (보통 10~11번째 열)
        curr_col_idx = df.columns.get_loc('현재') + 1
        rule_red = ConditionalFormatRule(
            ranges=[GridRange.from_a1_range(data_range, sheet)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('CUSTOM_FORMULA', [f'={chr(64+curr_col_idx)}8>0']),
                format=CellFormat(backgroundColor=Color(1, 0.9, 0.9))
            )
        )

        # 💡 규칙 3: 최고 수익률이 0% 미만일 때 (연한 파랑)
        max_col_idx = df.columns.get_loc('🔺최고') + 1
        rule_blue = ConditionalFormatRule(
            ranges=[GridRange.from_a1_range(data_range, sheet)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('CUSTOM_FORMULA', [f'={chr(64+max_col_idx)}8<0']),
                format=CellFormat(backgroundColor=Color(0.9, 0.9, 1))
            )
        )

        rules.append(rule_star)
        rules.append(rule_red)
        rules.append(rule_blue)
        rules.save()

        print(f"✅ [Ver 29.0] 구글 시트 '골든 스타' 상황판 업데이트 완료!")
        
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
