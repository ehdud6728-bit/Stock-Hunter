# google_sheet_managerEx.py
import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os, json, traceback
from datetime import datetime

# 💡 데이터프레임 시트 전송을 위한 필수 부품
try:
    from gspread_dataframe import set_with_dataframe
    print("✅ [Module] gspread_dataframe 로드 완료")
except ImportError:
    print("❌ [Module] gspread_dataframe 라이브러리가 설치되지 않았습니다!")

def update_commander_dashboard(df, macro_data, sheet_name, stats_df=None):
    print(f"📡 [Sheet] 시트 업데이트 작전 개시 (종목수: {len(df)})")
    json_key_path = 'stock-key.json' 
    
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 1. 인증 정보 확인 및 로드
        creds = None
        if os.path.exists(json_key_path):
            print(f"🔑 [Auth] {json_key_path} 파일을 통해 인증을 시도합니다.")
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            print("🔑 [Auth] 환경변수 GOOGLE_JSON_KEY를 통해 인증을 시도합니다.")
            key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        
        if not creds:
            raise ValueError("❌ 인증 키를 찾을 수 없습니다. (파일 또는 환경변수 누락)")

        client = gspread.authorize(creds)
        doc = client.open(sheet_name)
        sheet = doc.get_worksheet(0)
        sheet.clear() 

        # 2. 매크로 상단 영역 작성
        # macro_data가 dict 형태인지 확인하는 방어 로직
        try:
            m_nas = macro_data.get('nasdaq', {}).get('text', 'N/A')
            m_sp = macro_data.get('sp500', {}).get('text', 'N/A')
            m_vx = macro_data.get('vix', {}).get('text', 'N/A')
            m_fx = macro_data.get('fx', {}).get('text', 'N/A')
            m_kp = macro_data.get('kospi', 'N/A')
        except Exception as e:
            print(f"⚠️ 매크로 데이터 해석 오류: {e}")
            m_nas = m_sp = m_vx = m_fx = m_kp = "Data Error"

        macro_list = [
            ["💎 사령부 실시간 다이아몬드 관제 시스템", "", ""],
            [f"📅 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", ""],
            [m_nas, "", ""], [m_sp, "", ""], [m_vx, "", ""],
            [f"💵 달러환율: {m_fx}", "", ""],
            [f"🇰🇷 KOSPI 수급: {m_kp}", "", ""],
            ["[지침] 💎다이아몬드(구름+기준선 돌파) 포착 시 즉시 화력 집중", "", ""]
        ]
        sheet.update('A1', macro_list, value_input_option='USER_ENTERED')
        
        # 3. 메인 종목 데이터 작성 (Header 포함)
        display_df = df.copy()
        if '안전' in display_df.columns:
            display_df['종목'] = display_df.apply(lambda x: f"★ {x['종목']}" if int(x['안전']) >= 130 else x['종목'], axis=1)
        
        # ✅ [수정 후] 규격에 맞게 매개변수 정리
        set_with_dataframe(sheet, display_df, row=9, col=1, include_index=False)
        print("✅ [Sheet] 종목 리스트 전송 성공")

        # 4. 조건부 서식 (생략 가능하지만 시각화를 위해 유지)
        try:
            num_rows = len(display_df) + 10
            last_col_idx = len(display_df.columns)
            last_col_char = chr(64 + last_col_idx) if last_col_idx <= 26 else "Z"
            data_range = f"A10:{last_col_char}{num_rows}"
            
            rules = get_conditional_format_rules(sheet)
            rules.clear()
            rules.append(ConditionalFormatRule(
                ranges=[GridRange.from_a1_range(data_range, sheet)],
                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['💎']),
                format=CellFormat(backgroundColor=Color(0.9, 0.9, 1.0), textFormat=textFormat(bold=True, foregroundColor=Color(0.2, 0.2, 0.8))))
            ))
            rules.save()
        except: print("⚠️ [Sheet] 서식 적용 중 오류 (무시 가능)")

        # 5. 통계 리포트 탭 업데이트
        if stats_df is not None and not stats_df.empty:
            try:
                try: stats_sheet = doc.worksheet("전술통계_리포트")
                except: stats_sheet = doc.add_worksheet(title="전술통계_리포트", rows="100", cols="10")
                stats_sheet.clear()
                set_with_dataframe(stats_sheet, stats_df, include_index=False, value_input_option='USER_ENTERED')
                print("✅ [Sheet] 전술 통계 업데이트 완료")
            except: print("⚠️ [Sheet] 통계 탭 업데이트 실패")

    except Exception as e:
        print(f"❌ [Sheet] 치명적 오류 발생:\n{traceback.format_exc()}")
