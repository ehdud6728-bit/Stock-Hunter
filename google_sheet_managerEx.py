import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe  # 💡 필수 라이브러리 추가
import pandas as pd
import os, json
from datetime import datetime

def update_commander_dashboard(df, macro_data, sheet_name, stats_df=None):
    json_key_path = 'stock-key.json' 
    try:
        # 인증용 스코프 설정
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 1. 인증 로직 (파일 또는 환경변수)
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

        # --- [탭 1: 실시간 전수 관제판] ---
        sheet = doc.get_worksheet(0)
        sheet.clear() 

        # 💡 상단 매크로 현황판 (변수명 macro_data로 통일)
        macro_list = [
            ["💎 사령부 실시간 다이아몬드 관제 시스템", "", ""],
            [f"📅 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", ""],
            [macro_data['nasdaq']['text'], "", ""],
            [macro_data['sp500']['text'], "", ""],
            [macro_data['vix']['text'], "", ""],
            [f"💵 달러환율: {macro_data['fx']['text']}", "", ""],
            [f"🇰🇷 KOSPI 수급: {macro_data.get('kospi', '데이터없음')}", "", ""], # 💡 변수명 수정
            ["[지침] 💎다이아몬드(구름+기준선 돌파) 포착 시 즉시 화력 집중", "", ""]
        ]
        # value_input_option='USER_ENTERED'를 넣어야 수식이나 기호가 깨지지 않습니다.
        sheet.update('A1', macro_list, value_input_option='USER_ENTERED')
        
        # 💡 종목 리스트 전처리 (안전 점수 별표 부여)
        display_df = df.copy()
        if '안전' in display_df.columns:
            display_df['종목'] = display_df.apply(
                lambda x: f"★ {x['종목']}" if int(x['안전']) >= 130 else x['종목'], axis=1
            )
        
        # 💡 데이터프레임을 직접 시트에 꽂아넣기 (컬럼명 자동 포함)
        # 9행(A9)부터 시작하여 제목과 데이터를 안전하게 전송합니다.
        set_with_dataframe(sheet, display_df, row=9, col=1, include_index=False, value_input_option='USER_ENTERED')

        # 🎨 조건부 서식 프로토콜
        num_rows = len(display_df) + 10
        last_col = chr(64 + len(display_df.columns))
        data_range = f"A10:{last_col}{num_rows}"

        rules = get_conditional_format_rules(sheet)
        rules.clear()

        # 규칙 1: 💎다이아몬드 타점 (연한 보라색 배경 + 진한 파랑 글씨)
        rule_diamond = ConditionalFormatRule(
            ranges=[GridRange.from_a1_range(data_range, sheet)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('TEXT_CONTAINS', ['💎']),
                format=CellFormat(backgroundColor=Color(0.9, 0.9, 1.0), textFormat=textFormat(bold=True, foregroundColor=Color(0.2, 0.2, 0.8)))
            )
        )
        # 규칙 2: ★골든스타 (금색 배경)
        rule_star = ConditionalFormatRule(
            ranges=[GridRange.from_a1_range(data_range, sheet)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('TEXT_CONTAINS', ['★']),
                format=CellFormat(backgroundColor=Color(1, 0.95, 0.8), textFormat=textFormat(bold=True))
            )
        )
        rules.append(rule_diamond)
        rules.append(rule_star)
        rules.save()

        # --- [탭 2: 전술통계_리포트] ---
        if stats_df is not None and not stats_df.empty:
            try:
                try: stats_sheet = doc.worksheet("전술통계_리포트")
                except: stats_sheet = doc.add_worksheet(title="전술통계_리포트", rows="100", cols="10")
                
                stats_sheet.clear()
                set_with_dataframe(stats_sheet, stats_df, include_index=False, value_input_option='USER_ENTERED')
            except Exception as e:
                print(f"⚠️ 통계 탭 업데이트 중 사소한 오류: {e}")

        print(f"✅ [Ver 36.1] 구글 시트 업데이트 완료 (컬럼 제목 보정 완료)!")
    except Exception as e:
        import traceback
        print(f"❌ 구글 시트 작업 치명적 오류:\n{traceback.format_exc()}")