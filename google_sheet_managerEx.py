import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os
from datetime import datetime

def update_commander_dashboard(df, macro_data, sheet_name, stats_df=None):
    json_key_path = 'stock-key.json' 
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        if os.path.exists(json_key_path):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        else:
            print("❌ [Google] 인증 키 파일을 찾을 수 없습니다.")
            return

        client = gspread.authorize(creds)
        doc = client.open(sheet_name)

        # --- [탭 1: 실시간 전수 관제판] ---
        sheet = doc.get_worksheet(0)
        sheet.clear() 

        # 상단 매크로 현황판 (1~8행)
        macro_list = [
            ["💎 사령부 실시간 다이아몬드 관제 시스템", "", ""],
            [f"📅 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", ""],
            [macro_data['nasdaq']['text'], "", ""],
            [macro_data['sp500']['text'], "", ""],
            [macro_data['vix']['text'], "", ""],
            [f"💵 달러환율: {macro_data['fx']['text']}", "", ""],
            [f"🇰🇷 KOSPI 수급: {macro_status['kospi']}", "", ""],
            ["[지침] 💎다이아몬드(구름+기준선 돌파) 포착 시 즉시 화력 집중", "", ""]
        ]
        sheet.update('A1', macro_list)
        
        # 종목 리스트 (9행부터)
        # 안전 점수 130점 이상 금색 별(★) 부여
        if '안전' in df.columns:
            df['종목'] = df.apply(lambda x: f"★ {x['종목']}" if int(x['안전']) >= 130 else x['종목'], axis=1)
        
        stock_data = [df.columns.values.tolist()] + df.values.tolist()
        sheet.update('A9', stock_data)

        # 🎨 조건부 서식 프로토콜
        num_rows = len(stock_data) + 10
        last_col = chr(64 + len(df.columns))
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
        if stats_df is not None:
            try:
                stats_sheet = doc.worksheet("전술통계_리포트")
            except:
                stats_sheet = doc.add_worksheet(title="전술통계_리포트", rows="100", cols="10")
            stats_sheet.clear()
            set_with_dataframe(stats_sheet, stats_df, include_index=False)

        print(f"✅ [Ver 36.0] 구글 시트 업데이트 완료!")
    except Exception as e:
        print(f"❌ 구글 시트 작업 오류: {e}")