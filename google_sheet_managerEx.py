import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import os
from datetime import datetime

def update_commander_dashboard(df, macro_data, sheet_name, stats_df=None):
    """
    사령관님, 기존 대시보드 기능을 100% 유지하면서 
    복합 전술 통계 저장 기능을 추가한 통합 버전입니다.
    """
    json_key_path = 'stock-key.json' # ⚠️ 키 파일 이름 확인

    try:
        # 1. 인증 및 연결 프로토콜
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
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

        # ------------------------------------------------------------------
        # 탭 1: [실시간 전수 관제판] - 기존 기능 유지 및 데이터 확장
        # ------------------------------------------------------------------
        sheet = doc.get_worksheet(0)
        sheet.clear() 

        # 2. 상단 매크로 상황판 (1~8행)
        macro_list = [
            ["🌐 글로벌 관제 센터 실시간 상황판 (전 종목 전수 스캔)", "", ""],
            [f"📅 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", ""],
            [macro_data['nasdaq']['text'], "", ""],
            [macro_data['sp500']['text'], "", ""],
            [macro_data['vix']['text'], "", ""],
            [f"💵 달러환율: {macro_data['fx']['text']}", "", ""],
            [f"🇰🇷 KOSPI 수급: {macro_data['kospi']}", "", ""],
            ["[전술 지침] VMA-GC + BB-Break + Sto-GC = 다이아몬드 타점 | 🔋초강력응축 포착 시 화력 집중", "", ""]
        ]
        sheet.update('A1', macro_list)
        
        # 3. 종목 리포트 (9행부터)
        # 💡 금색 별(★) 유지: 안전 점수 110점 이상
        if '안전' in df.columns:
            df['종목'] = df.apply(lambda x: f"★ {x['종목']}" if int(x['안전']) >= 110 else x['종목'], axis=1)
        
        # 데이터 업로드 준비
        stock_data = [df.columns.values.tolist()] + df.values.tolist()
        sheet.update('A9', stock_data)

        # 4. 🎨 시각적 채색 (조건부 서식 유지)
        format_cell_range(sheet, 'A1:C1', cellFormat(textFormat=textFormat(bold=True, fontSize=12)))
        format_cell_range(sheet, 'A9:Q9', cellFormat(textFormat=textFormat(bold=True, fontSize=11)))
        
        num_rows = len(stock_data) + 10
        num_cols = len(df.columns)
        last_col_letter = chr(64 + num_cols)
        data_range = f"A10:{last_col_letter}{num_rows}"

        rules = get_conditional_format_rules(sheet)
        rules.clear()

        # 규칙 1: ★(골든스타) 행 강조
        rule_star = ConditionalFormatRule(
            ranges=[GridRange.from_a1_range(data_range, sheet)],
            booleanRule=BooleanRule(
                condition=BooleanCondition('TEXT_CONTAINS', ['★']),
                format=CellFormat(backgroundColor=Color(1, 0.95, 0.8), textFormat=textFormat(bold=True))
            )
        )
        
        # 규칙 2: 수익률 양수 (연한 빨강)
        try:
            curr_col_idx = df.columns.get_loc('현재') + 1
            curr_col_letter = chr(64 + curr_col_idx)
            rule_red = ConditionalFormatRule(
                ranges=[GridRange.from_a1_range(data_range, sheet)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition('CUSTOM_FORMULA', [f'=SEARCH("🔴", {curr_col_letter}10)']),
                    format=CellFormat(backgroundColor=Color(1, 0.9, 0.9))
                )
            )
            rules.append(rule_red)
        except: pass

        rules.append(rule_star)
        rules.save()

        # ------------------------------------------------------------------
        # 탭 2: [전술통계_리포트] - 신규 복합 전략 통계 기능
        # ------------------------------------------------------------------
        if stats_df is not None and not stats_df.empty:
            stats_tab_name = "전술통계_리포트"
            try:
                stats_sheet = doc.worksheet(stats_tab_name)
            except:
                stats_sheet = doc.add_worksheet(title=stats_tab_name, rows="100", cols="10")
            
            stats_sheet.clear()
            
            # 통계 탭 상단 브리핑
            stats_header = [
                ["⚔️ 사령부 복합 전술 타율 보고서", "", ""],
                [f"📊 분석 기간: 최근 {len(df)}개 신호 전수 조사", "", ""],
                ["", "", ""]
            ]
            stats_sheet.update('A1', stats_header)
            
            # 통계 표 주입
            stats_data = [stats_df.columns.values.tolist()] + stats_df.values.tolist()
            stats_sheet.update('A4', stats_data)
            
            # 통계 탭 디자인
            format_cell_range(stats_sheet, 'A1:C1', cellFormat(textFormat=textFormat(bold=True, fontSize=14, foregroundColor=Color(0.2, 0.2, 0.6))))
            format_cell_range(stats_sheet, 'A4:D4', cellFormat(backgroundColor=Color(0.9, 0.9, 0.9), textFormat=textFormat(bold=True)))
            
            print(f"📈 [전술통계] '{stats_tab_name}' 기록 완료!")

        print(f"✅ [Ver 30.0] 구글 시트 통합 상황판 업데이트 성공!")
        
    except Exception as e:
        print(f"❌ 구글 시트 작업 중 오류 발생: {e}")