import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os, json, traceback
from datetime import datetime

# 💡 데이터프레임 전송을 위한 특수 부품
try:
    from gspread_dataframe import set_with_dataframe
except ImportError:
    print("❌ [Fatal] gspread-dataframe 라이브러리가 없습니다.")

def update_commander_dashboard(df, macro_data, sheet_name, stats_df=None):
    """Stock_Test.py 전용: 메인 관제 및 통계 리포트 통합 기록"""
    print(f"📡 [Ex-Sheet] 시트 업데이트 작전 개시 (데이터: {len(df)}건)")
    json_key_path = 'stock-key.json' 
    
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 1. 인증 로직
        creds = None
        if os.path.exists(json_key_path):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        
        if not creds: raise ValueError("❌ 구글 인증 키 누락")

        client = gspread.authorize(creds)
        doc = client.open(sheet_name)
        
        # --- [탭 1: 실시간 전수 관제판] ---
        sheet = doc.get_worksheet(0)
        sheet.clear() 

        # 매크로 현황판 (A1~A8) - 여기는 .update 이므로 value_input_option 사용 가능
        m = macro_data
        macro_list = [
            ["💎 사령부 연구소(Ex) 실시간 다이아몬드 관제 시스템", "", ""],
            [f"📅 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", ""],
            [m.get('nasdaq',{}).get('text','나스닥 연결실패'), "", ""],
            [m.get('sp500',{}).get('text','S&P500 연결실패'), "", ""],
            [m.get('vix',{}).get('text','VIX 연결실패'), "", ""],
            [f"💵 달러환율: {m.get('fx',{}).get('text','환율오류')}", "", ""],
            [f"🇰🇷 KOSPI 수급: {m.get('kospi','데이터없음')}", "", ""],
            ["[연구] 🏆LEGEND 및 💎다이아몬드 복합 타점 정밀 검증 중", "", ""]
        ]
        sheet.update('A1', macro_list, value_input_option='USER_ENTERED')
        
        # 종목 리스트 (A9부터 제목 포함)
        if not df.empty:
            display_df = df.copy()
            if '안전' in display_df.columns:
                display_df['종목'] = display_df.apply(lambda x: f"★ {x['종목']}" if int(x['안전']) >= 130 else x['종목'], axis=1)
            
            # ✅ [수정] set_with_dataframe에서는 value_input_option을 제거함
            set_with_dataframe(sheet, display_df, row=9, col=1, include_index=False)
            print("✅ [Ex-Sheet] 메인 리스트 전송 성공")

        # --- [탭 2: 전술통계_리포트] ---
        if stats_df is not None and not stats_df.empty:
            try:
                # 탭이 없으면 생성, 있으면 가져오기
                try:
                    stats_sheet = doc.worksheet("전술통계_리포트")
                except:
                    stats_sheet = doc.add_worksheet(title="전술통계_리포트", rows="100", cols="10")
                
                stats_sheet.clear()
                # ✅ [수정] 여기에서도 value_input_option을 반드시 제거해야 함
                set_with_dataframe(stats_sheet, stats_df, include_index=False)
                print("✅ [Ex-Sheet] 전술 통계 탭 업데이트 성공")
            except Exception as e:
                print(f"⚠️ [Ex-Sheet] 통계 탭 내부 오류: {e}")
                traceback.print_exc()

        # 🎨 서식 규칙 적용 (선택 사항)
        try:
            num_rows = len(display_df) + 10
            data_range = f"A10:Z{num_rows}"
            rules = get_conditional_format_rules(sheet)
            rules.clear()
            rules.append(ConditionalFormatRule(
                ranges=[GridRange.from_a1_range(data_range, sheet)],
                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['💎']),
                format=CellFormat(backgroundColor=Color(0.9, 0.9, 1.0), textFormat=textFormat(bold=True, foregroundColor=Color(0.2, 0.2, 0.8))))
            ))
            rules.save()
        except: pass

    except Exception as e:
        print(f"❌ [Ex-Sheet] 치명적 오류:\n{traceback.format_exc()}")