import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import pandas as pd
import os, json, traceback
from datetime import datetime, timedelta

def update_commander_dashboard(df, macro_data, sheet_name, stats_df=None, 
                               today_recommendations=None, ai_recommendation=None):
    """
    사령부 통합 상황판: 
    1. 오늘의 정예 타격대 (오늘 날짜 + DNA 매칭)
    2. AI 추천 패턴 (TOP 5 승리 족보)
    3. 실시간 전수 관제판 (기존 메인 대시보드)
    4. 전술 통계 리포트 (기존 통계 데이터)
    """
    print(f"📡 [Ex-Sheet] 통합 관제 시스템 업데이트 개시")
    json_key_path = 'stock-key.json' 
    
    # 한국 시각(KST) 확정
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today_str = kst_now.strftime('%Y-%m-%d')
    
    
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
        
        # --- [1. 신규 기능: 오늘의_추천종목 (오늘만 + 정렬)] ---
        if today_recommendations is not None and not today_recommendations.empty:
            try:
                try: today_sheet = doc.worksheet("오늘의_추천종목")
                except: today_sheet = doc.add_worksheet(title="오늘의_추천종목", rows="200", cols="20", index=0)
                
                today_sheet.clear()
                
                # 오늘 날짜만 필터링
                if '날짜' in today_recommendations.columns:
                    today_recommendations['날짜'] = pd.to_datetime(today_recommendations['날짜']).dt.strftime('%Y-%m-%d')
                    today_only_df = today_recommendations[today_recommendations['날짜'] == today_str].copy()
                else:
                    today_only_df = today_recommendations.copy()

                if not today_only_df.empty:
                    # DNA 데이터와 결합
                    if ai_recommendation is not None:
                        final_today = pd.merge(
                            today_only_df, 
                            ai_recommendation[['종목', 'DNA_일치도', 'DNA_시퀀스', '최고수익률']], 
                            on='종목', how='left'
                        ).fillna({'DNA_일치도': '0%', '최고수익률': 0})
                        
                        # 정렬: 타율(수익률) -> 일치도 -> 안전점수
                        final_today = final_today.sort_values(by=['최고수익률', '안전점수'], ascending=False)
                        
                        header_info = [[f"🎯 금일 정예 타격 종목 (기준일: {today_str})", "", "", "", "", ""]]
                        today_sheet.update('A1', header_info)
                        
                        cols = ['종목', '현재가', '안전점수', 'DNA_일치도', '최고수익률', '구분']
                        set_with_dataframe(today_sheet, final_today[cols], row=4, col=1, include_index=False)
                        
                        # 오늘 데이터 하이라이트 (연노랑)
                        format_range = f"A4:F{len(final_today)+4}"
                        today_sheet.format(format_range, {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.85}, "textFormat": {"bold": True}})
                else:
                    today_sheet.update('A1', [[f"⚠️ {today_str} 오늘 탐지된 정예 종목이 없습니다."]])
            except Exception as e: print(f"⚠️ [Error] 오늘 추천 탭: {e}")

        # --- [2. 신규 기능: AI_추천패턴 (TOP 5 승리 족보)] ---
        if ai_recommendation is not None and not ai_recommendation.empty:
            try:
                try: ai_sheet = doc.worksheet("AI_추천패턴")
                except: ai_sheet = doc.add_worksheet(title="AI_추천패턴", rows="100", cols="10", index=1)
                ai_sheet.clear()
                # 기대값 높은 패턴 순서대로 정렬 (타율)
                ai_disp = ai_recommendation.sort_values(by='최고수익률', ascending=False).head(10)
                set_with_dataframe(ai_sheet, ai_disp, row=1, col=1, include_index=False)
            except Exception as e: print(f"⚠️ [Error] AI 패턴 탭: {e}")

        # --- [3. 기존 기능 유지: 실시간 전수 관제판 (메인)] ---
        sheet = doc.get_worksheet(0) # 첫 번째 시트 (보통 관제판)
        sheet.clear() 
        m = macro_data
        macro_list = [
            ["💎 사령부 실시간 다이아몬드 관제 시스템", "", ""],
            [f"📅 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", ""],
            [m.get('nasdaq',{}).get('text','N/A'), "", ""],
            [m.get('sp500',{}).get('text','N/A'), "", ""],
            [m.get('fx',{}).get('text','N/A'), "", ""],
            [f"🇰🇷 KOSPI 수급: {m.get('kospi','N/A')}", "", ""],
            ["[지침] 모든 데이터는 연구용이며, 최종 판단은 사령관님의 몫입니다.", "", ""]
        ]
        sheet.update('A1', macro_list, value_input_option='USER_ENTERED')
        
        if not df.empty:
            display_df = df.copy()
            # 별표(★) 수여 로직 그대로 유지
            score_col = '안전점수' if '안전점수' in display_df.columns else ('안전' if '안전' in display_df.columns else None)
            if score_col:
                display_df['종목'] = display_df.apply(lambda x: f"★ {x['종목']}" if int(x[score_col]) >= 130 else x['종목'], axis=1)
            set_with_dataframe(sheet, display_df, row=9, col=1, include_index=False)
            print("✅ [Main] 전수 관제판 업데이트 완료")

        # --- [4. 기존 기능 유지: 전술통계_리포트] ---
        if stats_df is not None and not stats_df.empty:
            try:
                try: stats_sheet = doc.worksheet("전술통계_리포트")
                except: stats_sheet = doc.add_worksheet(title="전술통계_리포트", rows="100", cols="10")
                stats_sheet.clear()
                set_with_dataframe(stats_sheet, stats_df, include_index=False)
                print("✅ [Stats] 전술 통계 리포트 업데이트 완료")
            except: pass

    except Exception as e:
        print(f"❌ [Ex-Sheet] 치명적 오류:\n{traceback.format_exc()}")