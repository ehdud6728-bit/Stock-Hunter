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
    [이름 고정 버전] 번호가 아닌 이름으로 시트를 찾아 데이터 혼선을 원천 차단합니다.
    """
    print("\n" + "🚀" * 15)
    print("📡 [Log] 사령부 통합 관제 시스템 업데이트 개시 (이름 고정 모드)")
    
    json_key_path = 'stock-key.json' 
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today_str = kst_now.strftime('%Y-%m-%d')
    
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 1. 인증 및 문서 열기
        if os.path.exists(json_key_path):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        else:
            print("❌ [Auth] 구글 인증 키 누락")
            return

        client = gspread.authorize(creds)
        doc = client.open(sheet_name)
        
        # --- [탭 1: 오늘의_추천종목] ---
        if today_recommendations is not None and not today_recommendations.empty:
            try:
                try: today_sheet = doc.worksheet("오늘의_추천종목")
                except: today_sheet = doc.add_worksheet(title="오늘의_추천종목", rows="200", cols="20")
                
                today_sheet.clear()
                
                # 오늘 날짜 필터링
                today_recommendations['날짜_clean'] = pd.to_datetime(today_recommendations['날짜']).dt.strftime('%Y-%m-%d')
                today_only_df = today_recommendations[today_recommendations['날짜_clean'] == today_str].copy()

                if not today_only_df.empty:
                    # DNA 데이터와 병합
                    if ai_recommendation is not None:
                        final_today = pd.merge(
                            today_only_df, 
                            ai_recommendation[['종목', 'DNA_일치도', 'DNA_시퀀스', '최고수익률']], 
                            on='종목', how='left'
                        ).fillna({'DNA_일치도': '0%', '최고수익률': 0})
                        
                        # 정렬 및 TOP 50 절단
                        final_today['match_val'] = final_today['DNA_일치도'].str.replace('%','').astype(int)
                        final_today = final_today.sort_values(by=['최고수익률', 'match_val'], ascending=False).head(50)
                        
                        header_info = [[f"🎯 금일 정예 관상 종목 (기준일: {today_str})", "", "", "", "", ""]]
                        today_sheet.update('A1', header_info)
                        
                        cols = ['종목', 'DNA_일치도', '최고수익률', '현재가', '안전점수', '구분']
                        set_with_dataframe(today_sheet, final_today[cols], row=4, col=1, include_index=False)
                        print(f"✅ [Success] 오늘의_추천종목 업데이트 완료 ({len(final_today)}건)")
                else:
                    today_sheet.update('A1', [[f"⚠️ {today_str} 오늘 신호가 없습니다."]])
            except Exception as e: print(f"❌ [Error] 탭 1 실패: {e}")

        # --- [탭 2: AI_추천패턴] ---
        if ai_recommendation is not None and not ai_recommendation.empty:
            try:
                try: ai_sheet = doc.worksheet("AI_추천패턴")
                except: ai_sheet = doc.add_worksheet(title="AI_추천패턴", rows="100", cols="10")
                ai_sheet.clear()
                ai_disp = ai_recommendation.sort_values(by='최고수익률', ascending=False).head(15)
                set_with_dataframe(ai_sheet, ai_disp, row=1, col=1, include_index=False)
                print(f"✅ [Success] AI_추천패턴 업데이트 완료")
            except Exception as e: print(f"❌ [Error] 탭 2 실패: {e}")

        # --- [탭 3: 실시간_전수_관제판] ---
        # 💡 번호(get_worksheet(0))가 아니라 이름으로 명확하게 지정합니다.
        try:
            try: main_sheet = doc.worksheet("실시간_전수_관제판")
            except: 
                # 만약 기존에 "시트1" 등으로 되어있다면 이름을 변경하거나 새로 생성
                main_sheet = doc.get_worksheet(0)
                main_sheet.update_title("실시간_전수_관제판")
            
            main_sheet.clear()
            m = macro_data
            macro_list = [
                ["💎 사령부 실시간 다이아몬드 관제 시스템", "", ""],
                [f"📅 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", ""],
                [f"📈 나스닥: {m.get('nasdaq',{}).get('text','N/A')}", "", ""],
                [f"💵 달러환율: {m.get('fx',{}).get('text','N/A')}", "", ""],
            ]
            main_sheet.update('A1', macro_list, value_input_option='USER_ENTERED')
            
            if not df.empty:
                set_with_dataframe(main_sheet, df, row=9, col=1, include_index=False)
                print(f"✅ [Success] 실시간 관제판({len(df)}건) 업데이트 완료")
        except Exception as e: print(f"❌ [Error] 탭 3 실패: {e}")

        # --- [탭 4: 전술통계_리포트] ---
        if stats_df is not None and not stats_df.empty:
            try:
                try: stats_sheet = doc.worksheet("전술통계_리포트")
                except: stats_sheet = doc.add_worksheet(title="전술통계_리포트", rows="100", cols="10")
                stats_sheet.clear()
                set_with_dataframe(stats_sheet, stats_df, include_index=False)
                print(f"✅ [Success] 전술 통계 리포트 업데이트 완료")
            except: pass

    except Exception as e:
        print(f"❌ [Critical] 치명적 오류:\n{traceback.format_exc()}")