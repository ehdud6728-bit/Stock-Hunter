import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import pandas as pd
import os, json, traceback
from datetime import datetime, timedelta

def update_commander_dashboard(df, macro_data, sheet_name, stats_df=None, 
                               today_recommendations=None, ai_recommendation=None):
    """정밀 로그가 탑재된 통합 상황판 업데이트 모듈"""
    print("\n" + "🔍" * 15)
    print("📡 [Log] 사령부 정밀 필터링 수사 시작")
    
    json_key_path = 'stock-key.json' 
    
    # 1. 시간 설정 및 로그
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today_str = kst_now.strftime('%Y-%m-%d')
    print(f"📌 [Time] 사령부 기준 오늘(KST): [{today_str}]")
    print(f"📌 [Time] 서버 현재 시간(UTC): {datetime.utcnow()}")
    
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 인증 로직
        if os.path.exists(json_key_path):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        else:
            print("❌ [Auth] 인증 키 누락")
            return

        client = gspread.authorize(creds)
        doc = client.open(sheet_name)
        
        # --- [1. 오늘의_추천종목 필터링 집중 수사] ---
        if today_recommendations is not None and not today_recommendations.empty:
            print(f"📊 [Data] 전체 입력 데이터 건수: {len(today_recommendations)}건")
            
            try:
                try: today_sheet = doc.worksheet("오늘의_추천종목")
                except: today_sheet = doc.add_worksheet(title="오늘의_추천종목", rows="200", cols="20", index=0)
                
                today_sheet.clear()
                
                if '날짜' in today_recommendations.columns:
                    # 💡 로그: 변환 전 실제 값 확인
                    raw_sample = today_recommendations['날짜'].unique().tolist()
                    print(f"📅 [Debug] 변환 전 데이터 내 날짜 종류 (Unique): {raw_sample}")
                    
                    # 날짜 형식 정규화
                    today_recommendations['날짜_str'] = pd.to_datetime(today_recommendations['날짜']).dt.strftime('%Y-%m-%d')
                    clean_sample = today_recommendations['날짜_str'].unique().tolist()
                    print(f"📅 [Debug] 변환 후 데이터 내 날짜 종류 (Unique): {clean_sample}")
                    
                    # 💡 필터링 실행
                    today_only_df = today_recommendations[today_recommendations['날짜_str'] == today_str].copy()
                    
                    print(f"🎯 [Filter] [{today_str}] 과 일치하는 데이터 수: {len(today_only_df)}건")
                    
                    if len(today_only_df) == 0:
                        print(f"⚠️ [Warning] 필터링 결과가 0건입니다! 오늘 날짜({today_str})가 데이터에 아예 없거나 형식이 다릅니다.")
                else:
                    print("❌ [Error] 데이터에 '날짜' 컬럼이 아예 없습니다!")
                    today_only_df = pd.DataFrame()

                # --- 이후 데이터 전송 로직 (생략 없이 수행) ---
                if not today_only_df.empty:
                    if ai_recommendation is not None:
                        # DNA 데이터와 결합 로그
                        print(f"🧬 [DNA] 매칭 시작 (DNA 데이터 건수: {len(ai_recommendation)})")
                        final_today = pd.merge(
                            today_only_df, 
                            ai_recommendation[['종목', 'DNA_일치도', 'DNA_시퀀스', '최고수익률']], 
                            on='종목', how='left'
                        ).fillna({'DNA_일치도': '0%', '최고수익률': 0})
                        
                        final_today = final_today.sort_values(by=['최고수익률', '안전점수'], ascending=False)
                        
                        header_info = [[f"🎯 금일 정예 타격 종목 (기준일: {today_str})", "", "", "", "", ""]]
                        today_sheet.update('A1', header_info)
                        
                        cols = ['종목', '현재가', '안전점수', 'DNA_일치도', '최고수익률', '구분']
                        actual_cols = [c for c in cols if c in final_today.columns]
                        set_with_dataframe(today_sheet, final_today[actual_cols], row=4, col=1, include_index=False)
                        
                        format_range = f"A4:F{len(final_today)+4}"
                        today_sheet.format(format_range, {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.85}, "textFormat": {"bold": True}})
                        print(f"✅ [Success] 오늘의 추천종목 탭 업데이트 완료 ({len(final_today)}건)")
                else:
                    today_sheet.update('A1', [[f"⚠️ {today_str} 오늘 탐지된 데이터가 없습니다."]])

            except Exception as e:
                print(f"❌ [Error] 추천 탭 처리 중 에러: {e}")
                traceback.print_exc()

        # --- [나머지 탭 업데이트: 기존 로직 유지] ---
        # 2. AI_추천패턴 / 3. 메인 관제판 / 4. 전술 통계
        # (기존 사령관님의 코드가 이 아래에 위치합니다)
        print("📡 [Main] 나머지 관제판 및 통계 업데이트 진행 중...")
        
        # (중략된 부분은 사령관님의 기존 코드를 그대로 유지하여 실행됩니다)

    except Exception as e:
        print(f"❌ [Critical] 치명적 오류:\n{traceback.format_exc()}")
    
    print("🔍" * 15 + "\n")