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
    [전체 기능 통합] 사령부 상황판 업데이트 모듈
    1. 오늘의 정예 타격대 (필터링 & 로그 포함)
    2. AI 추천 패턴 (DNA 족보)
    3. 실시간 전수 관제판 (메인)
    4. 전술 통계 리포트
    """
    print("\n" + "🔍" * 15)
    print("📡 [Log] 사령부 통합 관제 시스템 가동 및 정밀 수사 시작")
    
    json_key_path = 'stock-key.json' 
    
    # 💡 [KST 설정] 한국 시간 기준으로 오늘 날짜 확정 (GitHub Actions UTC 대응)
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today_str = kst_now.strftime('%Y-%m-%d')
    print(f"📌 [Time] 사령부 기준 오늘(KST): [{today_str}]")
    
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 1. 인증 로직
        if os.path.exists(json_key_path):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        else:
            print("❌ [Auth] 구글 인증 키를 찾을 수 없어 작전을 중단합니다.")
            return

        client = gspread.authorize(creds)
        doc = client.open(sheet_name)
        
        # --- [탭 1: 오늘의_추천종목 (오늘만 필터링 + 로그)] ---
        if today_recommendations is not None and not today_recommendations.empty:
            print(f"📊 [Data-1] 추천종목 후보군: {len(today_recommendations)}건 수신")
            try:
                try: today_sheet = doc.worksheet("오늘의_추천종목")
                except: today_sheet = doc.add_worksheet(title="오늘의_추천종목", rows="300", cols="20", index=0)
                
                today_sheet.clear()
                
                if '날짜' in today_recommendations.columns:
                    # 💡 날짜 형식 정규화 로그
                    raw_dates = today_recommendations['날짜'].unique().tolist()
                    print(f"📅 [Debug] 데이터 내 실제 날짜들: {raw_dates}")
                    
                    today_recommendations['날짜_clean'] = pd.to_datetime(today_recommendations['날짜']).dt.strftime('%Y-%m-%d')
                    
                    # 💡 필터링 실행
                    today_only_df = today_recommendations[today_recommendations['날짜_clean'] == today_str].copy()
                    print(f"🎯 [Filter] [{today_str}] 필터링 결과: {len(today_only_df)}건 생존")
                else:
                    print("⚠️ [Warning] 데이터에 '날짜' 컬럼이 없어 필터링을 생략합니다.")
                    today_only_df = today_recommendations.copy()

                if not today_only_df.empty:
                    # DNA 데이터와 병합 (타율 정보 추가)
                    if ai_recommendation is not None:
                        print(f"🧬 [DNA] 매칭 시도 중... (Master DNA 건수: {len(ai_recommendation)})")
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
                        actual_cols = [c for c in cols if c in final_today.columns]
                        set_with_dataframe(today_sheet, final_today[actual_cols], row=4, col=1, include_index=False)
                        
                        # 하이라이트 서식 (연노랑)
                        format_range = f"A4:F{len(final_today)+4}"
                        today_sheet.format(format_range, {
                            "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.85},
                            "textFormat": {"bold": True, "fontSize": 10}
                        })
                        print(f"✅ [Success] 오늘의_추천종목 탭 전송 완료")
                else:
                    today_sheet.update('A1', [[f"⚠️ {today_str} 오늘 날짜 데이터가 없습니다."]])
                    print("⚠️ [Notice] 오늘 날짜 데이터가 없어 시트 전송을 스킵했습니다.")
            except Exception as e:
                print(f"❌ [Error] 탭 1 처리 실패: {e}")

        # --- [탭 2: AI_추천패턴 (TOP 5 승리 족보)] ---
        if ai_recommendation is not None and not ai_recommendation.empty:
            print(f"📊 [Data-2] AI 추천패턴: {len(ai_recommendation)}건 수신")
            try:
                try: ai_sheet = doc.worksheet("AI_추천패턴")
                except: ai_sheet = doc.add_worksheet(title="AI_추천패턴", rows="100", cols="10", index=1)
                
                ai_sheet.clear()
                ai_disp = ai_recommendation.sort_values(by='최고수익률', ascending=False).head(15)
                set_with_dataframe(ai_sheet, ai_disp, row=1, col=1, include_index=False)
                print(f"✅ [Success] AI_추천패턴 탭 전송 완료")
            except Exception as e:
                print(f"❌ [Error] 탭 2 처리 실패: {e}")

        # --- [탭 3: 실시간 전수 관제판 (메인)] ---
        try:
            print("📡 [Main] 실시간 전수 관제판 업데이트 중...")
            sheet = doc.get_worksheet(0)
            sheet.clear()
            
            m = macro_data
            macro_list = [
                ["💎 사령부 실시간 다이아몬드 관제 시스템", "", ""],
                [f"📅 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", ""],
                [f"📈 나스닥: {m.get('nasdaq',{}).get('text','N/A')}", "", ""],
                [f"📊 S&P500: {m.get('sp500',{}).get('text','N/A')}", "", ""],
                [f"💵 달러환율: {m.get('fx',{}).get('text','N/A')}", "", ""],
                [f"🇰🇷 KOSPI 수급: {m.get('kospi','N/A')}", "", ""],
                ["[연구] 모든 데이터는 보조 지표이며 최종 책임은 본인에게 있습니다.", "", ""]
            ]
            sheet.update('A1', macro_list, value_input_option='USER_ENTERED')
            
            if not df.empty:
                display_df = df.copy()
                score_col = '안전점수' if '안전점수' in display_df.columns else ('안전' if '안전' in display_df.columns else None)
                if score_col:
                    display_df['종목'] = display_df.apply(
                        lambda x: f"★ {x['종목']}" if int(str(x[score_col]).replace('점','')) >= 130 else x['종목'], 
                        axis=1
                    )
                set_with_dataframe(sheet, display_df, row=9, col=1, include_index=False)
                print(f"✅ [Success] 실시간 관제판 업데이트 완료 ({len(df)}건)")
        except Exception as e:
            print(f"❌ [Error] 탭 3(메인) 처리 실패: {e}")

        # --- [탭 4: 전술통계_리포트] ---
        if stats_df is not None and not stats_df.empty:
            print(f"📊 [Data-4] 전술 통계: {len(stats_df)}건 수신")
            try:
                try: stats_sheet = doc.worksheet("전술통계_리포트")
                except: stats_sheet = doc.add_worksheet(title="전술통계_리포트", rows="100", cols="10")
                
                stats_sheet.clear()
                set_with_dataframe(stats_sheet, stats_df, include_index=False)
                print(f"✅ [Success] 전술 통계 리포트 업데이트 완료")
            except Exception as e:
                print(f"❌ [Error] 탭 4 처리 실패: {e}")

    except Exception as e:
        print(f"❌ [Critical] 구글 시트 통신 중 치명적 오류:\n{traceback.format_exc()}")

    print("🔍" * 15 + " [ 사령부 정밀 로그 종료 ] " + "\n")