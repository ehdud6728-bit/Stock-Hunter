import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os, json, traceback
from datetime import datetime

# 💡 데이터프레임 전송을 위한 필수 부품
try:
    from gspread_dataframe import set_with_dataframe
except ImportError:
    print("❌ [Fatal] gspread-dataframe 라이브러리가 없습니다.")

def update_commander_dashboard(df, macro_data, sheet_name, stats_df=None, 
                               today_recommendations=None, ai_recommendation=None):
    """Stock_Test.py 전용: 정렬 및 오늘 날짜 필터링 기능 추가 버전"""
    print(f"📡 [Ex-Sheet] 시트 업데이트 작전 개시 (데이터: {len(df)}건)")
    json_key_path = 'stock-key.json' 
    today_str = datetime.now().strftime('%Y-%m-%d')
    
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
        
        # 💡 [신규 탭 1: 오늘의_추천종목] - 정렬 및 필터링 강화
        if today_recommendations is not None and not today_recommendations.empty:
            try:
                try:
                    today_sheet = doc.worksheet("오늘의_추천종목")
                except:
                    today_sheet = doc.add_worksheet(title="오늘의_추천종목", rows="200", cols="20", index=0)
                
                today_sheet.clear()
                
                # --- [추천종목 데이터 전처리: 필터링 & 정렬] ---
                # 1. 오늘 날짜 데이터만 필터링
                if '날짜' in today_recommendations.columns:
                    today_recommendations = today_recommendations[today_recommendations['날짜'] == today_str]
                
                # 2. 정렬 로직: 기대값(있을 경우) -> 안전점수(있을 경우) 순으로 내림차순
                sort_cols = []
                if '기대값' in today_recommendations.columns: sort_cols.append('기대값')
                if '안전점수' in today_recommendations.columns: sort_cols.append('안전점수')
                elif '안전' in today_recommendations.columns: sort_cols.append('안전')
                
                if sort_cols:
                    today_recommendations = today_recommendations.sort_values(by=sort_cols, ascending=False)
                
                if not today_recommendations.empty:
                    header_info = [
                        [f"🎯 오늘의 실시간 AI 추천종목 (기준일: {today_str})", "", "", "", ""],
                        ["전략 사령부 정밀 필터링 완료 (오늘 날짜 데이터만 표시)", "", "", "", ""],
                        ["", "", "", "", ""]
                    ]
                    today_sheet.update('A1', header_info, value_input_option='USER_ENTERED')
                    set_with_dataframe(today_sheet, today_recommendations, row=4, col=1, include_index=False)
                    
                    # 서식 적용
                    try:
                        num_rows = len(today_recommendations) + 5
                        data_range = f"A5:Z{num_rows}"
                        rules = get_conditional_format_rules(today_sheet)
                        rules.clear()
                        rules.append(ConditionalFormatRule(
                            ranges=[GridRange.from_a1_range(data_range, today_sheet)],
                            booleanRule=BooleanRule(
                                condition=BooleanCondition('TEXT_CONTAINS', ['💎다이아몬드']),
                                format=CellFormat(backgroundColor=Color(1.0, 0.95, 0.8), textFormat=textFormat(bold=True))
                            )
                        ))
                        rules.save()
                    except: pass
                    print(f"✅ [Ex-Sheet] 오늘의 추천종목 ({len(today_recommendations)}건) 필터링 및 정렬 완료")
                else:
                    today_sheet.update('A1', [[f"⚠️ {today_str} 당일 탐지된 종목이 없습니다."]])

            except Exception as e:
                print(f"⚠️ [Ex-Sheet] 오늘의 추천종목 탭 오류: {e}")

        # 💡 [신규 탭 2: AI_추천패턴]
        if ai_recommendation is not None and not ai_recommendation.empty:
            try:
                try:
                    ai_sheet = doc.worksheet("AI_추천패턴")
                except:
                    ai_sheet = doc.add_worksheet(title="AI_추천패턴", rows="100", cols="10", index=1)
                
                ai_sheet.clear()
                # 기대값 높은 순 정렬
                if '기대값' in ai_recommendation.columns:
                    ai_recommendation = ai_recommendation.sort_values(by='기대값', ascending=False)
                
                ai_header = [
                    ["🏆 AI 분석 기반 TOP 5 전략 패턴", "", "", "", "", "", ""],
                    [f"업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", "", "", "", "", ""],
                    ["", "", "", "", "", "", ""]
                ]
                ai_sheet.update('A1', ai_header, value_input_option='USER_ENTERED')
                set_with_dataframe(ai_sheet, ai_recommendation, row=5, col=1, include_index=False)
                print("✅ [Ex-Sheet] AI 추천패턴 전송 완료")
            except Exception as e:
                print(f"⚠️ [Ex-Sheet] AI 추천패턴 탭 오류: {e}")

        # --- [기존 탭 1: 실시간 전수 관제판] ---
        sheet = doc.get_worksheet(0)
        sheet.clear() 

        m = macro_data
        macro_list = [
            ["💎 사령부 연구소(Ex) 실시간 다이아몬드 관제 시스템", "", ""],
            [f"📅 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", ""],
            [m.get('nasdaq',{}).get('text','나스닥 연결실패'), "", ""],
            [m.get('sp500',{}).get('text','S&P500 연결실패'), "", ""],
            [m.get('vix',{}).get('text','VIX 연결실패'), "", ""],
            [f"💵 달러환율: {m.get('fx',{}).get('text','환율오류')}", "", ""],
            [f"🇰🇷 KOSPI 수급: {m.get('kospi','데이터없음')}", "", ""],
            ["[지침] 🏆LEGEND 및 💎다이아몬드 복합 타점 정밀 검증 중", "", ""]
        ]
        sheet.update('A1', macro_list, value_input_option='USER_ENTERED')
        
        if not df.empty:
            display_df = df.copy()
            score_col = '안전점수' if '안전점수' in display_df.columns else '안전'
            if score_col in display_df.columns:
                display_df['종목'] = display_df.apply(
                    lambda x: f"★ {x['종목']}" if int(x[score_col]) >= 130 else x['종목'], axis=1
                )
            set_with_dataframe(sheet, display_df, row=9, col=1, include_index=False)
            print("✅ [Ex-Sheet] 메인 관제판 업데이트 완료")

        # --- [기존 탭 2: 전술통계_리포트] ---
        if stats_df is not None and not stats_df.empty:
            try:
                try:
                    stats_sheet = doc.worksheet("전술통계_리포트")
                except:
                    stats_sheet = doc.add_worksheet(title="전술통계_리포트", rows="100", cols="10")
                stats_sheet.clear()
                set_with_dataframe(stats_sheet, stats_df, include_index=False)
                print("✅ [Ex-Sheet] 전술 통계 업데이트 완료")
            except: pass

    except Exception as e:
        print(f"❌ [Ex-Sheet] 치명적 오류:\n{traceback.format_exc()}")
