import time
import gspread
from gspread_formatting import *
# 💡 최신 인증 라이브러리로 교체
from google.oauth2.service_account import Credentials 
from gspread_dataframe import set_with_dataframe
import pandas as pd
import os, json, traceback
from datetime import datetime, timedelta
from DNA_Analyzer import analyze_dna_sequences, find_winning_pattern, find_winning_pattern_by_tier

def update_commander_dashboard(df, macro_data, sheet_name, stats_df=None, 
                               today_recommendations=None, ai_recommendation=None,
                               s_grade_special=None,
                               df_backtest=None, df_realistic=None,
                               df_combo=None, best_combos=None, worst_combos=None,
                               df_profit_dist=None):
    """
    [경량화 버전] 함수 내 복잡한 계산을 제거하고 저장에만 집중합니다.
    """
    print(f"📡 [Ex-Sheet] 시트 전송 시작...")
    json_key_path = 'stock-key.json' 
    today_str = (datetime.utcnow() + timedelta(hours=9)).strftime('%Y-%m-%d')
    
    try:
        # 1. 🔑 인증 (환경변수 또는 파일)
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        if os.path.exists(json_key_path):
            creds = Credentials.from_service_account_file(json_key_path, scopes=scope)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            creds = Credentials.from_service_account_info(json.loads(os.environ.get('GOOGLE_JSON_KEY')), scopes=scope)
        else:
            print("❌ [Auth] 인증 키가 없습니다."); return

        client = gspread.authorize(creds)
        doc = client.open(sheet_name)

        # --- [탭 1: 오늘의_추천종목] (심플하게 변경) ---
        if today_recommendations is not None and not today_recommendations.empty:
            try:
                try: t_sheet = doc.worksheet("오늘의_추천종목")
                except: t_sheet = doc.add_worksheet(title="오늘의_추천종목", rows="200", cols="20")
                t_sheet.clear()
                legend_today = today_recommendations.copy()
																		# '👑등급' 컬럼에서 '👑LEGEND'인 행만 추출합니다.
               #legend_today = today_recommendations[today_recommendations['👑등급'] == "👑LEGEND"].copy()
               #legend_today = legend_today[legend_today['날짜'] == today_str].copy()
                #
                # 헤더 정보
                t_sheet.update('A1', [[f"🎯 {today_str} 레이더 포착 종목 (안전점수 순)"]])
                # 데이터 전송 (함수 밖에서 이미 가공된 데이터를 그대로 넣음)
                set_with_dataframe(t_sheet, legend_today, row=3, col=1, include_index=False)
                print("✅ [오늘의_추천종목] {today_str} 저장 완료")
            except Exception as e: print(f"❌ 탭 1 에러: {e}")

        # --- [탭 2: AI_추천패턴] (데이터가 있을 때만 가동) ---
        if ai_recommendation is not None and not ai_recommendation.empty:
            try:
                time.sleep(5)  # ✅ 5초 대기

                try: ai_sheet = doc.worksheet("AI_추천패턴")
                except: ai_sheet = doc.add_worksheet(title="AI_추천패턴", rows="200", cols="15")
                ai_sheet.clear()
                set_with_dataframe(ai_sheet, ai_recommendation, include_index=False)
                print("✅ [AI_추천패턴] 저장 완료")
            except Exception as e: print(f"❌ 탭 2 에러: {e}")

        # --- [탭 3: 실시간_전수_관제판] ---
        if df is not None and not df.empty:
            try:
                time.sleep(5)  # ✅ 5초 대기

                try: m_sheet = doc.worksheet("실시간_전수_관제판")
                except:
                    m_sheet = doc.add_worksheet(title="실시간_전수_관제판", rows="200", cols="15")
                m_sheet.clear()
                
                # 상단 매크로 정보
                m = macro_data
                macro_list = [
                    [f"📅 업데이트: {datetime.now().strftime('%H:%M:%S')}"],
                    [f"📈 나스닥: {m.get('nasdaq',{}).get('text','-')}"],
                    [f"💵 달러환율: {m.get('fx',{}).get('text','-')}"]
                ]
                m_sheet.update('A1', macro_list)
                # 데이터 전송
                #legend_list = df[df['👑등급'].isin(["👑LEGEND", "⚔️정예"])].sort_values(by='안전점수', ascending=False).copy()
				legend_list = df.sort_values(by='안전점수', ascending=False).copy()
                set_with_dataframe(m_sheet, legend_list, row=6, col=1, include_index=False)
                print("✅ [실시간_전수_관제판] 저장 완료")
            except Exception as e: print(f"❌ 탭 3 에러: {e}")

        # --- [탭 4: 전술통계_리포트] ---
        if stats_df is not None and not stats_df.empty:
            try:
                time.sleep(5)  # ✅ 5초 대기

                try: s_sheet = doc.worksheet("전술통계_리포트")
                except: s_sheet = doc.add_worksheet(title="전술통계_리포트", rows="100", cols="10")
                s_sheet.clear()
                set_with_dataframe(s_sheet, stats_df, include_index=False)
                print("✅ [전술통계_리포트] 저장 완료")
            except Exception as e: print(f"❌ 탭 4 에러: {e}")
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 💎 [신규 탭 1: 조합별 성과]
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        if df_combo is not None and not df_combo.empty:
            try:
                time.sleep(5)  # ✅ 5초 대기

                try:
                    combo_sheet = doc.worksheet("조합별_성과")
                except:
                    combo_sheet = doc.add_worksheet(
                        title="조합별_성과", 
                        rows="200", 
                        cols="15", 
                        index=2
                    )
                
                combo_sheet.clear()
                
                # 헤더
                header = [
                    ["🏆 조합별 성과 분석 (실전 예상)", "", "", "", ""],
                    [f"분석 기간: 과거 30일", "", "", "", ""],
                    [f"업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", "", "", ""],
                    ["※ 다음날 시초가 매수 + 최고가 70% + 수수료 0.26% 반영", "", "", "", ""],
                    ["", "", "", "", ""]
                ]
                combo_sheet.update('A1', header, value_input_option='USER_ENTERED')
                
                # 데이터
                set_with_dataframe(combo_sheet, df_combo, row=6, col=1, include_index=False)
                
                # 서식
                combo_sheet.format('A6:O6', {
                    'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9},
                    'textFormat': {
                        'bold': True, 
                        'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}
                    }
                })
                
                # S급 조합 강조 (골드)
                if len(df_combo) > 0:
                    for idx, row in df_combo.iterrows():
                        row_num = 7 + idx
                        if 'S급' in str(row.get('등급', '')):
                            combo_sheet.format(f'A{row_num}:O{row_num}', {
                                'backgroundColor': {'red': 1.0, 'green': 0.95, 'blue': 0.7}
                            })
                
                print("✅ [Ex-Sheet] 조합별 성과 시트 생성 완료")
            except Exception as e:
                print(f"⚠️ [Ex-Sheet] 조합별 성과 오류: {e}")
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🥇 [신규 탭 2: TOP/WORST 조합]
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        if best_combos and worst_combos:
            try:
                time.sleep(5)  # ✅ 5초 대기

                try:
                    top_sheet = doc.worksheet("TOP_WORST_조합")
                except:
                    top_sheet = doc.add_worksheet(
                        title="TOP_WORST_조합", 
                        rows="100", 
                        cols="12", 
                        index=3
                    )
                
                top_sheet.clear()
                
                # TOP 10 섹션
                top_header = [
                    ["🥇 TOP 10 최고 성과 조합", "", "", "", ""],
                    [f"업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", "", "", ""],
                    ["", "", "", "", ""],
                    ["순위", "조합", "등급", "건수", "승률(%)", "평균수익(%)", "기대값", "샤프비율", "안정성"]
                ]
                top_sheet.update('A1', top_header, value_input_option='USER_ENTERED')
                
                # TOP 10 데이터
                top_data = []
                for idx, combo in enumerate(best_combos[:10], 1):
                    medal = "🥇" if idx == 1 else "🥈" if idx == 2 else "🥉" if idx == 3 else str(idx)
                    top_data.append([
                        medal,
                        combo['조합'],
                        combo['등급'],
                        combo['건수'],
                        combo['승률(%)'],
                        combo['평균수익(%)'],
                        combo['기대값'],
                        combo['샤프비율'],
                        combo['안정성']
                    ])
                
                if top_data:
                    top_sheet.update('A5', top_data, value_input_option='USER_ENTERED')
                
                # TOP 10 서식
                top_sheet.format('A4:I4', {
                    'backgroundColor': {'red': 1.0, 'green': 0.84, 'blue': 0.0},
                    'textFormat': {'bold': True}
                })
                
                # 1-3위 강조
                top_sheet.format('A5:I7', {
                    'backgroundColor': {'red': 1.0, 'green': 0.95, 'blue': 0.8}
                })
                
                # WORST 5 섹션
                worst_start_row = 5 + len(best_combos) + 3
                worst_header = [
                    ["", "", "", "", ""],
                    ["⚠️ WORST 5 저성과 조합 (개선 필요)", "", "", "", ""],
                    ["", "", "", "", ""],
                    ["순위", "조합", "건수", "승률(%)", "평균수익(%)", "기대값", "샤프비율", "문제점"]
                ]
                top_sheet.update(f'A{worst_start_row}', worst_header, value_input_option='USER_ENTERED')
                
                # WORST 5 데이터
                worst_data = []
                for idx, combo in enumerate(worst_combos, 1):
                    # 문제점 분석
                    issues = []
                    if combo['승률(%)'] < 70:
                        issues.append("승률↓")
                    if combo['평균수익(%)'] < 15:
                        issues.append("수익↓")
                    if combo['샤프비율'] < 3:
                        issues.append("안정성↓")
                    
                    worst_data.append([
                        idx,
                        combo['조합'],
                        combo['건수'],
                        combo['승률(%)'],
                        combo['평균수익(%)'],
                        combo['기대값'],
                        combo['샤프비율'],
                        ", ".join(issues) if issues else "건수부족"
                    ])
                
                if worst_data:
                    data_start = worst_start_row + 3
                    top_sheet.update(f'A{data_start}', worst_data, value_input_option='USER_ENTERED')
                
                # WORST 서식 (빨강)
                top_sheet.format(f'A{worst_start_row+3}:H{worst_start_row+3}', {
                    'backgroundColor': {'red': 1.0, 'green': 0.7, 'blue': 0.7},
                    'textFormat': {'bold': True}
                })
                
                if len(worst_data) > 0:
                    top_sheet.format(f'A{data_start}:H{data_start+len(worst_data)-1}', {
                        'backgroundColor': {'red': 1.0, 'green': 0.9, 'blue': 0.9}
                    })
                
                print("✅ [Ex-Sheet] TOP/WORST 조합 시트 생성 완료")
            except Exception as e:
                print(f"⚠️ [Ex-Sheet] TOP/WORST 조합 오류: {e}")
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 📊 [신규 탭 3: 수익률 분포]
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        if df_profit_dist is not None and not df_profit_dist.empty:
            try:
                time.sleep(5)  # ✅ 5초 대기

                try:
                    dist_sheet = doc.worksheet("수익률_분포")
                except:
                    dist_sheet = doc.add_worksheet(
                        title="수익률_분포", 
                        rows="50", 
                        cols="10", 
                        index=4
                    )
                
                dist_sheet.clear()
                
                # 헤더
                header = [
                    ["📊 수익률 구간별 분포 분석", "", "", ""],
                    [f"전체 케이스: {df_profit_dist['건수'].sum()}건", "", "", ""],
                    [f"업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", "", ""],
                    ["", "", "", ""],
                ]
                dist_sheet.update('A1', header, value_input_option='USER_ENTERED')
                
                # 데이터
                set_with_dataframe(dist_sheet, df_profit_dist, row=5, col=1, include_index=False)
                
                # 서식 (헤더)
                dist_sheet.format('A5:D5', {
                    'backgroundColor': {'red': 0.4, 'green': 0.7, 'blue': 0.4},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}}
                })
                
                # 구간별 색상
                if len(df_profit_dist) > 0:
                    for idx, row in df_profit_dist.iterrows():
                        row_num = 6 + idx
                        구간 = str(row['구간'])
                        
                        if '🔴' in 구간:  # 손실
                            color = {'red': 1.0, 'green': 0.8, 'blue': 0.8}
                        elif '⚪' in 구간:  # 미미
                            color = {'red': 1.0, 'green': 1.0, 'blue': 1.0}
                        elif '🟡' in 구간:  # 소폭
                            color = {'red': 1.0, 'green': 1.0, 'blue': 0.8}
                        elif '🟢' in 구간:  # 보통
                            color = {'red': 0.8, 'green': 1.0, 'blue': 0.8}
                        elif '🔵' in 구간:  # 양호
                            color = {'red': 0.8, 'green': 0.9, 'blue': 1.0}
                        elif '🟣' in 구간:  # 우수
                            color = {'red': 0.9, 'green': 0.8, 'blue': 1.0}
                        elif '⭐' in 구간:  # 대박
                            color = {'red': 1.0, 'green': 0.95, 'blue': 0.7}
                        elif '💎' in 구간:  # 초대박
                            color = {'red': 1.0, 'green': 0.84, 'blue': 0.0}
                        else:
                            color = {'red': 1.0, 'green': 1.0, 'blue': 1.0}
                        
                        dist_sheet.format(f'A{row_num}:D{row_num}', {
                            'backgroundColor': color
                        })
                
                print("✅ [Ex-Sheet] 수익률 분포 시트 생성 완료")
            except Exception as e:
                print(f"⚠️ [Ex-Sheet] 수익률 분포 오류: {e}")
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 📈 [신규 탭 4: 백테스트 비교]
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        if df_backtest is not None and df_realistic is not None:
            try:
                time.sleep(5)  # ✅ 5초 대기

                try:
                    bt_sheet = doc.worksheet("백테스트_비교")
                except:
                    bt_sheet = doc.add_worksheet(
                        title="백테스트_비교", 
                        rows="50", 
                        cols="12", 
                        index=5
                    )
                
                bt_sheet.clear()
                
                # 백테스트 섹션
                bt_header = [
                    ["📊 백테스트 vs 실전 비교", "", "", ""],
                    ["", "", "", ""],
                    ["🔬 백테스트 (이상적 시나리오)", "", "", ""],
                    ["※ 최고가 정확히 매도 가정", "", "", ""],
                    ["", "", "", ""]
                ]
                bt_sheet.update('A1', bt_header, value_input_option='USER_ENTERED')
                
                # 백테스트 데이터
                set_with_dataframe(bt_sheet, df_backtest, row=6, col=1, include_index=False)
                
                # 실전 섹션
                real_start = 6 + len(df_backtest) + 3
                real_header = [
                    ["", "", "", ""],
                    ["💡 실전 예상 (현실적 시나리오)", "", "", ""],
                    ["※ 다음날 시초가 + 최고가 70% + 수수료 0.26%", "", "", ""],
                    ["", "", "", ""]
                ]
                bt_sheet.update(f'A{real_start}', real_header, value_input_option='USER_ENTERED')
                
                # 실전 데이터
                set_with_dataframe(bt_sheet, df_realistic, row=real_start+4, col=1, include_index=False)
                
                # 서식
                bt_sheet.format('A6:J6', {
                    'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 1.0},
                    'textFormat': {'bold': True}
                })
                
                bt_sheet.format(f'A{real_start+4}:J{real_start+4}', {
                    'backgroundColor': {'red': 0.8, 'green': 1.0, 'blue': 0.8},
                    'textFormat': {'bold': True}
                })
                
                print("✅ [Ex-Sheet] 백테스트 비교 시트 생성 완료")
            except Exception as e:
                print(f"⚠️ [Ex-Sheet] 백테스트 비교 오류: {e}")
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 📊 [등급별 분석 시트 - 백테스트 기반]
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # ✅ df_backtest와 df_realistic를 사용하여 등급별 시트 생성
        if df_backtest is not None and not df_backtest.empty:
            try:
                time.sleep(5)  # ✅ 5초 대기

                try:
                    grade_sheet = doc.worksheet("등급별_분석")
                except:
                    grade_sheet = doc.add_worksheet(
                        title="등급별_분석", 
                        rows="50", 
                        cols="12", 
                        index=1
                    )
                
                grade_sheet.clear()
                
                # 헤더
                header = [
                    ["📊 등급별 백테스트 분석", "", "", ""],
                    ["", "", "", ""],
                    ["🔬 백테스트 (이상적)", "", "", ""],
                    ["", "", "", ""]
                ]
                grade_sheet.update('A1', header, value_input_option='USER_ENTERED')
                
                # 백테스트 데이터
                set_with_dataframe(grade_sheet, df_backtest, row=5, col=1, include_index=False)
                
                # 실전 섹션
                real_start = 5 + len(df_backtest) + 3
                real_header = [
                    ["", "", "", ""],
                    ["💡 실전 예상 (현실적)", "", "", ""],
                    ["", "", "", ""]
                ]
                grade_sheet.update(f'A{real_start}', real_header, value_input_option='USER_ENTERED')
                
                # 실전 데이터
                if df_realistic is not None and not df_realistic.empty:
                    set_with_dataframe(grade_sheet, df_realistic, row=real_start+3, col=1, include_index=False)
                
                # S급 강조
                grade_sheet.format('A5:J5', {
                    'backgroundColor': {'red': 1.0, 'green': 0.95, 'blue': 0.7},
                    'textFormat': {'bold': True}
                })
                
                print("✅ [Ex-Sheet] 등급별 분석 시트 생성 완료")
            except Exception as e:
                print(f"⚠️ [Ex-Sheet] 등급별 분석 오류: {e}")        

    except Exception as e:
        print(f"🚨 [Critical] 구글 시트 전송 실패: {e}")

def update_commander_dashboard_back(df, macro_data, sheet_name, stats_df=None, 
                               today_recommendations=None, ai_recommendation=None):
    """
    [최신 인증 반영] google-auth를 사용하여 보안성이 강화된 통합 관제 모듈
    """
    print(f"📡 [Ex-Sheet] Ver 45.0 최신 보안 인증 모드 가동")
    json_key_path = 'stock-key.json' 
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today_str = kst_now.strftime('%Y-%m-%d')
    
    try:
        # 1. 💡 신규 인증 로직 (google-auth)
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = None
        # (1) 로컬 파일 검사
        if os.path.exists(json_key_path):
            creds = Credentials.from_service_account_file(json_key_path, scopes=scope)
            print("🔑 [Auth] 로컬 JSON 키파일 인증 성공")
        # (2) 환경 변수 검사 (GitHub Secrets 전용)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
            creds = Credentials.from_service_account_info(key_dict, scopes=scope)
            print("🔑 [Auth] 환경 변수(GOOGLE_JSON_KEY) 인증 성공")
        else:
            print("❌ [Auth] 인증 키를 찾을 수 없습니다. (파일 혹은 환경변수 확인 요망)")
            return

        client = gspread.authorize(creds)
        doc = client.open(sheet_name)
        
        # --- [탭 1: 오늘의_추천종목 (👑장군 등급 포함)] ---
        if today_recommendations is not None and not today_recommendations.empty:
            try:
                try: today_sheet = doc.worksheet("오늘의_추천종목")
                except: today_sheet = doc.add_worksheet(title="오늘의_추천종목", rows="200", cols="20")
                today_sheet.clear()
                
                # 오늘 날짜 필터링
                today_recommendations['날짜_clean'] = pd.to_datetime(today_recommendations['날짜']).dt.strftime('%Y-%m-%d')
                today_only_df = today_recommendations[today_recommendations['날짜_clean'] == today_str].copy()

                if not today_only_df.empty and ai_recommendation is not None:
                    # DNA 데이터와 병합
                    final_today = pd.merge(
                        today_only_df, 
                        ai_recommendation[['종목', 'DNA_일치도', 'DNA_시퀀스', '최고수익률']], 
                        on='종목', how='left'
                    ).fillna({'DNA_일치도': '0%', '최고수익률': 0})
                    
                    # 관상 점수 계산 및 훈장 수여
                    final_today['match_val'] = final_today['DNA_일치도'].str.replace('%','').astype(int)
                    final_today['관상_등급'] = final_today['match_val'].apply(
                        lambda x: "👑장군" if x >= 90 else ("⚔️정예" if x >= 80 else "🛡️일반")
                    )
                    
                    # 정렬 (수익률 -> 관상점수) 및 상위 50개
                    final_today = final_today.sort_values(by=['최고수익률%'], ascending=False).head(100)
                    
                    header_info = [[f"🎯 금일 정예 관상 종목 (기준일: {today_str})"]]
                    today_sheet.update('A1', header_info)
                    
                    cols = ['관상_등급', '종목', 'DNA_일치도', '최고수익률', '현재가', '안전점수', '구분']
                    set_with_dataframe(today_sheet, final_today[cols], row=4, col=1, include_index=False)
                    
                    # 🎨 장군 등급 황금색 하이라이트
                    num_rows = len(final_today) + 4
                    today_sheet.format(f"A4:G{num_rows}", {
                        "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.88},
                        "textFormat": {"fontSize": 10, "bold": True}
                    })
                    print(f"✅ [Success] 오늘의_추천종목 업데이트 완료")
                else:
                    today_sheet.update('A1', [[f"⚠️ {today_str} 오늘 신호가 없습니다."]])
            except Exception as e: print(f"❌ [Error] 탭 1 실패: {e}")

        # --- [탭 2: AI_추천패턴 (상위 15선)] ---
        if ai_recommendation is not None and not ai_recommendation.empty:
            try:
                try: ai_sheet = doc.worksheet("AI_추천패턴")
                except: ai_sheet = doc.add_worksheet(title="AI_추천패턴", rows="200", cols="15")
                ai_sheet.clear()

            # 체급별 패턴 랭킹 추출 (위에서 만든 함수 호출)
                tier_patterns = find_winning_pattern(ai_recommendation)
                print(f"📦 [Step 1] 수신된 ai_recommendation 건수: {len(ai_recommendation)}건")
                print(f"📊 [Step 1-1] 데이터 컬럼: {ai_recommendation.columns.tolist()}")
                print(f"📊 [Step 1-2] 데이터 샘플 (상위 2건):\n{ai_recommendation.head(2)}")
              
                if not tier_patterns:
                    print("❌ [Step 2] tier_patterns 결과가 사전(dict) 형태가 아니거나 비어있습니다.")
              
                curr_row = 1
                valid_tier_count = 0
                for tier, patterns in tier_patterns.items():
                    print(f"🔍 [Step 3] 체급별 수색: {tier} -> {len(patterns)}건 발견")
                    if not patterns.empty:
                        # 체급 헤더 작성
                        ai_sheet.update(f'A{curr_row}', [[f"🏆 {tier} 체급별 타율 상위 패턴"]])
                        # 데이터 전송 (헤더 포함)
                        set_with_dataframe(ai_sheet, patterns, row=curr_row+1, col=1, include_index=False)
                        curr_row += (len(patterns) + 4) # 다음 체급을 위해 줄 띄움
                        valid_tier_count += 1
                    else:
                        print(f"⚠️ [Step 3-1] {tier} 체급은 조건(수익률 등)을 만족하는 패턴이 0건입니다.")
                print(f"✅ [Step 4] 총 {valid_tier_count}개의 체급 섹션이 시트에 기록되었습니다.")
                print("✅ [Success] 체급별 AI 족보 전송 완료")
            except Exception as e:
                print(f"❌ [Error] 탭 2 체급별 분리 실패: {e}")
        else:
            print("❌ [Critical] ai_recommendation 인자가 None으로 넘어왔습니다. (데이터 전송 누락)")

        # --- [탭 3: 실시간_전수_관제판] ---
        try:
            try: main_sheet = doc.worksheet("실시간_전수_관제판")
            except: main_sheet = doc.get_worksheet(0); main_sheet.update_title("실시간_전수_관제판")
            main_sheet.clear()
            m = macro_data
            macro_list = [
                ["💎 사령부 실시간 다이아몬드 관제 시스템", "", ""],
                [f"📅 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", ""],
                [f"📈 나스닥: {m.get('nasdaq',{}).get('text','N/A')}", "", ""],
                [f"💵 달러환율: {m.get('fx',{}).get('text','N/A')}", "", ""],
            ]
            main_sheet.update('A1', macro_list, value_input_option='USER_ENTERED')
            set_with_dataframe(main_sheet, df, row=9, col=1, include_index=False)
        except Exception as e: print(f"❌ [Error] 탭 3 실패: {e}")

        # --- [탭 4: 전술통계_리포트] ---
        if stats_df is not None and not stats_df.empty:
            try:
                try: stats_sheet = doc.worksheet("전술통계_리포트")
                except: stats_sheet = doc.add_worksheet(title="전술통계_리포트", rows="100", cols="10")
                stats_sheet.clear()
                set_with_dataframe(stats_sheet, stats_df, include_index=False)
            except: pass

    except Exception as e:
        print(f"❌ [Critical] {traceback.format_exc()}")
