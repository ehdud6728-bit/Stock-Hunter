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

def update_commander_dashboard(df, macro_data, sheet_name, stats_df=None, 
                               today_recommendations=None, ai_recommendation=None):
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
        
        # 💡 [신규 탭 1: 오늘의_추천종목] - 맨 앞에 배치
        if today_recommendations is not None and not today_recommendations.empty:
            try:
                try:
                    today_sheet = doc.worksheet("오늘의_추천종목")
                except:
                    # 탭이 없으면 생성하고 맨 앞으로 이동
                    today_sheet = doc.add_worksheet(title="오늘의_추천종목", rows="200", cols="20", index=0)
                
                today_sheet.clear()
                
                # 헤더 정보
                header_info = [
                    [f"🎯 오늘의 AI 추천종목 ({datetime.now().strftime('%Y-%m-%d %H:%M')})", "", "", "", ""],
                    ["안전점수 기준 상위 종목 (과거 30일 패턴 검증 완료)", "", "", "", ""],
                    ["", "", "", "", ""]  # 빈 줄
                ]
                today_sheet.update('A1', header_info, value_input_option='USER_ENTERED')
                
                # 데이터 전송 (A4부터)
                set_with_dataframe(today_sheet, today_recommendations, row=4, col=1, include_index=False)
                
                # 🎨 서식: 안전점수 높은 종목 강조
                try:
                    num_rows = len(today_recommendations) + 5
                    data_range = f"A5:Z{num_rows}"
                    rules = get_conditional_format_rules(today_sheet)
                    rules.clear()
                    
                    # 다이아몬드 패턴 강조
                    rules.append(ConditionalFormatRule(
                        ranges=[GridRange.from_a1_range(data_range, today_sheet)],
                        booleanRule=BooleanRule(
                            condition=BooleanCondition('TEXT_CONTAINS', ['💎다이아몬드']),
                            format=CellFormat(
                                backgroundColor=Color(1.0, 0.95, 0.8),
                                textFormat=textFormat(bold=True, foregroundColor=Color(0.8, 0.4, 0.0))
                            )
                        )
                    ))
                    
                    # 역매공파완전체 강조
                    rules.append(ConditionalFormatRule(
                        ranges=[GridRange.from_a1_range(data_range, today_sheet)],
                        booleanRule=BooleanRule(
                            condition=BooleanCondition('TEXT_CONTAINS', ['🎯역매공파완전체']),
                            format=CellFormat(
                                backgroundColor=Color(0.9, 1.0, 0.9),
                                textFormat=textFormat(bold=True, foregroundColor=Color(0.0, 0.6, 0.0))
                            )
                        )
                    ))
                    
                    rules.save()
                except: pass
                
                print("✅ [Ex-Sheet] 오늘의 추천종목 탭 생성 완료")
            except Exception as e:
                print(f"⚠️ [Ex-Sheet] 오늘의 추천종목 탭 오류: {e}")
                traceback.print_exc()
        
      if ai_recommendation is not None and not ai_recommendation.empty:
          try:
              try:
                  ai_sheet = doc.worksheet("AI_추천패턴")
              except:
                  ai_sheet = doc.add_worksheet(title="AI_추천패턴", rows="100", cols="10", index=1)
        
           ai_sheet.clear()
        
           # 헤더
           ai_header = [
            ["🏆 AI 분석 기반 TOP 5 패턴 추천", "", "", "", "", "", ""],
            [f"분석 기준: 과거 30일 데이터 | 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}", "", "", "", "", "", ""],
            ["기대값 = (승률 × 평균수익) | 높을수록 수익 가능성 높음", "", "", "", "", "", ""],
            ["", "", "", "", "", "", ""]
        ]
           ai_sheet.update('A1', ai_header, value_input_option='USER_ENTERED')
        
           # 추천 패턴 데이터
           set_with_dataframe(ai_sheet, ai_recommendation, row=5, col=1, include_index=False)
        
        # 🎨 서식
           try:
               # 헤더 서식
              ai_sheet.format('A5:H5', {
                'backgroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.8},
                'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}}
            })
            
               # 1위 강조 (6번째 행 = 데이터 첫 줄)
              ai_sheet.format('A6:H6', {
                'backgroundColor': {'red': 1.0, 'green': 0.95, 'blue': 0.7},
                'textFormat': {'bold': True}
            })
            
              # 2위 강조
              if len(ai_recommendation) >= 2:
                ai_sheet.format('A7:H7', {
                    'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.95},
                    'textFormat': {'bold': True}
                })
        except: pass
        
        print("✅ [Ex-Sheet] AI 추천패턴 탭 생성 완료 (TOP 5)")
    except Exception as e:
        print(f"⚠️ [Ex-Sheet] AI 추천패턴 탭 오류: {e}")
        traceback.print_exc()        
        # --- [기존 탭 1: 실시간 전수 관제판] ---
        sheet = doc.get_worksheet(0)
        sheet.clear() 

        # 매크로 현황판 (A1~A8)
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
            if '안전점수' in display_df.columns:
                display_df['종목'] = display_df.apply(
                    lambda x: f"★ {x['종목']}" if int(x['안전점수']) >= 130 else x['종목'], 
                    axis=1
                )
            elif '안전' in display_df.columns:  # 기존 호환성
                display_df['종목'] = display_df.apply(
                    lambda x: f"★ {x['종목']}" if int(x['안전']) >= 130 else x['종목'], 
                    axis=1
                )
            
            set_with_dataframe(sheet, display_df, row=9, col=1, include_index=False)
            print("✅ [Ex-Sheet] 메인 리스트 전송 성공")

        # --- [기존 탭 2: 전술통계_리포트] ---
        if stats_df is not None and not stats_df.empty:
            try:
                try:
                    stats_sheet = doc.worksheet("전술통계_리포트")
                except:
                    stats_sheet = doc.add_worksheet(title="전술통계_리포트", rows="100", cols="10")
                
                stats_sheet.clear()
                set_with_dataframe(stats_sheet, stats_df, include_index=False)
                print("✅ [Ex-Sheet] 전술 통계 탭 업데이트 성공")
            except Exception as e:
                print(f"⚠️ [Ex-Sheet] 통계 탭 내부 오류: {e}")
                traceback.print_exc()

        # 🎨 서식 규칙 적용 (메인 시트)
        try:
            num_rows = len(display_df) + 10
            data_range = f"A10:Z{num_rows}"
            rules = get_conditional_format_rules(sheet)
            rules.clear()
            rules.append(ConditionalFormatRule(
                ranges=[GridRange.from_a1_range(data_range, sheet)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition('TEXT_CONTAINS', ['💎']),
                    format=CellFormat(
                        backgroundColor=Color(0.9, 0.9, 1.0), 
                        textFormat=textFormat(bold=True, foregroundColor=Color(0.2, 0.2, 0.8))
                    )
                )
            ))
            rules.save()
        except: pass

    except Exception as e:
        print(f"❌ [Ex-Sheet] 치명적 오류:\n{traceback.format_exc()}")