import yfinance as yf
import pandas as pd
import numpy as np
# 👇 구글 시트 매니저 연결 (파일명 확인 필수)
try:
    from google_sheet_managerEx import update_commander_dashboard
except ImportError:
    def update_commander_dashboard(*args, **kwargs): print("⚠️ 구글 시트 모듈 연결 실패")
        
# 1. 나스닥 정예 부대 명단 (Nasdaq 100 등)
nasdaq_tickers = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'GOOGL', 'AMZN', 'META', 'AVGO', 'COST'] # 예시

def analyze_save_googleSheet(all_hits, isNasdaq):
    if all_hits:
        df_total = pd.DataFrame(all_hits)
    
        # 백테스트 분석
        df_backtest, df_realistic, s_grade_info = proper_backtest_analysis(all_hits)
    
        # 조합별 성과 분석
        df_combo, best_combos, worst_combos = analyze_combination_performance(all_hits)
    
        # 수익률 분포
        df_profit_dist = analyze_profit_distribution(all_hits)
    
        # 조합별 통계
        stats_df, top_5 = calculate_strategy_stats(all_hits)
    
        # 통계 계산 (상위 5개 추천 정보 포함)
        stats_df, top_recommendations = calculate_strategy_stats(all_hits)
    
        # 4. 결과 분류
        today = df_total[df_total['보유일'] == 0]
        today = today[today['N점수'] >= 0]
        today = today.sort_values(by='N점수', ascending=False)
    
        today = df_total[df_total['보유일'] == 0].sort_values(by='확신점수', ascending=False)
    
        s_grade_today = today[today['N등급'] == 'S']
    
        desired_cols = ['날짜',
                '👑등급',
                '종목',
                'N등급',
                'N점수',
                'N조합',
                '정류장',
                'RSI',
                '대칭비율',
                '매집봉',
                '🎯목표타점',
                '🚨손절가',
                '매입가',
                '현재가',
                '최고수익날',
                '소요기간',
                '최고수익률%',
                '최저수익률%',
                '기상',
                '매집',
                '이격',
                '꼬리%',
                'BB40',
                'MA수렴',
                '📜서사히스토리',
                'N구분',
                '구분',
                '확신점수',
                '안전점수',
                '섹터',
                '보유일']
        display_cols = [c for c in desired_cols if c in today.columns]
    
        if not today.empty:
            print(today[display_cols].head(50))
        # 5. 구글 시트 전송
        try:
            update_commander_dashboard(
                df_total,
                macro_status,
                "사령부_통합_상황판",
                stats_df=stats_df,
                today_recommendations=today,
                ai_recommendation=pd.DataFrame(top_5) if top_5 else None,
                s_grade_special=s_grade_today if not s_grade_today.empty else None,
                df_backtest=df_backtest,
                df_realistic=df_realistic,
                df_combo=df_combo,
                best_combos=best_combos,
                worst_combos=worst_combos,
                df_profit_dist=df_profit_dist,
                isNasdaq=isNasdaq
            )
        
            print("\n" + "="*100)
            print("✅ 구글 시트 업데이트 성공!")
            print("="*100)
            print("📋 생성된 시트:")
            print("   1. 메인 시트: 전체 30일 데이터")
            print("   2. 오늘의_추천종목: 오늘 신호 (등급별)")
            print("   3. S급_긴급: S급 종목 특별 모니터링")
            print("   4. 등급별_분석: S/A/B급 백테스트")
            print("   5. AI_추천패턴: TOP 5 조합")
            print("   ✅ 6. 조합별_성과: 전체 조합 성과 (신규!)")
            print("   ✅ 7. TOP_WORST_조합: 최고/최악 조합 (신규!)")
            print("   ✅ 8. 수익률_분포: 구간별 분포 (신규!)")
            print("   ✅ 9. 백테스트_비교: 이상 vs 현실 (신규!)")
            print("="*100)
        except Exception as e:
            print(f"\n❌ 시트 업데이트 실패: {e}")
    else:
        print("\n⚠️ 검색 결과가 없습니다.")
    return False, 0
