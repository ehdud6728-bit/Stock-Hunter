# ------------------------------------------------------------------
# 💎 [Ultimate Masterpiece] 전천후 AI 전략 사령부 (Ver 36.7 엑셀저장+추천시스템)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import os, re, time, pytz
from pykrx import stock
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import warnings
import requests
from bs4 import BeautifulSoup
from DNA_Analyzer import analyze_dna_sequences, find_winning_pattern
from tactics_engine import get_global_and_leader_status, analyze_all_narratives, get_dynamic_sector_leaders, calculate_dante_symmetry, watermelon_indicator_complete, judge_yeok_break_sequence_v2
import traceback
from triangle_combo_analyzer import jongbe_triangle_combo_v3
from pykrx import stock
import pandas as pd
from datetime import datetime

# 👇 구글 시트 매니저 연결 (파일명 확인 필수)
try:
    from google_sheet_managerEx import update_commander_dashboard
except ImportError:
    def update_commander_dashboard(*args, **kwargs): print("⚠️ 구글 시트 모듈 연결 실패")

warnings.filterwarnings('ignore')

# =================================================
# ⚙️ [1. 설정 및 글로벌 변수]
# =================================================
DNA_CHECK = False
SCAN_DAYS = 20       # 최근 30일 내 타점 전수 조사
TOP_N = 600         # 거래대금 상위 종목 수 (필요시 2500으로 확장 가능)
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')
START_DATE = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
END_DATE_STR = datetime.now().strftime('%Y%m%d')

# 사령관님의 21개 라운드넘버 리스트
RN_LIST = [500, 1000, 1500, 2000, 3000, 5000, 7500, 10000, 15000, 20000, 
           30000, 50000, 75000, 100000, 150000, 200000, 300000, 500000, 
           750000, 1000000, 1500000]

print(f"📡 [Ver 38 ] 사령부 무결성 통합 가동... 💎다이아몬드 & 📊복합통계 엔진 탑재")

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
                'D20매집봉',
                '저항터치',
                'BB-GC',
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
                '삼각패턴',
                '삼각수렴%',
                '꼭지잔여',
                '종베GC',
                '삼각점수',
                '삼각등급',
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

def get_target_levels(current_price):
    """현재가 기준 위/아래 정거장을 찾아주는 함수"""
    # 현재가보다 큰 RN들 중 가장 작은 것이 '위 정거장'
    upper_rns = [rn for rn in RN_LIST if rn > current_price]
    # 현재가보다 작은 RN들 중 가장 큰 것이 '아래 정거장'
    lower_rns = [rn for rn in RN_LIST if rn <= current_price]
    
    upper = upper_rns[0] if upper_rns else None
    lower = lower_rns[-1] if lower_rns else None
    return lower, upper

def classify_market_period(date_str):
    """날짜로 시장 구간 분류"""
    date = pd.to_datetime(date_str)
    
    for period_name, period_info in MARKET_PERIODS.items():
        start = pd.to_datetime(period_info['start'])
        end = pd.to_datetime(period_info['end'])
        
        if start <= date <= end:
            return period_name
    
    return 'unknown'


def get_market_trend(period_name):
    """시장 구간의 추세 반환"""
    if period_name in MARKET_PERIODS:
        return MARKET_PERIODS[period_name]['trend']
    return 'unknown'


# =================================================

# =================================================
# 📡 [전술 1] 나스닥 100 티커 자동 수집 (403 에러 우회)
# =================================================
def get_nasdaq100_tickers():
    try:
        url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        # 위장막(Header) 장착: 브라우저인 척 위장합니다.
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, headers=headers)
        
        tables = pd.read_html(response.text)
        # 보통 4번째 또는 5번째 테이블이 구성 종목입니다.
        df_nasdaq100 = tables[4] if len(tables) > 4 else tables[3]
        
        ticker_column = 'Ticker' if 'Ticker' in df_nasdaq100.columns else 'Symbol'
        nasdaq_tickers = df_nasdaq100[ticker_column].tolist()
        return [ticker.replace('.', '-') for ticker in nasdaq_tickers]
    except Exception as e:
        print(f"🚨 위키피디아 정찰 실패(403 우회불가): {e}")
        return ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'GOOGL', 'AMZN', 'META', 'AVGO', 'COST']

# 📊 조합별 성과 분석 (상세 버전)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def analyze_combination_performance(all_hits):
    """
    조합 패턴별 수익률 분석
    
    Returns:
        - df_combination: 조합별 통계 DataFrame
        - best_combinations: TOP 10 조합
        - worst_combinations: WORST 5 조합
    """
    
    # 과거 데이터만 (보유일 > 0)
    past_hits = [h for h in all_hits if h['보유일'] > 0]
    
    if not past_hits:
        return pd.DataFrame(), [], []
    
    # 상폐주 제거
    past_hits = [h for h in past_hits if h['최저수익률_raw'] > -50]
    
    # 조합별 그룹화
    combination_stats = {}
    
    for hit in past_hits:
        combo = hit['N조합']
        
        if combo not in combination_stats:
            combination_stats[combo] = {
                'hits': [],
                'gains': [],
                'losses': []
            }
        
        combination_stats[combo]['hits'].append(hit)
        combination_stats[combo]['gains'].append(hit['최고수익률_raw'])
        combination_stats[combo]['losses'].append(hit['최저수익률_raw'])
    
    # 통계 계산
    results = []
    
    for combo, data in combination_stats.items():
        total = len(data['hits'])
        
        # 건수가 너무 적으면 신뢰도 낮음
        if total < 3:
            continue
        
        # 승률 (3.5% 이상)
        winners = len([g for g in data['gains'] if g >= 3.5])
        win_rate = (winners / total) * 100
        
        # 평균 수익/손실
        avg_gain = sum(data['gains']) / total
        avg_loss = sum(data['losses']) / total
        
        # 최대/최소
        max_gain = max(data['gains'])
        max_loss = min(data['losses'])
        
        # 중앙값 (평균보다 안정적)
        median_gain = sorted(data['gains'])[total // 2]
        
        # 기대값
        expected = (win_rate / 100) * avg_gain
        
        # 샤프비율
        sharpe = avg_gain / abs(avg_loss) if avg_loss != 0 else 0
        
        # 손익비
        profit_loss_ratio = abs(avg_gain / avg_loss) if avg_loss != 0 else 0
        
        # 안정성 점수 (승률 + 샤프비율)
        stability_score = (win_rate * 0.5) + (sharpe * 10)
        
        results.append({
            '조합': combo,
            '건수': total,
            '승률(%)': round(win_rate, 1),
            '승리건수': f"{winners}/{total}",
            '평균수익(%)': round(avg_gain, 1),
            '중앙수익(%)': round(median_gain, 1),
            '평균손실(%)': round(avg_loss, 1),
            '최대수익(%)': round(max_gain, 1),
            '최대손실(%)': round(max_loss, 1),
            '기대값': round(expected, 2),
            '샤프비율': round(sharpe, 2),
            '손익비': round(profit_loss_ratio, 2),
            '안정성': round(stability_score, 1),
            
            # 등급 자동 부여
            '등급': assign_combination_grade(win_rate, expected, sharpe, total)
        })
    
    # DataFrame 생성
    df_combo = pd.DataFrame(results)
    
    if df_combo.empty:
        return df_combo, [], []
    
    # 정렬 (기대값 기준)
    df_combo = df_combo.sort_values(by='기대값', ascending=False)
    
    # TOP 10 / WORST 5
    best_combinations = df_combo.head(10).to_dict('records')
    worst_combinations = df_combo.tail(5).to_dict('records')
    
    return df_combo, best_combinations, worst_combinations


def assign_combination_grade(win_rate, expected, sharpe, count):
    """
    조합 등급 자동 부여
    """
    
    # 신뢰도 체크 (건수가 적으면 감점)
    reliability = min(count / 10, 1.0)  # 10건 이상이면 100%
    
    # 점수 계산
    score = (
        (win_rate * 0.4) +       # 승률 40%
        (expected * 0.4) +       # 기대값 40%
        (sharpe * 5) +           # 샤프비율 20%
        0
    ) * reliability
    
    if score >= 80:
        return 'S급 ⭐⭐⭐'
    elif score >= 60:
        return 'A급 ⭐⭐'
    elif score >= 40:
        return 'B급 ⭐'
    else:
        return 'C급'

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🔍 특정 조합 상세 분석
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def analyze_specific_combination(all_hits, combination_name):
    """
    특정 조합의 모든 케이스 상세 분석
    """
    
    # 해당 조합 필터
    combo_hits = [h for h in all_hits 
                  if h['조합'] == combination_name 
                  and h['보유일'] > 0
                  and h['최저수익률_raw'] > -50]
    
    if not combo_hits:
        print(f"⚠️ {combination_name} 데이터 없음")
        return None
    
    # DataFrame으로 변환
    df_detail = pd.DataFrame(combo_hits)
    
    # 수익률 기준 정렬
    df_detail = df_detail.sort_values(by='최고수익률_raw', ascending=False)
    
    # 통계 요약
    print(f"\n{'='*100}")
    print(f"🔍 [ {combination_name} 상세 분석 ]")
    print(f"{'='*100}")
    print(f"총 건수: {len(combo_hits)}건")
    print(f"승률: {len([h for h in combo_hits if h['최고수익률_raw'] >= 3.5]) / len(combo_hits) * 100:.1f}%")
    print(f"평균 수익: {sum([h['최고수익률_raw'] for h in combo_hits]) / len(combo_hits):.1f}%")
    print(f"평균 손실: {sum([h['최저수익률_raw'] for h in combo_hits]) / len(combo_hits):.1f}%")
    print(f"\n{'='*100}")
    print("개별 케이스:")
    print(f"{'='*100}")
    
    # 주요 컬럼만 출력
    display_cols = ['날짜', '종목', '매수가', '실전예상_최고(%)', 
                   '실전예상_최저(%)', '보유일', '구분']
    
    print(df_detail[display_cols].head(20))
    
    return df_detail


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 📈 수익률 구간별 분석
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def analyze_profit_distribution(all_hits):
    """
    수익률 구간별 분포 분석
    """
    
    past_hits = [h for h in all_hits 
                if h['보유일'] > 0 
                and h['최저수익률_raw'] > -50]
    
    if not past_hits:
        return pd.DataFrame()
    
    # 수익률 구간 정의
    ranges = [
        ('🔴 손실 (-50% ~ 0%)', -50, 0),
        ('⚪ 미미 (0% ~ 5%)', 0, 5),
        ('🟡 소폭 (5% ~ 10%)', 5, 10),
        ('🟢 보통 (10% ~ 20%)', 10, 20),
        ('🔵 양호 (20% ~ 30%)', 20, 30),
        ('🟣 우수 (30% ~ 50%)', 30, 50),
        ('⭐ 대박 (50% ~ 100%)', 50, 100),
        ('💎 초대박 (100%+)', 100, 10000)
    ]
    
    # 구간별 분류
    distribution = []
    
    for label, min_val, max_val in ranges:
        count = len([h for h in past_hits 
                    if min_val <= h['최고수익률_raw'] < max_val])
        
        ratio = (count / len(past_hits)) * 100
        
        # 해당 구간의 조합 분석
        range_hits = [h for h in past_hits 
                     if min_val <= h['최고수익률_raw'] < max_val]
        
        if range_hits:
            combo_counts = {}
            for h in range_hits:
                combo = h['N조합']
                combo_counts[combo] = combo_counts.get(combo, 0) + 1
            top_combo = max(combo_counts, key=combo_counts.get)
        else:
            top_combo = '-'
        
        distribution.append({
            '구간': label,
            '건수': count,
            '비율(%)': round(ratio, 1),
            '대표조합': top_combo
        })
    
    df_dist = pd.DataFrame(distribution)
    
    print(f"\n{'='*100}")
    print("📊 [ 수익률 구간별 분포 ]")
    print(f"{'='*100}")
    print(df_dist)
    
    # ✅ DataFrame 반환 추가
    return df_dist

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 📊 등급별 백테스트 분석 (실전 포함)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def proper_backtest_analysis(all_hits):
    """
    점수 구간별 성과 비교 (백테스트 vs 실전)
    """
    
    past_hits = [h for h in all_hits if h['보유일'] > 0]
    
    if not past_hits:
        return pd.DataFrame(), pd.DataFrame(), None
    
    # 상폐주 제거 (손실 -50% 이하)
    past_hits = [h for h in past_hits if h['최저수익률_raw'] > -50]
    
    # 점수 구간별 분류
    groups = {
        'S급 (300+)': [h for h in past_hits if h['N점수'] >= 300],
        'A급 (250-299)': [h for h in past_hits if 250 <= h['N점수'] < 300],
        'B급 (200-249)': [h for h in past_hits if 200 <= h['N점수'] < 250],
    }
    
    backtest_results = []
    realistic_results = []
    
    for grade, hits in groups.items():
        if not hits:
            continue
        
        total = len(hits)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 백테스트 통계 (이상적)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        winners_bt = len([h for h in hits if h['최고수익률_raw'] >= 3.5])
        avg_max_bt = sum([h['최고수익률_raw'] for h in hits]) / total
        avg_min_bt = sum([h['최저수익률_raw'] for h in hits]) / total
        max_gain_bt = max([h['최고수익률_raw'] for h in hits])
        max_loss_bt = min([h['최저수익률_raw'] for h in hits])
        
        win_rate_bt = (winners_bt / total) * 100
        expected_bt = (win_rate_bt / 100) * avg_max_bt
        sharpe_bt = avg_max_bt / abs(avg_min_bt) if avg_min_bt != 0 else 0
        
        backtest_results.append({
            '등급': grade,
            '건수': total,
            '승률(%)': round(win_rate_bt, 1),
            '승리건수': f"{winners_bt}/{total}",
            '평균수익(%)': round(avg_max_bt, 1),
            '평균손실(%)': round(avg_min_bt, 1),
            '최대수익(%)': round(max_gain_bt, 1),
            '최대손실(%)': round(max_loss_bt, 1),
            '기대값': round(expected_bt, 2),
            '샤프비율': round(sharpe_bt, 2)
        })
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 실전 통계 (현실적)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        winners_real = len([h for h in hits if h['최고수익률_raw'] >= 3.5])
        avg_max_real = sum([h['최고수익률_raw'] for h in hits]) / total
        avg_min_real = sum([h['최저수익률_raw'] for h in hits]) / total
        max_gain_real = max([h['최고수익률_raw'] for h in hits])
        max_loss_real = min([h['최저수익률_raw'] for h in hits])
        
        win_rate_real = (winners_real / total) * 100
        expected_real = (win_rate_real / 100) * avg_max_real
        sharpe_real = avg_max_real / abs(avg_min_real) if avg_min_real != 0 else 0
        
        realistic_results.append({
            '등급': grade,
            '건수': total,
            '승률(%)': round(win_rate_real, 1),
            '승리건수': f"{winners_real}/{total}",
            '평균수익(%)': round(avg_max_real, 1),
            '평균손실(%)': round(avg_min_real, 1),
            '최대수익(%)': round(max_gain_real, 1),
            '최대손실(%)': round(max_loss_real, 1),
            '기대값': round(expected_real, 2),
            '샤프비율': round(sharpe_real, 2)
        })
    
    df_backtest = pd.DataFrame(backtest_results)
    df_realistic = pd.DataFrame(realistic_results)
    
    # S급 정보 (실전 기준)
    s_grade_info = None
    if not df_realistic.empty:
        s_grade = df_realistic[df_realistic['등급'].str.contains('S급')]
        if not s_grade.empty:
            s_grade_info = s_grade.iloc[0].to_dict()
    
    return df_backtest, df_realistic, s_grade_info

def get_stock_sector(ticker, sector_map):
    """
    기존에 수집된 섹터 마스터 맵에서 종목의 업종을 판독합니다.
    """
    # 1. 마스터 맵에서 해당 종목의 업종명 추출
    raw_sector = sector_map.get(ticker, "일반")
    
    # 2. 키워드 매칭을 통한 섹터 정규화 (대장주 동기화용)
    if any(k in raw_sector for k in ['반도체', 'IT부품', '장비']): 
        return "반도체"
    if any(k in raw_sector for k in ['제약', '바이오', '의료기기', '생물']): 
        return "바이오"
    if any(k in raw_sector for k in ['전기차', '배터리', '에너지', '축전지']): 
        return "2차전지"
    
    return "일반"

def get_commander_market_cap():
    """
    이름과 코드, 어떤 것으로도 체급을 즉시 판독할 수 있는 마스터 맵을 생성합니다.
    """
    print("📡 [Cap-Scanner] 전 종목 마스터 데이터 수집 중...")
    try:
        now = datetime.now().strftime("%Y%m%d")
        # 1. 시가총액 데이터 (인덱스가 종목코드)
        df_cap = stock.get_market_cap(now, market="ALL")
        
        # 2. 종목명 데이터 (종목코드, 종목명 매핑)
        df_desc = stock.get_market_net_purchases_of_equities_by_ticker(now, now, "ALL") # 이름 가져오기용 팁
        # 더 확실한 이름-코드 매핑
        tickers = stock.get_market_ticker_list(now, market="ALL")
        names = [stock.get_market_ticker_name(t) for t in tickers]
        df_name = pd.DataFrame({'Code': tickers, 'Name': names}).set_index('Code')

        # 3. 데이터 병합
        master_df = df_cap.join(df_name)
        
        # 💡 [핵심] 두 가지 타입의 딕셔너리 생성
        code_to_cap = master_df['시가총액'].to_dict()
        name_to_cap = master_df.set_index('Name')['시가총액'].to_dict()

        print(f"✅ [Cap-Scanner] 마스터 데이터 {len(code_to_cap)}건 로드 완료.")
        return {"code": code_to_cap, "name": name_to_cap}
    except Exception as e:
        print(f"❌ [Cap-Scanner] 수집 실패: {e}")
        return {"code": {}, "name": {}}

def assign_tier(name, code, master_map):
    """
    코드 우선, 이름 차선으로 체급을 결정합니다.
    """
    # 1. 코드로 조회 시도
    cap = master_map['code'].get(code, 0)
    
    # 2. 코드로 실패 시 이름으로 조회 시도
    if cap == 0:
        cap = master_map['name'].get(name, 0)
    
    # 3. 체급 결정
    if cap >= 1_000_000_000_000: return "👑HEAVY", cap
    if cap >= 200_000_000_000: return "⚔️MIDDLE", cap
    if cap > 0: return "🚀LIGHT", cap
    
    return "❓미확인", 0

# ---------------------------------------------------------
# 🌍 [매크로 엔진] 글로벌 지수 및 수급 데이터 수집
# ---------------------------------------------------------
def get_safe_macro(symbol, name):
    try:
        df = fdr.DataReader(symbol, start=(datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d'))
        curr, prev = df.iloc[-1]['Close'], df.iloc[-2]['Close']
        ma5 = df['Close'].tail(5).mean()
        chg = ((curr - prev) / prev) * 100
        status = "☀️맑음" if curr > ma5 else "🌪️폭풍우"
        if "VIX" in name: status = "☀️안정" if curr < ma5 else "🌪️위험"
        return {"val": curr, "chg": chg, "status": status, "text": f"{name}: {curr:,.2f}({chg:+.2f}%) {status}"}
    except: return {"status": "☁️불명", "text": f"{name}: 연결실패"}

def get_index_investor_data(market_name):
    try:
        df = stock.get_market_net_purchases_of_equities(END_DATE_STR, END_DATE_STR, market_name)
        if df.empty:
            prev_day = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            df = stock.get_market_net_purchases_of_equities(prev_day, prev_day, market_name)
        total = df.sum()
        return f"개인 {total['개인']:+,.0f} | 외인 {total['외국인']:+,.0f} | 기관 {total['기관합계']:+,.0f}"
    except: return "데이터 수신 중..."

def prepare_historical_weather():
    """역사적 기상도를 작성하여 analyze_final에 보급합니다."""
    start_point = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
    ndx = fdr.DataReader('^IXIC', start=start_point)[['Close']]
    sp5 = fdr.DataReader('^GSPC', start=start_point)[['Close']]
    ndx['ixic_ma5'] = ndx['Close'].rolling(5).mean()
    sp5['sp500_ma5'] = sp5['Close'].rolling(5).mean()
    weather_df = pd.concat([
        ndx.rename(columns={'Close': 'ixic_close'}),
        sp5.rename(columns={'Close': 'sp500_close'})
    ], axis=1).fillna(method='ffill')
    return weather_df

# ---------------------------------------------------------
# 📊 [전술 통계] 복합 전술 통계 엔진 (상위 5개 추천)
# ---------------------------------------------------------
def calculate_strategy_stats(all_hits):
    past_hits = [h for h in all_hits if h['보유일'] > 0]
    if not past_hits: return pd.DataFrame(), None
    
    stats = {}
    for h in past_hits:
        raw_tags = h['구분'].split()
        if not raw_tags: continue
        
        # 개별 태그 및 복합 태그 생성
        combos = []
        for tag in raw_tags:
            combos.append(tag)
        
        # 2개 조합
        if len(raw_tags) >= 2:
            sorted_tags = sorted(raw_tags)
            for i in range(len(sorted_tags)):
                for j in range(i+1, len(sorted_tags)):
                    combos.append(f"{sorted_tags[i]} + {sorted_tags[j]}")
        
        # 전체 조합
        if len(raw_tags) > 1:
            combos.append(" + ".join(sorted(raw_tags)))
        
        for strategy in set(combos):
            if strategy not in stats: 
                stats[strategy] = {'total': 0, 'hits': 0, 'yields': [], 'min_yields': []}
            stats[strategy]['total'] += 1
            if h['최고수익률_raw'] >= 3.5: stats[strategy]['hits'] += 1
            stats[strategy]['yields'].append(h['최고수익률_raw'])
            stats[strategy]['min_yields'].append(h['최저수익률_raw'])

    report_data = []
    for strategy, data in stats.items():
        avg_max_yield = sum(data['yields']) / data['total']
        avg_min_yield = sum(data['min_yields']) / data['total']
        hit_rate = (data['hits'] / data['total']) * 100
        
        # 기대값 계산 (확률 * 수익률)
        expected_value = (hit_rate / 100) * avg_max_yield
        
        report_data.append({
            '전략명': strategy, 
            '포착건수': data['total'], 
            '타율(승률)': round(hit_rate, 1), 
            '평균최고수익': round(avg_max_yield, 1),
            '평균최저수익': round(avg_min_yield, 1),
            '기대값': round(expected_value, 2)
        })
    
    df_stats = pd.DataFrame(report_data).sort_values(
        by=['기대값', '평균최고수익', '타율(승률)'], 
        ascending=False
    )
    
    # 💡 상위 3~5개 패턴 추천
    top_recommendations = []
    if len(df_stats) > 0:
        # 최소 5건 이상 데이터 있는 패턴 우선
        reliable_patterns = df_stats[df_stats['포착건수'] >= 5]
        
        if len(reliable_patterns) >= 3:
            # 신뢰도 높은 패턴 중 상위 5개
            top_5 = reliable_patterns.head(5)
            for idx, row in top_5.iterrows():
                top_recommendations.append({
                    '순위': len(top_recommendations) + 1,
                    '패턴': row['전략명'],
                    '타율': row['타율(승률)'],
                    '평균수익': row['평균최고수익'],
                    '기대값': row['기대값'],
                    '건수': row['포착건수'],
                    '신뢰도': '⭐⭐⭐ 높음'
                })
        else:
            # 데이터 부족시 전체에서 상위 5개
            top_5 = df_stats.head(5)
            for idx, row in top_5.iterrows():
                reliability = '⭐⭐⭐ 높음' if row['포착건수'] >= 5 else '⭐⭐ 보통' if row['포착건수'] >= 3 else '⭐ 주의'
                top_recommendations.append({
                    '순위': len(top_recommendations) + 1,
                    '패턴': row['전략명'],
                    '타율': row['타율(승률)'],
                    '평균수익': row['평균최고수익'],
                    '기대값': row['기대값'],
                    '건수': row['포착건수'],
                    '신뢰도': reliability
                })
    
    return df_stats, top_recommendations

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎯 시퀀스 확인 통합함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def judge_trade_with_sequence(df, signals):
    """
    df: 최근 N봉 (시퀀스용)
    signals: 기존 calculate_combination_score용 신호 dict

    return: score_result dict
    """

    # 1️⃣ 시퀀스 판별
    seq_ok = judge_yeok_break_sequence_v2(df)

    # 2️⃣ signals에 반영
    signals = signals.copy()  # 원본 보호
    signals['yeok_break'] = seq_ok

    # 3️⃣ 조합 점수 계산
    result = calculate_combination_score(signals)

    # 4️⃣ 보조 태그 추가
    if seq_ok:
        result['tags'].append('🧬시퀀스확인')

    result['sequence'] = seq_ok

    return result

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎯 조합 중심 점수 산정 시스템
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def calculate_combination_score(signals):
    """
    신호 조합을 분석해서 확정 점수 부여
    
    Args:
        signals: dict with boolean flags
            {
                'watermelon_signal': True/False,
                'watermelon_red': True/False,
                'watermelon_green_7d': True/False,
                'explosion_ready': True/False,
                'bottom_area': True/False,
                'silent_perfect': True/False,
                'silent_strong': True/False,
                'yeok_break': True/False,
                'volume_surge': True/False,
                'obv_rising': True/False,
                'mfi_strong': True/False,
                'dobanzi': True/False, 
            }
    
    Returns:
        {
            'score': int,
            'grade': str,
            'combination': str,
            'tags': list
        }
    """
    
    score = 100  # 기본 점수 (거래대금 상위 350 진입)
    grade = 'D'
    combination = '기본'
    tags = []
    
    # silent_perfect는 silent_strong을 포함
    effective = signals.copy()
    if effective.get('silent_perfect'):
        effective['silent_strong'] = True

    candidates = []
    
    # 🌌 [GOD급 핵무기] 잃어버린 전설의 패턴 복구!
    # 독사가 수박을 물고 200일선(돌반지)을 같이 뚫어버리는 미친 시너지
    if effective.get('viper_hook') and effective.get('dolbanzi') and effective.get('watermelon_signal'):
        candidates.append({
            'score': 10000, # 측정 불가 (무조건 1순위)
            'grade': 'GOD', 
            'combination': '🌌🍉💍독사품은수박돌반지',
            'tags': ['🚀대시세확정', '💥200일선폭파', '🐍단기개미털기완료', '🍉수급대폭발'],
            'type': '🌌' 
        })

    # 👑 [SSS+급 각성] 수박품은독사에 '킥(Kick)'을 더했다!
    # 기존 조건에 'explosion_ready(폭발 직전/볼밴 돌파 등)'를 킥으로 추가!
    elif (effective.get('viper_hook') and effective.get('watermelon_signal') and effective.get('watermelon_red') and effective.get('obv_bullish') and 
         effective.get('explosion_ready') and effective.get('Real_Viper_Hook')):
        candidates.append({
            'score': 999,  
            'grade': 'SSS+', 
            'combination': '👑🍉🐍수박품은독사(각성)',
            # 사령관님이 주문하신 '킥'이 들어갔습니다!
            'tags': ['🔥최종병기', '🧲OBV매집', '💥볼밴폭발(Kick)', '🍉속살폭발'],
            'type': '👑' 
        })
        
    # 🐍 [SS+급 일반 독사] 킥(폭발)이 없는 일반 수박독사는 점수 하향 (사령관님 지시)
    # 돌반지(500점)보다 수익률이 떨어지므로 480점으로 낮췄습니다.
    elif (effective.get('viper_hook') and effective.get('watermelon_signal') and effective.get('obv_bullish') and 
         effective.get('Real_Viper_Hook')):
        candidates.append({
            'score': 480,  
            'grade': 'SS+', 
            'combination': '🐍🍉일반수박독사',
            'tags': ['🐍독사대가리', '🧲OBV매집', '🍉단기수급'],
            'type': '👑' 
        })
    
    # 🐍 [S+급] 독사출현 단독 판독 로직
    # 하극상 방지를 위해 460점에서 440점으로 점수 소폭 하향 조정
    elif (effective.get('viper_hook') and effective.get('Real_Viper_Hook')):
        candidates.append({
            'score': 440, 'grade': 'S+', 
            'combination': '🐍5-20독사훅',
            'tags': ['🐍독사대가리', '📉개미털기완료', '📈기울기상승턴'],
            'type': '👑' 
        })
        
    # 👑 [SSS급] 수박 돌반지 챔피언 (최강의 시너지)
    # 안전장치: dolbanzi_Count가 없을 경우 기본값 0을 반환하도록 get 옵션 추가
    ring_count = effective.get('dolbanzi_Count', 0) 
    if effective.get('watermelon_signal') and effective.get('dolbanzi'):
        combo_name = '👑💍수박첫돌반지' if ring_count == 1 else '🍉💍수박돌반지'
        final_score = 500 if ring_count == 1 else 450
        ring_tag = '🥇최초의반지' if ring_count == 1 else f'💍{ring_count}회차반지'
        candidates.append({
            'score': final_score, 'grade': 'SSS',
            'combination': combo_name,
            # 🚨 [수정 완료] tags 리스트 맨 끝에 ring_tag를 추가했습니다!
            'tags': ['🍉수박전환', '💍돌반지완성', '🔥최종병기', '🚀대시세시작', ring_tag],
            'type': '👑'
        })

    # 🚀 ── SS급: 돌반지 완성 (단독) ──────────────────────
    elif effective.get('dolbanzi'): # 200일 돌파 + 300% Vol + 쌍바닥
        if ring_count == 1:
            combo_name, ring_tag, bonus = '🥇💍첫번째돌반지', '🔥GoldenEntry', 30
        elif ring_count == 2:
            combo_name, ring_tag, bonus = '🥈💍두번째돌반지', '📈추세지속', 0
        else:
            combo_name, ring_tag, bonus = '🥉💍늙은돌반지', '⚠️과열주의', -50 # 3회부턴 감점 
            
        candidates.append({
            'score': 480 + bonus, 'grade': 'SS', 
            'combination': combo_name,
            # 🚨 [수정 완료] 여기도 tags 리스트 맨 끝에 ring_tag를 추가했습니다!
            'tags': ['💍돌반지완성', '⚡300%폭발', '👣쌍바닥확인', ring_tag],
            'type': '👑' 
        })

    # 🚀 [SS급] 골파기 V자 반등 (개미 무덤 돌파)
    if effective.get('Golpagi_Trap') and effective.get('watermelon_signal'):
        candidates.append({
            'score': 470,  
            'grade': 'SS', 
            'combination': '🕳️🚀수박품은골파기',
            'tags': ['🕳️가짜하락(개미털기)', '🧲OBV방어', '📈20일선탈환', '🍉단기수급폭발'],
            'type': '👑' 
        })
    
    # ── S급 ──────────────────────────────────
    if (effective.get('watermelon_signal') and effective.get('explosion_ready') and
        effective.get('bottom_area') and effective.get('silent_perfect')):
        candidates.append({
            'score': 350, 'grade': 'S',
            'combination': '💎전설조합',
            'tags': ['🍉수박전환', '💎폭발직전', '📍바닥권', '🤫조용한매집완전'],
            'type': '🗡'
        })

    if (effective.get('yeok_break') and
        effective.get('watermelon_signal') and effective.get('volume_surge')):
        candidates.append({
            'score': 320, 'grade': 'S',
            'combination': '💎돌파골드',
            'tags': ['🏆역매공파돌파', '🍉수박전환', '⚡거래량폭발'],
            'type': '🛡'
        })

    if (effective.get('silent_perfect') and
        effective.get('watermelon_signal') and effective.get('explosion_ready')):
        candidates.append({
            'score': 310, 'grade': 'S',
            'combination': '💎매집완성',
            'tags': ['🤫조용한매집완전', '🍉수박전환', '💎폭발직전'],
            'type': '🛡'
        })

    if (effective.get('bottom_area') and effective.get('explosion_ready') and
        effective.get('watermelon_signal')):
        candidates.append({
            'score': 300, 'grade': 'S',
            'combination': '💎바닥폭발',
            'tags': ['📍바닥권', '💎폭발직전', '🍉수박전환'],
            'type': '🗡'
        })

    # ── A급 ──────────────────────────────────
    if effective.get('watermelon_signal')   and effective.get('watermelon_red') and effective.get('explosion_ready'):
        candidates.append({
            'score': 280, 'grade': 'A',
            'combination': '🔥수박폭발',
            'tags': ['🍉수박전환', '💎폭발직전'],
            'type': '🗡'
        })

    if effective.get('yeok_break') and effective.get('volume_surge'):
        candidates.append({
            'score': 260, 'grade': 'A',
            'combination': '🔥돌파확인',
            'tags': ['🏆역매공파돌파', '⚡거래량폭발'],
            'type': '🛡'
        })

    if effective.get('silent_strong') and effective.get('explosion_ready'):
        candidates.append({
            'score': 250, 'grade': 'A',
            'combination': '🔥조용폭발',
            'tags': ['🤫조용한매집강', '💎폭발직전'],
            'type': '🛡'
        })

    # ── B급 ──────────────────────────────────
    if effective.get('watermelon_signal')  and effective.get('watermelon_red'):
        candidates.append({
            'score': 230, 'grade': 'B',
            'combination': '📍수박단독',
            'tags': ['🍉수박전환'],
            'type': '🔍'
        })

    if effective.get('bottom_area'):
        candidates.append({
            'score': 210, 'grade': 'B',
            'combination': '📍바닥단독',
            'tags': ['📍바닥권'],
            'type': '🔍'
        })

    # 최고점 조합 반환 (결과가 여러 개라도 가장 점수가 높은 1개만 사령관님께 보고합니다)
    if candidates:
        return max(candidates, key=lambda x: x['score'])

    # ── C급 ──────────────────────────────────
    if effective.get('obv_rising') and effective.get('mfi_strong'):
        return {'score': 170, 'grade': 'C', 'combination': '📊OBV+MFI', 'tags': ['📊OBV', '💰MFI'], 'type': None}
    if effective.get('volume_surge') and effective.get('obv_rising'):
        return {'score': 155, 'grade': 'C', 'combination': '⚡거래량+OBV', 'tags': ['⚡거래량', '📊OBV'], 'type': None}

    # ── D급 ──────────────────────────────────
    tags, bonus = [], 0
    if effective.get('obv_rising'):   bonus += 30; tags.append('📊OBV')
    if effective.get('mfi_strong'):   bonus += 20; tags.append('💰MFI')
    if effective.get('volume_surge'): bonus += 10; tags.append('⚡거래량')

    return {'score': 100 + bonus, 'grade': 'D', 'combination': '🔍기본', 'tags': tags, 'type': None}

def get_indicators(df):
    df = df.copy()
    count = len(df)

    # 1. 이동평균선 및 거래량 이평 (단테 112/224 포함)
    for n in [5, 10, 20, 40, 60, 112, 224]:
        df[f'MA{n}'] = df['Close'].rolling(window=min(count, n)).mean()
        df[f'VMA{n}'] = df['Volume'].rolling(window=min(count, n)).mean()

    # 2. 볼린저 밴드 (20/40 이중 응축)
    std20 = df['Close'].rolling(20).std()
    df['BB_Upper'] = df['MA20'] + (std20 * 2)
    df['BB20_Width'] = (std20 * 4) / df['MA20'] * 100
    
    std40 = df['Close'].rolling(40).std()
    df['BB40_Upper'] = df['MA40'] + (std40 * 2)
    df['BB40_Lower'] = df['MA40'] - (std40 * 2)
    df['BB40_Width'] = (std40 * 4) / df['MA40'] * 100
    df['BB40_PercentB'] = (df['Close'] - df['BB40_Lower']) / (df['BB40_Upper'] - df['BB40_Lower'])

    # 3. 이평선 수렴도 및 이격도
    df['MA_Convergence'] = abs(df['MA20'] - df['MA60']) / df['MA60'] * 100
    df['Disparity'] = (df['Close'] / df['MA20']) * 100

    # 4. 일목균형표 (구름대 및 기준선)
    df['Tenkan_sen'] = (df['High'].rolling(9).max() + df['Low'].rolling(9).min()) / 2
    df['Kijun_sen'] = (df['High'].rolling(26).max() + df['Low'].rolling(26).min()) / 2
    df['Span_A'] = ((df['Tenkan_sen'] + df['Kijun_sen']) / 2).shift(26)
    df['Span_B'] = ((df['High'].rolling(52).max() + df['Low'].rolling(52).min()) / 2).shift(26)
    df['Cloud_Top'] = df[['Span_A', 'Span_B']].max(axis=1)

    # 5. 스토캐스틱 (K, D, SD)
    l_min, h_max = df['Low'].rolling(12).min(), df['High'].rolling(12).max()
    df['Sto_K'] = ((df['Close'] - l_min) / (h_max - l_min)) * 100
    df['Sto_D'] = df['Sto_K'].rolling(5).mean()
    df['Sto_SD'] = df['Sto_D'].rolling(5).mean()

    # 6. ADX (방향성 지수)
    high, low, close = df['High'], df['Low'], df['Close']
    tr = pd.concat([high - low, abs(high - close.shift(1)), abs(low - close.shift(1))], axis=1).max(axis=1)
    dm_plus = (high - high.shift(1)).clip(lower=0)
    dm_minus = (low.shift(1) - low).clip(lower=0)
    df['ADX'] = ((abs(dm_plus.rolling(14).sum() - dm_minus.rolling(14).sum()) / 
                (dm_plus.rolling(14).sum() + dm_minus.rolling(14).sum())) * 100).rolling(14).mean()

    # 7. MACD
    ema12 = df['Close'].ewm(span=12).mean()
    ema26 = df['Close'].ewm(span=26).mean()
    df['MACD'] = ema12 - ema26
    df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
    df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']

    # 8. OBV (수박 로직 통합)
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
    df['OBV_MA10'] = df['OBV'].rolling(10).mean()
    df['OBV_Rising'] = df['OBV'] > df['OBV_MA10']
    df['OBV_Slope'] = (df['OBV'] - df['OBV'].shift(5)) / df['OBV'].shift(5).abs() * 100

    # 9. RSI (정밀 Wilder's 방식 - 100 초과 방지)
    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0).ewm(com=13, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(com=13, adjust=False).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # 10. MFI (수박 로직 통합)
    typical_price = (df['High'] + df['Low'] + df['Close']) / 3
    money_flow = typical_price * df['Volume']
    pos_f = money_flow.where(typical_price > typical_price.shift(1), 0).rolling(14).sum()
    neg_f = money_flow.where(typical_price < typical_price.shift(1), 0).rolling(14).sum()
    df['MFI'] = 100 - (100 / (1 + (pos_f / neg_f)))
    df['MFI_Strong'] = df['MFI'] > 50
    df['MFI_Prev5'] = df['MFI'].shift(5)

    # 11. 매집 파워 및 조용한 매집용 ATR
    df['Buy_Power'] = df['Volume'] * (df['Close'] - df['Open'])
    df['Buy_Power_MA'] = df['Buy_Power'].rolling(10).mean()
    df['Buying_Pressure'] = df['Buy_Power'] > df['Buy_Power_MA']
    
    tr_atr = pd.concat([high - low, abs(high - close.shift(1)), abs(low - close.shift(1))], axis=1).max(axis=1)
    df['ATR'] = tr_atr.rolling(14).mean()
    df['ATR_MA20'] = df['ATR'].rolling(20).mean()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 💡 [신규 추가] 조용한 매집 지속성 체크용
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # ATR이 평균 아래인 날 카운트 (최근 10일)
    df['ATR_Below_MA'] = (df['ATR'] < df['ATR_MA20']).astype(int)
    df['ATR_Below_Days'] = df['ATR_Below_MA'].rolling(10).sum()
    
    # MFI 50 이상인 날 카운트 (최근 10일)
    df['MFI_Above50'] = (df['MFI'] > 50).astype(int)
    df['MFI_Strong_Days'] = df['MFI_Above50'].rolling(10).sum()
    
    # MFI 10일 전 값 (상승 추세 확인용)
    df['MFI_10d_ago'] = df['MFI'].shift(10)
    df['MFI_Strong']= df['MFI'] > 50
    # 112일선 근접도 (스윙 검색용)
    df['Near_MA112'] = (abs(df['Close'] - df['MA112']) / df['MA112'] * 100)
    
    # 장기 바닥권 체크 (최근 60일 중 112선 아래 일수)
    df['Below_MA112'] = (df['Close'] < df['MA112']).astype(int)
    df['Below_MA112_60d'] = df['Below_MA112'].rolling(60).sum()
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    # 12. 수박 색상 및 신호 시스템
    red_score = (
        df['OBV_Rising'].astype(int) + 
        df['MFI_Strong'].astype(int) + 
        df['Buying_Pressure'].astype(int)
    )
    df['Watermelon_Color'] = np.where(red_score >= 2, 'red', 'green')
    
    color_change = (df['Watermelon_Color'] == 'red') & (df['Watermelon_Color'].shift(1) == 'green')
    df['Green_Days_10'] = (df['Watermelon_Color'].shift(1) == 'green').rolling(10).sum()
    volume_surge = df['Volume'] >= df['Volume'].rolling(20).mean() * 1.2
    
    df['Watermelon_Signal'] = color_change & (df['Green_Days_10'] >= 7) & volume_surge
    df['Watermelon_Score'] = red_score # 0~3점

    # 13. 기타 (박스권 범위 등)
    df['Box_Range'] = df['High'].rolling(10).max() / df['Low'].rolling(10).min()

    ma200 = df['Close'].rolling(224).mean()
    vol_avg20 = df['Volume'].rolling(20).mean()
    
    # 1. 거래량 300% 폭발 (Vol Power >= 3.0)
    vol_power = df['Volume'].iloc[-1] / vol_avg20.iloc[-1]
    
    # 2. 200일선 돌파 및 안착 (Stone-Ring)
    is_above_ma200 = df['Close'].iloc[-1] > ma200.iloc[-1]
    
    # 3. 쌍바닥 감지 (최근 30일 내 200일선 근처 저점 2개)
    lows = df['Low'].iloc[-30:]
    near_ma200 = lows[abs(lows - ma200.iloc[-1]) / ma200.iloc[-1] < 0.03]
    is_double_bottom = len(near_ma200[near_ma200 == near_ma200.rolling(5, center=True).min()]) >= 2

    df['Dolbanzi'] = (vol_power >= 3.0) & (is_above_ma200) & (is_double_bottom)
    
    # 2. [전체 시리즈에 대해 diff()와 cumsum()을 실행]
    # 200일선 위/아래 상태가 변할 때마다 그룹 번호가 생성됩니다.
    # 🚀 [MA200 생성] 모든 로직의 최상단에 배치하세요!
    df['MA200'] = df['Close'].rolling(window=224).mean()
    
    # [추가 전술] 상장한 지 200일이 안 된 종목은 NaN(공백)이 생깁니다.
    # 이를 0으로 채우거나, 데이터가 부족한 경우를 대비해 처리해주는 것이 안전합니다.
    df['MA200'] = df['MA200'].ffill().fillna(0)
    is_above_series = df['Close'] > df['MA200']
    df['Trend_Group'] = is_above_series.astype(int).diff().fillna(0).ne(0).cumsum()
    
    # 3. [최적화] 동일 그룹 내에서만 돌반지 횟수 누적
    # 현재가 200일선 위에 있을 때만(is_above_ma200) 카운트를 쌓습니다.
    df['Dolbanzi_Count'] = 0
    df['Dolbanzi_Count'] = df.groupby('Trend_Group')['Dolbanzi'].cumsum()

    print(f"✅ OBV 세력 매집 지표 계산!")
    # 2. 🧲 [OBV 세력 매집 지표 계산]
    # 주가가 오를 때의 거래량은 더하고, 내릴 때의 거래량은 뺍니다.
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
    df['OBV_MA10'] = df['OBV'].rolling(window=10).mean() # OBV의 추세선
    
    # [핵심] 5일선이 지하실에 박혀있던 최근 10일간, OBV 추세는 상승(매집)했는가?
    df['OBV_Bullish'] = df['OBV_MA10'] > df['OBV_MA10'].shift(1)

    # 1. 기존에 사령관님이 쓰시던 60일선 계산 코드
    df['MA60'] = df['Close'].rolling(window=60).mean()
    
    # 🚨 2. [탄약 보급 완료] 60일선의 "기울기"를 미리 계산해서 통째로 박아 넣습니다!
    # .diff()는 "오늘 값 - 어제 값"을 자동으로 계산해 주는 파이썬의 마법 함수입니다.
    df['MA60_Slope'] = df['MA60'].diff()
    
    # (참고: 두산밥캣 뚜껑 박치기 방지용 112일선 기울기도 필요하다면 같이 넣어주십시오)
    df['MA112_Slope'] = df['MA112'].diff()
    df['Dist_to_MA112'] = (df['MA112'] - df['Close']) / df['Close']

    # 2. [조건 1] 똬리 수축: 5, 10, 20일선이 2% 이내로 밀집 (에너지 응축)
    # 3개 이평선 중 최고값과 최저값의 차이가 2% 이하인지 판별
    max_ma = df[['MA5', 'MA10', 'MA20']].max(axis=1)
    min_ma = df[['MA5', 'MA10', 'MA20']].min(axis=1)
    is_squeezed = (max_ma - min_ma) / min_ma <= 0.02
    
    # 3. [조건 2] 늪지대 함정: 최근 10일 이내에 5일선이 20일선 아래로 빠진 적이 있는가?
    # True(1) 상태가 지난 10일 중 한 번이라도 있었는지 검사합니다.
    is_below_20 = (df['MA5'] < df['MA20']).astype(int)
    was_below_20 = is_below_20.rolling(window=10).max() == 1

    print(f"✅ 독사 대가리 + 기울기 방어선!")
    # 4. [조건 3 & 4] 독사 대가리 + 기울기 방어선 (사령관님 특별 지시!)
    # 어제보다 5일선이 올라갔고(상승 턴), 현재 5일선이 20일선을 뚫었거나 바짝 붙었을 때!
    is_slope_up = df['MA5'] > df['MA5'].shift(1)
    is_head_up = is_slope_up & (df['MA5'] >= df['MA20'] * 0.99)

    print(f"✅ 60일선의 기울기")
    # 🚨 [KILL SWITCH 1] LG화학 사살: 60일선의 "기울기"가 하락 중이면 무조건 탈락!
    # 주가가 60일선 위에 있든 아래에 있든, 60일선 자체가 쏟아져 내리면 그건 악성 시체밭입니다.
    is_ma60_safe = df['MA60_Slope'] >= 0

    print(f"✅ 5일선(대가리)")
    # 🚨 [KILL SWITCH 2] 두산밥캣 사살: "5일선(대가리)"에서 너무 멀어지면 탈락!
    # 20일선이 아니라, 당장 오늘 꺾어 올린 '5일선' 위로 주가가 5% 이상 혼자 튀어 나가면 허공답보입니다.
    distance_from_ma5 = (df['Close'] - df['MA5']) / df['MA5']
    is_hugging_ma5 = distance_from_ma5 < 0.05  # 5일선에 5% 이내로 바짝 붙어있어야 진짜 뱀!

    print(f"✅ 역배열 폭포수 사살")
    # 🚨 [KILL SWITCH 3] 역배열 폭포수 사살: 112일선(반년 선)이 200일선 아래로 곤두박질치는가?
    # 장기 이평선이 완벽한 역배열 폭포수라면 뱀이 아니라 미꾸라지입니다.
    is_not_waterfall = df['MA112'] >= df['MA200'] * 0.9  # 최소한 200일선 근처에서 놀아야 함
    print(f"✅ 역배열 폭포수 사살 - 1")
    is_heading_ceiling = (df['Close'] < df['MA112']) & (df['MA112_Slope'] < 0) & (df['Dist_to_MA112'] <= 0.04)
    print(f"✅ 역배열 폭포수 사살 - 2")
    is_not_blocked = ~is_heading_ceiling

    # 🚨 [킬 스위치 1] 두산밥캣 뚜껑 박치기 방지 (Blocked)
    is_heading_ceiling = (df['Close'] < df['MA112']) & (df['MA112_Slope'] < 0) & (df['Dist_to_MA112'] <= 0.04)
    df['is_not_blocked'] = ~is_heading_ceiling  # 👈 뚜껑 필터는 뚜껑 명찰로!

    # 🚨 [킬 스위치 2] 장기 역배열 지하실 폭포수 방지 (Waterfall)
    df['is_not_waterfall'] = df['MA112'] >= df['MA200'] * 0.9 # 👈 폭포수 필터는 폭포수 명찰로!
    
    # 🚨 [킬 스위치 3] LG화학 60일선 하락 방지 (Safe MA60)
    df['is_ma60_safe'] = df['MA60_Slope'] >= 0

    # 🎯 [복구된 킬 스위치 4] 두산밥캣 절대 사살용: 5일선 허공답보 방지!
    # 오늘 종가가 5일선(MA5)보다 8% 이상 높게 허공에 떠 있다면 '오버슈팅(에너지 고갈)'으로 간주!
    df['Dist_from_MA5'] = (df['Close'] - df['MA5']) / df['MA5']
    df['is_hugging_ma5'] = df['Dist_from_MA5'] < 0.08

    # 🚨 [킬 스위치 6] 전고점 쌍봉 박치기 방지 (Double Top Trap)
    # 최근 10일간의 최고가를 구합니다. (어제 기준)
    df['recent_high_10d'] = df['High'].rolling(window=10).max().shift(1)
    
    # 오늘 종가가 최근 최고가 턱밑(2% 이내)에 바짝 붙었는데, 돌파는 못 했는가?
    # 돌파를 못 하고 턱밑에 멈췄다면 내일 쌍봉 맞고 떨어질 확률 90%입니다.
    is_hitting_wall = ((df['recent_high_10d'] - df['Close']) / df['Close'] < 0.02)
    is_breaking_high = df['Close'] > df['recent_high_10d']
    
    # 턱밑에 붙었더라도 시원하게 돌파(breaking)했다면 봐주고, 돌파 못 하고 막혔다면(False) 탈락!
    df['is_not_double_top'] = ~(is_hitting_wall & ~is_breaking_high)
    
    # 👑 [최종 융합] 이 모든 필터를 통과한 '진짜 독사'만 찾아라!
    df['Real_Viper_Hook'] = (df['is_not_blocked'] & df['is_not_waterfall'] & df['is_ma60_safe'] & df['is_hugging_ma5'] & df['is_not_double_top'])
    
    print(f"✅ 최종판독")
    # 5. [최종 판독] 모든 조건이 일치하는 날을 'Viper_Hook'으로 명명!
    df['Viper_Hook'] = is_squeezed & was_below_20 & is_head_up

    # 🚨 [사령부 특수 전술] 골파기(Bear Trap) 감별 레이더
    
    # 1. [함정 발생] 최근 5일 이내에 20일선(생명선)을 깬 적이 있는가? (개미 털기 구간)
    df['was_broken_20'] = (df['Close'].shift(1) < df['MA20'].shift(1)) | \
                          (df['Close'].shift(2) < df['MA20'].shift(2)) | \
                          (df['Close'].shift(3) < df['MA20'].shift(3))

    # 2. [가짜 하락 인증] 20일선을 깰 때(하락할 때) 거래량이 말라붙었는가?
    # 최근 5일 중 가장 거래량이 적었던 날이 20일 평균 거래량의 절반 이하라면 '가짜'로 판정!
    df['lowest_vol_5d'] = df['Volume'].rolling(window=5).min()
    df['is_fake_drop'] = df['lowest_vol_5d'] < (df['Volume'].rolling(window=20).mean() * 0.5)

    # 3. [돈줄 방어] 주가는 최근 5일 전보다 빠졌는데, OBV는 오히려 올랐는가? (다이버전스)
    df['obv_divergence'] = (df['Close'] < df['Close'].shift(5)) & (df['OBV'] >= df['OBV'].shift(5))

    # 4. [반격 개시] 오늘 드디어 20일선을 다시 강하게 탈환했는가? (V자 반등)
    df['reclaim_20'] = (df['Close'] > df['MA20']) & (df['Close'] > df['Open']) & (df['Volume'] > df['Volume'].shift(1))

    # 👑 [최종 융합] 이 모든 조건이 맞아떨어지면 완벽한 '골파기 후 반등' 패턴!
    df['Golpagi_Trap'] = df['was_broken_20'] & df['is_fake_drop'] & df['obv_divergence'] & df['reclaim_20']

    # 1. 파란 점선: VWMA (거래량 가중 40일 이평)
    # 종가에 거래량을 곱한 값의 합을 거래량의 합으로 나눕니다.
    df['VWMA40'] = (df['Close'] * df['Volume']).rolling(window=40).mean() / df['Volume'].rolling(window=40).mean()

    # 3. 수박 에너지 (화력) 계산 - 사령관님의 '킥(Kick)' 적용
    # 이격도(현재가/VWMA40)에 거래량 가속도(당일거래량/5일평균)를 곱함
    df['Vol_Accel'] = df['Volume'] / df['Volume'].rolling(window=5).mean()
    df['Watermelon_Fire'] = (df['Close'] / df['VWMA40'] - 1) * 100 * df['Vol_Accel']
    
    # 4. 수박 상태 판독
    # 초록수박: 파란점선 위 + 에너지가 모이는 중 (밴드폭 10% 이내)
    df['Watermelon_Green'] = (df['Close'] > df['VWMA40']) & (df['BB40_Width'] < 0.10)
    
    # 빨간수박(폭발): 초록수박 상태에서 화력이 임계값(예: 5)을 돌파할 때
    df['Watermelon_Red'] = df['Watermelon_Green'] & (df['Watermelon_Fire'] > 5.0)

    df['Watermelon_Red2'] = ((df['Close'].iloc[-1] > df['VWMA40'].iloc[-1]) and
                            (df['Close'].iloc[-1] >= df['Open'].iloc[-1]))

    # ── 저항선 계산 (BB 상한선 추가) 
    # ── 저항선 터치 흔적 스캔 (최근 20일) ──────────
    # 각 저항선 중 현재 주가보다 위에 있는 가장 강력한 선들을 타겟으로 함
    def check_touch(row):
        resistances = [row['BB_Upper'], row['BB40_Upper'], row['MA60'], row['MA112']]
        # 현재가보다 높은 저항선들 중, 고가(High)가 저항선의 99%~101% 범위에 닿았는지 확인
        touches = 0
        for res in resistances:
            if pd.notna(res) and row['Close'] < res: # 현재가 위에 있는 저항선만
                if row['High'] >= res * 0.995: # 0.5% 오차 범위 내 터치
                    touches += 1
        return touches

    df['MA20_slope'] = (df['MA20'] - df['MA20'].shift(5)) / (df['MA20'].shift(5) + 1e-9) * 100
    df['MA40_slope'] = (df['MA40'] - df['MA40'].shift(5)) / (df['MA40'].shift(5) + 1e-9) * 100

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    df['Daily_Touch'] = df.apply(check_touch, axis=1)
    # 최근 20일 동안 성벽을 두드린 총 횟수
    df['Total_hammering'] = int(df['Daily_Touch'].iloc[-20:].sum())
    
    # 현재 봉이 저항선을 완전히 돌파했는지 여부
    current_res_max = max(curr['BB_Upper'], curr['BB40_Upper'], curr['MA60'], curr['MA112'])
    df['Is_resistance_break'] = curr['Close'] > current_res_max

    # ── 매집봉 (거래량 급증 양봉) ──────────────
    df['Is_Maejip'] = (
        (df['Volume'] > df['Volume'].shift(1) * 2) &
        (df['Close'] > df['Open']) &
        (df['Close'] > df['Close'].shift(1))
    )

    df['Maejip_Count'] = int(df['Is_Maejip'].iloc[-20:].sum())

    # 1. 종베 골든크로스 (전환 순간)
    gap_ratio = abs(curr['MA20'] - curr['MA40']) / (curr['MA40'] + 1e-9)
    cross_series = (df['MA20'] > df['MA40']) & (df['MA20'].shift(1) <= df['MA40'].shift(1))
    cross_recent = cross_series.iloc[-5:].any()
    cross_near   = (curr['MA20'] > curr['MA40']) and (gap_ratio < 0.03)

    ma20_rising  = curr['MA20_slope'] > 0
    ma40_rising  = curr['MA40_slope'] > -0.05
    ma20_accel   = curr['MA20_slope'] > df['MA20_slope'].rolling(3).mean().iloc[-2]

    df['Jongbe_Break'] = (
    (cross_recent or cross_near) and
    ma20_rising and
    ma40_rising and
    ma20_accel and
    curr['Close'] > curr['MA20']
)
    return df

def analyze_final_longterm(ticker, name, historical_indices, scan_days=750, sampling='weekly'):
    """
    장기 백테스트용 분석 함수 (샘플링 지원)
    """
    
    try:
        # 데이터 다운로드 (3년치)
        df = yf.download(ticker, period='3y', interval='1d', progress=False)
        
        if df.empty or len(df) < 200:
            return []
        
        df.columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
        df.index = pd.to_datetime(df.index)
        
        # 매크로 지표 추가
        for idx_name, idx_data in historical_indices.items():
            matching = idx_data[idx_data.index.isin(df.index)]
            df[f'{idx_name}_close'] = matching['Close']
            df[f'{idx_name}_ma5'] = matching['Close'].rolling(5).mean()
        
        # 지표 계산
        df = get_indicators(df)
        
        today_price = df.iloc[-1]['Close']
        
        # 샘플링 (주 1회 또는 월 1회)
        if sampling == 'weekly':
            # 매주 금요일만 스캔
            df_scan = df[df.index.dayofweek == 4]  # 4 = 금요일
        elif sampling == 'monthly':
            # 매월 마지막 거래일만
            df_scan = df.groupby(df.index.to_period('M')).tail(1)
        else:  # full
            df_scan = df.tail(scan_days)
        
        # 분석 (기존 로직과 동일)
        hits = []
        
        for curr_idx, row in df_scan.iterrows():
            raw_idx = df.index.get_loc(curr_idx)
            
            # ... (기존 analyze_final과 동일) ...
            
            # 신호 수집
            signals = {
                'watermelon_signal': row['Watermelon_Signal'],
                'explosion_ready': (
                    row['BB40_Width'] <= 10.0 and 
                    row['OBV_Rising'] and 
                    row['MFI_Strong']
                ),
                'bottom_area': (
                    row['Near_MA112'] <= 5.0 and 
                    row['Below_MA112_60d'] >= 40
                ),
                # ... (나머지 동일)
            }
            
            result = calculate_combination_score(signals)
            
            if result['score'] < 200:
                continue
            
            # 수익률 계산
            returns = calculate_realistic_returns(df, raw_idx, row['Close'])
            
            # 결과 저장
            hits.append({
                '날짜': curr_idx.strftime('%Y-%m-%d'),
                '등급': result['grade'],
                '점수': result['score'],
                '조합': result['combination'],
                '종목': name,
                '매수가': int(returns['entry_price']),
                '최고수익률_real': returns['max_gain_real'],
                '최저수익률_real': returns['min_loss_real'],
                '보유일': returns['hold_days'],
                # ... (나머지 필드)
            })
        
        return hits
        
    except Exception as e:
        return []

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 📊 시장 국면별 성과 분석
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def analyze_by_market_condition(df_longterm):
    """
    시장 국면별 성과 분석
    """
    
    # 보유 기간 있는 것만 (과거 데이터)
    df = df_longterm[df_longterm['보유일'] > 0].copy()
    
    # 상폐주 제거
    df = df[df['최저수익률_raw'] > -50]
    
    print("\n" + "=" * 100)
    print("📊 시장 국면별 성과 분석")
    print("=" * 100)
    
    results = []
    
    # 추세별 분석
    for trend in ['down', 'sideways', 'up']:
        trend_df = df[df['시장추세'] == trend]
        
        if len(trend_df) == 0:
            continue
        
        # 등급별 분석
        for grade in ['S', 'A', 'B']:
            grade_df = trend_df[trend_df['등급'] == grade]
            
            if len(grade_df) < 3:  # 최소 3건
                continue
            
            total = len(grade_df)
            winners = len(grade_df[grade_df['최고수익률_raw'] >= 3.5])
            
            avg_gain = grade_df['최고수익률_raw'].mean()
            avg_loss = grade_df['최저수익률_raw'].mean()
            
            win_rate = (winners / total) * 100
            expected = (win_rate / 100) * avg_gain
            
            sharpe = avg_gain / abs(avg_loss) if avg_loss != 0 else 0
            
            # 시장 이름
            if trend == 'down':
                market_name = '📉 약세장'
            elif trend == 'sideways':
                market_name = '➡️ 횡보장'
            else:
                market_name = '📈 강세장'
            
            results.append({
                '시장': market_name,
                '등급': f'{grade}급',
                '건수': total,
                '승률(%)': round(win_rate, 1),
                '평균수익(%)': round(avg_gain, 1),
                '평균손실(%)': round(avg_loss, 1),
                '기대값': round(expected, 2),
                '샤프비율': round(sharpe, 2)
            })
    
    df_results = pd.DataFrame(results)
    
    print("\n전체 분석:")
    print(df_results)
    
    # 핵심 인사이트
    print("\n" + "=" * 100)
    print("💡 핵심 인사이트")
    print("=" * 100)
    
    # S급 비교
    s_grade = df_results[df_results['등급'] == 'S급']
    
    if len(s_grade) >= 2:
        down = s_grade[s_grade['시장'] == '📉 약세장']
        up = s_grade[s_grade['시장'] == '📈 강세장']
        
        if not down.empty and not up.empty:
            down_val = down.iloc[0]['평균수익(%)']
            up_val = up.iloc[0]['평균수익(%)']
            
            print(f"\n🏆 S급 성과:")
            print(f"   약세장: {down_val}%")
            print(f"   강세장: {up_val}%")
            print(f"   차이: {up_val - down_val}%p")
            
            if down_val > 15:
                print(f"   ✅ 약세장에서도 {down_val}% 수익! (전천후 전략)")
            elif down_val > 5:
                print(f"   ⚠️ 약세장에서는 성과 감소 ({down_val}%)")
            else:
                print(f"   ❌ 약세장에서는 부진 ({down_val}%)")
    
    return df_results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎯 조합별 시장 적합도 분석
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def analyze_combination_by_market(df_longterm):
    """
    조합별로 어느 시장에서 강한지 분석
    """
    
    df = df_longterm[df_longterm['보유일'] > 0].copy()
    df = df[df['최저수익률_raw'] > -50]
    
    print("\n" + "=" * 100)
    print("🎯 조합별 시장 적합도 분석")
    print("=" * 100)
    
    # 주요 조합만
    top_combos = df['조합'].value_counts().head(10).index
    
    results = []
    
    for combo in top_combos:
        combo_df = df[df['조합'] == combo]
        
        # 시장별 성과
        down_df = combo_df[combo_df['시장추세'] == 'down']
        side_df = combo_df[combo_df['시장추세'] == 'sideways']
        up_df = combo_df[combo_df['시장추세'] == 'up']
        
        def calc_stats(df):
            if len(df) < 3:
                return None
            total = len(df)
            winners = len(df[df['최고수익률_real'] >= 3.5])
            avg = df['최고수익률_real'].mean()
            return {
                'count': total,
                'win_rate': (winners/total)*100,
                'avg': avg
            }
        
        down_stats = calc_stats(down_df)
        side_stats = calc_stats(side_df)
        up_stats = calc_stats(up_df)
        
        # 최적 시장 결정
        best_market = '없음'
        best_avg = 0
        
        if down_stats and down_stats['avg'] > best_avg:
            best_market = '약세장'
            best_avg = down_stats['avg']
        if side_stats and side_stats['avg'] > best_avg:
            best_market = '횡보장'
            best_avg = side_stats['avg']
        if up_stats and up_stats['avg'] > best_avg:
            best_market = '강세장'
            best_avg = up_stats['avg']
        
        results.append({
            '조합': combo,
            '최적시장': best_market,
            '약세_수익(%)': round(down_stats['avg'], 1) if down_stats else '-',
            '약세_건수': down_stats['count'] if down_stats else 0,
            '횡보_수익(%)': round(side_stats['avg'], 1) if side_stats else '-',
            '횡보_건수': side_stats['count'] if side_stats else 0,
            '강세_수익(%)': round(up_stats['avg'], 1) if up_stats else '-',
            '강세_건수': up_stats['count'] if up_stats else 0
        })
    
    df_results = pd.DataFrame(results)
    print("\n조합별 시장 적합도:")
    print(df_results)
    
    return df_results

# ---------------------------------------------------------
# 🕵️‍♂️ [분석] 정밀 분석 엔진 (Ver 36.7 최저수익률 추가)
# ---------------------------------------------------------
def analyze_final(ticker, name, historical_indices, g_env, l_env, s_map):
    try:
        df = fdr.DataReader(ticker, start=START_DATE)
        if len(df) < 100: return []
        df = get_indicators(df)
        df = df.join(historical_indices, how='left').fillna(method='ffill')

        # 1. 내 종목의 섹터 확인
        my_sector = s_map.get(ticker, "일반")
    
        # 2. 우리 섹터 대장주의 상태 확인 (leader_status 맵 활용)
        current_leader_condition = l_env.get(my_sector, "Normal")
    
        # 3. 확신 점수에 반영
        l_score = 25 if current_leader_condition == "🔥강세" else 0
    
        # 🕵️ 신규 추가: 서사 분석기 호출
        #print(f"✅ [본진] 서사 분석기 호출 : {name}")
        sector = get_stock_sector(ticker, sector_master_map) # 섹터 판독 함수 필요
        #grade, narrative, target, stop, conviction = analyze_all_narratives(
        #    df, name, my_sector, g_env, l_env
        #)
        
        
        # 최신 수급 데이터 수집
        try:
            #print(f"✅ [본진] 최신 수급 데이터 수집")
            url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            res.encoding = 'euc-kr'
            supply_df = pd.read_html(res.text)[2].dropna()
            f_qty = int(str(supply_df.iloc[0]['외국인']).replace('.0','').replace(',',''))
            i_qty = int(str(supply_df.iloc[0]['기관']).replace('.0','').replace(',',''))
            twin_b = (f_qty > 0 and i_qty > 0)
            whale_score = int(((f_qty + i_qty) * df.iloc[-1]['Close']) / 100000000)
        except:
            f_qty, i_qty, twin_b, whale_score = 0, 0, False, 0

        recent_df = df.tail(SCAN_DAYS)
        hits = []

        #print(f"✅ [본진] 패턴 찾기")
        for curr_idx, row in recent_df.iterrows():
            raw_idx = df.index.get_loc(curr_idx)
            if raw_idx < 100: continue
            prev = df.iloc[raw_idx-1]
            prev_5 = df.iloc[max(0, raw_idx-5)]
            prev_10 = df.iloc[max(0, raw_idx-10)]

            # ✅ [필수] 가격 변수 정의
            close_p = row['Close']      # 당일 종가
            open_p = row['Open']        # 당일 시가
            high_p = row['High']        # 당일 고가
            low_p = row['Low']          # 당일 저가
            
            temp_df = df.iloc[:raw_idx + 1]

            # analyze_final 함수 내부 루프 안에서
            # 최근 5일간의 진짜 거래대금 계산 (단위: 억)
            recent_avg_amount = (df['Close'] * df['Volume']).tail(5).mean() / 100000000
        
            if recent_avg_amount < 50: # 평균 거래대금 50억 미만은 탈락!
                continue
            
            #하락기간과 횡보(공구리)기간 비교(1이상 추천)
            dante_data = calculate_dante_symmetry(temp_df)
        
            if dante_data is None:
                dante_data_ratio = 0
                dante_data_mae_jip = 0
            else:
                dante_data_ratio = dante_data['ratio']
                dante_data_mae_jip = dante_data['mae_jip']

            grade, narrative, target, stop, conviction = analyze_all_narratives(
                temp_df, name, my_sector, g_env, l_env
            )

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 1. 신호 수집
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            print(f"✅ [본진] 신호 수집!")
            signals = {
                # 수박지표
                'watermelon_signal': row['Watermelon_Signal'],
                'watermelon_red': row['Watermelon_Red'],
                
                'watermelon_green_7d': row['Green_Days_10'] >= 7,
                
                # 폭발 직전
                'explosion_ready': (
                    row['BB40_Width'] <= 10.0 and 
                    row['OBV_Rising'] and 
                    row['MFI_Strong']
                ),
                
                # 바닥권
                'bottom_area': (
                    row['Near_MA112'] <= 5.0 and 
                    row['Below_MA112_60d'] >= 40
                ),
                
                # 조용한 매집
                'silent_perfect': (
                    row['ATR_Below_Days'] >= 7 and
                    row['MFI_Strong_Days'] >= 7 and
                    row['MFI'] > 50 and
                    row['MFI'] > row['MFI_10d_ago'] and
                    row['OBV_Rising'] and
                    row['Box_Range'] <= 1.15
                ),
                'silent_strong': (
                    row['ATR_Below_Days'] >= 5 and
                    row['MFI_Strong_Days'] >= 5 and
                    row['OBV_Rising']
                ),
                
                # 역매공파 돌파
                'yeok_break': (
                    close_p > row['MA112'] and 
                    prev['Close'] <= row['MA112']
                ),
                
                # 기타
                'volume_surge': row['Volume'] >= row['VMA20'] * 1.5,
                'obv_rising': row['OBV_Rising'],
                'mfi_strong': row['MFI_Strong'],
                # 돌반지
                'dolbanzi': row['Dolbanzi'],
                'dolbanzi_Trend_Group': row['Trend_Group'],
                'dolbanzi_Count': row['Dolbanzi_Count'],

                #독사 5-20
                'viper_hook': row['Viper_Hook'],
                'obv_bullish': row['OBV_Bullish'],
                'Real_Viper_Hook': row['Real_Viper_Hook'],
                'Golpagi_Trap': row['Golpagi_Trap'],

                # ✅ 신규: 삼각수렴 + 종베 신호 추가
                'jongbe_break':    row.get('Jongbe_Break', False),
                'triangle_signal': False,   # 아래에서 채워짐
                'triangle_apex':   None,
                'triangle_pattern': 'None',
            }

            tri_result = jongbe_triangle_combo_v3(temp_df)

            if tri_result is not None:
                signals['triangle_signal']  = tri_result['pass']
                signals['triangle_apex']    = tri_result['apex_remain']
                signals['triangle_pattern'] = tri_result['triangle_pattern']
                signals['jongbe_ok']        = tri_result['jongbe']
                signals['explosion_ready']  = signals['explosion_ready'] or tri_result['score'] >= 70
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 2. 조합 점수 계산
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            print(f"✅ [본진] 조합 점수 계산!")
            result = judge_trade_with_sequence(temp_df, signals)
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 3. 추가 정보 태그
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            new_tags = result['tags'].copy()
            
            # 세부 정보 추가
            if signals['watermelon_signal']:
                new_tags.append(f"🍉강도{row['Watermelon_Score']}/3")
            
            if signals['bottom_area']:
                new_tags.append(f"📍거리{row['Near_MA112']:.1f}%")
            
            if signals['silent_perfect'] or signals['silent_strong']:
                new_tags.append(f"🔇ATR{int(row['ATR_Below_Days'])}일")
                new_tags.append(f"💰MFI{int(row['MFI_Strong_Days'])}일")

            if row['Dolbanzi']:
                new_tags.append(f"🟡돌반지")

            if signals['watermelon_red']:
                new_tags.append(f"🍉진짜수박")
            
            # 💡 오늘의 현재가 저장 (나중에 사용)
            today_price = df.iloc[-1]['Close']

            print(f"✅ [본진] 꼬리% 정밀 계산!")
            # 1. 꼬리% 정밀 계산
            high_p, low_p, close_p, open_p = row['High'], row['Low'], row['Close'], row['Open']
            body_max = max(open_p, close_p)
            t_pct = int((high_p - body_max) / (high_p - low_p) * 100) if high_p != low_p else 0

            # 2. 기존 핵심 전술 신호 판정
            is_cloud_brk = prev['Close'] <= prev['Cloud_Top'] and close_p > row['Cloud_Top']
            is_kijun_sup = close_p > row['Kijun_sen'] and prev['Close'] <= prev['Kijun_sen']
            is_diamond = is_cloud_brk and is_kijun_sup
            
            is_super_squeeze = row['BB20_Width'] < 10 and row['BB40_Width'] < 15
            is_yeok_mae_old = close_p > row['MA112'] and prev['Close'] <= row['MA112']
            is_vol_power = row['Volume'] > row['VMA20'] * 2.5

            print(f"✅ [본진] 역매공파 계산!")
            # --- [역매공파 통합 7단계 로직] ---
            # 1. [역(逆)] 역배열 바닥 탈출 (5/20 골든크로스)
            # 의미: 하락을 멈추고 단기 추세를 돌리는 첫 신호
            is_yeok = (prev['MA5'] <= prev['MA20']) and (row['MA5'] > row['MA20'])

            # 2. [매(埋)] 에너지 응축 (이평선 밀집)
            # 의미: 5, 20, 60일선이 3% 이내로 모여 에너지가 압축된 상태
            is_mae = row['MA_Convergence'] <= 3.0 and (row['BB40_Width'] <= 10.0) and row['ATR'] < row['ATR_MA20'] and row['OBV_Slope'] > 0

            # 3. [공(空)] 공구리 돌파 (MA112 돌파) - 사령관님이 찾아낸 핵심!
            # 의미: 6개월 장기 저항선(공구리)을 종가로 뚫어버리는 순간
            is_gong = (close_p > row['MA112']) and (prev['Close'] <= row['MA112']) and (row['Volume'] > row['VMA20'] * 1.5)

            # 4. [파(破)] 파동의 시작 (BB40 상단 돌파)
            # 의미: 볼린저밴드 상단을 뚫고 변동성이 위로 터지는 시점
            is_pa = (row['Close'] > row['BB40_Upper']) and (prev['Close'] <= row['BB40_Upper']) and row['Disparity'] <= 106

            # 5. [화력] 거래량 동반 (VMA5 대비 2배)
            # 의미: 가짜 돌파를 걸러내는 세력의 입성 증거
            is_volume = row['Volume'] >= row['VMA5'] * 2.0

            # 6. [안전] 적정 이격도 (100~106%)
            # 의미: 이미 너무 날아간 종목(추격매수)은 거르는 안전장치
            is_safe = 100.0 <= row['Disparity'] <= 106.0

            # 7. [수급] OBV 우상향 유지
            # 의미: 주가는 흔들어도 돈(매집세)은 빠져나가지 않는 상태
            is_obv = row['OBV_Slope'] > 0

            # ⛔ 무효화 조건 (패턴 붕괴)
            invalid = row['Close'] < row['MA60']

            #돌반지
            isDolbanzi = row['Dolbanzi']
            
            print(f"✅ [본진] 역매공파 최종 계산!")
            # 🏆 [최종 판정] 7가지 중 5가지 이상 만족 시 '정예', 7가지 모두 만족 시 'LEGEND'
            conditions = [is_yeok, is_mae, is_gong, is_pa, is_volume, is_safe, is_obv]
            match_count = sum(conditions)
            
            # 💡 매집 5가지 조건 체크
            acc_1_obv_rising = (row['OBV'] > prev_5['OBV']) and (row['OBV'] > prev_10['OBV'])
            acc_2_box_range = row['Box_Range'] <= 1.15
            acc_3_macd_golden = row['MACD'] > row['MACD_Signal']
            acc_4_rsi_healthy = 40 <= row['RSI'] <= 70
            acc_5_sto_golden = row['Sto_K'] > row['Sto_D']
    
            # 💡 [신규] 조용한 매집 패턴 (당신이 말한 이상적 조건!)
            silent_1_atr_low = row['ATR'] < row['ATR_MA20']  # ATR이 20일 평균 아래
            silent_2_mfi_strong = row['MFI'] > 50  # MFI 50 이상
            silent_3_mfi_rising = row['MFI'] > row['MFI_Prev5']  # MFI 상승 중
            silent_4_obv_rising = row['OBV'] > prev_5['OBV']  # OBV 상승 중
            
            # 💡 조용한 매집 완성 조건 (4개 모두 충족)
            is_silent_accumulation = (silent_1_atr_low and silent_2_mfi_strong and 
                                     silent_3_mfi_rising and silent_4_obv_rising)
   
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🤫 조용한 매집 (신규 지표 활용!)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            print(f"✅ [본진] 조용한 매집 계산!")
            silent_1_atr = row['ATR_Below_Days'] >= 7
            silent_2_mfi_persist = row['MFI_Strong_Days'] >= 7
            silent_3_mfi_current = row['MFI'] > 50
            silent_4_mfi_rising = row['MFI'] > row['MFI_10d_ago']
            silent_5_obv = row['OBV_Rising']
            silent_6_box = row['Box_Range'] <= 1.15
            
            silent_count = sum([silent_1_atr, silent_2_mfi_persist, 
                              silent_3_mfi_current, silent_4_mfi_rising,
                              silent_5_obv, silent_6_box])
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🏆 역매공파 바닥권 (신규 지표 활용!)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            print(f"✅ [본진] 역매공파 바닥권 계산!")
            near_ma112 = row['Near_MA112'] <= 5.0
            long_bottom = row['Below_MA112_60d'] >= 40
            bottom_area = near_ma112 and long_bottom
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 💎 폭발 직전 (BB수축 + 수급)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            print(f"✅ [본진] 폭발 직전 (BB수축 + 수급) 계산!")
            bb_squeeze = row['BB40_Width'] <= 10.0
            supply_strong = row['OBV_Rising'] and row['MFI_Strong']
            explosion_ready = bb_squeeze and supply_strong

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🔺 삼각수렴 + 종베 골든크로스
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if tri_result is not None:
                tri = tri_result.get('triangle') or {}
        
            # 삼각수렴 감지
            if tri_result.get('has_triangle') and tri.get('is_triangle'):
                pattern_labels = {
                    'Symmetrical': '대칭삼각',
                    'Ascending':   '상승삼각',
                    'Descending':  '하락삼각',
                }
                pat_label = pattern_labels.get(tri.get('pattern', ''), '')
                conf      = tri.get('confidence', 'LOW')
                conv      = tri.get('convergence_pct', 0)
                
                s_score += 60
                tags.append(f"🔺{pat_label}수렴({conv:.0f}%)")
                
                if conf == 'HIGH':
                    s_score += 20
                    tags.append("🔺고신뢰삼각")
        
            # 꼭지점 임박
            apex = tri_result.get('apex_remain')
            if apex is not None:
                if 0 <= apex <= 5:
                    s_score += 40
                    tags.append(f"🔺꼭지{apex}봉임박")
                elif apex < 0:
                    s_score -= 20
                    tags.append(f"🔺꼭지초과{abs(apex)}봉")
        
            # 수렴선 교차 (에너지 소멸)
            if tri.get('lines_crossed'):
                s_score -= 30
                tags.append("⚠️수렴에너지소멸")
        
            # 상방 돌파
            if tri.get('breakout_up'):
                s_score += 50
                tags.append("🚀삼각상방돌파")
        
            # 하방 이탈
            if tri.get('breakout_down'):
                s_score -= 50
                tags.append("🔻삼각하방이탈")
        
            # 종베 골든크로스
            if tri_result.get('jongbe'):
                s_score += 40
                tags.append("💛종베GC")
                detail = tri_result.get('jongbe_detail', {})
                if detail.get('cross_recent'):
                    tags.append("💛종베크로스(최근5일)")
                if detail.get('ma20_accel'):
                    tags.append("💛MA가속중")
        
            # 종베 + 삼각수렴 동시 달성 (최강 조합)
            if tri_result.get('jongbe') and tri_result.get('has_triangle') and tri.get('is_triangle'):
                s_score += 80
                tags.append("💎종베+삼각수렴")
        
            # 삼각수렴 DNA
            dna = tri_result.get('ma20_dna', '0%')
            if int(dna.replace('%', '')) >= 70:
                s_score += 20
                tags.append(f"🧬MA지지DNA({dna})")

            #수박지표
            print(f"✅ [본진] 수박지표 계산!")
            is_watermelon = row['Watermelon_Signal']
            watermelon_color = row['Watermelon_Color']
            watermelon_red = row['Watermelon_Red']
            watermelon_red2 = row['Watermelon_Red2']
            watermelon_score = row['Watermelon_Score']
            # 마지막 날(오늘)의 수박 상태 확인
            is_hot_watermelon = row['Watermelon_Red']
            watermelon_power = row['Watermelon_Fire']
            
            red_score = (
                int(row['OBV_Rising']) +
                int(row['MFI_Strong']) +
                int(row['Buying_Pressure'])
            )
            
            #상단저항선 터치횟수
            total_hammering = row['Total_hammering']
            #최근20일간 매집봉 카운트
            maejip_count =                row['Maejip_Count']
            #볼린저밴드 20,40 골든크로스
            jongbe_break = row['Jongbe_Break']
            #MA밀집도
            converge = df['Converge']

            # 3. 점수 산출 및 태그 부여
            s_score = 100
            tags = []
            print(f"✅ [본진] 라운드넘버 계산!")
            # 라운드넘버 정거장 매매법 => 현재가 기준 정거장 파악
            lower_rn, upper_rn = get_target_levels(row['Close'])
            avg_money = (row['Close'] * row['Volume']) # 간이 거래대금
            is_leader = avg_money >= 100000000000 # 1,000억 기준 (시장 상황에 따라 조정)
            is_1st_buy = False
            is_2nd_buy = False
            is_rapid_target = False
            is_rn_signal = False
            
            if lower_rn and upper_rn:
                # 🕵️ 조건 A: 최근 20일 내에 위 정거장(+4%)을 터치했었나?
                # (세력이 위쪽 물량을 체크하고 내려왔다는 증거)
                lookback_df = df.iloc[max(0, raw_idx-20) : raw_idx]
                hit_upper = any(lookback_df['High'] >= upper_rn * 1.04)
                
                # 🕵️ 조건 B: 현재 아래 정거장 근처(±4%)에 도달했나?
                # (분할 매수 1차 타점 진입)
                at_lower_station = lower_rn * 0.96 <= row['Close'] <= lower_rn * 1.04
                
                # 🏆 [최종 판정] '정거장 회귀' 신호
                is_rn_signal = hit_upper and at_lower_station
              
            if lower_rn:
                # 🚩 [신호 발생] 최근 20일간 정거장 대비 +30% 상단선을 터치했는가?
                # 예: 10,000원 정거장 기준 13,000원 돌파 이력 체크
                signal_line_30 = lower_rn * 1.30
                lookback_df = df.iloc[max(0, raw_idx-20) : raw_idx]
                has_surged_30 = any(lookback_df['High'] >= signal_line_30)
            
                # 🎯 [급등존 설정] Round Number ±4% 구간
                zone_upper = lower_rn * 1.04
                zone_lower = lower_rn * 0.96
            
                # 🚀 [1차 매수 타점] 급등 후 조정받아 급등존 상단 터치
                is_1st_buy = has_surged_30 and (row['Low'] <= zone_upper <= row['High'])
                
                # 🚀 [2차 매수 타점] 급등존 하단 터치
                is_2nd_buy = has_surged_30 and (row['Low'] <= zone_lower <= row['High'])
            
                if is_1st_buy:
                    tags.append("🚀급등_1차타점")
                    s_score += 100 # 급등주 전술이므로 높은 가점
                if is_2nd_buy:
                    tags.append("🚀급등_2차타점")
                    s_score += 120 # 비중을 더 싣는 구간
            
                # 결과 전송을 위한 데이터 저장
                rn_signal_data = {
                    'base_rn': lower_rn,
                    'is_rapid': has_surged_30,
                    'status': "급등존진입" if zone_lower <= row['Close'] <= zone_upper else "관찰중"
                }
              
            # 라운드 넘버
            if is_rn_signal:
                tags.append("🚉정거장회귀")
                s_score += 70 # 강력한 매수 근거로 활용
            
            # 기존 시그널들
            if is_diamond:
                s_score += 150
                tags.append("💎다이아몬드")
                if t_pct < 10:
                    s_score += 50
                    tags.append("🔥폭발직전")
            elif is_cloud_brk:
                s_score += 40
                tags.append("☁️구름돌파")

            if is_super_squeeze: 
                s_score += 40
                tags.append("🔋초강력응축")
                
            if is_vol_power: 
                s_score += 30
                tags.append("⚡거래폭발")
            
            # 💡 매집 시그널 체크
            acc_count = sum([acc_1_obv_rising, acc_2_box_range, acc_3_macd_golden,
                           acc_4_rsi_healthy, acc_5_sto_golden])
            
            if acc_count >= 4:
                s_score += 60
                tags.append("🐋세력매집")
            elif acc_count >= 3:
                s_score += 30
                tags.append("🐋매집징후")
                
            if acc_1_obv_rising:
                tags.append("📊OBV상승")
            
            # 조용한 매집
            if silent_count >= 5:
                s_score += 100
                tags.append("🤫조용한매집완전")
            elif silent_count >= 4:
                s_score += 60
                tags.append("🤫조용한매집강")
            elif silent_count >= 3:
                s_score += 30
                tags.append("🤫조용한매집약")

            # 세부 조건 태그
            if silent_1_atr_low:
                tags.append("🔇ATR수축")
            if silent_2_mfi_strong and silent_3_mfi_rising:
                tags.append("💰MFI강세")

            # RSI 정보
            rsi_val = row['RSI']
            if rsi_val >= 80:
                tags.append("🔥RSI강세")
                s_score += 10
            elif rsi_val >= 70:
                tags.append("📈RSI상승")
            elif rsi_val >= 50:
                tags.append("✅RSI중립상")
            elif rsi_val >= 30:
                tags.append("📉RSI하락")
            else:
                tags.append("❄️RSI약세")

            #수박지표
            if watermelon_red2:
                tags.append(f"📍수박지표검증")
            if is_hot_watermelon:
                tags.append(f"🍉진짜수박 화력 {watermelon_power}")
            if is_watermelon:
                s_score += 100
                tags.append("🍉수박신호")
                tags.append(f"🍉빨강전환(강도{red_score}/3)")
                tags.append(f"🍉강도{watermelon_score}/3")
            elif watermelon_color == 'red' and red_score >= 2:
                s_score += 60
                tags.append("🍉빨강상태")    
            elif row['Green_Days_10'] >= 7:
                s_score += 30
                tags.append("🍉초록축적")

            # 기존 감점 로직
            if t_pct > 40:
                s_score -= 25
                tags.append("⚠️윗꼬리")

            # 세부 태그
            if silent_1_atr:
                tags.append(f"🔇ATR조용{int(row['ATR_Below_Days'])}일")
            if silent_2_mfi_persist:
                tags.append(f"💰MFI강세{int(row['MFI_Strong_Days'])}일")
            
            # 역매공파 바닥권
            if bottom_area:
                s_score += 80
                tags.append("🏆112선바닥권")
                tags.append(f"📍거리{row['Near_MA112']:.1f}%")
            
            # 폭발 직전
            if explosion_ready:
                s_score += 90
                tags.append("💎폭발직전")
            
            # 최강 조합
            if is_watermelon and explosion_ready and bottom_area:
                s_score += 80
                tags.append("💎💎💎스윙골드")

            # 기상도 감점
            storm_count = sum([1 for m in ['ixic', 'sp500'] if row[f'{m}_close'] <= row[f'{m}_ma5']])
            s_score -= (storm_count * 20)
            s_score -= max(0, int((row['Disparity']-108)*5)) 
            
            #print(f"🕵️ [분석 중] {name}: {conviction}점 | 서사: {narrative}")

            # 4. 💡 수익률 검증 데이터 생성 (최고/최저 추가)
            h_df = df.iloc[raw_idx+1:]
            
            if not h_df.empty:
                max_r = ((h_df['High'].max() - close_p) / close_p) * 100
                min_r = ((h_df['Low'].min() - close_p) / close_p) * 100

                max_close_series = h_df['Close']
                max_close_val = max_close_series.max() # 최고가(종가)
                max_date_ts = max_close_series.idxmax() # 최고가인 날의 Timestamp
                # 📅 날짜 포맷팅 (예: 2024-05-20)
                max_r_date = max_date_ts.strftime('%Y-%m-%d')

                # ⏳ 도달 소요 시간 (보유일 기준 몇 일째에 최고점이었나?)
                days_to_max = (max_date_ts - curr_idx).days
    
                # 💡 오늘이면 현재가 = 오늘 종가, 아니면 해당 시점의 마지막 종가
                is_today = (len(h_df) == 0)  # 보유일 0이면 오늘
                current_price = today_price if not is_today else close_p
            else:
                max_r = 0
                min_r = 0
                current_price = close_p
                max_date_ts = curr_idx.strftime('%Y-%m-%d')

            hits.append({
                '날짜': curr_idx.strftime('%Y-%m-%d'),
                '👑등급': grade,
                'N등급': f"{result['type']}{result['grade']}",
                'N점수': result['score'],
                'N조합': result['combination'],
                '정류장': is_rn_signal | is_1st_buy | is_2nd_buy,
                  # 👈 서사 엔진 결과물 1
                '📜서사히스토리': narrative,    # 👈 서사 엔진 결과물 2
                '확신점수': conviction,        # 👈 서사 엔진 결과물 3
                '🎯목표타점': int(target),      # 👈 서사 기반 타점
                '🚨손절가': int(stop),         # 👈 서사 기반 손절가
                '기상': "☀️" * (2-storm_count) + "🌪️" * storm_count,
                '안전점수': int(max(0, s_score + whale_score)),
                '대칭비율': dante_data_ratio,
                '매집봉': dante_data_mae_jip,
                'D20매집봉' : maejip_count,
                '저항터치': total_hammering,
                'BB-GC': jongbe_break,
                '섹터': sector,
                '종목': name,
                '매입가': int(close_p),
                '현재가': int(current_price),
                'RSI' : rsi_val,
                '꼬리%': t_pct,
                '이격': int(row['Disparity']),
                'BB40': f"{row['BB40_Width']:.1f}",
                'MA수렴': f"{row['MA_Convergence']:.1f}",
                '매집': f"{acc_count}/5",
                '최고수익날': max_r_date,
                '소요기간': days_to_max,
                '최고수익률%': f"{max_r:+.1f}%",
                '최저수익률%': f"{min_r:+.1f}%",
                '최고수익률_raw': max_r,
                '최저수익률_raw': min_r,
                'N구분': " ".join(new_tags),
                '구분': " ".join(tags),
                '보유일': len(h_df),
                # ✅ 신규 컬럼 추가
                '삼각패턴':   tri_result['triangle_pattern'] if tri_result else 'None',
                '삼각수렴%':  tri_result['triangle']['convergence_pct'] if tri_result and tri_result.get('triangle') else 0,
                '꼭지잔여':   tri_result['apex_remain'] if tri_result else 'N/A',
                '종베GC':    tri_result['jongbe'] if tri_result else False,
                '삼각점수':   tri_result['score'] if tri_result else 0,
                '삼각등급':   tri_result['grade'] if tri_result else 'N/A',
            })
        return hits
    except Exception as e:
        print(f"🚨 [본진] 데이터 로드 실패: {e}")
        print(f"✅ [본진] 오류!")
        return []
# ---------------------------------------------------------
# 단타/스윙 분리형 시퀀스        
# ---------------------------------------------------------
def classify_style(row):
    vol_ratio = row['ATR'] / row['Close']

    if vol_ratio > 0.05:
        return "SCALP"   # 단타
    elif row['BB40_Width'] < 12 and row['MA_Convergence'] < 3:
        return "SWING"
    else:
        return "NONE"

# ---------------------------------------------------------
# 💾 [엑셀 저장] 오늘의 추천종목 저장
# ---------------------------------------------------------
def save_today_recommendations(df_today, recommendation_info):
    """오늘의 추천종목을 엑셀로 저장"""
    try:
        filename = f"추천종목_{TODAY_STR}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # 시트1: 오늘의 추천 종목
            df_today.to_excel(writer, sheet_name='오늘의_추천', index=False)
            
            # 시트2: 추천 정보
            if recommendation_info:
                rec_df = pd.DataFrame([recommendation_info])
                rec_df.to_excel(writer, sheet_name='추천_패턴_정보', index=False)
        
        print(f"\n💾 엑셀 저장 완료: {filename}")
        return filename
    except Exception as e:
        print(f"\n❌ 엑셀 저장 실패: {e}")
        return None

# =================================================
# 🚀 [실행] 메인 컨트롤러 (수정 버전)
# =================================================
if __name__ == "__main__":
    print(f"📡 [Ver 36.7] {TODAY_STR} 전술 사령부 통합 가동...")
    
    try:
        # 1. 기본 환경 및 데이터 로드
        #global_env, leader_env = get_global_and_leader_status()
        status = get_global_and_leader_status()

        # 데이터가 아예 없거나(None), 내용이 없는 경우를 대비한 방어막
        if status is None or not status:
            print("⚠️ [주의] 글로벌/대장주 데이터를 가져오지 못했습니다. 기본값으로 진행합니다.")
            global_env = {"status": "UNKNOWN", "score": 50} # 기본 중립 상태
            leader_env = []                                 # 빈 리스트로 초기화
        else:
            # 데이터가 정상일 때만 언패킹 진행
            global_env, leader_env = status
            print("✅ [성공] 시장 환경 데이터 로드 완료.")

        df_krx = fdr.StockListing('KRX')
        if df_krx is None or not df_krx:
            print("⚠️ KRX 데이터를 가져오지 못했습니다.")

        # 위키피디아에서 나스닥 100 티커 자동 수집 (이전에 만든 함수 활용)
        nasdaq_100_list = get_nasdaq100_tickers() 
        # 데이터프레임 형태로 변환 (기존 코드와 호환성을 위해)
        df_us_all = pd.DataFrame({
                'Symbol': nasdaq_100_list,
                'Name': nasdaq_100_list  # 이름 데이터가 없으면 티커로 대체
            })
        print(f"✅ [글로벌 전면전] 총 {len(df_us_all)}개 미국 종목 확보")

        # 2. 국내주식 정제 및 타겟팅
        df_clean = df_krx[df_krx['Market'].isin(['KOSPI', 'KOSDAQ'])]
        df_clean = df_clean[~df_clean['Name'].str.contains('ETF|ETN|스팩|제[0-9]+호|우$|우A|우B|우C')]
        
        # 💰 거래대금 상위 추출 (국내)
        target_stocks = df_clean.sort_values(by='Amount', ascending=False).head(TOP_N)
        
        # 💰 시가총액 상위 추출 (미국) - 미국 fdr 데이터는 Marcap 기준이 안정적입니다.
        target_Nasdaq_stocks = df_us_all.head(TOP_N)

        # 3. 매크로 및 기상 데이터
        macro_status = {
            'nasdaq': get_safe_macro('^IXIC', '나스닥'),
            'sp500': get_safe_macro('^GSPC', 'S&P500'),
            'vix': get_safe_macro('^VIX', 'VIX공포'),
            'fx': get_safe_macro('USD/KRW', '달러환율'),
            'kospi': get_index_investor_data('KOSPI')
        }
        weather_data = prepare_historical_weather()
        sector_master_map = df_krx.set_index('Code')['Sector'].to_dict() if 'Sector' in df_krx.columns else {}

        # 4. [국내전] 스캔
        all_hits = []
        print(f"🔍 [국내] {len(target_stocks)}개 종목 레이더 가동...")
        with ThreadPoolExecutor(max_workers=15) as executor:
            results = list(executor.map(
                lambda p: analyze_final(p[0], p[1], weather_data, global_env, leader_env, sector_master_map), 
                zip(target_stocks['Code'], target_stocks['Name'])
            ))
            all_hits = [item for r in results if r for item in r]
        
        analyze_save_googleSheet(all_hits, False)

        # 5. [나스닥전] 스캔
        all_Nasdaq_hits = []
        print(f"🔍 [미국] {len(target_Nasdaq_stocks)}개 종목 레이더 가동...")
        with ThreadPoolExecutor(max_workers=15) as executor:
            # 미국 데이터프레임은 'Symbol'과 'Name' 컬럼을 사용합니다.
            results = list(executor.map(
                lambda p: analyze_final(p[0], p[1], weather_data, global_env, leader_env, {}), 
                zip(target_Nasdaq_stocks['Symbol'], target_Nasdaq_stocks['Name'])
            ))
            all_Nasdaq_hits = [item for r in results if r for item in r]
            
        analyze_save_googleSheet(all_Nasdaq_hits, True)
        
    except Exception as main_error:
        print(f"🚨 [치명적 오류] 메인 엔진 정지: {main_error}")
