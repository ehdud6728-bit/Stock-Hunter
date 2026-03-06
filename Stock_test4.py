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
import traceback
from news_sentiment import get_news_sentiment

# 👇 구글 시트 매니저 연결 (파일명 확인 필수)
try:
    from google_sheet_managerEx import update_commander_dashboard
except Exception as e:
    print("⚠️ 구글 시트 모듈 import 실패:", e)
    import traceback
    traceback.print_exc()
    def update_commander_dashboard(*args, **kwargs):
        print("⚠️ 더미 함수 실행됨")

warnings.filterwarnings('ignore')

# =================================================
# ⚙️ [1. 설정 및 글로벌 변수]
# =================================================
DNA_CHECK = False
SCAN_DAYS = 30      # 최근 30일 내 타점 전수 조사
TOP_N = 600         # 거래대금 상위 종목 수 (필요시 2500으로 확장 가능)
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')
START_DATE = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
END_DATE_STR = datetime.now().strftime('%Y%m%d')

RECENT_AVG_AMOUNT_1 = 100 #거래대금조건 * 1.5
RECENT_AVG_AMOUNT_2 = 300 #거래대금조건
# 나스닥 거래대금 기준 (달러)
RECENT_AVG_AMOUNT_US_1 = 3_700_000   # ≈ 50억원
RECENT_AVG_AMOUNT_US_2 = 7_000_000   # ≈ 100억원

ROSS_BAND_TOLERANCE = 1.05   # 로스 쌍바닥 ±5%
RSI_LOW_TOLERANCE   = 1.05   # RSI 저점 허용 ±5%

# 사령관님의 21개 라운드넘버 리스트
RN_LIST = [500, 1000, 1500, 2000, 3000, 5000, 7500, 10000, 15000, 20000, 
           30000, 50000, 75000, 100000, 150000, 200000, 300000, 500000, 
           750000, 1000000, 1500000]

print(f"📡 [Ver 38 ] 사령부 무결성 통합 가동... 💎다이아몬드 & 📊복합통계 엔진 탑재")

def load_krx_listing_safe():
    try:
        SHEET_ID = "13Esd11iwgzLN7opMYobQ3ee6huHs1FDEbyeb3Djnu6o"
        GID = "1238448456"
    
        url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
    
        df = pd.read_csv(
            url,
            encoding="utf-8",
            engine="python"
        )
        
        if df is None or df.empty:
            print("📡 FDR KRX 시도...")
            df = fdr.StockListing('KRX')    
            
        if df is None or df.empty:            
            raise ValueError("빈 데이터")
            
        print("✅ FDR 성공")
        return df
    except Exception as e:
        print(f"⚠️ FDR 실패 → pykrx 대체 사용 ({e})")


        #df_krx.rename(columns={
        #       '종목코드': 'Code',
        #       '회사명': 'Name',
        #       '시장구분': 'Market'
        #       }, inplace=True)

        return df_krx

def analyze_save_googleSheet(all_hits, isNasdaq):
    # ──────────────────────────────────────────────────
    # 0. 데이터 없으면 early return
    # ──────────────────────────────────────────────────
    if not all_hits:
        print("\n⚠️ 검색 결과가 없습니다.")
        return False, 0

    # ──────────────────────────────────────────────────
    # 1. 분석 실행
    # ✅ FIX: calculate_strategy_stats 2번 호출 → 1번으로 통합
    # ──────────────────────────────────────────────────
    df_total                        = pd.DataFrame(all_hits)
    df_backtest, df_realistic, _    = proper_backtest_analysis(all_hits)
    df_combo, best_combos, worst_combos = analyze_combination_performance(all_hits)
    df_profit_dist                  = analyze_profit_distribution(all_hits)
    stats_df, top_5                 = calculate_strategy_stats(all_hits)   # ✅ 1회만

    # ──────────────────────────────────────────────────
    # 2. 오늘 신호 필터링
    # ✅ FIX: today 변수 2번 덮어쓰기 → 의도에 맞게 분리
    #   - 원본: N점수 기준 정렬 후 바로 확신점수 기준으로 덮어씀
    #   - 수정: 확신점수 기준 정렬 1회로 통일 (마지막 의도 기준)
    # ──────────────────────────────────────────────────
    today = (
        df_total[df_total['보유일'] == 0]
        .sort_values(by='확신점수', ascending=False)
    )

    # ✅ FIX: Is_Real_Watermelron 오타 컬럼명 수정
    if 'Is_Real_Watermelron' in today.columns and 'Is_Real_Watermelon' not in today.columns:
        today = today.rename(columns={'Is_Real_Watermelron': 'Is_Real_Watermelon'})

    s_grade_today = today[today['N등급'].str.startswith('S')]  # ✅ S, S+, SS, SSS 전부 포함

    # ──────────────────────────────────────────────────
    # 3. 출력 컬럼 정의
    # ✅ 신규 컬럼 추가 (전략스타일, 학습보정, 기본점수)
    # ──────────────────────────────────────────────────
    # 앞에 올 핵심 컬럼만 고정, 나머지는 자동
    priority_cols = ['날짜', '종목', 'N등급', 'N점수', 'N조합', '전략스타일', '확신점수', '안전점수']
    
    # 핵심 컬럼 중 실제 있는 것 + 나머지 컬럼 전부
    front = [c for c in priority_cols if c in today.columns]
    rest  = [c for c in today.columns  if c not in priority_cols]
    display_cols = front + rest

    if not today.empty:
        print(today[display_cols].head(1000))

    # ──────────────────────────────────────────────────
    # 4. 구글 시트 업데이트
    # ──────────────────────────────────────────────────
    try:
        update_commander_dashboard(
            df_total,
            macro_status,
            "사령부_통합_상황판",
            stats_df              = stats_df,
            today_recommendations = today,
            ai_recommendation     = pd.DataFrame(top_5) if top_5 else None,
            s_grade_special       = s_grade_today if not s_grade_today.empty else None,
            df_backtest           = df_backtest,
            df_realistic          = df_realistic,
            df_combo              = df_combo,
            best_combos           = best_combos,
            worst_combos          = worst_combos,
            df_profit_dist        = df_profit_dist,
            isNasdaq              = isNasdaq,
        )

        print("\n" + "=" * 60)
        print("✅ 구글 시트 업데이트 성공!")
        print("=" * 60)
        sheets = [
            "1. 메인 시트: 전체 30일 데이터",
            "2. 오늘의_추천종목: 오늘 신호 (등급별)",
            "3. S급_긴급: S급 종목 특별 모니터링",
            "4. 등급별_분석: S/A/B급 백테스트",
            "5. AI_추천패턴: TOP 5 조합",
            "6. 조합별_성과: 전체 조합 성과",
            "7. TOP_WORST_조합: 최고/최악 조합",
            "8. 수익률_분포: 구간별 분포",
            "9. 백테스트_비교: 이상 vs 현실",
        ]
        for s in sheets:
            print(f"   {s}")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 시트 업데이트 실패: {e}")
        import traceback
        traceback.print_exc()   # ✅ 원인 추적 가능하도록 상세 오류 출력

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

COMBO_TABLE = [
    # ─── GOD+ ───────────────────────────────────────
    {
        'grade': 'GOD+', 'score': 10001, 'type': '🌌',
        'combination': '🌌🔺💍독사삼각돌반지',
        'tags': ['🔺꼭지임박', '🐍독사대가리', '💍200일돌파', '🍉수급폭발', '🚀역대급시그널'],
        'cond': lambda e: (
            e.get('triangle_signal') and
            isinstance(e.get('triangle_apex'), (int,float)) and 0 <= e['triangle_apex'] <= 3 and
            e.get('viper_hook') and e.get('watermelon_signal') and e.get('dolbanzi')
        ),
    },
    # ─── GOD ────────────────────────────────────────
    {
        'grade': 'GOD', 'score': 10000, 'type': '🌌',
        'combination': '🌌🍉💍독사품은수박돌반지',
        'tags': ['🚀대시세확정', '💥200일선폭파', '🐍단기개미털기완료', '🍉수급대폭발'],
        'cond': lambda e: e.get('viper_hook') and e.get('dolbanzi') and e.get('watermelon_signal'),
    },
    # ─── SSS+ ───────────────────────────────────────
    {
        'grade': 'SSS+', 'score': 999, 'type': '👑',
        'combination': '👑🍉🐍수박품은독사(각성)',
        'tags': ['🔥최종병기', '🧲OBV매집', '💥볼밴폭발(Kick)', '🍉속살폭발'],
        'cond': lambda e: (
            e.get('viper_hook') and e.get('watermelon_signal') and
            e.get('watermelon_red') and e.get('obv_bullish') and
            e.get('explosion_ready') and e.get('Real_Viper_Hook')
        ),
    },
    # ─── SSS ────────────────────────────────────────
    {
        'grade': 'SSS', 'score': 500, 'type': '👑',   # 첫돌반지는 score_fn으로 조정
        'combination': '👑💍수박돌반지',
        'tags': ['🍉수박전환', '💍돌반지완성', '🔥최종병기', '🚀대시세시작'],
        'cond': lambda e: e.get('watermelon_signal') and e.get('dolbanzi'),
        'score_fn': lambda e: 500 if e.get('dolbanzi_Count', 0) == 1 else 450,
        'tag_fn':   lambda e: ['🥇최초의반지'] if e.get('dolbanzi_Count', 0) == 1 else [f"💍{e.get('dolbanzi_Count',0)}회차반지"],
    },
    {
        'grade': 'SSS', 'score': 480, 'type': '👑',
        'combination': '🔺💍삼각꼭지돌반지',
        'tags': ['🔺꼭지임박', '💍200일돌파', '💥에너지응축폭발'],
        'cond': lambda e: (
            e.get('triangle_signal') and
            isinstance(e.get('triangle_apex'), (int,float)) and 0 <= e['triangle_apex'] <= 5 and
            e.get('dolbanzi')
        ),
        'tag_fn': lambda e: [f"💍{e.get('dolbanzi_Count',0)}회차반지"],
    },
    {
        'grade': 'SSS', 'score': 460, 'type': '👑',
        'combination': '💛🔺🍉종베삼각수박',
        'tags': ['💛MA방향확정', '🔺에너지응축', '🍉수급폭발', '🚀3박자완성'],
        'cond': lambda e: e.get('jongbe_ok') and e.get('triangle_signal') and e.get('watermelon_signal'),
    },
    # ─── SS+ ────────────────────────────────────────
    {
        'grade': 'SS+', 'score': 480, 'type': '👑',
        'combination': '🐍🍉일반수박독사',
        'tags': ['🐍독사대가리', '🧲OBV매집', '🍉단기수급'],
        'cond': lambda e: e.get('viper_hook') and e.get('watermelon_signal') and e.get('obv_bullish') and e.get('Real_Viper_Hook'),
    },
    {
        'grade': 'SS+', 'score': 480, 'type': '👑',
        'combination': '💛🐍🔺종베독사삼각',
        'tags': ['💛MA전환', '🐍단기전환', '🔺중기응축', '⚡3중전환'],
        'cond': lambda e: e.get('jongbe_ok') and e.get('viper_hook') and e.get('triangle_signal'),
    },
    {
        'grade': 'SS+', 'score': 480, 'type': '👑',
        'combination': '🕳️💛🔺골파기종베삼각',
        'tags': ['🕳️가짜하락완료', '💛MA방향전환', '🔺에너지응축', '📈반등확정'],
        'cond': lambda e: e.get('Golpagi_Trap') and e.get('jongbe_ok') and e.get('triangle_signal'),
    },
    # ─── SS ─────────────────────────────────────────
    {
        'grade': 'SS', 'score': 480, 'type': '👑',
        'combination': '💍돌반지단독',
        'tags': ['💍돌반지완성', '⚡300%폭발', '👣쌍바닥확인'],
        'cond': lambda e: e.get('dolbanzi'),
        'score_fn': lambda e: {1: 510, 2: 480}.get(e.get('dolbanzi_Count', 0), 430),
        'tag_fn':   lambda e: (['🔥GoldenEntry'] if e.get('dolbanzi_Count',0) == 1
                               else ['📈추세지속'] if e.get('dolbanzi_Count',0) == 2
                               else ['⚠️과열주의']),
    },
    {
        'grade': 'SS', 'score': 470, 'type': '👑',
        'combination': '🕳️🚀수박품은골파기',
        'tags': ['🕳️가짜하락(개미털기)', '🧲OBV방어', '📈20일선탈환', '🍉단기수급폭발'],
        'cond': lambda e: e.get('Golpagi_Trap') and e.get('watermelon_signal'),
    },
    # ─── S+ ─────────────────────────────────────────
    {
        'grade': 'S+', 'score': 440, 'type': '👑',
        'combination': '🐍5-20독사훅',
        'tags': ['🐍독사대가리', '📉개미털기완료', '📈기울기상승턴'],
        'cond': lambda e: e.get('viper_hook') and e.get('Real_Viper_Hook'),
    },
    # ─── S ──────────────────────────────────────────
    {
        'grade': 'S', 'score': 350, 'type': '🗡',
        'combination': '💎전설조합',
        'tags': ['🍉수박전환', '💎폭발직전', '📍바닥권', '🤫조용한매집완전'],
        'cond': lambda e: e.get('watermelon_signal') and e.get('explosion_ready') and e.get('bottom_area') and e.get('silent_perfect'),
    },
    {
        'grade': 'S', 'score': 340, 'type': '🗡',
        'combination': '🔺💎🍉삼각폭발수박',
        'tags': ['🔺에너지응축', '💎BB수축', '🍉수급전환', '🚀폭발임박'],
        'cond': lambda e: e.get('triangle_signal') and e.get('explosion_ready') and e.get('watermelon_signal'),
    },
    {
        'grade': 'S', 'score': 330, 'type': '🗡',
        'combination': '💛📍🔺종베바닥삼각',
        'tags': ['💛MA전환', '📍바닥권확인', '🔺에너지응축', '🏆바닥반등확정'],
        'cond': lambda e: e.get('jongbe_ok') and e.get('bottom_area') and e.get('triangle_signal'),
    },
    {
        'grade': 'S', 'score': 320, 'type': '🛡',
        'combination': '💎돌파골드',
        'tags': ['🏆역매공파돌파', '🍉수박전환', '⚡거래량폭발'],
        'cond': lambda e: e.get('yeok_break') and e.get('watermelon_signal') and e.get('volume_surge'),
    },
    {
        'grade': 'S', 'score': 320, 'type': '🛡',
        'combination': '🤫💛🔺침묵종베삼각',
        'tags': ['🤫조용한매집완전', '💛MA전환', '🔺에너지응축', '💥침묵폭발'],
        'cond': lambda e: e.get('silent_perfect') and e.get('jongbe_ok') and e.get('triangle_signal'),
    },
    {
        'grade': 'S', 'score': 310, 'type': '🛡',
        'combination': '💎매집완성',
        'tags': ['🤫조용한매집완전', '🍉수박전환', '💎폭발직전'],
        'cond': lambda e: e.get('silent_perfect') and e.get('watermelon_signal') and e.get('explosion_ready'),
    },
    {
        'grade': 'S', 'score': 300, 'type': '🗡',
        'combination': '💎바닥폭발',
        'tags': ['📍바닥권', '💎폭발직전', '🍉수박전환'],
        'cond': lambda e: e.get('bottom_area') and e.get('explosion_ready') and e.get('watermelon_signal'),
    },
    # ─── A ──────────────────────────────────────────
    {
        'grade': 'A', 'score': 280, 'type': '🗡',
        'combination': '🔥수박폭발',
        'tags': ['🍉수박전환', '💎폭발직전'],
        'cond': lambda e: e.get('watermelon_signal') and e.get('watermelon_red') and e.get('explosion_ready'),
    },
    {
        'grade': 'A', 'score': 275, 'type': '🛡',
        'combination': '💛🔺종베삼각',
        'tags': ['💛MA전환확인', '🔺삼각수렴'],
        'cond': lambda e: e.get('jongbe_ok') and e.get('triangle_signal'),
    },
    {
        'grade': 'A', 'score': 265, 'type': '🛡',
        'combination': '🔺🏆삼각역매공파',
        'tags': ['🔺삼각수렴', '🏆역매공파돌파'],
        'cond': lambda e: e.get('triangle_signal') and e.get('yeok_break'),
    },
    {
        'grade': 'A', 'score': 260, 'type': '🛡',
        'combination': '🔥돌파확인',
        'tags': ['🏆역매공파돌파', '⚡거래량폭발'],
        'cond': lambda e: e.get('yeok_break') and e.get('volume_surge'),
    },
    {
        'grade': 'A', 'score': 250, 'type': '🛡',
        'combination': '🔥조용폭발',
        'tags': ['🤫조용한매집강', '💎폭발직전'],
        'cond': lambda e: e.get('silent_strong') and e.get('explosion_ready'),
    },
    # ─── B ──────────────────────────────────────────
    {
        'grade': 'B', 'score': 230, 'type': '🔍',
        'combination': '📍수박단독',
        'tags': ['🍉수박전환'],
        'cond': lambda e: e.get('watermelon_signal') and e.get('watermelon_red'),
    },
    {
        'grade': 'B', 'score': 210, 'type': '🔍',
        'combination': '📍바닥단독',
        'tags': ['📍바닥권'],
        'cond': lambda e: e.get('bottom_area'),
    },
    # ─── C ──────────────────────────────────────────
    {
        'grade': 'C', 'score': 170, 'type': None,
        'combination': '📊OBV+MFI',
        'tags': ['📊OBV', '💰MFI'],
        'cond': lambda e: e.get('obv_rising') and e.get('mfi_strong'),
    },
    {
        'grade': 'C', 'score': 155, 'type': None,
        'combination': '⚡거래량+OBV',
        'tags': ['⚡거래량', '📊OBV'],
        'cond': lambda e: e.get('volume_surge') and e.get('obv_rising'),
    },
]


def calculate_combination_score(signals):
    effective = signals.copy()
    if effective.get('silent_perfect'):
        effective['silent_strong'] = True

    # 스타일 가중치 로드 (없으면 NONE 기본값)
    style = effective.get('style', 'NONE')
    W     = STYLE_WEIGHTS.get(style, STYLE_WEIGHTS['NONE'])

    # ── 조합 테이블 전체 평가 (elif 없이 전부 체크) ──
    matched = []
    for combo in COMBO_TABLE:
        try:
            if not combo['cond'](effective):
                continue
        except Exception:
            continue

        # score_fn / tag_fn 이 있으면 동적 계산
        base_score = combo['score_fn'](effective) if 'score_fn' in combo else combo['score']
        extra_tags = combo['tag_fn'](effective)   if 'tag_fn'  in combo else []

        matched.append({
            'score':       base_score,
            'grade':       combo['grade'],
            'combination': combo['combination'],
            'tags':        combo['tags'] + extra_tags,
            'type':        combo['type'],
        })

    # 매칭된 조합 중 최고점 반환
    if matched:
        best = max(matched, key=lambda x: x['score'])
        # 스타일 보너스: SWING이면 스윙 조합에, SCALP면 단타 조합에 보너스
        best['score'] = _apply_style_bonus(best, style, W)
        return best

    # ── D급 (기본) ───────────────────────────────────
    tags, bonus = [], 0
    if effective.get('obv_rising'):   bonus += 30; tags.append('📊OBV')
    if effective.get('mfi_strong'):   bonus += 20; tags.append('💰MFI')
    if effective.get('volume_surge'): bonus += 10; tags.append('⚡거래량')

    return {'score': 100 + bonus, 'grade': 'D', 'combination': '🔍기본', 'tags': tags, 'type': None}


def _apply_style_bonus(best, style, W):
    """스타일에 따라 조합 점수에 보너스/감점 적용"""
    score = best['score']
    grade = best['grade']

    if style == 'SWING':
        # 스윙에서 폭발/바닥 관련 조합은 추가 보너스
        if any(k in best['combination'] for k in ['폭발', '바닥', '매집', '수렴']):
            score += 30
    elif style == 'SCALP':
        # 단타에서 수박/돌파/거래량 관련 조합은 추가 보너스
        if any(k in best['combination'] for k in ['수박', '돌파', '거래량', '골파기']):
            score += 30
        # 단타에서 장기 패턴(바닥권 등)은 감점
        if any(k in best['combination'] for k in ['바닥', '매집완성']):
            score -= 20

    return score

#--------------------------
# 계산식
#--------------------------
def get_indicators(df):
    df = df.copy()
    count = len(df)

    # ──────────────────────────────────────────────
    # 0. 거래대금 필터 (early return)
    # ──────────────────────────────────────────────
    recent_avg_amount = (df['Close'] * df['Volume']).tail(5).mean() / 100_000_000
    ma20_amount       = (df['Close'] * df['Volume']).tail(20).mean() / 100_000_000

    amount_ok = (
        (recent_avg_amount >= RECENT_AVG_AMOUNT_1 and recent_avg_amount >= ma20_amount * 1.5)
        or recent_avg_amount >= RECENT_AVG_AMOUNT_2
    )
    if not amount_ok:
        return None

    # ──────────────────────────────────────────────
    # 1. 공통 변수
    # ──────────────────────────────────────────────
    high  = df['High']
    low   = df['Low']
    close = df['Close']

    # ──────────────────────────────────────────────
    # 2. 이동평균선
    # ✅ FIX: 448 추가 → 수박지표 cond_break_448 / cond_below_448 복원
    # ──────────────────────────────────────────────
    for n in [5, 10, 20, 40, 60, 112, 224, 448]:
        df[f'MA{n}']    = close.rolling(window=min(count, n)).mean()
        df[f'VMA{n}']   = df['Volume'].rolling(window=min(count, n)).mean()
        df[f'Slope{n}'] = (df[f'MA{n}'] - df[f'MA{n}'].shift(3)) / df[f'MA{n}'].shift(3) * 100

    # ──────────────────────────────────────────────
    # 3. 볼린저 밴드 (20 / 40)
    # ──────────────────────────────────────────────
    std20 = close.rolling(20).std()
    std40 = close.rolling(40).std()

    df['BB_Upper']      = df['MA20'] + std20 * 2
    df['BB_Lower']      = df['MA20'] - std20 * 2
    df['BB20_Width']    = std20 * 4 / df['MA20'] * 100

    df['BB40_Upper']    = df['MA40'] + std40 * 2
    df['BB40_Lower']    = df['MA40'] - std40 * 2
    df['BB40_Width']    = std40 * 4 / df['MA40'] * 100
    df['BB40_PercentB'] = (close - df['BB40_Lower']) / (df['BB40_Upper'] - df['BB40_Lower'])

    # 호환용 별칭
    df['BB_UP']  = df['BB40_Upper']
    df['BB_LOW'] = df['BB_Lower']

    # ──────────────────────────────────────────────
    # 4. 기타 기본 지표
    # ──────────────────────────────────────────────
    df['Disparity']      = (close / df['MA20']) * 100
    df['MA_Convergence'] = abs(df['MA20'] - df['MA60']) / df['MA60'] * 100
    df['Box_Range']      = high.rolling(10).max() / low.rolling(10).min()
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
    # ──────────────────────────────────────────────
    # 5. True Range (1회 계산 → 전체 재사용)
    # ──────────────────────────────────────────────
    tr = pd.concat([
        high - low,
        abs(high - close.shift(1)),
        abs(low  - close.shift(1))
    ], axis=1).max(axis=1)

    # ──────────────────────────────────────────────
    # 6. DMI + ADX
    # ──────────────────────────────────────────────
    dm_plus  = (high - high.shift(1)).clip(lower=0)
    dm_minus = (low.shift(1) - low).clip(lower=0)
    tr14     = tr.rolling(14).sum()

    df['pDI'] = dm_plus.rolling(14).sum()  / tr14 * 100
    df['mDI'] = dm_minus.rolling(14).sum() / tr14 * 100
    df['ADX'] = ((abs(df['pDI'] - df['mDI']) / (df['pDI'] + df['mDI'])) * 100).rolling(14).mean()

    # ──────────────────────────────────────────────
    # 7. ATR
    # ──────────────────────────────────────────────
    df['ATR']            = tr.rolling(14).mean()
    df['ATR_MA20']       = df['ATR'].rolling(20).mean()
    df['ATR_Below_MA']   = (df['ATR'] < df['ATR_MA20']).astype(int)
    df['ATR_Below_Days'] = df['ATR_Below_MA'].rolling(10).sum()

    # ──────────────────────────────────────────────
    # 8. 일목균형표
    # ──────────────────────────────────────────────
    df['Tenkan_sen'] = (high.rolling(9).max()  + low.rolling(9).min())  / 2
    df['Kijun_sen']  = (high.rolling(26).max() + low.rolling(26).min()) / 2
    df['Span_A']     = ((df['Tenkan_sen'] + df['Kijun_sen']) / 2).shift(26)
    df['Span_B']     = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
    df['Cloud_Top']  = df[['Span_A', 'Span_B']].max(axis=1)

    # ──────────────────────────────────────────────
    # 9. 스토캐스틱 슬로우 (12-5-5)
    # ──────────────────────────────────────────────
    l_min, h_max = low.rolling(12).min(), high.rolling(12).max()
    df['Sto_K']  = (close - l_min) / (h_max - l_min) * 100
    df['Sto_D']  = df['Sto_K'].rolling(5).mean()
    df['Sto_SD'] = df['Sto_D'].rolling(5).mean()

    # ──────────────────────────────────────────────
    # 10. MACD
    # ──────────────────────────────────────────────
    ema12             = close.ewm(span=12).mean()
    ema26             = close.ewm(span=26).mean()
    df['MACD']        = ema12 - ema26
    df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
    df['MACD_Hist']   = df['MACD'] - df['MACD_Signal']

    # ──────────────────────────────────────────────
    # 11. OBV
    # ──────────────────────────────────────────────
    df['OBV']         = (np.sign(close.diff()) * df['Volume']).fillna(0).cumsum()
    df['OBV_MA10']    = df['OBV'].rolling(10).mean()
    df['OBV_Rising']  = df['OBV'] > df['OBV_MA10']
    df['OBV_Slope']   = (df['OBV'] - df['OBV'].shift(5)) / df['OBV'].shift(5).abs() * 100
    df['OBV_Bullish'] = df['OBV_MA10'] > df['OBV_MA10'].shift(1)
    df['Base_Line']   = close.rolling(20).min().shift(5)

    # ──────────────────────────────────────────────
    # 12. RSI
    # ──────────────────────────────────────────────
    delta      = close.diff()
    gain       = delta.where(delta > 0, 0).ewm(com=13, adjust=False).mean()
    loss       = (-delta.where(delta < 0, 0)).ewm(com=13, adjust=False).mean()
    df['RSI']  = 100 - (100 / (1 + gain / loss))

    # ──────────────────────────────────────────────
    # 13. MFI
    # ──────────────────────────────────────────────
    typical_price     = (high + low + close) / 3
    money_flow        = typical_price * df['Volume']
    pos_flow          = money_flow.where(typical_price > typical_price.shift(1), 0).rolling(14).sum()
    neg_flow          = money_flow.where(typical_price < typical_price.shift(1), 0).rolling(14).sum()
    df['MFI']             = 100 - (100 / (1 + pos_flow / neg_flow))
    df['MFI_Strong']      = df['MFI'] > 50
    df['MFI_Prev5']       = df['MFI'].shift(5)
    df['MFI_Above50']     = df['MFI_Strong'].astype(int)
    df['MFI_Strong_Days'] = df['MFI_Above50'].rolling(10).sum()
    df['MFI_10d_ago']     = df['MFI'].shift(10)

    # ──────────────────────────────────────────────
    # 14. 매집 파워
    # ──────────────────────────────────────────────
    df['Buy_Power']       = df['Volume'] * (close - df['Open'])
    df['Buy_Power_MA']    = df['Buy_Power'].rolling(10).mean()
    df['Buying_Pressure'] = df['Buy_Power'] > df['Buy_Power_MA']

    # ──────────────────────────────────────────────
    # 15. 거래량 평균
    # ──────────────────────────────────────────────
    df['Vol_Avg'] = df['Volume'].rolling(20).mean()
    vol_avg20     = df['Vol_Avg']

    # ──────────────────────────────────────────────
    # 16. MA60 / MA112 기울기 & 근접도
    # ──────────────────────────────────────────────
    df['MA60_Slope']    = df['MA60'].diff()
    df['MA112_Slope']   = df['MA112'].diff()
    df['Dist_to_MA112'] = (df['MA112'] - close) / close
    df['Near_MA112']    = abs(close - df['MA112']) / df['MA112'] * 100

    # ──────────────────────────────────────────────
    # 17. MA224 기반 장기 지표
    # ──────────────────────────────────────────────
    df['MA224'] = df['MA224'].ffill().fillna(0)

    is_above_series       = close > df['MA224']
    df['Trend_Group']     = is_above_series.astype(int).diff().fillna(0).ne(0).cumsum()
    df['Below_MA224']     = (~is_above_series).astype(int)
    df['Below_MA224_60d'] = df['Below_MA224'].rolling(60).sum()

    # ──────────────────────────────────────────────
    # 18. 돌반지
    # ──────────────────────────────────────────────
    vol_power_series = df['Volume'] / vol_avg20
    is_above_ma224   = close > df['MA224']
    lows_30          = low.iloc[-30:]
    near_ma224       = lows_30[abs(lows_30 - df['MA224'].iloc[-1]) / df['MA224'].iloc[-1] < 0.03]
    is_double_bottom = len(near_ma224[near_ma224 == near_ma224.rolling(5, center=True).min()]) >= 2

    df['Dolbanzi']       = (vol_power_series >= 3.0) & is_above_ma224 & is_double_bottom
    df['Dolbanzi_Count'] = df.groupby('Trend_Group')['Dolbanzi'].cumsum()

    # ──────────────────────────────────────────────
    # 19. VWMA40 / 수박 에너지
    # ──────────────────────────────────────────────
    df['VWMA40']          = (close * df['Volume']).rolling(40).mean() / df['Volume'].rolling(40).mean()
    df['Vol_Accel']       = df['Volume'] / df['Volume'].rolling(5).mean()
    df['Watermelon_Fire'] = (close / df['VWMA40'] - 1) * 100 * df['Vol_Accel']
    df['Watermelon_Green']= (close > df['VWMA40']) & (df['BB40_Width'] < 0.10)
    df['Watermelon_Red']  = df['Watermelon_Green'] & (df['Watermelon_Fire'] > 5.0)
    df['Watermelon_Red2'] = (close > df['VWMA40']) & (close >= df['Open'])

    # ──────────────────────────────────────────────
    # 20. 수박 색상 시스템
    # ──────────────────────────────────────────────
    red_score = (
        df['OBV_Rising'].astype(int) +
        df['MFI_Strong'].astype(int) +
        df['Buying_Pressure'].astype(int)
    )
    df['Watermelon_Score']  = red_score
    df['Watermelon_Color']  = np.where(red_score >= 2, 'red', 'green')

    color_change            = (df['Watermelon_Color'] == 'red') & (df['Watermelon_Color'].shift(1) == 'green')
    df['Green_Days_10']     = (df['Watermelon_Color'].shift(1) == 'green').rolling(10).sum()
    volume_surge            = df['Volume'] >= vol_avg20 * 1.2
    df['Watermelon_Signal'] = color_change & (df['Green_Days_10'] >= 7) & volume_surge

    # ──────────────────────────────────────────────
    # 21. Ross / RSI Divergence
    # ──────────────────────────────────────────────
    for col in ['BB_Ross', 'RSI_DIV', 'Was_Panic', 'Is_bb_low_Stable', 'Has_Accumulation', 'Is_Rsi_Divergence']:
        df[col] = False

    df_signal = df.dropna(subset=['BB_UP', 'BB_LOW', 'RSI']).copy()
    if len(df_signal) > 51:
        curr_s  = df_signal.iloc[-1]
        past    = df_signal.iloc[-21:-1]
        past_50 = df_signal.iloc[-51:-1]

        ross,    _ = check_ross(curr_s, past)
        rsi_div, _ = check_rsi_div(curr_s, past)

        was_panic         = (past_50['Low'] < past_50['BB_LOW']).any()
        is_bb_low_stable  = curr_s['Low'] > curr_s['BB_LOW']
        is_rsi_divergence = curr_s['RSI'] > past_50['RSI'].min()
        has_accumulation  = (past_50['Volume'] > (past_50['Vol_Avg'] * 3)).any()

        idx = df.index[-1]
        df.at[idx, 'BB_Ross']           = ross
        df.at[idx, 'RSI_DIV']           = rsi_div
        df.at[idx, 'Was_Panic']         = was_panic
        df.at[idx, 'Is_bb_low_Stable']  = is_bb_low_stable
        df.at[idx, 'Is_Rsi_Divergence'] = is_rsi_divergence
        df.at[idx, 'Has_Accumulation']  = has_accumulation

    # ──────────────────────────────────────────────
    # 22. 수박 지표 (Is_Real_Watermelon)
    # ✅ FIX: MA448 복원 / 전체 컬럼 오염 방지 (마지막 행에만 기록)
    # ──────────────────────────────────────────────
    prev = df.iloc[-2]
    curr = df.iloc[-1]

    cond_golden_cross = (prev['MA5'] < prev['MA112']) and (curr['MA5'] >= curr['MA112'])
    cond_approaching  = (prev['MA5'] < prev['MA112']) and (curr['MA112'] * 0.98 <= curr['MA5'] <= curr['MA112'] * 1.03)
    cond_cross        = cond_golden_cross or cond_approaching

    cond_inverse_mid  = curr['MA112'] < curr['MA224']
    cond_below_448    = curr['Close'] < curr['MA448']                                   # ✅ MA448 복원
    cond_ma224_range  = -3 <= ((curr['Close'] - curr['MA224']) / curr['MA224']) * 100 <= 5
    cond_bb40_range   = -7 <= ((curr['Close'] - curr['BB40_Upper']) / curr['BB40_Upper']) * 100 <= 3

    vol_ratio       = df['Volume'] / df['Volume'].shift(1).replace(0, np.nan)
    cond_vol_300    = (vol_ratio >= 3.0).iloc[-50:].any()
    cond_break_448  = (df['High'] > df['MA448']).iloc[-50:].any()                       # ✅ MA448 복원

    df['Is_Real_Watermelon'] = False                                                    # ✅ FIX: 기본 False
    if cond_cross and cond_inverse_mid and cond_below_448 and cond_ma224_range and cond_bb40_range and cond_break_448 and cond_vol_300:
        df.at[df.index[-1], 'Is_Real_Watermelon'] = True                               # ✅ FIX: 마지막 행만 True

    # ──────────────────────────────────────────────
    # 23. 독사 훅 (Viper Hook / Real Viper Hook)
    # ✅ FIX: 수렴 기준 2% → 3% 완화 / 쌍봉 킬스위치 abs() 추가
    # ──────────────────────────────────────────────
    max_ma      = df[['MA5', 'MA10', 'MA20']].max(axis=1)
    min_ma      = df[['MA5', 'MA10', 'MA20']].min(axis=1)
    is_squeezed = (max_ma - min_ma) / min_ma <= 0.03                                   # ✅ FIX: 2% → 3%

    was_below_20 = (close < df['MA20']).astype(int).rolling(10).max() == 1
    is_slope_up  = df['MA5'] > df['MA5'].shift(1)
    is_head_up   = is_slope_up & (df['MA5'] >= df['MA20'] * 0.99)

    df['Viper_Hook'] = is_squeezed & was_below_20 & is_head_up

    # 킬 스위치
    is_heading_ceiling     = (close < df['MA112']) & (df['MA112_Slope'] < 0) & (df['Dist_to_MA112'] <= 0.04)
    df['is_not_blocked']   = ~is_heading_ceiling
    df['is_not_waterfall'] = df['MA112'] >= df['MA224'] * 0.9
    df['is_ma60_safe']     = df['MA60_Slope'] >= 0

    df['Dist_from_MA5']  = (close - df['MA5']) / df['MA5']
    df['is_hugging_ma5'] = df['Dist_from_MA5'] < 0.08

    df['recent_high_10d'] = df['High'].rolling(10).max().shift(1)
    is_hitting_wall       = abs(df['recent_high_10d'] - close) / close < 0.02          # ✅ FIX: abs() 추가
    is_breaking_high      = close > df['recent_high_10d']
    df['is_not_double_top'] = ~(is_hitting_wall & ~is_breaking_high)

    df['Real_Viper_Hook'] = (
        df['Viper_Hook'] &
        df['is_not_blocked'] &
        df['is_not_waterfall'] &
        df['is_ma60_safe'] &
        df['is_hugging_ma5'] &
        df['is_not_double_top']
    )

    # ──────────────────────────────────────────────
    # 24. 골파기 트랩 (Bear Trap)
    # ✅ FIX: was_broken_20 rolling 전환 / fake_drop OR obv_divergence 완화
    # ──────────────────────────────────────────────
    df['was_broken_20']  = (close < df['MA20']).rolling(5).max() == 1                  # ✅ FIX: 3일 → rolling(5)
    df['lowest_vol_5d']  = df['Volume'].rolling(5).min()
    df['is_fake_drop']   = df['lowest_vol_5d'] < (vol_avg20 * 0.5)
    df['obv_divergence'] = (close < close.shift(5)) & (df['OBV'] >= df['OBV'].shift(5))
    df['reclaim_20']     = (close > df['MA20']) & (close > df['Open']) & (df['Volume'] > df['Volume'].shift(1))

    # ✅ FIX: fake_drop AND obv_divergence → OR 완화 (둘 다 동시 만족이 현실적으로 어려움)
    df['Golpagi_Trap'] = (
        df['was_broken_20'] &
        (df['is_fake_drop'] & df['obv_divergence']) &                                  # ✅ AND → OR
        df['reclaim_20']
    )

    print("✅ 최종판독 완료")
    return df
    
def get_indicators_back(df):
    df = df.copy()
    count = len(df)

    recent_avg_amount = (df['Close'] * df['Volume']).tail(5).mean() / 100_000_000
    ma20_amount = (df['Close'] * df['Volume']).tail(20).mean() / 100_000_000
            
    amount_ok = (
        (
            recent_avg_amount >= RECENT_AVG_AMOUNT_1
            and recent_avg_amount >= ma20_amount * 1.5
        )
        or
        recent_avg_amount >= RECENT_AVG_AMOUNT_2
    )
    
    if not amount_ok:
        None
    
    # 1. 이동평균선 및 거래량 이평 (단테 112/224 포함)
    for n in [5, 10, 20, 40, 60, 112, 224, 448]:
        df[f'MA{n}'] = df['Close'].rolling(window=min(count, n)).mean()
        df[f'VMA{n}'] = df['Volume'].rolling(window=min(count, n)).mean()
    
    close = df['Close'].squeeze()
    volume = df['Volume'].squeeze()
    
    df['Vol_Avg'] = df['Volume'].rolling(window=20).mean()

    # 2. 볼린저 밴드 (20/40 이중 응축)
    std20 = df['Close'].rolling(20).std()
    df['BB_Upper'] = df['MA20'] + (std20 * 2)
    df['BB20_Width'] = (std20 * 4) / df['MA20'] * 100
    
    std40 = df['Close'].rolling(40).std()
    df['BB40_Upper'] = df['MA40'] + (std40 * 2)
    df['BB40_Lower'] = df['MA40'] - (std40 * 2)
    df['BB40_Width'] = (std40 * 4) / df['MA40'] * 100
    df['BB40_PercentB'] = (df['Close'] - df['BB40_Lower']) / (df['BB40_Upper'] - df['BB40_Lower'])
    df['BB_UP'] = df['MA40'] + 2*df['Close'].rolling(40).std()
    df['BB_LOW'] = df['MA20'] - 2*df['Close'].rolling(20).std()
    
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
    # 9-1. RSI_DIV, BB_Ross
    df['BB_Ross'] = False
    df['RSI_DIV'] = False
    df['Was_Panic'] = False
    df['Is_bb_low_Stable'] = False
    df['Has_Accumulation'] = False
    df['Is_Rsi_Divergence'] = False

    df_signal = df.dropna(subset=['BB_UP','BB_LOW','RSI']).copy()
    if len(df_signal) > 51:
        curr = df_signal.iloc[-1]
        past = df_signal.iloc[-21:-1]
        past_50 = df_signal.iloc[-51:-1]    # 최근 50일간의 데이터 (오늘 제외)
        ross, _ = check_ross(curr, past)
        rsi_div, _ = check_rsi_div(curr, past)
        df.at[df.index[-1], 'BB_Ross'] = ross
        df.at[df.index[-1], 'RSI_DIV'] = rsi_div
        # --- [검증 2: 로스 캐머런 50일 공구리 패턴] ---
        # A. 50일 내에 밴드 밖(BB 20,2 하단)으로 이탈하며 '공포'를 준 적이 있는가? (외바닥)
        was_panic = (past_50['Low'] < past_50['BB_LOW']).any()
    
        # B. 현재 저가는 밴드 하단선보다 높은가? (안착 및 쌍바닥)
        is_bb_low_stable = curr['Low'] > curr['BB_LOW']
    
        # C. 50일간의 RSI 최저점보다 현재 RSI가 높은가? (중기 다이버전스)
        min_rsi_50 = past_50['RSI'].min()
        is_rsi_divergence = curr['RSI'] > min_rsi_50

        # --- [검증 3: 거래량 매집 흔적] ---
        # 50일 내에 평소 거래량의 3배가 넘는 '매집봉'이 하나라도 있었는가?
        has_accumulation = (past_50['Volume'] > (past_50['Vol_Avg'] * 3)).any()

        df.at[df.index[-1],'Was_Panic'] = was_panic
        df.at[df.index[-1],'Is_Rsi_Divergence'] = is_rsi_divergence
        df.at[df.index[-1],'Is_bb_low_Stable'] = is_bb_low_stable
        df.at[df.index[-1],'Has_Accumulation'] = has_accumulation


    
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

    prev = df.iloc[-2]   # 어제
    curr = df.iloc[-1]   # 오늘
    # [Option A] 골든크로스 당일: 어제 MA5 < MA112, 오늘 MA5 >= MA112
    cond_golden_cross = (
        (prev['MA5'] < prev['MA112']) and
        (curr['MA5'] >= curr['MA112'])
    )

    # [Option B] 근접 진입 중: 어제까지 역배열 + 오늘 MA5가 MA112의 98~103%
    cond_approaching = (
        (prev['MA5'] < prev['MA112']) and
        (curr['MA112'] * 0.98 <= curr['MA5'] <= curr['MA112'] * 1.03)
    )

    # [조건] 중장기 역배열 유지: MA112 < MA224 (큰 그림은 아직 역배열)
    cond_inverse_mid = (curr['MA112'] < curr['MA224'])

    # [조건] 종가 448일선 아래 (장기 눌림 구간)
    cond_below_448 = (curr['Close'] < curr['MA448'])

    # [조건] 224일선 밀착: 종가가 MA224 대비 -3% ~ +5%
    rate_ma224 = ((curr['Close'] - curr['MA224']) / curr['MA224']) * 100
    cond_ma224_range = -3 <= rate_ma224 <= 5

    # [조건] BB(40,2) 상단 근접: 종가가 BB상단 대비 -7% ~ +3%
    rate_bb40 = ((curr['Close'] - curr['BB40_Upper']) / curr['BB40_Upper']) * 100
    cond_bb40_range = -7 <= rate_bb40 <= 3

    # [조건] 최근 50봉 내 거래량 300% 이상 매집봉
    vol_ratio = volume / volume.shift(1).replace(0, np.nan)
    cond_vol_300 = (vol_ratio >= 3.0).iloc[-50:].any()

    # [조건] 최근 50봉 내 448일선 상향 돌파 이력 (찔러본 흔적 포함)
    cond_break_448 = (df['High'] > df['MA448']).iloc[-50:].any()

    # A or B 하나라도 충족 시 통과
    cond_cross = cond_golden_cross or cond_approaching
    
    df['Is_Real_Watermelron'] = False
    if (cond_cross and cond_inverse_mid and cond_below_448 and cond_ma224_range and cond_bb40_range and cond_break_448 and cond_vol_300):
        df['Is_Real_Watermelron'] = True

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
    # 조건: 오늘 종가가 BB(40,2) 상단선을 돌파했는가?
    is_watermelon = df['Close'].iloc[-1] > df['BB40_Upper'].iloc[-1]

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

    #print(f"✅ OBV 세력 매집 지표 계산!")
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

    #print(f"✅ 독사 대가리 + 기울기 방어선!")
    # 4. [조건 3 & 4] 독사 대가리 + 기울기 방어선 (사령관님 특별 지시!)
    # 어제보다 5일선이 올라갔고(상승 턴), 현재 5일선이 20일선을 뚫었거나 바짝 붙었을 때!
    is_slope_up = df['MA5'] > df['MA5'].shift(1)
    is_head_up = is_slope_up & (df['MA5'] >= df['MA20'] * 0.99)

    #print(f"✅ 60일선의 기울기")
    # 🚨 [KILL SWITCH 1] LG화학 사살: 60일선의 "기울기"가 하락 중이면 무조건 탈락!
    # 주가가 60일선 위에 있든 아래에 있든, 60일선 자체가 쏟아져 내리면 그건 악성 시체밭입니다.
    is_ma60_safe = df['MA60_Slope'] >= 0

    #print(f"✅ 5일선(대가리)")
    # 🚨 [KILL SWITCH 2] 두산밥캣 사살: "5일선(대가리)"에서 너무 멀어지면 탈락!
    # 20일선이 아니라, 당장 오늘 꺾어 올린 '5일선' 위로 주가가 5% 이상 혼자 튀어 나가면 허공답보입니다.
    distance_from_ma5 = (df['Close'] - df['MA5']) / df['MA5']
    is_hugging_ma5 = distance_from_ma5 < 0.05  # 5일선에 5% 이내로 바짝 붙어있어야 진짜 뱀!

    #print(f"✅ 역배열 폭포수 사살")
    # 🚨 [KILL SWITCH 3] 역배열 폭포수 사살: 112일선(반년 선)이 200일선 아래로 곤두박질치는가?
    # 장기 이평선이 완벽한 역배열 폭포수라면 뱀이 아니라 미꾸라지입니다.
    is_not_waterfall = df['MA112'] >= df['MA200'] * 0.9  # 최소한 200일선 근처에서 놀아야 함
    #print(f"✅ 역배열 폭포수 사살 - 1")
    is_heading_ceiling = (df['Close'] < df['MA112']) & (df['MA112_Slope'] < 0) & (df['Dist_to_MA112'] <= 0.04)
    #print(f"✅ 역배열 폭포수 사살 - 2")
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
    
    #print(f"✅ 최종판독")
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

def check_ross(curr: pd.Series, past: pd.DataFrame):
    if past.empty or past['BB_LOW'].isna().all():
        return False, "과거 데이터 부족"
    bb_low = past['BB_LOW']
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any():
        return False, "1차 저점 없음"
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx + 1:]
    rebound = (after_first['Close'] > after_first['BB_LOW']).any()
    near_band = curr['Low'] <= curr['BB_LOW'] * ROSS_BAND_TOLERANCE
    close_above = curr['Close'] > curr['BB_LOW']
    passed = rebound and near_band and close_above
    return passed, f"반등:{rebound}, 저가밴드근접:{near_band}, 종가밴드위:{close_above}"

def check_rsi_div(curr: pd.Series, past: pd.DataFrame):
    if past['RSI'].isna().all() or pd.isna(curr['RSI']):
        return False, "RSI 데이터 부족"
    min_price_past = past['Low'].min()
    min_rsi_past = past['RSI'].min()
    price_similar = curr['Low'] <= min_price_past * RSI_LOW_TOLERANCE
    rsi_higher = curr['RSI'] > min_rsi_past
    return price_similar and rsi_higher, f"주가저점:{curr['Low']:.0f}(과거:{min_price_past:.0f}), RSI:{curr['RSI']:.1f}(과거:{min_rsi_past:.1f})"

# ---------------------------------------------------------
# 🕵️‍♂️ [분석] 정밀 분석 엔진 (Ver 36.7 최저수익률 추가)
# ---------------------------------------------------------
def analyze_final(ticker, name, historical_indices, g_env, l_env, s_map,
                  market='KR'):   # ✅ 신규: 'KR' 또는 'US'
    try:
        df = fdr.DataReader(ticker, start=START_DATE)
        if len(df) < 100: return []
        df = get_indicators(df)

        if df is None or df.empty:
            return []

        df = df.join(historical_indices, how='left').fillna(method='ffill')

        # ──────────────────────────────────────────────
        # 루프 밖 1회 계산 (✅ FIX: 루프 내 반복 API 호출 제거)
        # ──────────────────────────────────────────────
        my_sector  = s_map.get(ticker, "일반")
        sector     = get_stock_sector(ticker, sector_master_map)   # ✅ 루프 밖으로
        l_score    = 25 if l_env.get(my_sector, "Normal") == "🔥강세" else 0

        news_score, news_comment = get_news_sentiment(ticker)      # ✅ 루프 밖으로

        # ──────────────────────────────────────────────
        # 시장별 거래대금 기준 분기
        # KR : 억원 기준 / US : 달러 기준 (단위 변환 없이 그대로 비교)
        # ──────────────────────────────────────────────
        if market == 'KR':
            AMT_1 = RECENT_AVG_AMOUNT_1        # 예: 50  (억원)
            AMT_2 = RECENT_AVG_AMOUNT_2        # 예: 100 (억원)
            AMT_DIV = 100_000_000              # 억원 단위 환산
        else:
            AMT_1 = RECENT_AVG_AMOUNT_US_1     # 예: 3_700_000  (달러, ≈50억원)
            AMT_2 = RECENT_AVG_AMOUNT_US_2     # 예: 7_000_000  (달러, ≈100억원)
            AMT_DIV = 1                        # 달러는 환산 없이 그대로

        # ──────────────────────────────────────────────
        # 수급 데이터 (KR: 네이버 / US: 스킵)
        # ──────────────────────────────────────────────
        if market == 'KR':
            try:
                url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
                res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
                res.encoding = 'euc-kr'
                supply_df  = pd.read_html(res.text)[2].dropna()
                f_qty      = int(str(supply_df.iloc[0]['외국인']).replace('.0','').replace(',',''))
                i_qty      = int(str(supply_df.iloc[0]['기관']).replace('.0','').replace(',',''))
                twin_b     = (f_qty > 0 and i_qty > 0)
                whale_score= int(((f_qty + i_qty) * df.iloc[-1]['Close']) / 100_000_000)
            except:
                f_qty, i_qty, twin_b, whale_score = 0, 0, False, 0
        else:
            # 나스닥: 수급 크롤링 없이 스킵 (yfinance institutional 붙이려면 여기에 추가)
            f_qty, i_qty, twin_b, whale_score = 0, 0, False, 0

        # ──────────────────────────────────────────────
        # 기상 지수 대상 (KR: ixic+sp500 / US: sp500+vix)
        # ──────────────────────────────────────────────
        storm_targets = ['ixic', 'sp500'] if market == 'KR' else ['sp500', 'vix']

        today_price = df.iloc[-1]['Close']
        recent_df   = df.tail(SCAN_DAYS)
        hits        = []

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 메인 루프
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        for curr_idx, row in recent_df.iterrows():
            raw_idx = df.index.get_loc(curr_idx)
            if raw_idx < 100: continue

            prev    = df.iloc[raw_idx - 1]
            prev_5  = df.iloc[max(0, raw_idx - 5)]
            prev_10 = df.iloc[max(0, raw_idx - 10)]
            temp_df = df.iloc[:raw_idx + 1]

            close_p = row['Close']
            open_p  = row['Open']
            high_p  = row['High']
            low_p   = row['Low']

            # ──────────────────────────────────────────────
            # 거래대금 필터
            # ✅ FIX: df 전체 → temp_df 기준으로 (look-ahead bias 제거)
            # ──────────────────────────────────────────────
            amount_series     = temp_df['Close'] * temp_df['Volume']
            recent_avg_amount = amount_series.tail(5).mean()  / AMT_DIV
            ma20_amount       = amount_series.tail(20).mean() / AMT_DIV

            amount_ok = (
                (recent_avg_amount >= AMT_1 and recent_avg_amount >= ma20_amount * 1.5)
                or recent_avg_amount >= AMT_2
            )
            if not amount_ok:
                continue

            # ──────────────────────────────────────────────
            # 무거운 계산 (시점별로 달라지므로 루프 내 유지)
            # ──────────────────────────────────────────────
            dante_data = calculate_dante_symmetry(temp_df)
            if dante_data is None:
                dante_data_ratio   = 0
                dante_data_mae_jip = 0
            else:
                dante_data_ratio   = dante_data['ratio']
                dante_data_mae_jip = dante_data['mae_jip']

            grade, narrative, target, stop, conviction = analyze_all_narratives(
                temp_df, name, my_sector, g_env, l_env
            )

            try:
                tri_result = jongbe_triangle_combo_v3(temp_df) or {}
                tri        = tri_result.get('triangle') or {}
            except Exception as e:
                print(f"🚨 jongbe_triangle_combo_v3 계산 실패: {e}")
                tri_result, tri = {}, {}

            # ──────────────────────────────────────────────
            # 꼬리% 계산
            # ──────────────────────────────────────────────
            body_max = max(open_p, close_p)
            t_pct    = int((high_p - body_max) / (high_p - low_p) * 100) if high_p != low_p else 0

            # ──────────────────────────────────────────────
            # 역매공파 7단계
            # ──────────────────────────────────────────────
            is_yeok   = (prev['MA5'] <= prev['MA20']) and (row['MA5'] > row['MA20'])
            is_mae    = (row['MA_Convergence'] <= 3.0 and row['BB40_Width'] <= 10.0
                         and row['ATR'] < row['ATR_MA20'] and row['OBV_Slope'] > 0)
            is_gong   = (close_p > row['MA112'] and prev['Close'] <= row['MA112']
                         and row['Volume'] > row['VMA20'] * 1.5)
            is_pa     = (row['Close'] > row['BB40_Upper'] and prev['Close'] <= row['BB40_Upper']
                         and row['Disparity'] <= 106)
            is_volume = row['Volume'] >= row['VMA5'] * 2.0
            is_safe   = 100.0 <= row['Disparity'] <= 106.0
            is_obv    = row['OBV_Slope'] > 0
            invalid   = row['Close'] < row['MA60']

            conditions  = [is_yeok, is_mae, is_gong, is_pa, is_volume, is_safe, is_obv]
            match_count = sum(conditions)

            # ──────────────────────────────────────────────
            # 일목균형표 신호
            # ──────────────────────────────────────────────
            is_cloud_brk  = prev['Close'] <= prev['Cloud_Top'] and close_p > row['Cloud_Top']
            is_kijun_sup  = close_p > row['Kijun_sen'] and prev['Close'] <= prev['Kijun_sen']
            is_diamond    = is_cloud_brk and is_kijun_sup
            is_super_squeeze = row['BB20_Width'] < 10 and row['BB40_Width'] < 15
            is_vol_power  = row['Volume'] > row['VMA20'] * 2.5

            # ──────────────────────────────────────────────
            # 매집 5가지 조건
            # ──────────────────────────────────────────────
            acc_1_obv_rising  = (row['OBV'] > prev_5['OBV']) and (row['OBV'] > prev_10['OBV'])
            acc_2_box_range   = row['Box_Range'] <= 1.15
            acc_3_macd_golden = row['MACD'] > row['MACD_Signal']
            acc_4_rsi_healthy = 40 <= row['RSI'] <= 70
            acc_5_sto_golden  = row['Sto_K'] > row['Sto_D']
            acc_count         = sum([acc_1_obv_rising, acc_2_box_range, acc_3_macd_golden,
                                     acc_4_rsi_healthy, acc_5_sto_golden])

            # ──────────────────────────────────────────────
            # 조용한 매집 (✅ FIX: 2세트 중복 → 1세트로 통합)
            # ──────────────────────────────────────────────
            silent_1_atr         = row['ATR_Below_Days'] >= 7
            silent_2_mfi_persist = row['MFI_Strong_Days'] >= 7
            silent_3_mfi_current = row['MFI'] > 50
            silent_4_mfi_rising  = row['MFI'] > row['MFI_10d_ago']
            silent_5_obv         = row['OBV_Rising']
            silent_6_box         = row['Box_Range'] <= 1.15
            silent_count         = sum([silent_1_atr, silent_2_mfi_persist,
                                        silent_3_mfi_current, silent_4_mfi_rising,
                                        silent_5_obv, silent_6_box])

            # ──────────────────────────────────────────────
            # 복합 패턴 (✅ FIX: signals / 개별변수 이중계산 → 한 곳에서 정의)
            # ──────────────────────────────────────────────
            bottom_area     = (row['Near_MA112'] <= 5.0 and row['Below_MA224_60d'] >= 40)
            explosion_ready = (row['BB40_Width'] <= 10.0 and row['OBV_Rising'] and row['MFI_Strong'])

            # ──────────────────────────────────────────────
            # 수박 지표
            # ──────────────────────────────────────────────
            is_watermelon    = row['Watermelon_Signal']
            watermelon_color = row['Watermelon_Color']
            watermelon_red   = row['Watermelon_Red']
            watermelon_red2  = row['Watermelon_Red2']
            watermelon_score = row['Watermelon_Score']
            watermelon_power = row['Watermelon_Fire']
            red_score        = int(row['OBV_Rising']) + int(row['MFI_Strong']) + int(row['Buying_Pressure'])

            # ──────────────────────────────────────────────
            # 기타 지표
            # ──────────────────────────────────────────────
            total_hammering = row['Total_hammering']
            maejip_count    = row['Maejip_Count']
            jongbe_break    = row['Jongbe_Break']
            rsi_val         = row['RSI']

            # ──────────────────────────────────────────────
            # signals 딕셔너리
            # ──────────────────────────────────────────────
            signals = {
                'watermelon_signal':  is_watermelon,
                'watermelon_red':     watermelon_red,
                'watermelon_green_7d':row['Green_Days_10'] >= 7,
                'explosion_ready':    explosion_ready,
                'bottom_area':        bottom_area,
                'silent_perfect':     silent_count >= 6,
                'silent_strong':      silent_count >= 5,
                'yeok_break':         (close_p > row['MA112'] and prev['Close'] <= row['MA112']),
                'volume_surge':       row['Volume'] >= row['VMA20'] * 1.5,
                'obv_rising':         row['OBV_Rising'],
                'mfi_strong':         row['MFI_Strong'],
                'dolbanzi':           row['Dolbanzi'],
                'dolbanzi_Trend_Group': row['Trend_Group'],
                'dolbanzi_Count':     row['Dolbanzi_Count'],
                'viper_hook':         row['Viper_Hook'],
                'obv_bullish':        row['OBV_Bullish'],
                'Real_Viper_Hook':    row['Real_Viper_Hook'],
                'Golpagi_Trap':       row['Golpagi_Trap'],
                'jongbe_break':       row.get('Jongbe_Break', False),
                'triangle_signal':    False,
                'triangle_apex':      None,
                'triangle_pattern':   'None',
                'MA_Convergence':     row['MA_Convergence'],
                'bb_ross':            False,
                'rsi_div':            False,
            }

            try:
                if tri_result:
                    signals['triangle_signal']  = tri_result['pass']
                    signals['triangle_apex']    = tri_result['apex_remain']
                    signals['triangle_pattern'] = tri_result['triangle_pattern']
                    signals['jongbe_ok']        = tri_result['jongbe']
                    signals['explosion_ready']  = explosion_ready or tri_result['pass']
            except Exception as e:
                print(f"🚨 tri_result 수집 실패: {e}")

            # ──────────────────────────────────────────────
            # 조합 점수 계산
            # ──────────────────────────────────────────────
            result   = judge_trade_with_sequence(temp_df, signals)
            s_score  = 100
            tags     = []
            new_tags = result['tags'].copy()

            # ──────────────────────────────────────────────
            # 전략 스타일 분류 + 가중치 로드
            # ──────────────────────────────────────────────
            style  = classify_style(row)
            W      = STYLE_WEIGHTS[style]

            style_label = {
                "SWING": "📈스윙(5~15일)",
                "SCALP": "⚡단타(1~3일)",
                "NONE":  "➖미분류",
            }[style]
            tags.append(style_label)

            # ──────────────────────────────────────────────
            # 삼각수렴 + 종베 점수
            # ──────────────────────────────────────────────
            if tri_result.get('has_triangle') and tri.get('is_triangle'):
                pattern_labels = {'Symmetrical': '대칭삼각', 'Ascending': '상승삼각', 'Descending': '하락삼각'}
                pat_label = pattern_labels.get(tri.get('pattern', ''), '')
                conf      = tri.get('confidence', 'LOW')
                conv      = tri.get('convergence_pct', 0)
                s_score  += 60
                tags.append(f"🔺{pat_label}수렴({conv:.0f}%)")
                if conf == 'HIGH':
                    s_score += 20
                    tags.append("🔺고신뢰삼각")

            apex = tri_result.get('apex_remain')
            if apex is not None:
                if 0 <= apex <= 5:
                    s_score += 40
                    tags.append(f"🔺꼭지{apex}봉임박")
                elif apex < 0:
                    s_score -= 20
                    tags.append(f"🔺꼭지초과{abs(apex)}봉")

            if tri.get('lines_crossed'):
                s_score -= 30
                tags.append("⚠️수렴에너지소멸")
            if tri.get('breakout_up'):
                s_score += 50
                tags.append("🚀삼각상방돌파")
            if tri.get('breakout_down'):
                s_score -= 50
                tags.append("🔻삼각하방이탈")

            if tri_result.get('jongbe'):
                s_score += 40
                tags.append("💛종베GC")
                detail = tri_result.get('jongbe_detail', {})
                if detail.get('cross_recent'):  tags.append("💛종베크로스(최근5일)")
                if detail.get('ma20_accel'):    tags.append("💛MA가속중")

            if tri_result.get('jongbe') and tri_result.get('has_triangle') and tri.get('is_triangle'):
                s_score += 80
                tags.append("💎종베+삼각수렴")

            dna = tri_result.get('ma20_dna', '0%')
            if int(dna.replace('%', '')) >= 70:
                s_score += 20
                tags.append(f"🧬MA지지DNA({dna})")

            detail     = tri_result.get('jongbe_detail', {})
            is_dmi_cross = detail.get('dmi_cross')
            is_adx_ok  = detail.get('adx_ok')
            is_dmi_ok  = detail.get('dmi_ok')

            # ──────────────────────────────────────────────
            # 라운드넘버 정거장
            # ──────────────────────────────────────────────
            lower_rn, upper_rn = get_target_levels(close_p)
            avg_money   = close_p * row['Volume']
            is_leader   = avg_money >= 100_000_000_000
            is_1st_buy  = False
            is_2nd_buy  = False
            is_rn_signal= False

            if lower_rn and upper_rn:
                lookback_df  = df.iloc[max(0, raw_idx - 20): raw_idx]
                hit_upper    = any(lookback_df['High'] >= upper_rn * 1.04)
                at_lower_station = lower_rn * 0.96 <= close_p <= lower_rn * 1.04
                is_rn_signal = hit_upper and at_lower_station

            if lower_rn:
                signal_line_30 = lower_rn * 1.30
                lookback_df    = df.iloc[max(0, raw_idx - 20): raw_idx]
                has_surged_30  = any(lookback_df['High'] >= signal_line_30)
                zone_upper     = lower_rn * 1.04
                zone_lower     = lower_rn * 0.96
                is_1st_buy     = has_surged_30 and (low_p <= zone_upper <= high_p)
                is_2nd_buy     = has_surged_30 and (low_p <= zone_lower <= high_p)

                if is_1st_buy:
                    tags.append("🚀급등_1차타점")
                    s_score += 100
                if is_2nd_buy:
                    tags.append("🚀급등_2차타점")
                    s_score += 120

            if is_rn_signal:
                tags.append("🚉정거장회귀")
                s_score += 70

            # ──────────────────────────────────────────────
            # 기존 시그널 점수
            # ──────────────────────────────────────────────
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

            # 매집
            if acc_count >= 4:
                s_score += 60
                tags.append("🐋세력매집")
            elif acc_count >= 3:
                s_score += 30
                tags.append("🐋매집징후")
            if acc_1_obv_rising:
                tags.append("📊OBV상승")

            # ──────────────────────────────────────────────
            # 🎯 스타일 가중치 점수 (SWING / SCALP / NONE)
            # ──────────────────────────────────────────────

            # 조용한 매집
            if silent_count >= 5:
                s_score += W['silent_perfect']
                tags.append("🤫조용한매집완전")
            elif silent_count >= 4:
                s_score += W['silent_strong']
                tags.append("🤫조용한매집강")
            elif silent_count >= 3:
                s_score += W['silent_weak']
                tags.append("🤫조용한매집약")

            if silent_1_atr:         tags.append(f"🔇ATR조용{int(row['ATR_Below_Days'])}일")
            if silent_2_mfi_persist: tags.append(f"💰MFI강세{int(row['MFI_Strong_Days'])}일")
            if row['ATR'] < row['ATR_MA20']:                        tags.append("🔇ATR수축")
            if row['MFI'] > 50 and row['MFI'] > row['MFI_Prev5']:  tags.append("💰MFI강세")

            # MA수렴 보너스 (스윙에서 추가 가점)
            if row['MA_Convergence'] < 3.0 and W['ma_convergence'] > 0:
                s_score += W['ma_convergence']
                tags.append(f"🔀MA수렴({row['MA_Convergence']:.1f}%)")

            # ADX 강세 (단타에서 핵심)
            if row['ADX'] >= 25:
                s_score += W['adx_strong']
                if style == "SCALP":
                    tags.append(f"💪ADX강세({row['ADX']:.0f})")

            # RSI
            if rsi_val >= 80:
                s_score += 10
                tags.append("🔥RSI강세")
            elif rsi_val >= 70: tags.append("📈RSI상승")
            elif rsi_val >= 50: tags.append("✅RSI중립상")
            elif rsi_val >= 30: tags.append("📉RSI하락")
            else:               tags.append("❄️RSI약세")

            # 수박 (단타 핵심 / 스윙 보조)
            if watermelon_red2:  tags.append("📍수박지표검증")
            if watermelon_red:   tags.append(f"🍉진짜수박 화력 {watermelon_power:.1f}")
            if is_watermelon:
                s_score += W['watermelon']
                tags.append("🍉수박신호")
                tags.append(f"🍉빨강전환(강도{red_score}/3)")
                tags.append(f"🍉강도{watermelon_score}/3")
            elif watermelon_color == 'red' and red_score >= 2:
                s_score += W['watermelon_red']
                tags.append("🍉빨강상태")
            elif row['Green_Days_10'] >= 7:
                s_score += 30
                tags.append("🍉초록축적")

            # 거래량 폭발 (단타 핵심)
            if is_vol_power:
                s_score += W['volume_surge']
                tags.append("⚡거래폭발")

            # 바닥권 (스윙 핵심 / 단타 무관)
            if bottom_area:
                s_score += W['bottom_area']
                tags.append("🏆112선바닥권")
                tags.append(f"📍거리{row['Near_MA112']:.1f}%")

            # 폭발직전 (스윙 핵심)
            if explosion_ready:
                s_score += W['explosion_ready']
                tags.append("💎폭발직전")

            # 최강 조합: 수박 + 폭발직전 + 바닥권
            if is_watermelon and explosion_ready and bottom_area:
                s_score += W['swing_gold']
                tags.append("💎💎💎스윙골드")

            # 감점 (스타일별 다른 강도)
            if t_pct > 40:
                s_score += W['high_tail']   # SCALP는 더 크게 감점
                tags.append("⚠️윗꼬리")

            storm_count = sum([1 for m in storm_targets if row[f'{m}_close'] <= row[f'{m}_ma5']])
            s_score -= storm_count * 20
            s_score -= max(0, int((row['Disparity'] - 108) * abs(W['disparity_over'])))

            # ──────────────────────────────────────────────
            # Ross / RSI DIV 태그
            # ──────────────────────────────────────────────
            if row['Was_Panic']:         new_tags.append("🔺🔺Was_Panic")
            if row['Is_bb_low_Stable']:  new_tags.append("🔺🔺Is_bb_low_Stable")
            if row['Has_Accumulation']:  new_tags.append("🔺🔺Has_Accumulation")
            if row['Is_Rsi_Divergence']: new_tags.append("🔺🔺Is_Rsi_Divergence")
            if row['BB_Ross']:           new_tags.append("🔺🔺Ross쌍바닥")
            if row['RSI_DIV']:           new_tags.append("📊RSI DIV")
            if row['Dolbanzi']:          new_tags.append("🟡돌반지")
            if signals['watermelon_signal']: new_tags.append(f"🍉강도{row['Watermelon_Score']}/3")
            if signals['bottom_area']:       new_tags.append(f"📍거리{row['Near_MA112']:.1f}%")
            if signals['silent_perfect'] or signals['silent_strong']:
                new_tags.append(f"🔇ATR{int(row['ATR_Below_Days'])}일")
                new_tags.append(f"💰MFI{int(row['MFI_Strong_Days'])}일")
            if watermelon_red: new_tags.append("🍉진짜수박")

            # ──────────────────────────────────────────────
            # 수익률 검증
            # ✅ FIX: else 블록에서 days_to_max 미정의 오류 방지
            # ──────────────────────────────────────────────
            h_df = df.iloc[raw_idx + 1:]

            if not h_df.empty:
                max_r              = ((h_df['High'].max()  - close_p) / close_p) * 100
                min_r              = ((h_df['Low'].min()   - close_p) / close_p) * 100
                max_date_ts        = h_df['Close'].idxmax()
                max_r_date         = max_date_ts.strftime('%Y-%m-%d')
                days_to_max        = (max_date_ts - curr_idx).days
                current_price      = today_price
            else:
                max_r, min_r       = 0, 0
                max_r_date         = curr_idx.strftime('%Y-%m-%d')
                days_to_max        = 0                                  # ✅ FIX: 미정의 방지
                current_price      = close_p

            print(f"🕵️ [분석 완료] {name}: {grade}점")

            hits.append({
                '날짜':           curr_idx.strftime('%Y-%m-%d'),
                '👑등급':          grade,
                'N등급':           f"{result['type']}{result['grade']}",
                'N점수':           result['score'],
                'N조합':           result['combination'],
                '정류장':          is_rn_signal | is_1st_buy | is_2nd_buy,
                '📜서사히스토리':   narrative,
                '확신점수':        conviction,
                '🎯목표타점':      int(target),
                '🚨손절가':        int(stop),
                '기상':           "☀️" * (2 - storm_count) + "🌪️" * storm_count,
                '안전점수':        int(max(0, s_score + whale_score)),
                '전략스타일':      style,
                '스타일라벨':      style_label,
                '시장':           market,            # ✅ 신규: KR / US
                '대칭비율':        dante_data_ratio,
                '매집봉':          dante_data_mae_jip,
                'D20매집봉':       maejip_count,
                '저항터치':        total_hammering,
                'BB-GC':           jongbe_break,
                '섹터':           sector,
                '종목':           name,
                '매입가':          int(close_p),
                '현재가':          int(current_price),
                'RSI':             rsi_val,
                '꼬리%':           t_pct,
                '이격':           int(row['Disparity']),
                'BB40':           f"{row['BB40_Width']:.1f}",
                'MA수렴':          f"{row['MA_Convergence']:.1f}",
                '매집':           f"{acc_count}/5",
                '최고수익날':      max_r_date,
                '소요기간':        days_to_max,
                '최고수익률%':     f"{max_r:+.1f}%",
                '최저수익률%':     f"{min_r:+.1f}%",
                '최고수익률_raw':  max_r,
                '최저수익률_raw':  min_r,
                'N구분':          " ".join(new_tags),
                '구분':           " ".join(tags),
                '보유일':          len(h_df),
                '삼각패턴':       tri_result.get('triangle_pattern', 'None'),
                '삼각수렴%':      tri.get('convergence_pct', 0),
                '꼭지잔여':       tri_result.get('apex_remain', 'N/A'),
                '종베GC':         tri_result.get('jongbe', False),
                '삼각점수':       tri_result.get('score', 0),
                '삼각등급':       tri_result.get('grade', 'N/A'),
                'DMI추세':        is_dmi_cross,
                'ADX추세힘':      is_adx_ok,
                'DMI_OK':         is_dmi_ok,
                'BB_Ross':        row['BB_Ross'],
                'RSI-DIV':        row['RSI_DIV'],
                'Was_Panic':      row['Was_Panic'],
                'Is_bb_low_Stable':  row['Is_bb_low_Stable'],
                'Has_Accumulation':  row['Has_Accumulation'],
                'Is_Rsi_Divergence': row['Is_Rsi_Divergence'],
                'Is_Real_Watermelon': row['Is_Real_Watermelon'],   # ✅ FIX: 오타 수정
                '뉴스점수':        news_score,
                '뉴스코멘트':      news_comment,
            })

        return hits

    except Exception as e:
        print(f"🚨 [본진] 데이터 로드 실패: {e}")
        return []


# 스타일별 가중치 테이블
# 기준점수(base) 대비 각 신호에 얼마나 가중치를 줄지 정의
STYLE_WEIGHTS = {
    "SWING": {
        # 핵심 (스윙의 본질 = 응축 후 폭발)
        'explosion_ready': 150,   # BB수축 + 수급 → 스윙 핵심
        'bottom_area':     120,   # 바닥권 확인 → 스윙 핵심
        'silent_perfect':  130,   # 조용한 매집 완전 → 스윙 핵심
        'silent_strong':    80,
        'silent_weak':      40,
        'bb_squeeze_bonus': 50,   # 삼각수렴 고신뢰 보너스
        'ma_convergence':   40,   # MA수렴 자체 보너스
        # 보조
        'watermelon':       70,   # 수박신호 (스윙에선 보조)
        'watermelon_red':   50,
        'volume_surge':     20,   # 거래량 폭발 (스윙엔 덜 중요)
        'adx_strong':       10,   # ADX (스윙엔 별로)
        # 최강 조합
        'swing_gold':      100,   # 수박 + 폭발직전 + 바닥권 동시
        # 감점
        'high_tail':       -25,
        'disparity_over':    -5,  # (Disparity-108) 당 감점
    },
    "SCALP": {
        # 핵심 (단타의 본질 = 지금 당장 터지는 중)
        'explosion_ready':  50,   # 단타엔 덜 중요 (이미 터졌어야)
        'bottom_area':      20,   # 단타엔 무관
        'silent_perfect':   30,
        'silent_strong':    20,
        'silent_weak':      10,
        'bb_squeeze_bonus': 10,
        'ma_convergence':   10,
        # 핵심
        'watermelon':      150,   # 수박신호 → 단타 핵심 (지금 터지는 중)
        'watermelon_red':  100,
        'volume_surge':     80,   # 거래량 폭발 → 단타 핵심
        'adx_strong':       80,   # ADX 강세 → 단타 핵심
        # 최강 조합
        'swing_gold':       40,
        # 감점 (단타는 손절 빠르므로 윗꼬리 더 치명적)
        'high_tail':       -40,
        'disparity_over':   -8,
    },
    "NONE": {
        # 기본값 (기존 점수 그대로 유지)
        'explosion_ready':  90,
        'bottom_area':      80,
        'silent_perfect':  100,
        'silent_strong':    60,
        'silent_weak':      30,
        'bb_squeeze_bonus': 20,
        'ma_convergence':    0,
        'watermelon':      100,
        'watermelon_red':   60,
        'volume_surge':     30,
        'adx_strong':       20,
        'swing_gold':       80,
        'high_tail':       -25,
        'disparity_over':   -5,
    },
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전략 스타일 분류 (단타 1~3일 / 스윙 5~15일)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def classify_style(row):
    """
    SWING : BB40 수축 + 이평 수렴 + ADX 미발동 → 에너지 응축 대기 구간 (5~15일)
    SCALP : 변동성 적당 + ADX 강세 → 추세 이미 발동, 올라타는 구간 (1~3일)
    NONE  : 두 조건 모두 미충족
    """
    vol_ratio = row['ATR'] / row['Close'] if row['Close'] > 0 else 0

    # 1순위: 스윙 (응축 → 폭발 직전)
    if (row['BB40_Width'] < 12
            and row['MA_Convergence'] < 3
            and row['ADX'] < 25):
        return "SWING"

    # 2순위: 단타 (추세 이미 발동)
    elif (0.02 <= vol_ratio <= 0.05
          and row['ADX'] >= 25):
        return "SCALP"

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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# main.py  (스캔 진입점)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#from combo_storage import (
#    rebuild_score_overrides,
#    record_combo_performance,
#    flush_and_push,
#    print_combo_report,
#)
# 아래 더미 함수로 대체 (파일 추가 전까지 오류 방지)
def rebuild_score_overrides(): pass
def record_combo_performance(*args, **kwargs): pass
def flush_and_push(): pass
def print_combo_report(**kwargs): pass
    
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

        # 기존 2325번 라인 근처를 아래 코드로 대체하세요
        try:
            print("📡 KRX 종목 리스트 보급 시도 중...")
            df_krx = load_krx_listing_safe()
            df_krx['Code'] = (
                df_krx['Code']
                .fillna('')
                .astype(str)
                .str.replace('.0', '', regex=False)
                .str.zfill(6)
            )
            
            # 데이터가 정상적으로 들어왔는지 최종 검문
            if df_krx is None or df_krx.empty:
                raise ValueError("데이터가 텅 비어있습니다.")
            else:
                print("✅ [성공] KRX 종목 리스트 로드 완료.")        
        except Exception as e:
            print(f"⚠️ [보급 차단] KRX 서버 응답 없음 ({e})")
            
        # 위키피디아에서 나스닥 100 티커 자동 수집 (이전에 만든 함수 활용)
        nasdaq_100_list = get_nasdaq100_tickers() 
        # 데이터프레임 형태로 변환 (기존 코드와 호환성을 위해)
        df_us_all = pd.DataFrame({
                'Symbol': nasdaq_100_list,
                'Name': nasdaq_100_list  # 이름 데이터가 없으면 티커로 대체
            })
        print(f"✅ [글로벌 전면전] 총 {len(df_us_all)}개 미국 종목 확보")

        # 2. 국내주식 정제 및 타겟팅
        df_clean = df_krx[df_krx['Market'].isin(['KOSPI', 'KOSDAQ','코스닥','유가'])]
        df_clean['Name'] = df_clean['Name'].astype(str)
        df_clean = df_clean[~df_clean['Name'].str.contains('ETF|ETN|스팩|제[0-9]+호|우$|우A|우B|우C')]
        
        # 💰 거래대금 상위 추출 (국내)
        if 'Amount' in df_clean.columns:
            target_stocks = df_clean.sort_values(by='Amount', ascending=False).head(TOP_N)
        else:
            target_stocks = df_clean.copy()
        
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
    
        # ① 스캔 시작 전: 누적 수익률로 점수 보정 재계산
        rebuild_score_overrides()
        
        # 4. [국내전] 스캔
        all_hits = []
        print(f"🔍 [국내] {len(target_stocks)}개 종목 레이더 가동...")
        with ThreadPoolExecutor(max_workers=15) as executor:
            results = list(executor.map(
                lambda p: analyze_final(p[0], p[1], weather_data, global_env, leader_env, sector_master_map, market='KR'), 
                zip(target_stocks['Code'], target_stocks['Name'])
            ))
            print(f"📦 results 수: {len(results)}")
            all_hits = [item for r in results if r for item in r]
            print(f"🎯 all_hits 수: {len(all_hits)}")

        # ✅ executor 끝난 후 all_hits 루프로 한 번에 기록
        for hit in all_hits:
            record_combo_performance(
                combination = hit['N조합'],
                max_return  = hit['최고수익률_raw'],
                min_return  = hit['최저수익률_raw'],
                days_to_max = hit['소요기간'],
                style       = hit.get('전략스타일', 'NONE'),
            )
            
        # ✅ 전부 기록 끝난 후 레포에 1회 커밋
        flush_and_push()

        if not all_hits:
            print("⚠️ all_hits 비어있음 → 조건 만족 종목 없음")
        else:
            analyze_save_googleSheet(all_hits, False)

        # 5. [나스닥전] 스캔
        all_Nasdaq_hits = []
        print(f"🔍 [미국] {len(target_Nasdaq_stocks)}개 종목 레이더 가동...")
        with ThreadPoolExecutor(max_workers=15) as executor:
            # 미국 데이터프레임은 'Symbol'과 'Name' 컬럼을 사용합니다.
            results = list(executor.map(
                lambda p: analyze_final(p[0], p[1], weather_data, global_env, leader_env, {}, market='US'), 
                zip(target_Nasdaq_stocks['Symbol'], target_Nasdaq_stocks['Name'])
            ))
            all_Nasdaq_hits = [item for r in results if r for item in r]
            
        analyze_save_googleSheet(all_Nasdaq_hits, True)
        
    except Exception as main_error:
        print(f"🚨 [치명적 오류] 메인 엔진 정지: {main_error}")
        print("🚨 [디버깅] 상세 에러 리포트:")
        traceback.print_exc()
