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
from tactics_engine import get_global_and_leader_status, analyze_all_narratives, get_dynamic_sector_leaders, calculate_dante_symmetry, watermelon_indicator_complete
import traceback

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
SCAN_DAYS = 25       # 최근 30일 내 타점 전수 조사
TOP_N = 350         # 거래대금 상위 종목 수 (필요시 2500으로 확장 가능)
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')
START_DATE = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
END_DATE_STR = datetime.now().strftime('%Y%m%d')

print(f"📡 [Ver 36.7 엑셀저장+추천] 사령부 무결성 통합 가동... 💎다이아몬드 & 📊복합통계 엔진 탑재")



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # S급 조합 체크 (300~350점)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # 💎전설조합 (350점)
    if (signals['watermelon_signal'] and 
        signals['explosion_ready'] and 
        signals['bottom_area'] and 
        signals['silent_perfect']):
        score = 350
        grade = 'S'
        combination = '💎전설조합'
        tags = ['🍉수박전환', '💎폭발직전', '📍바닥권', '🤫조용한매집완전']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 💎스윙골드 (330점)
    if (signals['watermelon_signal'] and 
        signals['explosion_ready'] and 
        signals['bottom_area']):
        score = 330
        grade = 'S'
        combination = '💎스윙골드'
        tags = ['🍉수박전환', '💎폭발직전', '📍바닥권']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 💎돌파골드 (320점)
    if (signals['yeok_break'] and 
        signals['watermelon_signal'] and 
        signals['volume_surge']):
        score = 320
        grade = 'S'
        combination = '💎돌파골드'
        tags = ['🏆역매공파돌파', '🍉수박전환', '⚡거래량폭발']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 💎매집완성 (310점)
    if (signals['silent_perfect'] and 
        signals['watermelon_signal'] and 
        signals['explosion_ready']):
        score = 310
        grade = 'S'
        combination = '💎매집완성'
        tags = ['🤫조용한매집완전', '🍉수박전환', '💎폭발직전']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 💎바닥폭발 (300점)
    if (signals['bottom_area'] and 
        signals['explosion_ready'] and 
        signals['watermelon_signal']):
        score = 300
        grade = 'S'
        combination = '💎바닥폭발'
        tags = ['📍바닥권', '💎폭발직전', '🍉수박전환']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # A급 조합 체크 (250~290점)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # 🔥수박폭발 (280점)
    if signals['watermelon_signal'] and signals['explosion_ready']:
        score = 280
        grade = 'A'
        combination = '🔥수박폭발'
        tags = ['🍉수박전환', '💎폭발직전']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 🔥바닥수박 (270점)
    if signals['bottom_area'] and signals['watermelon_signal']:
        score = 270
        grade = 'A'
        combination = '🔥바닥수박'
        tags = ['📍바닥권', '🍉수박전환']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 🔥돌파확인 (260점)
    if signals['yeok_break'] and signals['volume_surge']:
        score = 260
        grade = 'A'
        combination = '🔥돌파확인'
        tags = ['🏆역매공파돌파', '⚡거래량폭발']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 🔥조용폭발 (250점)
    if signals['silent_strong'] and signals['explosion_ready']:
        score = 250
        grade = 'A'
        combination = '🔥조용폭발'
        tags = ['🤫조용한매집강', '💎폭발직전']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # B급 조합 체크 (200~240점)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # 📍수박단독 (230점)
    if signals['watermelon_signal']:
        score = 230
        grade = 'B'
        combination = '📍수박단독'
        tags = ['🍉수박전환']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 📍폭발단독 (220점)
    if signals['explosion_ready']:
        score = 220
        grade = 'B'
        combination = '📍폭발단독'
        tags = ['💎폭발직전']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 📍바닥단독 (210점)
    if signals['bottom_area']:
        score = 210
        grade = 'B'
        combination = '📍바닥단독'
        tags = ['📍바닥권']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 📍조용단독 (200점)
    if signals['silent_strong']:
        score = 200
        grade = 'B'
        combination = '📍조용단독'
        tags = ['🤫조용한매집강']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # C급 조합 체크 (150~190점)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # 🔍빨강상태 (180점)
    if signals['watermelon_red']:
        score = 180
        grade = 'C'
        combination = '🔍빨강상태'
        tags = ['🍉빨강유지']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 🔍초록축적 (170점)
    if signals['watermelon_green_7d']:
        score = 170
        grade = 'C'
        combination = '🔍초록축적'
        tags = ['🍉초록7일']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # 🔍수급흐름 (160점)
    if signals['obv_rising'] and signals['mfi_strong']:
        score = 160
        grade = 'C'
        combination = '🔍수급흐름'
        tags = ['📊OBV상승', '💰MFI강세']
        return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # D급 (100~140점) - 기본 점수만
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # 개별 신호 카운트 (보너스)
    bonus = 0
    if signals['obv_rising']: 
        bonus += 10
        tags.append('📊OBV')
    if signals['mfi_strong']: 
        bonus += 10
        tags.append('💰MFI')
    if signals['volume_surge']: 
        bonus += 10
        tags.append('⚡거래량')
    
    score = 100 + bonus
    grade = 'D'
    combination = '🔍기본'
    
    return {'score': score, 'grade': grade, 'combination': combination, 'tags': tags}


# ---------------------------------------------------------
# 📈 [데이터] 마스터 지표 엔진 (Ver 36.7)
# ---------------------------------------------------------
def get_indicators_back(df):
    df = df.copy()
    count = len(df)

    # 1. 이동평균선 및 거래량 이평 (단테 112/224 포함)
    for n in [5, 20, 40, 60, 112, 224]:
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

    return df

def get_indicators(df):
    df = df.copy()
    count = len(df)

    # 1. 이동평균선 및 거래량 이평 (단테 112/224 포함)
    for n in [5, 20, 40, 60, 112, 224]:
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

    return df

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
        grade, narrative, target, stop, conviction = analyze_all_narratives(
            df, name, my_sector, g_env, l_env
        )
        
        
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

            #하락기간과 횡보(공구리)기간 비교(1이상 추천)
            dante_data = calculate_dante_symmetry(temp_df)
        
            if dante_data is None:
                dante_data_ratio = 0
                dante_data_mae_jip = 0
            else:
                dante_data_ratio = dante_data['ratio']
                dante_data_mae_jip = dante_data['mae_jip']

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 1. 신호 수집
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            signals = {
                # 수박지표
                'watermelon_signal': row['Watermelon_Signal'],
                'watermelon_red': row['Watermelon_Color'] == 'red',
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
            }
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 2. 조합 점수 계산
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
            result = calculate_combination_score(signals)

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

            # 💡 오늘의 현재가 저장 (나중에 사용)
            today_price = df.iloc[-1]['Close']

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

            # --- [역매공파 통합 7단계 로직] ---
            # 1. [역(逆)] 역배열 바닥 탈출 (5/20 골든크로스)
            # 의미: 하락을 멈추고 단기 추세를 돌리는 첫 신호
            is_yeok = (prev['MA5'] <= prev['MA20']) and (row['MA5'] > row['MA20'])

            # 2. [매(埋)] 에너지 응축 (이평선 밀집)
            # 의미: 5, 20, 60일선이 3% 이내로 모여 에너지가 압축된 상태
            is_mae = row['MA_Convergence'] <= 3.0

            # 3. [공(空)] 공구리 돌파 (MA112 돌파) - 사령관님이 찾아낸 핵심!
            # 의미: 6개월 장기 저항선(공구리)을 종가로 뚫어버리는 순간
            is_gong = (close_p > row['MA112']) and (prev['Close'] <= row['MA112'])

            # 4. [파(破)] 파동의 시작 (BB40 상단 돌파)
            # 의미: 볼린저밴드 상단을 뚫고 변동성이 위로 터지는 시점
            is_pa = (row['Close'] > row['BB40_Upper']) and (prev['Close'] <= row['BB40_Upper'])

            # 5. [화력] 거래량 동반 (VMA5 대비 2배)
            # 의미: 가짜 돌파를 걸러내는 세력의 입성 증거
            is_volume = row['Volume'] >= row['VMA5'] * 2.0

            # 6. [안전] 적정 이격도 (100~106%)
            # 의미: 이미 너무 날아간 종목(추격매수)은 거르는 안전장치
            is_safe = 100.0 <= row['Disparity'] <= 106.0

            # 7. [수급] OBV 우상향 유지
            # 의미: 주가는 흔들어도 돈(매집세)은 빠져나가지 않는 상태
            is_obv = row['OBV_Slope'] > 0

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
            near_ma112 = row['Near_MA112'] <= 5.0
            long_bottom = row['Below_MA112_60d'] >= 40
            bottom_area = near_ma112 and long_bottom
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 💎 폭발 직전 (BB수축 + 수급)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            bb_squeeze = row['BB40_Width'] <= 10.0
            supply_strong = row['OBV_Rising'] and row['MFI_Strong']
            explosion_ready = bb_squeeze and supply_strong

            #수박지표
            is_watermelon = row['Watermelon_Signal']
            watermelon_color = row['Watermelon_Color']
            watermelon_score = row['Watermelon_Score']
            red_score = (
                int(row['OBV_Rising']) +
                int(row['MFI_Strong']) +
                int(row['Buying_Pressure'])
            )

            # 3. 점수 산출 및 태그 부여
            s_score = 100
            tags = []
            
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
                
                # 💡 오늘이면 현재가 = 오늘 종가, 아니면 해당 시점의 마지막 종가
                is_today = (len(h_df) == 0)  # 보유일 0이면 오늘
                current_price = today_price if not is_today else close_p
            else:
                max_r = 0
                min_r = 0
                current_price = close_p

            hits.append({
                '날짜': curr_idx.strftime('%Y-%m-%d'),
                '👑등급': grade,
                'N등급': result['grade'],
                'N점수': result['score'],
                'N조합': result['combination'],
                  # 👈 서사 엔진 결과물 1
                '📜서사히스토리': narrative,    # 👈 서사 엔진 결과물 2
                '확신점수': conviction,        # 👈 서사 엔진 결과물 3
                '🎯목표타점': int(target),      # 👈 서사 기반 타점
                '🚨손절가': int(stop),         # 👈 서사 기반 손절가
                '기상': "☀️" * (2-storm_count) + "🌪️" * storm_count,
                '안전점수': int(max(0, s_score + whale_score)),
                '대칭비율': dante_data_ratio,
                '매집봉': dante_data_mae_jip,
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
                '최고수익률%': f"{max_r:+.1f}%",
                '최저수익률%': f"{min_r:+.1f}%",
                '최고수익률_raw': max_r,
                '최저수익률_raw': min_r,
                'N구분': " ".join(new_tags),
                '구분': " ".join(tags),
                '보유일': len(h_df)
            })
        return hits
    except Exception as e:
        print(f"🚨 [본진] 데이터 로드 실패: {e}")
        print(f"✅ [본진] 오류!")
        return []

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
    print(f"📡 [Ver 36.7 구글시트 강화] {TODAY_STR} 전술 사령부 통합 가동...")
    commander_cap_map = get_commander_market_cap()
    # 💡 1. 전쟁 시작 전 '대장주 지도'와 '그들의 상태'를 딱 한 번만 생성
    # leader_map: {섹터: 코드}, leader_status: {섹터: 강세/침체}
    global_env, leader_env = get_global_and_leader_status()

    # 2. 전 종목 리스트 로드 및 명찰 강제 통일
    try:
        df_krx = fdr.StockListing('KRX')
        
        # 💡 [핵심] 첫 번째 열은 'Code', 두 번째 열은 'Name'으로 강제 개명
        # KRX 데이터 구조상 보통 0번이 코드, 1번이 종목명입니다.
        #df_krx.columns.values[0] = target_stocks['Code']
        #df_krx.columns.values[1] = target_stocks['Name']
        
        # 섹터 컬럼도 있으면 'Sector'로 통일
        s_col = next((c for c in ['Sector', 'Industry', '업종'] if c in df_krx.columns), None)
        if s_col:
            df_krx = df_krx.rename(columns={s_col: 'Sector'})
            sector_master_map = df_krx.set_index('Code')['Sector'].to_dict()
        else:
            sector_master_map = {k: '일반' for k in df_krx['Code']}
            
        print(f"✅ [본진] 명찰 통일 완료: {len(df_krx)}개 종목 로드")

    except Exception as e:
        print(f"🚨 [본진] 데이터 로드 실패: {e}")
        sector_master_map = {}
        # 여기서 죽지 않게 빈 데이터프레임이라도 생성
        df_krx = pd.DataFrame(columns=['Code', 'Name', 'Sector'])

    target_stocks = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    
    # 1. 매크로 데이터 수집
    m_ndx = get_safe_macro('^IXIC', '나스닥')
    m_sp5 = get_safe_macro('^GSPC', 'S&P500')
    m_vix = get_safe_macro('^VIX', 'VIX공포')
    m_fx  = get_safe_macro('USD/KRW', '달러환율')
    
    kospi_supply = get_index_investor_data('KOSPI')
    macro_status = {'nasdaq': m_ndx, 'sp500': m_sp5, 'vix': m_vix, 'fx': m_fx, 'kospi': kospi_supply}

    print("\n" + "🌍 " * 5 + "[ 글로벌 사령부 통합 관제 센터 ]" + " 🌍" * 5)
    print(f"🇺🇸 {m_ndx['text']} | {m_sp5['text']} | ⚠️ {m_vix['text']}")
    print(f"💵 {m_fx['text']} | 🇰🇷 KOSPI 수급: {kospi_supply}")
    print("=" * 115)
    
    weather_data = prepare_historical_weather()
    
    # 2. 글로벌/대장주 상태 스캔
    g_status, l_sync = get_global_and_leader_status()
  
    # 3. 전술 스캔 (멀티스레딩)
    all_hits = []
    print(f"🔍 총 {len(target_stocks)}개 종목 💎다이아몬드 & 🎯역매공파 레이더 가동...")
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = list(executor.map(
            lambda p: analyze_final(p[0], p[1], weather_data, global_env, leader_env, sector_master_map), 
            zip(target_stocks['Code'], target_stocks['Name'])
        ))
        for r in results:
            if r:
                # 💡 [신규] 포착된 종목에 즉시 체급(Tier) 및 시총 데이터 주입
                for hit in r:
                    # hit['종목코드']가 있다고 가정, 없으면 ticker를 찾아야 함
                    name = hit['종목']
                    ticker_code = hit.get('코드')
                    all_hits.append(hit)

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
                'RSI',
                '대칭비율',
                '매집봉',
                '🎯목표타점',
                '🚨손절가',
                '매입가',
                '현재가',
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
                
                # ✅ 수정: grade_analysis 제거하고 df_backtest, df_realistic 직접 전달
                # grade_analysis=grade_analysis,  # ← 삭제
                
                df_backtest=df_backtest,
                df_realistic=df_realistic,
                df_combo=df_combo,
                best_combos=best_combos,
                worst_combos=worst_combos,
                df_profit_dist=df_profit_dist
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
