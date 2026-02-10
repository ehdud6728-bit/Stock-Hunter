import pandas as pd
from collections import Counter
import traceback

# 1. 성공 공식(Master DNA) 추출 부품
def get_master_dna(all_hits, top_k=5):
    if not all_hits: return []
    try:
        df = pd.DataFrame(all_hits)
        if '최고_raw' not in df.columns: df['최고_raw'] = 0.0
        df = df.sort_values(by=['종목', '날짜'])
        
        success_sequences = []
        for ticker, group in df.groupby('종목'):
            # 15% 이상 수익을 낸 전설적인 종목들의 패턴만 수집
            if group['최고수익률_raw'].max() >= 15.0:
                seq = tuple(group['구분'].tolist())
                success_sequences.append(seq)
        
        most_common = Counter(success_sequences).most_common(top_k)
        return [pattern for pattern, count in most_common]
    except:
        return []

# 2. 일치도 계산 부품
def calculate_dna_score(current_seq, master_patterns):
    if not master_patterns or not current_seq: return 0
    try:
        max_match_rate = 0
        current_set = set(current_seq)
        
        for master in master_patterns:
            master_set = set(master)
            if not master_set: continue
            
            intersection = current_set.intersection(master_set)
            match_rate = (len(intersection) / len(master_set)) * 100
            
            # 순서가 완벽히 일치하면 가산점
            if list(master) == list(current_seq):
                match_rate += 10 
            max_match_rate = max(max_match_rate, match_rate)
        return min(100, int(max_match_rate))
    except:
        return 0

# 3. 승리 패턴 랭킹 요약 부품
def find_winning_pattern(dna_df):
    """분석된 DNA 결과에서 '전설의 패턴 랭킹'을 추출합니다."""
    if dna_df is None or dna_df.empty: 
        return pd.DataFrame(columns=['DNA_시퀀스', '포착수', '평균수익'])
    try:
        success_cases = dna_df[dna_df['최고수익률'] >= 10.0]
        if success_cases.empty: return pd.DataFrame()
        
        summary = success_cases.groupby('DNA_시퀀스').agg({
            'DNA_시퀀스': 'count',
            '최고수익률': 'mean'
        }).rename(columns={'DNA_시퀀스': '포착수', '최고수익률': '평균수익'}).reset_index()
        
        # 💡 [수정] 5개에서 30개로 대폭 늘려 보급합니다.
        return summary.sort_values(by='포착수', ascending=False).head(30)
    except:
        return pd.DataFrame()

def find_winning_pattern_back(dna_df):
    if dna_df is None or dna_df.empty: 
        return pd.DataFrame(columns=['DNA_시퀀스', '포착수', '평균수익'])
    try:
        success_cases = dna_df[dna_df['최고수익률'] >= 10.0]
        if success_cases.empty: return pd.DataFrame()
        
        summary = success_cases.groupby('DNA_시퀀스').agg({
            'DNA_시퀀스': 'count',
            '최고수익률': 'mean'
        }).rename(columns={'DNA_시퀀스': '포착수', '최고수익률_raw': '평균수익'}).reset_index()
        
        return summary.sort_values(by='포착수', ascending=False).head(5)
    except:
        return pd.DataFrame()

# 4. [메인 엔진] 통합 분석 함수 - 모든 부품을 여기서 호출합니다.
def analyze_dna_sequences(all_hits):
    """
    모든 부품을 조립하여 종목별 DNA 일치도를 최종 산출합니다.
    """
    if not all_hits:
        print("⚠️ [DNA] 분석할 데이터(all_hits)가 없습니다.")
        return pd.DataFrame()
    
    try:
        # 💡 위에서 정의된 get_master_dna를 호출합니다.
        master_patterns = get_master_dna(all_hits)
        
        df = pd.DataFrame(all_hits)
        if '최고_raw' not in df.columns: df['최고_raw'] = 0.0
        df = df.sort_values(by=['종목', '날짜'])
        
        dna_reports = []
        for ticker, group in df.groupby('종목'):
            curr_seq = group['구분'].tolist()
            max_yield = group['최고수익률_raw'].max()
            
            # 💡 위에서 정의된 calculate_dna_score를 호출합니다.
            match_score = calculate_dna_score(curr_seq, master_patterns)
            
            dna_reports.append({
                '종목': ticker,
                'DNA_시퀀스': " ➔ ".join(curr_seq),
                'DNA_일치도': f"{match_score}%",
                '최고수익률': max_yield,
                '유형': "🔥전설과일치" if match_score >= 80 else ("✅검증필요" if match_score >= 50 else "미확인")
            })
            
        return pd.DataFrame(dna_reports).sort_values(by='최고수익률', ascending=False)
    except Exception as e:
        print(f"❌ [DNA] 분석 중 치명적 오류: {e}")
        traceback.print_exc()
        return pd.DataFrame()

def analyze_dna_with_cap(all_hits, ticker_info_df):
    """
    사령관님의 통찰을 반영한 시가총액 가중치 분석 엔진
    """
    # 1. 시가총액 정보 병합
    df = pd.merge(pd.DataFrame(all_hits), ticker_info_df[['종목', '시가총액']], on='종목', how='left')
    
    # 2. 체급 판정 로직
    def get_segment(cap):
        if cap >= 1_000_000_000_000: return 'HEAVY'
        if cap >= 200_000_000_000: return 'MIDDLE'
        return 'LIGHT'
    
    df['체급'] = df['시가총액'].apply(get_segment)
    
    # 3. 체급별 마스터 DNA 추출 (미리 계산되어 있다고 가정)
    master_patterns = {
        'HEAVY': get_master_dna(df[df['체급'] == 'HEAVY']),
        'MIDDLE': get_master_dna(df[df['체급'] == 'MIDDLE']),
        'LIGHT': get_master_dna(df[df['체급'] == 'LIGHT'])
    }
    
    results = []
    for ticker, group in df.groupby('종목'):
        seg = group['체급'].iloc[0]
        curr_seq = group['구분'].tolist()
        
        # 💡 해당 체급의 족보와 대조
        raw_score = calculate_dna_score(curr_seq, master_patterns[seg])
        
        # 💡 가중치 적용 (HEAVY는 신뢰도 가산, LIGHT는 변동성 감산)
        weight = 1.2 if seg == 'HEAVY' else (1.0 if seg == 'MIDDLE' else 0.8)
        final_score = min(100, int(raw_score * weight))
        
        results.append({
            '종목': ticker,
            '체급': seg,
            'DNA_일치도': f"{final_score}%",
            '패턴유형': f"{seg}_유전자"
        })
    return pd.DataFrame(results)

def find_winning_pattern_by_tier(dna_df):
    """
    [체급별 랭킹] 👑HEAVY, ⚔️MIDDLE, 🚀LIGHT 별로 상위 패턴을 각각 추출합니다.
    """
    if dna_df is None or dna_df.empty: return {}

    tier_results = {}
    # '미확인' 데이터가 있다면 '🚀LIGHT'로 취급하여 분석합니다.
    dna_df['유형'] = dna_df['유형'].replace('미확인', '🚀LIGHT')
    
    tiers = ['👑HEAVY', '⚔️MIDDLE', '🚀LIGHT']

    for tier in tiers:
        # 해당 체급 데이터만 분리
        tier_df = dna_df[dna_df['유형'].str.contains(tier, na=False)]
        
        if not tier_df.empty:
            # 10% 이상 수익을 낸 성공 사례 집계
            success_cases = tier_df[tier_df['최고수익률'] >= 1.0]
            if not success_cases.empty:
                summary = success_cases.groupby('DNA_시퀀스').agg({
                    'DNA_시퀀스': 'count',
                    '최고수익률': 'mean'
                }).rename(columns={'DNA_시퀀스': '포착수', '최고수익률': '평균수익'}).reset_index()
                
                # 체급별 상위 10개 패턴 저장
                tier_results[tier] = summary.sort_values(by='포착수', ascending=False).head(10)
            else:
                tier_results[tier] = pd.DataFrame()
        else:
            tier_results[tier] = pd.DataFrame()
            
    return tier_results
