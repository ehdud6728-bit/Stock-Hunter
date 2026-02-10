# [독립 모듈] DNA_Analyzer.py

import pandas as pd
from collections import Counter

def analyze_dna_sequences(all_hits):
    """
    사령관님, '최고_raw' 데이터가 없더라도 멈추지 않는 무결성 시퀀싱 함수입니다.
    """
    if not all_hits:
        print("⚠️ [DNA] 분석할 신호(all_hits)가 비어 있습니다.")
        return pd.DataFrame()
    
    # 1. 데이터프레임 변환 및 컬럼 체크
    df = pd.DataFrame(all_hits)
    
    # 💡 [방어 코드] '최고_raw' 컬럼이 없으면 0.0으로 강제 생성
    if '최고_raw' not in df.columns:
        print("⚠️ [DNA] 데이터에 '최고_raw' 컬럼이 없어 기본값(0.0)을 생성합니다.")
        df['최고_raw'] = 0.0
    
    # 2. 날짜순 정렬
    df = df.sort_values(by=['종목', '날짜'])
    
    dna_reports = []
    
    # 💡 Master DNA 추출을 위해 현재 데이터를 다시 get_master_dna에 전달
    master_patterns = get_master_dna(all_hits)
    
    for ticker, group in df.groupby('종목'):
        curr_seq = group['구분'].tolist()
        # 💡 안전하게 데이터 추출
        max_yield = group['최고_raw'].max()
        
        # DNA 일치도 계산
        match_score = calculate_dna_score(curr_seq, master_patterns)
        
        dna_reports.append({
            '종목': ticker,
            'DNA_시퀀스': " ➔ ".join(curr_seq),
            'DNA_일치도': f"{match_score}%",
            '최고수익률': max_yield,
            '유형': "🔥전설과일치" if match_score >= 80 else ("✅검증필요" if match_score >= 50 else "미확인")
        })
        
    return pd.DataFrame(dna_reports).sort_values(by='최고수익률', ascending=False)

def find_winning_pattern(dna_df):
    """
    성공DNA 중 가장 많이 중복되는 패턴 서열을 추출합니다.
    """
    success_only = dna_df[dna_df['최고수익률'] >= 10]
    pattern_counts = success_only['DNA_시퀀스'].value_counts().head(5)
    return pattern_counts


def extract_success_dna(ticker_history_df, threshold=0.20):
    """
    사령관님, 이 함수는 특정 종목의 과거 데이터에서 
    '폭등 전 20일' 동안 어떤 태그들이 찍혔는지 유전자를 추출합니다.
    """
    # 1. 폭등 시점 찾기
    breakout_points = ticker_history_df[ticker_history_df['수익률'] >= threshold]
    
    dna_sequences = []
    for idx in breakout_points.index:
        # 폭등일 기준 과거 20거래일의 태그들만 추출
        lookback = ticker_history_df.loc[:idx].tail(20)
        # 존재했던 태그들을 시간순으로 리스트화 (DNA 지도)
        sequence = lookback['구분'].tolist() 
        dna_sequences.append(sequence)
        
    return dna_sequences

def find_golden_formula(all_dna_data):
    """
    모든 성공주의 DNA를 모아 가장 확률 높은 '패턴 순서'를 찾아냅니다.
    """
    from collections import Counter
    
    # 1. 태그들의 조합 빈도 계산
    # 예: (매집봉, 역매공파) 조합이 몇 번이나 수익을 냈는가?
    formula_counts = Counter([tuple(dna) for dna in all_dna_data])
    return formula_counts.most_common(5)