# [독립 모듈] DNA_Analyzer.py

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