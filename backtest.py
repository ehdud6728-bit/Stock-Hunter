import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import time

# =========================================================
# ⚙️ [설정] 여기서 내 입맛대로 조건을 바꿉니다
# =========================================================
INITIAL_CAPITAL = 10000000  # 원금: 1,000만원
STOP_LOSS = -0.05         # 손절: -5% (칼손절)
TAKE_PROFIT = 0.15        # 익절: +15% (추세는 길게)
MAX_HOLDING = 10          # 최대 보유일: 10일 (안 오르면 자름)

# 테스트할 종목 (대장주 + 급등끼 있는 종목 20개)
TEST_TICKERS = {
    '005930': '삼성전자', '000660': 'SK하이닉스', '086520': '에코프로',
    '247540': '에코프로비엠', '005380': '현대차', '000270': '기아',
    '005490': 'POSCO홀딩스', '035420': 'NAVER', '035720': '카카오',
    '042700': '한미반도체', '028300': 'HLB', '010130': '고려아연',
    '041510': '에스엠', '035900': 'JYP Ent.', '068270': '셀트리온',
    '000100': '유한양행', '010120': 'LS ELECTRIC', '042660': '대우조선해양',
    '034020': '두산에너빌리티', '009150': '삼성전기'
}

# =========================================================
# 📊 보조지표 계산 (검색식과 동일하게)
# =========================================================
def add_indicators(df):
    df['MA5'] = df['Close'].rolling(5).mean()
    df['MA20'] = df['Close'].rolling(20).mean()
    df['MA60'] = df['Close'].rolling(60).mean()
    
    # RSI
    delta = df['Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / loss)))
    
    # 스토캐스틱 (추세용 비기)
    high = df['High'].rolling(5).max()
    low = df['Low'].rolling(5).min()
    fast_k = ((df['Close'] - low) / (high - low)) * 100
    df['Slow_K'] = fast_k.rolling(3).mean()
    df['Slow_D'] = df['Slow_K'].rolling(3).mean()
    
    return df

# =========================================================
# 🤖 매수 신호 판별 (선생님 전략 100% 반영)
# =========================================================
def check_buy_signal(row, prev_row, strategy_name):
    # 공통: 거래대금 50억 이상 (백테스트니까 조금 낮춰서 많이 잡히게)
    if (row['Close'] * row['Volume']) < 5000000000:
        return False

    # 1. 🦁 [추세] (거래량2배 + 5%상승 + 정배열 + 스토캐스틱)
    if strategy_name == "추세":
        if (row['Change'] >= 0.05) and \
           (row['Volume'] >= prev_row['Volume'] * 2.0) and \
           (row['MA5'] > row['MA20']) and \
           (row['Slow_K'] > row['Slow_D']):
            return True

    # 2. 🕵️ [잠입] (3% 미만 횡보 + 20일선 위 + RSI안정)
    elif strategy_name == "잠입":
        pct = row['Change'] * 100
        if (row['Close'] > row['MA20']) and \
           (-2.0 < pct < 3.0) and \
           (row['RSI'] <= 60) and \
           (row['MA5'] > row['MA20']):
            return True

    # 3. 🎣 [바닥] (역배열 + RSI침체 + 5일선 회복)
    elif strategy_name == "바닥":
        if (row['Close'] < row['MA60']) and \
           (row['RSI'] <= 40) and \
           (row['Close'] > row['MA5']):
            return True
            
    return False

# =========================================================
# 🧪 백테스팅 엔진
# =========================================================
def run_simulation(strategy_name):
    print(f"\n🎮 === [{strategy_name} 전략] 수익률 검증 중... ===")
    
    total_balance = INITIAL_CAPITAL * len(TEST_TICKERS) # 전체 시드
    total_profit = 0
    trade_count = 0
    wins = 0
    
    print(f"📅 기간: 최근 1년 (2023.06 ~ 2024.06)")
    
    for code, name in TEST_TICKERS.items():
        # 데이터 로드
        df = fdr.DataReader(code, '2023-06-01', '2024-06-01')
        if len(df) < 100: continue
        df = add_indicators(df)
        
        # 시뮬레이션
        holding = False
        buy_price = 0
        days_held = 0
        stock_profit = 0
        
        for i in range(1, len(df)-1):
            curr = df.iloc[i]
            prev = df.iloc[i-1]
