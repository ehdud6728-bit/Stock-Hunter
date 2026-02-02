import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor

# ---------------------------------------------------------
# ⚙️ 백테스트 설정
# ---------------------------------------------------------
TEST_DAYS = 200     # 최근 200일(약 10개월) 검증
HOLDING_DAYS = 3    # 매수 후 3일 뒤 매도 (단기 스윙)
TOP_N = 50          # 거래대금 상위 50개만 테스트 (속도 위해)

# ---------------------------------------------------------
# 🧮 지표 계산 함수 (벡터화 연산 - 속도 최적화)
# ---------------------------------------------------------
def add_indicators(df):
    # 1. 이동평균
    df['MA5'] = df['Close'].rolling(5).mean()
    df['MA20'] = df['Close'].rolling(20).mean()
    df['MA60'] = df['Close'].rolling(60).mean()
    
    # 2. 이격도
    df['Disp'] = (df['Close'] / df['MA20']) * 100
    
    # 3. RSI
    delta = df['Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / loss)))

    # 4. Stochastic
    high = df['High'].rolling(9).max()
    low = df['Low'].rolling(9).min()
    fast_k = ((df['Close'] - low) / (high - low)) * 100
    df['Stoch_K'] = fast_k.rolling(3).mean()
    df['Stoch_D'] = df['Stoch_K'].rolling(3).mean()

    # 5. OBV
    direction = df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    df['OBV'] = (direction * df['Volume']).cumsum()
    # OBV 상승 여부 (어제보다 오늘 높은가)
    df['OBV_Rising'] = df['OBV'] > df['OBV'].shift(1)
    
    # 전일 대비 데이터
    df['Prev_Close'] = df['Close'].shift(1)
    df['Prev_Vol'] = df['Volume'].shift(1)
    df['Prev_Change'] = df['Change'].shift(1) # 어제 등락률
    df['Pct'] = df['Change'] * 100 # 오늘 등락률(%)
    df['Vol_Ratio'] = df['Volume'] / df['Prev_Vol']
    
    # 2일전, 3일전 데이터 (골파기 확인용)
    df['Prev2_Close'] = df['Close'].shift(2)
    df['MA20_Prev'] = df['MA20'].shift(1)
    df['MA20_Prev2'] = df['MA20'].shift(2)

    return df

# ---------------------------------------------------------
# 🕵️ 전략 시뮬레이션 (검색식 로직 동일 적용)
# ---------------------------------------------------------
def simulate_stock(ticker, name):
    try:
        # 데이터 가져오기 (넉넉하게 가져와서 지표 계산)
        df = fdr.DataReader(ticker)
        if len(df) < TEST_DAYS + 60: return [] # 데이터 부족하면 패스
        
        df = add_indicators(df)
        
        # 최근 TEST_DAYS 기간만 잘라서 테스트
        target_df = df.iloc[-TEST_DAYS:].copy()
        trades = []
        
        # 날짜별 루프 (여기가 백테스트 핵심)
        for i in range(len(target_df) - HOLDING_DAYS):
            row = target_df.iloc[i]     # 오늘 (매수 신호 뜨는지 확인)
            
            # 미래 데이터 (수익률 확인용)
            future_row = target_df.iloc[i + HOLDING_DAYS] 
            
            # --- [조건 검사] ---
            # 1. 공통 필터 (OBV, RSI, Stoch)
            if not (row['OBV_Rising']): continue
            if not (30 <= row['RSI'] <= 75): continue
            if row['Stoch_K'] < row['Stoch_D']: continue
            
            # 매수 신호 발생 여부
            signal = None
            
            # 🏳️ 숨고르기
            # 어제 10%상승 & 오늘 거래량 절반 & 주가 횡보
            if (row['Prev_Change'] >= 0.10) and (row['Volume'] < row['Prev_Vol'] * 0.5) and (-2.0 <= row['Pct'] <= 2.0):
                signal = "🏳️숨고르기"
            
            # ⛏️ 골파기
            # 어제 20일선 이탈 & 오늘 복구
            elif (row['Prev_Close'] < row['MA20_Prev']) and (target_df.iloc[i-2]['Close'] > row['MA20_Prev2']) and \
                 (row['Close'] > row['MA20']) and (row['Pct'] > 0):
                 signal = "⛏️골파기"
                 
            # 🦁 상승초입
            elif (row['Disp'] <= 110):
                if (row['Vol_Ratio'] >= 1.5) and (row['Pct'] >= 1.0):
                    signal = "🦁돌파"
                elif (-3.0 <= row['Pct'] <= 1.0) and (row['Disp'] <= 105):
                    signal = "🦁눌림"
            
            # --- [수익률 계산] ---
            if signal:
                buy_price = row['Close']
                sell_price = future_row['Close'] # 3일 뒤 종가 매도 가정
                
                # 수익률 (%)
                profit = ((sell_price - buy_price) / buy_price) * 100
                
                trades.append({
                    'Date': target_df.index[i].strftime('%Y-%m-%d'),
                    'Ticker': ticker,
                    'Name': name,
                    'Signal': signal,
                    'Buy': buy_price,
                    'Sell': sell_price,
                    'Return': profit
                })
                
        return trades
    except: return []

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🧪 [백테스트 시작] 최근 {TEST_DAYS}일간 데이터 검증")
    print(f"🎯 대상: 거래대금 상위 {TOP_N}개 종목")
    print(f"⏳ 매매 규칙: 신호 발생 시 매수 -> {HOLDING_DAYS}일 뒤 무조건 매도\n")
    
    # 대상 종목 선정
    df_krx = fdr.StockListing('KRX')
    df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    target_dict = dict(zip(df_leaders['Code'], df_leaders['Name']))
    
    all_trades = []
    
    # 병렬 처리로 속도 향상
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(simulate_stock, t, n) for t, n in target_dict.items()]
        for future in futures:
            res = future.result()
            all_trades.extend(res)
            
    # 결과 분석
    if all_trades:
        df_res = pd.DataFrame(all_trades)
        
        print("-" * 60)
        print(f"📊 [백테스트 결과 리포트]")
        print("-" * 60)
        
        total_count = len(df_res)
        win_count = len(df_res[df_res['Return'] > 0])
        win_rate = (win_count / total_count) * 100
        avg_return = df_res['Return'].mean()
        
        print(f"총 매매 횟수: {total_count}회")
        print(f"승률 (익절): {win_rate:.2f}%")
        print(f"건당 평균 수익: {avg_return:.2f}%")
        print(f"최고 수익: {df_res['Return'].max():.2f}% ({df_res.loc[df_res['Return'].idxmax()]['Name']})")
        print(f"최악 손실: {df_res['Return'].min():.2f}% ({df_res.loc[df_res['Return'].idxmin()]['Name']})")
        print("-" * 60)
        
        # 전략별 승률 분석
        print("📈 [전략별 성적표]")
        strategy_group = df_res.groupby('Signal')['Return'].agg(['count', 'mean', 'min', 'max'])
        # 승률 계산 추가
        win_rates = df_res[df_res['Return'] > 0].groupby('Signal')['Return'].count() / df_res.groupby('Signal')['Return'].count() * 100
        strategy_group['WinRate(%)'] = win_rates
        print(strategy_group)
        
        # 엑셀 저장 (선택)
        # df_res.to_csv('backtest_result.csv', index=False)
    else:
        print("❌ 매매 신호가 하나도 발생하지 않았습니다.")