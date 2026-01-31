import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta
import pytz

# ---------------------------------------------------------
# ⚙️ 백테스트 설정
# ---------------------------------------------------------
START_DATE = "2026-01-05"  # 검증 시작일
END_DATE = datetime.now().strftime('%Y-%m-%d')
HOLDING_DAYS = 10          # 선생님 요청대로 10일!

print(f"🕵️‍♂️ [정밀 백테스트] 기간: {START_DATE} ~ {END_DATE}")
print(f"🎯 전략: 10일간의 최고점(High)과 최저점(Low) 추적")
print("-" * 60)

# 시가총액 상위 50개 (우량주 대상 검증)
krx = fdr.StockListing('KRX')
top50 = krx.sort_values(by='Marcap', ascending=False).head(50)
TARGET_CODES = top50['Code'].astype(str).tolist()
NAME_MAP = dict(zip(krx['Code'].astype(str), krx['Name']))

# ---------------------------------------------------------
# 🧮 전략 로직 (Wide Mode 적용)
# ---------------------------------------------------------
def check_strategy(df, i):
    # 데이터 부족 시 패스
    if i < 60: return None 

    curr = df.iloc[i]
    prev = df.iloc[i-1]
    
    # 과거 데이터만 사용해서 지표 계산
    subset = df.iloc[:i+1]
    close = subset['Close']
    
    ma5 = close.rolling(5).mean().iloc[-1]
    ma20 = close.rolling(20).mean().iloc[-1]
    ma60 = close.rolling(60).mean().iloc[-1]
    
    # RSI
    delta = close.diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rsi_val = 100 - (100 / (1 + (gain.iloc[-1] / loss.iloc[-1])))
    
    pct = curr['Change'] * 100
    
    # 1. 🦁 [추세] (완화된 조건)
    if (ma5 > ma20) and (pct >= 2.0) and (curr['Close'] > ma20):
        if curr['Volume'] >= prev['Volume'] * 1.0:
            return "🦁 추세"

    # 2. 🎣 [바닥]
    elif (curr['Close'] < ma60) and (curr['Close'] > ma5) and (rsi_val <= 55):
        return "🎣 바닥"

    # 3. 🕵️ [잠입]
    elif (curr['Close'] > ma20) and (-3.0 < pct < 5.0) and (curr['Volume'] < prev['Volume']):
        return "🕵️ 잠입"
        
    return None

# ---------------------------------------------------------
# 🚀 검증 실행
# ---------------------------------------------------------
total_trades = 0
total_max_profit = 0.0 # 최고 수익률 합계
total_final_profit = 0.0 # 최종 수익률 합계

print("⚡ 과거 데이터로 시뮬레이션 중...")

for code in TARGET_CODES:
    name = NAME_MAP.get(code, code)
    try:
        # 넉넉하게 데이터 로드
        df = fdr.DataReader(code, '2025-10-01', END_DATE)
        dates = df.index.strftime('%Y-%m-%d').tolist()
        
        try:
            start_idx = dates.index(START_DATE)
        except: continue # 시작일 데이터 없으면 패스

        # 시뮬레이션
        # (10일 뒤 데이터가 있는 곳까지만 반복)
        for i in range(start_idx, len(df) - HOLDING_DAYS):
            signal = check_strategy(df, i)
            
            if signal:
                buy_date = dates[i]
                buy_price = df.iloc[i]['Close']
                
                # 향후 10일간의 데이터 조회
                future_window = df.iloc[i+1 : i+1+HOLDING_DAYS]
                
                if len(future_window) < HOLDING_DAYS: continue

                # 1. 최고가 (Best Case)
                highest_price = future_window['High'].max()
                max_profit = ((highest_price - buy_price) / buy_price) * 100
                
                # 2. 최저가 (Worst Case)
                lowest_price = future_window['Low'].min()
                max_loss = ((lowest_price - buy_price) / buy_price) * 100
                
                # 3. 10일 뒤 종가 (Final Case)
                final_price = future_window.iloc[-1]['Close']
                final_profit = ((final_price - buy_price) / buy_price) * 100
                
                total_trades += 1
                total_max_profit += max_profit
                total_final_profit += final_profit
                
                print(f"[{buy_date}] {signal} {name}")
                print(f"   └ 진입가: {format(int(buy_price),',')}원")
                print(f"   🔥 최고: +{max_profit:.2f}%  (이때 팔았으면 대박)")
                print(f"   💧 최저: {max_loss:.2f}%  (이때 팔았으면 쪽박)")
                print(f"   🏁 최종: {final_profit:.2f}%  (10일 존버 결과)")
                print("-" * 40)

    except Exception as e:
        continue

# ---------------------------------------------------------
# 📊 종합 결산
# ---------------------------------------------------------
print("\n" + "=" * 60)
print(f"📊 [10일 보유 전략] 최종 성적표")
if total_trades > 0:
    avg_max = total_max_profit / total_trades
    avg_final = total_final_profit / total_trades
    
    print(f"총 매매 기회: {total_trades}번")
    print(f"🔥 평균 최고 수익률: +{avg_max:.2f}% (잠재력)")
    print(f"🏁 평균 최종 수익률: {avg_final:+.2f}% (실현손익)")
    
    print("\n[AI의 한줄평]")
    if avg_final > 5: print("대박입니다! 10일 스윙 전략이 아주 잘 먹힙니다. 🚀")
    elif avg_final > 0: print("나쁘지 않습니다. 은행 이자보단 낫네요. 🏦")
    else: print("전략 수정이 필요합니다. 10일은 너무 긴가 봅니다. 📉")
else:
    print("해당 기간에 포착된 종목이 없습니다.")
print("=" * 60)
