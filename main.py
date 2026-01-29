import FinanceDataReader as fdr
from pykrx import stock
import pandas as pd
import numpy as np
import requests
import os
import time
from datetime import datetime, timedelta

# --- [환경변수] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

# 🔥 [설정] 수급 최소 금액 (단위: 원)
# 1억이 부담스럽다면 5000만원(50000000) or 3000만원(30000000)으로 조절하세요.
MIN_BUY_AMOUNT = 50000000 

# ---------------------------------------------------------
# [수급 데이터 분석 함수] (금액 기준으로 변경됨!)
# ---------------------------------------------------------
def get_supply_filtered_tickers():
    """
    최근 5일간 외국인/기관 순매수 '금액' 조건을 만족하는 종목 리스트 반환
    조건: 5천만원 이상 매수 (O, P, Q, R 조건 적용)
    """
    print(f"⚡ [1단계] 수급 분석 시작 (기준: {int(MIN_BUY_AMOUNT/10000)}만원 이상 순매수)...")
    
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=12)).strftime("%Y%m%d") # 휴일 고려 넉넉히
    
    # 영업일 추출
    dates = stock.get_index_ohlcv_by_date(start_date, end_date, "1001").index
    target_dates = dates[-5:] # 최근 5일
    
    if len(target_dates) < 5:
        print("데이터 부족")
        return []

    supply_data = {} 
    
    for date in target_dates:
        ymd = date.strftime("%Y%m%d")
        try:
            # 🚨 중요: "value" 옵션으로 '거래대금(원)'을 가져옵니다.
            df = stock.get_market_net_purchases_of_equities_by_ticker(ymd, "ALL", "value")
            
            for ticker, row in df.iterrows():
                if ticker not in supply_data:
                    supply_data[ticker] = {'for': [], 'inst': []}
                
                supply_data[ticker]['for'].append(row['외국인'])
                supply_data[ticker]['inst'].append(row['기관합계'])
                
        except Exception as e:
            continue
            
    # 조건 검증
    passed_tickers = []
    
    for ticker, data in supply_data.items():
        if len(data['for']) < 5: continue
        
        f_list = np.array(data['for'])
        i_list = np.array(data['inst'])
        
        # [O] 5일중 3일 이상 '5천만원' 이상 순매수
        cond_O = (f_list >= MIN_BUY_AMOUNT).sum() >= 3
        
        # [P] 5일중 3일 이상 '5천만원' 이상 순매수
        cond_P = (i_list >= MIN_BUY_AMOUNT).sum() >= 3
        
        # [Q] 오늘(마지막날) '5천만원' 이상 순매수
        cond_Q = f_list[-1] >= MIN_BUY_AMOUNT
        
        # [R] 오늘(마지막날) '5천만원' 이상 순매수
        cond_R = i_list[-1] >= MIN_BUY_AMOUNT
        
        # 최종 수급 논리 (OR 조건)
        if (cond_O or cond_P) or (cond_Q and cond_R):
            passed_tickers.append(ticker)
            
    print(f"✅ 수급(5천만원↑) 통과: {len(passed_tickers)}개 종목")
    return passed_tickers

# ---------------------------------------------------------
# [보조지표 및 차트 분석] (이전과 동일)
# ---------------------------------------------------------
def calc_rsi(series, period=14):
    delta = series.diff(1)
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calc_mfi(high, low, close, volume, period=14):
    typical_price = (high + low + close) / 3
    money_flow = typical_price * volume
    positive_flow = money_flow.where(typical_price > typical_price.shift(1), 0).rolling(window=period).sum()
    negative_flow = money_flow.where(typical_price < typical_price.shift(1), 0).rolling(window=period).sum()
    mfi = 100 - (100 / (1 + positive_flow / negative_flow))
    return mfi

def calc_stochastic(high, low, close, n=5, m=3, t=3):
    lowest_low = low.rolling(window=n).min()
    highest_high = high.rolling(window=n).max()
    fast_k = ((close - lowest_low) / (highest_high - lowest_low)) * 100
    slow_k = fast_k.rolling(window=m).mean()
    slow_d = slow_k.rolling(window=t).mean()
    return slow_k, slow_d

def calc_dmi_adx(high, low, close, n=14):
    tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.rolling(window=n).mean()
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
    plus_di = 100 * (pd.Series(plus_dm).rolling(window=n).mean() / atr)
    minus_di = 100 * (pd.Series(minus_dm).rolling(window=n).mean() / atr)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    adx = dx.rolling(window=n).mean()
    return plus_di, minus_di, adx

def check_technical_condition(ticker):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=250)).strftime('%Y-%m-%d'))
        if len(df) < 125: return None

        curr = df.iloc[-1]
        prev = df.iloc[-2]
        close = df['Close']
        vol = df['Volume']
        
        # 최소 거래대금 30억 (잡주 방지)
        if (curr['Close'] * curr['Volume']) < 3000000000: return None 

        # --- 지표 계산 ---
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        ma120 = close.rolling(120).mean()
        
        mfi = calc_mfi(df['High'], df['Low'], close, vol, 14)
        slow_k, slow_d = calc_stochastic(df['High'], df['Low'], close, 5, 3, 3)
        p_di, m_di, adx = calc_dmi_adx(df['High'], df['Low'], close, 14)

        # --- 조건 검증 ---
        
        # [D] 거래량비율 200%
        if prev['Volume'] == 0: return None
        cond_D = (curr['Volume'] / prev['Volume']) >= 2.0
        
        # [E] 정배열 초입 (종가 > 20)
        cond_E = curr['Close'] > ma20.iloc[-1]

        # [F] 5일선 골든크로스
        cond_F = (prev['Close'] <= ma5.iloc[-2]) and (curr['Close'] > ma5.iloc[-1])

        # [G, H] 추세 유지
        cond_G = ma120.iloc[-1] >= ma120.iloc[-2]
        cond_H = ma60.iloc[-1] >= ma60.iloc[-2]

        # [I or J] MFI or DMI
        cond_I = (mfi.iloc[-2] <= 50) and (mfi.iloc[-1] > 50)
        cond_J = (p_di.iloc[-2] <= m_di.iloc[-2]) and (p_di.iloc[-1] > m_di.iloc[-1])
        
        # [L or M or N] 스토캐스틱 or 등락률 or ADX
        cond_L = (slow_k.iloc[-2] <= slow_d.iloc[-2]) and (slow_k.iloc[-1] > slow_d.iloc[-1])
        cond_M = ((curr['Close'] - prev['Close']) / prev['Close']) >= 0.05
        cond_N = adx.iloc[-1] > adx.iloc[-2]

        if cond_D and cond_E and cond_F and cond_G and cond_H and (cond_I or cond_J) and (cond_L or cond_M or cond_N):
            name = stock.get_market_ticker_name(ticker)
            return f"💎 {name}({ticker})\n- 가격: {format(int(curr['Close']), ',')}원 (+{round((curr['Close']/prev['Close']-1)*100,2)}%)\n- 거래량: 전일대비 {round(curr['Volume']/prev['Volume']*100)}% 터짐\n- 수급: 5천만원 이상 유입 ✅"
            
    except:
        return None
    return None

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {'chat_id': TELEGRAM_CHAT_ID, 'text': message}
    try: requests.post(url, data=data)
    except: pass

# --- 실행 ---
print("🚀 [최종 업데이트] 5천만원 수급 필터 검색기 가동")
filtered_tickers = get_supply_filtered_tickers()

if not filtered_tickers:
    print("수급 조건 만족 종목 없음")
    send_telegram("🔔 [수급 필터] 5천만원 이상 매수 종목이 없습니다.")
else:
    print(f"⚡ {len(filtered_tickers)}개 종목 2차 분석 중...")
    results = []
    for ticker in filtered_tickers:
        res = check_technical_condition(ticker)
        if res:
            results.append(res)
            print(f"[발견] {ticker}")

    if results:
        header = f"🔥 [거거익선 5천] 포착 종목 ({datetime.now().strftime('%Y-%m-%d')})\n조건: 수급(5천만원↑) + 차트 급등\n\n"
        full_msg = header + "\n\n".join(results)
        
        if len(full_msg) > 4000:
            for i in range(0, len(results), 5):
                send_telegram(header + "\n\n".join(results[i:i+5]))
        else:
            send_telegram(full_msg)
    else:
        send_telegram(f"🔔 수급(5천만원↑) 종목 {len(filtered_tickers)}개 중 차트 조건 만족 종목이 없습니다.")
