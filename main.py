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

# ---------------------------------------------------------
# [수급 데이터 분석 함수] (여기가 핵심!)
# ---------------------------------------------------------
def get_supply_filtered_tickers():
    """
    최근 5일간 외국인/기관 수급 조건을 만족하는 종목 리스트 반환
    조건: ((O or P) or (Q and R))
    """
    print("⚡ [1단계] 수급 데이터(외인/기관) 분석 중... (약 1~2분 소요)")
    
    # 1. 최근 영업일 5일 구하기
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d") # 넉넉히
    
    # pykrx로 일자별 등락률 데이터 등에서 영업일 추출
    # (휴일 처리를 위해 인덱스용으로 하나 호출)
    dates = stock.get_index_ohlcv_by_date(start_date, end_date, "1001").index
    target_dates = dates[-5:] # 최근 5일
    
    if len(target_dates) < 5:
        print("데이터 부족으로 최근 5일치 확보 실패 (연휴 등)")
        return []

    # 2. 일자별 수급 데이터 수집 (Bulk)
    # daily_supply[티커] = { 'foreign': [5일치], 'inst': [5일치] } 형태
    supply_data = {} 
    
    # 5일치 데이터를 하루씩 긁어옵니다 (속도 최적화)
    for date in target_dates:
        ymd = date.strftime("%Y%m%d")
        try:
            # 해당 날짜의 전 종목 투자자별 순매수 (단위: 원? 주? -> pykrx 기본은 '거래대금'이 아니라 '거래량'으로 가져오거나 설정 가능)
            # 여기선 '순매수량(주)' 기준으로 봅니다. (검색식 조건: 1주 이상)
            df = stock.get_market_net_purchases_of_equities_by_ticker(ymd, "ALL", "volume")
            
            for ticker, row in df.iterrows():
                if ticker not in supply_data:
                    supply_data[ticker] = {'for': [], 'inst': []}
                
                # 외국인, 기관 데이터 저장
                supply_data[ticker]['for'].append(row['외국인'])
                supply_data[ticker]['inst'].append(row['기관합계'])
                
        except Exception as e:
            print(f"Date {ymd} Error: {e}")
            continue
            
    # 3. 조건 검증 ((O or P) or (Q and R))
    passed_tickers = []
    
    for ticker, data in supply_data.items():
        if len(data['for']) < 5: continue # 신규상장 등 데이터 부족
        
        # 리스트 순서: [D-4, D-3, D-2, D-1, D-0(오늘)]
        f_list = np.array(data['for'])
        i_list = np.array(data['inst'])
        
        # [O] 5일중 3일 외국인 순매수 (양수)
        cond_O = (f_list > 0).sum() >= 3
        
        # [P] 5일중 3일 기관 순매수
        cond_P = (i_list > 0).sum() >= 3
        
        # [Q] 오늘(마지막날) 외국인 순매수
        cond_Q = f_list[-1] > 0
        
        # [R] 오늘(마지막날) 기관 순매수
        cond_R = i_list[-1] > 0
        
        # 최종 수급 논리
        if (cond_O or cond_P) or (cond_Q and cond_R):
            passed_tickers.append(ticker)
            
    print(f"✅ 수급 조건 통과: {len(passed_tickers)}개 종목 (전체 {len(supply_data)}개 중)")
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
        # 최근 150일 데이터 (차트 분석용)
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=250)).strftime('%Y-%m-%d'))
        if len(df) < 125: return None

        curr = df.iloc[-1]
        prev = df.iloc[-2]
        close = df['Close']
        vol = df['Volume']
        
        # [K] 시가총액 필터는 여기서 생략 (이미 수급 들어온 놈들은 어느정도 규모 있음 or 나중에 네이버 등에서 확인)
        # 그래도 최소한의 거래대금 체크
        if (curr['Close'] * curr['Volume']) < 3000000000: return None # 30억 미만 제외

        # --- 지표 계산 ---
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        ma120 = close.rolling(120).mean()
        
        mfi = calc_mfi(df['High'], df['Low'], close, vol, 14)
        slow_k, slow_d = calc_stochastic(df['High'], df['Low'], close, 5, 3, 3)
        p_di, m_di, adx = calc_dmi_adx(df['High'], df['Low'], close, 14)

        # --- 조건 검증 (이미지 내용) ---
        
        # [D] 거래량비율: 전일 대비 200% 이상 (수급 폭발)
        if prev['Volume'] == 0: return None
        cond_D = (curr['Volume'] / prev['Volume']) >= 2.0
        
        # [E] 정배열 초입 (종가 > 20이평)
        cond_E = curr['Close'] > ma20.iloc[-1]

        # [F] 5일선 골든크로스 (어제는 아래, 오늘은 위)
        cond_F = (prev['Close'] <= ma5.iloc[-2]) and (curr['Close'] > ma5.iloc[-1])

        # [G, H] 추세 유지 (120일, 60일 상승)
        cond_G = ma120.iloc[-1] >= ma120.iloc[-2]
        cond_H = ma60.iloc[-1] >= ma60.iloc[-2]

        # [I or J] MFI or DMI
        cond_I = (mfi.iloc[-2] <= 50) and (mfi.iloc[-1] > 50)
        cond_J = (p_di.iloc[-2] <= m_di.iloc[-2]) and (p_di.iloc[-1] > m_di.iloc[-1])
        
        # [L or M or N] 스토캐스틱 or 등락률 or ADX
        cond_L = (slow_k.iloc[-2] <= slow_d.iloc[-2]) and (slow_k.iloc[-1] > slow_d.iloc[-1])
        cond_M = ((curr['Close'] - prev['Close']) / prev['Close']) >= 0.05
        cond_N = adx.iloc[-1] > adx.iloc[-2]

        # 최종 조합 (수급은 이미 통과했으므로 생략)
        if cond_D and cond_E and cond_F and cond_G and cond_H and (cond_I or cond_J) and (cond_L or cond_M or cond_N):
            name = stock.get_market_ticker_name(ticker) # pykrx로 이름 가져오기
            return f"💎 {name}({ticker})\n- 가격: {format(int(curr['Close']), ',')}원 (+{round((curr['Close']/prev['Close']-1)*100,2)}%)\n- 거래량: 전일대비 {round(curr['Volume']/prev['Volume']*100)}% 터짐\n- 수급: 외인/기관 조건 만족 ✅"
            
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
print("🚀 [완전 자동화 검색기] 가동 시작")
filtered_tickers = get_supply_filtered_tickers()

if not filtered_tickers:
    print("수급 조건 만족 종목 없음")
    send_telegram("🔔 [수급 필터] 조건을 만족하는 종목이 하나도 없습니다.")
else:
    print(f"⚡ [2단계] {len(filtered_tickers)}개 종목 기술적 정밀 분석 시작...")
    results = []
    for ticker in filtered_tickers:
        res = check_technical_condition(ticker)
        if res:
            results.append(res)
            print(f"[발견!] {ticker}")

    if results:
        header = f"🔥 [거거익선 Final] 포착 종목 ({datetime.now().strftime('%Y-%m-%d')})\n조건: 수급(외/기) + 거래량 + 차트 완벽\n\n"
        full_msg = header + "\n\n".join(results)
        
        if len(full_msg) > 4000:
            for i in range(0, len(results), 5):
                send_telegram(header + "\n\n".join(results[i:i+5]))
        else:
            send_telegram(full_msg)
    else:
        send_telegram(f"🔔 수급 좋은 종목 {len(filtered_tickers)}개를 샅샅이 뒤졌으나, 차트 조건(거래량/보조지표)까지 맞는 게 없습니다.")
