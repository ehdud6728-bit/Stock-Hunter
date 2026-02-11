import pandas as pd
import yfinance as yf
from pykrx import stock

def get_global_and_leader_status():
    """나스닥 섹터와 국장 대장주 상태를 아침마다 스캔합니다."""
    # 1. 나스닥 섹터 (yfinance)
    sectors = {'SOXX': '반도체', 'XLK': '빅테크', 'XBI': '바이오', 'LIT': '2차전지'}
    global_status = {}
    for t, name in sectors.items():
        try:
            hist = yf.Ticker(t).history(period="2d")
            change = ((hist['Close'].iloc[-1] - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100
            global_status[name] = round(change, 2)
        except: global_status[name] = 0.0

    # 2. 국장 대장주 (pykrx) - 예시: 하이닉스(반도체), 셀트리온(바이오), LG엔솔(2차전지)
    leaders = {'000660': '반도체', '068270': '바이오', '373220': '2차전지'}
    leader_sync = {}
    for t, name in leaders.items():
        try:
            df_l = stock.get_market_ohlcv_by_date("20260101", "20261231", t) # 2026년 날짜 적용
            ma5 = df_l['종가'].rolling(5).mean().iloc[-1]
            curr = df_l['종가'].iloc[-1]
            leader_sync[name] = "🔥강세" if curr > ma5 else "❄️침체"
        except: leader_sync[name] = "Normal"
        
    return global_status, leader_sync

def analyze_all_narratives(df, ticker_name, sector_name, g_status, l_sync):
    """개별 종목의 서사와 글로벌/대장주 동기화를 종합 분석합니다."""
    row = df.iloc[-1]
    prev = df.iloc[-2]
    
    # [1] 기술적 서사 체크 (역매공파)
    is_yeok = (df['MA5'].iloc[-20:] > df['MA20'].iloc[-20:]).any()
    is_mae = df['MA_Convergence'].iloc[-10:].min() <= 3.0
    is_gong = (row['Close'] > row['MA112']) and (prev['Close'] <= row['MA112'])
    is_pa = (row['Close'] > row['BB40_Upper']) and (prev['Close'] <= row['BB40_Upper'])

    # [2] 서사 요약 및 점수
    narrative_score = 0
    history = []
    if is_yeok: narrative_score += 20; history.append("바닥확인")
    if is_mae: narrative_score += 20; history.append("에너지응축")
    if is_gong: narrative_score += 30; history.append("공구리돌파")
    if is_pa: narrative_score += 30; history.append("파동시작")

    # [3] 확신 지수(Conviction) 산출
    # $$Conviction = (Narrative \times 0.5) + (Global \times 0.25) + (Leader \times 0.25)$$
    g_score = 25 if g_status.get(sector_name, 0) > 0 else 0
    l_score = 25 if l_sync.get(sector_name) == "🔥강세" else 0
    total_conviction = narrative_score + g_score + l_score

    # [4] 정밀 타점
    target = round(row['MA112'] * 1.005, 0)
    stop_loss = round(row['MA112'] * 0.98, 0)
    
    # 등급 부여
    if total_conviction >= 90: grade = "👑LEGEND"
    elif total_conviction >= 70: grade = "⚔️정예"
    else: grade = "🛡️일반"

    report = " ➔ ".join(history)
    return grade, report, target, stop_loss, total_conviction
