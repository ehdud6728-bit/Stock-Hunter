import pandas as pd
import yfinance as yf
from pykrx import stock

def get_dynamic_sector_leaders():
    """아침마다 시총 기준 섹터별 대장주를 선정합니다."""
    print("📡 [Leader-Scanner] 오늘의 섹터별 대장주 선출 중...")
    
    # 1. 전 종목 리스트 및 섹터 정보 (FinanceDataReader)
    df_krx = fdr.StockListing('KRX') 
    
    # 2. 전 종목 시가총액 정보 (Pykrx)
    now = datetime.now().strftime("%Y%m%d")
    df_cap = stock.get_market_cap(now, market="ALL")[['시가총액']]
    
    # 3. 데이터 병합 및 섹터별 1위 추출
    df_master = df_krx.set_index('Symbol').join(df_cap)
    df_valid = df_master.dropna(subset=['Sector'])
    
    # {섹터명: 종목코드} 맵 생성
    sector_leader_map = df_valid.groupby('Sector')['시가총액'].idxmax().to_dict()
    
    # 추가: 대장주들의 '상태(강세/침체)'를 미리 분석해서 저장 (속도 최적화)
    leader_status_map = {}
    for sector, ticker in sector_leader_map.items():
        # 대장주 데이터 10일치만 가져와서 상태 판독
        df_l = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d'))
        curr = df_l['Close'].iloc[-1]
        ma5 = df_l['Close'].rolling(5).mean().iloc[-1]
        leader_status_map[sector] = "🔥강세" if curr > ma5 else "❄️침체"
        
    return sector_leader_map, leader_status_map
    
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
