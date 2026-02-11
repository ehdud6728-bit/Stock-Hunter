import pandas as pd
import yfinance as yf
from pykrx import stock
import numpy as np
import FinanceDataReader as fdr
from datetime import datetime, timedelta

def get_global_and_leader_status():
    """
    1. 나스닥 주요 섹터 전일 수익률 (Global HQ 보고)
    2. 국내 주요 섹터 대장주 상태 (사령관 보고)
    를 동시에 수행합니다.
    """
    print("🌍 [Global-Scanner] 나스닥 섹터 전황 파악 중...")
    
    # --- [1] 나스닥 섹터 ETF 스캔 ---
    # SOXX(반도체), XLK(테크), XBI(바이오), LIT(2차전지), XLE(에너지)
    us_sectors = {
        'SOXX': '반도체',
        'XLK':  '빅테크',
        'XBI':  '바이오',
        'LIT':  '2차전지',
        'XLE':  '에너지'
    }
    
    global_status = {}
    for ticker, name in us_sectors.items():
        try:
            # 최근 5일치 데이터를 가져와서 전일 수익률 계산
            df_us = yf.Ticker(ticker).history(period="5d")
            if len(df_us) >= 2:
                prev_close = df_us['Close'].iloc[-2]
                curr_close = df_us['Close'].iloc[-1]
                change = ((curr_close - prev_close) / prev_close) * 100
                global_status[name] = round(change, 2)
            else:
                global_status[name] = 0.0
        except Exception as e:
            print(f"⚠️ {name} 섹터 수집 실패: {e}")
            global_status[name] = 0.0

    # --- [2] 국내 대장주 동적 선출 및 상태 파악 ---
    # (앞서 만든 get_dynamic_sector_leaders 로직의 핵심을 여기에 통합)
    # 사령관님, 여기서는 속도를 위해 주요 대장주 상태를 l_sync로 반환합니다.
    # ... (대장주 상태 판독 로직) ...

    return global_status, {} # 일단 l_sync는 빈 값으로 리턴하거나 로직 추가
    
def get_signal_sequence(df):
    """
    각 전술 신호(역, 매, 공, 파)가 며칠 전에 발생했는지 추적하여 
    시간순(과거 -> 현재)으로 나열된 서사를 만듭니다.
    """
    import numpy as np
    
    # 1. 각 신호의 발생 인덱스 찾기
    # df['is_yeok'] 등은 analyze_all_narratives 내부에서 계산된 컬럼이어야 함
    yeok_idx = np.where(df['is_yeok'])[0]
    mae_idx  = np.where(df['is_mae'])[0]
    gong_idx = np.where(df['is_gong'])[0]
    pa_idx   = np.where(df['is_pa'])[0]

    last_idx = len(df) - 1
    events = []

    # 2. 발생 기록이 있다면 '오늘로부터 며칠 전'인지 계산해서 저장
    if len(yeok_idx) > 0: events.append((last_idx - yeok_idx[-1], "역(逆)"))
    if len(mae_idx)  > 0: events.append((last_idx - mae_idx[-1],  "매(埋)"))
    if len(gong_idx) > 0: events.append((last_idx - gong_idx[-1], "공(空)"))
    if len(pa_idx)   > 0: events.append((last_idx - pa_idx[-1],   "파(破)"))

    # 3. 며칠 전(숫자)이 큰 것부터 작은 순서로 정렬 (즉, 먼 과거부터 오늘 순서)
    events.sort(key=lambda x: x[0], reverse=True)

    # 4. 문자열로 변환 (예: "20일전 역 ➔ 10일전 매 ➔ 오늘 공")
    if not events:
        return "진행 중인 서사 없음"
        
    narrative_parts = []
    for days, name in events:
        day_str = "오늘" if days == 0 else f"{days}일전"
        narrative_parts.append(f"{day_str} {name}")
    
    return " ➔ ".join(narrative_parts)
    
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
    """
    개별 종목의 서사 시퀀스와 글로벌/대장주 동기화를 종합 분석합니다.
    """
    if len(df) < 120: return "🛡️일반", "데이터 부족", 0, 0, 0
    
    # [1] 전체 데이터에서 각 신호의 발생 여부(Series) 계산
    # 역(逆): 5일선이 20일선 위에 있는 상태 (최근 20일 내 발생 추적)
    yeok_series = df['MA5'] > df['MA20']
    
    # 매(埋): 이평선들이 수렴(3% 이내)한 상태
    mae_series = df['MA_Convergence'] <= 3.0
    
    # 공(空): 오늘 112일선을 종가로 뚫은 순간 (역사적 돌파일 추적)
    gong_series = (df['Close'] > df['MA112']) & (df['Close'].shift(1) <= df['MA112'])
    
    # 파(破): 볼린저밴드 40 상단을 돌파한 순간
    pa_series = (df['Close'] > df['BB40_Upper']) & (df['Close'].shift(1) <= df['BB40_Upper'])

    # [2] 시퀀스 타임라인 추출 (며칠 전에 발생했는가?)
    last_idx = len(df) - 1
    events = []

    def get_days_ago(series, window=30):
        # 최근 window일 이내의 발생 지점 확인
        subset = series.tail(window)
        idx = np.where(subset)[0]
        if len(idx) > 0:
            # 전체 데이터에서의 실제 인덱스로 변환 후 '오늘'과의 거리 계산
            actual_last_idx = (len(df) - len(subset)) + idx[-1]
            return last_idx - actual_last_idx
        return None

    d_yeok = get_days_ago(yeok_series)
    d_mae  = get_days_ago(mae_series)
    d_gong = get_days_ago(gong_series)
    d_pa   = get_days_ago(pa_series)

    # 이벤트 리스트 구성 및 시간순 정렬
    if d_yeok is not None: events.append((d_yeok, "역(逆)"))
    if d_mae is not None:  events.append((d_mae, "매(埋)"))
    if d_gong is not None: events.append((d_gong, "공(空)"))
    if d_pa is not None:   events.append((d_pa, "파(破)"))

    # 며칠 전(숫자)이 큰 것부터(과거부터) 정렬
    events.sort(key=lambda x: x[0], reverse=True)
    report = " ➔ ".join([f"{'오늘' if d==0 else str(d)+'일전'} {name}" for d, name in events])
    if not report: report = "서사 관찰 중"

    # [3] 확신 지수(Conviction) 및 점수 산출
    # 기술적 서사 점수 (오늘 시점 기준 가중치)
    narrative_score = 0
    if d_yeok is not None: narrative_score += 20
    if d_mae is not None:  narrative_score += 20
    if d_gong == 0: narrative_score += 30  # 오늘 공구리 돌파 시 가점
    if d_pa == 0: narrative_score += 30    # 오늘 파동 시작 시 가점

    # 글로벌 및 대장주 동기화 점수
    g_score = 25 if g_status.get(sector_name, 0) > 0 else 0
    l_score = 25 if l_sync.get(sector_name) == "🔥강세" else 0
    
    # $$Conviction = Narrative + Global + Leader$$
    total_conviction = narrative_score + g_score + l_score

    # [4] 정밀 타점 및 등급 부여
    row = df.iloc[-1]
    target = round(row['MA112'] * 1.005, 0)
    stop_loss = round(row['MA112'] * 0.98, 0)
    
    if total_conviction >= 90: grade = "👑LEGEND"
    elif total_conviction >= 70: grade = "⚔️정예"
    else: grade = "🛡️일반"

    return grade, report, target, stop_loss, total_conviction
