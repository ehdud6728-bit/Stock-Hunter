import pandas as pd
import numpy as np
import yfinance as yf
import FinanceDataReader as fdr
from pykrx import stock
from datetime import datetime, timedelta
import traceback

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

    # 💡 [수정 포인트] fdr의 KRX 데이터는 'Symbol'이 아니라 'Code' 컬럼을 사용합니다.
    if 'Code' in df_krx.columns:
        df_krx = df_krx.rename(columns={'Code': 'Symbol'}) # 통일성을 위해 Symbol로 이름을 바꿉니다.
        
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
    
# =================================================
# 📡 [1. 글로벌 & 대장주 상황 브리핑]
# =================================================
def get_global_and_leader_status():
    print("🌍 [Global-Scanner] 글로벌 및 국내 섹터 상태 점검 중...")
    global_status = {}
    leader_status = {}
    
    # [A] 나스닥 섹터 (이건 안정적입니다)
    try:
        us_sectors = {'SOXX': '반도체', 'XLK': '빅테크', 'XBI': '바이오', 'LIT': '2차전지', 'XLE': '에너지'}
        for ticker, name in us_sectors.items():
            df_us = yf.Ticker(ticker).history(period="5d")
            if len(df_us) >= 2:
                chg = ((df_us['Close'].iloc[-1] - df_us['Close'].iloc[-2]) / df_us['Close'].iloc[-2]) * 100
                global_status[name] = round(chg, 2)
    except: pass

    # [B] 국내 섹터 대장주 스캔 (무결성 강화)
    try:
        df_krx = fdr.StockListing('KRX')
        
        # 💡 [명찰 강제 집행] 0번은 Code, 1번은 Name으로 고정
        df_krx.columns.values[0] = 'Symbol'
        df_krx.columns.values[1] = 'Name'
        
        # 💡 [섹터 칸 강제 생성] Sector, Industry, 업종 중 하나라도 있으면 쓰고, 없으면 새로 만듬
        s_col = next((c for c in ['Sector', 'Industry', '업종', 'SectorName'] if c in df_krx.columns), None)
        
        if s_col:
            df_krx = df_krx.rename(columns={s_col: 'Sector'})
        else:
            # 섹터 정보가 아예 안 들어왔을 경우 (비상)
            df_krx['Sector'] = '일반'
            
        now_str = datetime.now().strftime("%Y%m%d")
        df_cap = stock.get_market_cap(now_str, market="ALL")[['시가총액']]
        
        # 데이터 병합
        df_master = df_krx.set_index('Symbol').join(df_cap)
        
        # 만약 병합 후 'Sector'가 유실되었다면 다시 '일반'으로 채움
        if 'Sector' not in df_master.columns:
            df_master['Sector'] = '일반'
        df_master['Sector'] = df_master['Sector'].fillna('일반')

        # 섹터별 대장주 추출 (이제 'Sector' 컬럼이 무조건 존재함)
        target_sects = ['반도체', '제약', '소프트웨어', '전기제품', '화학']
        
        # 시총 기준 정렬 후 그룹화하여 1위 추출
        sector_leader_map = df_master.sort_values('시가총액', ascending=False).groupby('Sector').head(1)
        leader_dict = sector_leader_map.set_index('Sector').index.to_series().to_dict() # 실제 존재하는 섹터 확인
        
        # 대장주 상태 파악
        for sect in target_sects:
            # 해당 섹터의 시총 1위 종목 코드 가져오기
            leader_row = df_master[df_master['Sector'] == sect].sort_values('시가총액', ascending=False).head(1)
            if not leader_row.empty:
                ticker = leader_row.index[0]
                df_l = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d'))
                curr, ma5 = df_l['Close'].iloc[-1], df_l['Close'].rolling(5).mean().iloc[-1]
                leader_status[sect] = "🔥강세" if curr > ma5 else "❄️침체"
                
    except Exception as e:
        # 에러가 나도 프로그램을 멈추지 않고 빈 딕셔너리 리턴
        print(f"⚠️ [Leader-Scanner] 국내 대장주 스캔 우회 중: {e}")
        leader_status = {}

    return global_status, leader_status

def get_global_and_leader_status_back():
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
    # --- [B] 국내 섹터별 동적 대장주 추출 및 상태 (pykrx + fdr) ---
    try:
        now_str = datetime.now().strftime("%Y%m%d")
        df_krx = fdr.StockListing('KRX')
    
        # 💡 핵심 수정: fdr은 'Symbol'이 아니라 'Code'를 사용합니다.
        # 이를 'Symbol'로 이름을 바꿔주면 뒤쪽 코드와 호환됩니다.
        if 'Code' in df_krx.columns:
            df_krx = df_krx.rename(columns={'Code': 'Symbol'})
    
        # 2. 섹터(업종) 컬럼 표준화 (Sector / Industry / 업종 대응)
        # 어떤 이름으로 들어오든 'Sector'로 통일합니다.
        possible_sector_names = ['Sector', 'Industry', '업종']
        found_sector_col = None
        for col in possible_sector_names:
            if col in df_krx.columns:
                found_sector_col = col
                break
        
        if found_sector_col:
            df_krx = df_krx.rename(columns={found_sector_col: 'Sector'})
        else:
            # 섹터 정보가 아예 없는 경우 (비상상황)
            # 빈 값이라도 채워서 에러를 방지합니다.
            df_krx['Sector'] = '기타'
            
        df_cap = stock.get_market_cap(now_str, market="ALL")[['시가총액']]
        
        # 섹터 정보와 시가총액 결합
        df_master = df_krx.set_index('Symbol').join(df_cap).dropna(subset=['Sector'])
    
        # 'Sector' 컬럼이 존재하는지 최종 확인 후 dropna 수행
        if 'Sector' in df_master.columns:
            df_master = df_master.dropna(subset=['Sector'])
        else:
            # 여기까지 왔는데 Sector가 없다면 병합 과정에서 유실된 것
            df_master['Sector'] = '기타'
        
        # 섹터별 시총 1위(대장주) 추출
        sector_leader_map = df_master.groupby('Sector')['시가총액'].idxmax().to_dict()
        
        leader_status = {}
        # 주요 섹터 대장주들의 컨디션(5일선 위/아래) 체크
        target_sectors = ['반도체', '제약', '소프트웨어', '전기제품', '화학'] # 국장 주요 섹터명
        
        for sect in target_sectors:
            ticker = sector_leader_map.get(sect)
            if ticker:
                try:
                    # 대장주 시세 10일치 확인
                    df_l = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d'))
                    curr = df_l['Close'].iloc[-1]
                    ma5 = df_l['Close'].rolling(5).mean().iloc[-1]
                    leader_status[sect] = "🔥강세" if curr > ma5 else "❄️침체"
                except: leader_status[sect] = "Normal"
    except Exception as e:
        # 💡 여기가 핵심! KRX 서버가 죽어있으면 에러를 뱉지 않고 '빈 장부'를 넘겨줍니다.
        print(f"⚠️ [비상] KRX 서버 통신 실패(장애). 대장주 분석을 생략하고 진행합니다.")
        leader_status = {} # 빈 값으로 리턴하여 메인 루프를 살립니다.
        
    return global_status, leader_status

# =================================================
# 🧬 [2. 통합 서사 및 확신 점수 계산]
# =================================================
def analyze_all_narratives(df, ticker_name, sector_name, g_env, l_env):
    if len(df) < 120: return "🛡️일반", "데이터부족", 0, 0, 0
    
    last_idx = len(df) - 1
    row = df.iloc[-1]
    
    # [1] 역매공파 시퀀스 (바닥 돌파형)
    def get_days_ago(condition_series):
        idx = np.where(condition_series)[0]
        return (last_idx - idx[-1]) if len(idx) > 0 else None

    d_yeok = get_days_ago(df['MA5'] > df['MA20'])
    d_mae  = get_days_ago(df['MA_Convergence'] <= 3.0)
    d_gong = get_days_ago((df['Close'] > df['MA112']) & (df['Close'].shift(1) <= df['MA112']))
    d_pa   = get_days_ago((df['Close'] > df['BB40_Upper']) & (df['Close'].shift(1) <= df['BB40_Upper']))

    # [2] 강창권 종베 로직 (눌림목 타격형)
    df['Env_Upper'] = df['MA20'] * 1.20
    is_hot = (df['High'].iloc[-20:-5] > df['Env_Upper'].iloc[-20:-5]).any()
    is_on_20ma = df['MA20'].iloc[-1] * 0.98 <= row['Close'] <= df['MA20'].iloc[-1] * 1.05
    is_jongbe = is_hot and is_on_20ma and (row['Close'] > row['Open'])

    # [3] 확신 점수 공식 (Conviction Score)
    # n_score (기술적 서사: 60점 만점)
    n_score = (20 if d_yeok is not None else 0) + (20 if d_mae is not None else 0)
    if d_gong == 0: n_score += 30
    if d_pa == 0: n_score += 30
    if is_jongbe: n_score += 20
    
    # 외부 버프 (40점 만점)
    us_map = {'제약': '바이오', '반도체': '반도체', '전기제품': '2차전지'}
    g_score = 20 if g_env.get(us_map.get(sector_name, ""), 0) > 1.0 else 0
    l_score = 20 if l_env.get(sector_name) == "🔥강세" else 0
    
    total_conviction = min(100, n_score + g_score + l_score)

    # [4] 리포트 작성
    events = []
    if d_yeok is not None: events.append((d_yeok, "역"))
    if d_mae is not None:  events.append((d_mae, "매"))
    if d_gong is not None: events.append((d_gong, "공"))
    if d_pa is not None:   events.append((d_pa, "파"))
    events.sort(key=lambda x: x[0], reverse=True)
    
    narrative = " ➔ ".join([f"{'오늘' if d==0 else str(d)+'일전'}{n}" for d, n in events])
    if is_jongbe: narrative += " | 🎖️종베타점"

    grade = "👑LEGEND" if total_conviction >= 80 else "⚔️정예" if total_conviction >= 55 else "🛡️일반"
    target = round(row['Close'] * 1.1, 0) if is_jongbe else round(row['MA112'] * 1.005, 0)
    stop = round(df['MA20'].iloc[-1] * 0.97, 0) if is_jongbe else round(row['MA112'] * 0.98, 0)

    return grade, narrative, target, stop, total_conviction
    
def analyze_all_narratives_back(df, ticker_name, sector_name, g_status, l_sync):
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

    # --- [전술 2] 강창권 종베(눌림목) 로직 ---
    # 엔벨로프 상단(20, 20%) 터치 여부
    df['Env_Upper'] = df['MA20'] * 1.20
    is_hot_stock = (df['High'].iloc[-20:-5] > df['Env_Upper'].iloc[-20:-5]).any()
    # 20일선 지지 여부
    is_on_20ma = df['MA20'].iloc[-1] * 0.98 <= row['Close'] <= df['MA20'].iloc[-1] * 1.05
    is_jongbe = is_hot_stock and is_on_20ma and (row['Close'] > row['Open'])
    
    # 이벤트 리스트 구성 및 시간순 정렬
    if d_yeok is not None: events.append((d_yeok, "역(逆)"))
    if d_mae is not None:  events.append((d_mae, "매(埋)"))
    if d_gong is not None: events.append((d_gong, "공(空)"))
    if d_pa is not None:   events.append((d_pa, "파(破)"))

    # 며칠 전(숫자)이 큰 것부터(과거부터) 정렬
    events.sort(key=lambda x: x[0], reverse=True)
    report = " ➔ ".join([f"{'오늘' if d==0 else str(d)+'일전'} {name}" for d, name in events])
    if is_jongbe: history += " | 🎖️종베타점"
    if not report: report = "서사 관찰 중"

    # [3] 확신 지수(Conviction) 및 점수 산출
    # 기술적 서사 점수 (오늘 시점 기준 가중치)
    narrative_score = 0
    if d_yeok is not None: narrative_score += 20
    if d_mae is not None:  narrative_score += 20
    if d_gong == 0: narrative_score += 30  # 오늘 공구리 돌파 시 가점
    if d_pa == 0: narrative_score += 30    # 오늘 파동 시작 시 가점
    if is_jongbe: n_score += 20 # 종베 신호 시 가점
        
    # 글로벌 및 대장주 동기화 점수
    g_score = 25 if g_status.get(sector_name, 0) > 0 else 0
    l_score = 25 if l_sync.get(sector_name) == "🔥강세" else 0
    
    # $$Conviction = Narrative + Global + Leader$$
    total_conviction = narrative_score + g_score + l_score

    # [4] 정밀 타점 및 등급 부여
    row = df.iloc[-1]
    target = round(row['MA112'] * 1.005, 0)
    stop_loss = round(row['MA112'] * 0.98, 0)
    
    if total_conviction >= 90: grade += "👑LEGEND"
    elif total_conviction >= 70: grade += "⚔️정예"
    else: grade = "🛡️일반"

    return grade, report, target, stop_loss, total_conviction

def calculate_dante_symmetry(df):
    """
    단테의 밥그릇 기법: 시간 대칭 및 매집 밀도 분석
    """
    if len(df) < 250: return None
    
    # 1. 🔍 하락 구간(A) 찾기: 최근 1년 최고점에서 최저점까지
    peak_idx = df['High'].tail(250).idxmax()
    after_peak_df = df.loc[peak_idx:]
    trough_idx = after_peak_df['Low'].idxmin()
    
    # 2. ⏳ 기간 계산
    # decline_days(A): 고점~저점 / sideways_days(B): 저점~현재
    decline_days = (trough_idx - peak_idx).days
    sideways_days = (df.index[-1] - trough_idx).days
    
    # 3. ⚖️ 시간 대칭 비율 (B / A)
    # 1.0 이상이면 하락한 만큼 충분히 기었다는 뜻!
    symmetry_ratio = round(sideways_days / decline_days, 1) if decline_days > 0 else 0
    
    # 4. 🐋 매집 밀도 분석 (횡보 구간 내 매집봉 카운트)
    # 조건: 거래량이 20일 평균의 3배 이상 + 윗꼬리가 몸통보다 김
    sideways_df = df.loc[trough_idx:]
    mae_jip_candles = sideways_df[
        (sideways_df['Volume'] > sideways_df['Volume'].rolling(20).mean() * 3) & 
        ((sideways_df['High'] - sideways_df['Close']) > (sideways_df['Close'] - sideways_df['Open']))
    ]
    mae_jip_count = len(mae_jip_candles)
    
    return {
        'ratio': symmetry_ratio,
        'mae_jip': mae_jip_count,
        'decline_period': decline_days,
        'sideways_period': sideways_days
    }
