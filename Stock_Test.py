# ------------------------------------------------------------------
# 💎 [Ultimate Masterpiece] 전천후 AI 전략 사령부 (Ver 36.0 통합판)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import os, re, time, pytz
from pykrx import stock
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import warnings
import requests
from bs4 import BeautifulSoup

# 👇 구글 시트 매니저 연결 (파일명 확인 필수)
try:
    from google_sheet_managerEx import update_commander_dashboard
except ImportError:
    def update_commander_dashboard(*args, **kwargs): print("⚠️ 구글 시트 모듈 연결 실패")

warnings.filterwarnings('ignore')

# =================================================
# ⚙️ [1. 설정 및 글로벌 변수]
# =================================================
SCAN_DAYS = 30     # 최근 30일 내 타점 전수 조사
TOP_N = 250        # 거래대금 상위 종목 수 (필요시 2500으로 확장 가능)
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')
START_DATE = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
END_DATE_STR = datetime.now().strftime('%Y%m%d')

print(f"📡 [Ver 36.0] 사령부 무결성 통합 가동... 💎다이아몬드 & 📊복합통계 엔진 탑재")


# ---------------------------------------------------------
# 🌍 [매크로 엔진] 글로벌 지수 및 수급 데이터 수집
# ---------------------------------------------------------
def get_safe_macro(symbol, name):
    try:
        df = fdr.DataReader(symbol, start=(datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d'))
        curr, prev = df.iloc[-1]['Close'], df.iloc[-2]['Close']
        ma5 = df['Close'].tail(5).mean()
        chg = ((curr - prev) / prev) * 100
        status = "☀️맑음" if curr > ma5 else "🌪️폭풍우"
        if "VIX" in name: status = "☀️안정" if curr < ma5 else "🌪️위험"
        return {"val": curr, "chg": chg, "status": status, "text": f"{name}: {curr:,.2f}({chg:+.2f}%) {status}"}
    except: return {"status": "☁️불명", "text": f"{name}: 연결실패"}

def get_index_investor_data(market_name):
    try:
        df = stock.get_market_net_purchases_of_equities(END_DATE_STR, END_DATE_STR, market_name)
        if df.empty:
            prev_day = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            df = stock.get_market_net_purchases_of_equities(prev_day, prev_day, market_name)
        total = df.sum()
        return f"개인 {total['개인']:+,.0f} | 외인 {total['외국인']:+,.0f} | 기관 {total['기관합계']:+,.0f}"
    except: return "데이터 수신 중..."

def prepare_historical_weather():
    """역사적 기상도를 작성하여 analyze_final에 보급합니다."""
    start_point = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
    ndx = fdr.DataReader('^IXIC', start=start_point)[['Close']]
    sp5 = fdr.DataReader('^GSPC', start=start_point)[['Close']]
    ndx['ixic_ma5'] = ndx['Close'].rolling(5).mean()
    sp5['sp500_ma5'] = sp5['Close'].rolling(5).mean()
    weather_df = pd.concat([
        ndx.rename(columns={'Close': 'ixic_close'}),
        sp5.rename(columns={'Close': 'sp500_close'})
    ], axis=1).fillna(method='ffill')
    return weather_df
# ---------------------------------------------------------
# 📊 [전술 통계] 복합 전술 통계 엔진
# ---------------------------------------------------------
def calculate_strategy_stats(all_hits):
    past_hits = [h for h in all_hits if h['보유일'] > 0]
    if not past_hits: return pd.DataFrame()
    stats = {}
    for h in past_hits:
        raw_tags = h['구분'].split()
        if not raw_tags: continue
        combos = [h['구분']]
        if len(raw_tags) > 1:
            raw_tags.sort()
            combos.append(" + ".join(raw_tags)) 
        for strategy in set(combos):
            if strategy not in stats: 
                stats[strategy] = {'total': 0, 'hits': 0, 'yields': []}
            stats[strategy]['total'] += 1
            if h['최고_raw'] >= 3.5: stats[strategy]['hits'] += 1
            stats[strategy]['yields'].append(h['최고_raw'])

    report_data = []
    for strategy, data in stats.items():
        avg_yield = sum(data['yields']) / data['total']
        hit_rate = (data['hits'] / data['total']) * 100
        report_data.append({'전략명': strategy, '포착건수': data['total'], '타율(승률)': round(hit_rate, 1), '평균최고수익': round(avg_yield, 1)})
    return pd.DataFrame(report_data).sort_values(by=['평균최고수익', '타율(승률)'], ascending=False)

# ---------------------------------------------------------
# 📈 [데이터] 마스터 지표 엔진 (Ver 36.0 일목균형표 포함)
# ---------------------------------------------------------
def get_indicators(df):
    df = df.copy()
    count = len(df)
    # 단테 장기선 포함 이평선
    for n in [5, 20, 40, 60, 112, 224]:
        df[f'MA{n}'] = df['Close'].rolling(window=min(count, n)).mean()
        df[f'VMA{n}'] = df['Volume'].rolling(window=min(count, n)).mean()
    
    # 20/40일 BB Width (이중 응축)
    std20 = df['Close'].rolling(20).std()
    df['BB_Upper'] = df['MA20'] + (std20 * 2)
    df['BB20_Width'] = (std20 * 4) / df['MA20'] * 100
    std40 = df['Close'].rolling(40).std()
    df['BB40_Upper'] = df['MA40'] + (std40 * 2)
    df['BB40_Width'] = (std40 * 4) / df['MA40'] * 100
    
    # 일목균형표 (의성 탐지)
    df['Tenkan_sen'] = (df['High'].rolling(9).max() + df['Low'].rolling(9).min()) / 2
    df['Kijun_sen'] = (df['High'].rolling(26).max() + df['Low'].rolling(26).min()) / 2
    df['Span_A'] = ((df['Tenkan_sen'] + df['Kijun_sen']) / 2).shift(26)
    df['Span_B'] = ((df['High'].rolling(52).max() + df['Low'].rolling(52).min()) / 2).shift(26)
    df['Cloud_Top'] = df[['Span_A', 'Span_B']].max(axis=1)

    # 스토캐스틱 / ADX / MACD / OBV
    l_min, h_max = df['Low'].rolling(12).min(), df['High'].rolling(12).max()
    df['Sto_K'] = ((df['Close'] - l_min) / (h_max - l_min)) * 100
    df['Sto_D'] = df['Sto_K'].rolling(5).mean()
    df['Sto_SD'] = df['Sto_D'].rolling(5).mean()
    
    high, low, close = df['High'], df['Low'], df['Close']
    tr = pd.concat([high - low, abs(high - close.shift(1)), abs(low - close.shift(1))], axis=1).max(axis=1)
    df['ADX'] = ((abs((high-high.shift(1)).clip(lower=0).rolling(14).sum() - (low.shift(1)-low).clip(lower=0).rolling(14).sum()) / 
                ((high-high.shift(1)).clip(lower=0).rolling(14).sum() + (low.shift(1)-low).clip(lower=0).rolling(14).sum())) * 100).rolling(14).mean()
    df['MACD_Hist'] = (df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()) - (df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()).ewm(span=9).mean()
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
    df['OBV_Slope'] = (df['OBV'] - df['OBV'].shift(5)) / df['OBV'].shift(5).abs() * 100
    df['Disparity'] = (df['Close'] / df['MA20']) * 100
    return df

# ---------------------------------------------------------
# 🕵️‍♂️ [분석] 정밀 분석 엔진 (Ver 36.0 다이아몬드 통합)
# ---------------------------------------------------------
# ---------------------------------------------------------
# 🕵️‍♂️ [분석] 정밀 분석 엔진 (Ver 36.5: 폭발직전 필터 통합)
# ---------------------------------------------------------
def analyze_final(ticker, name, historical_indices):
    try:
        df = fdr.DataReader(ticker, start=START_DATE)
        if len(df) < 100: return []
        df = get_indicators(df)
        df = df.join(historical_indices, how='left').fillna(method='ffill')
        
        # 최신 수급 데이터 수집 (생략 방지)
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        res.encoding = 'euc-kr'
        supply_df = pd.read_html(res.text)[2].dropna()
        f_qty = int(str(supply_df.iloc[0]['외국인']).replace('.0','').replace(',',''))
        i_qty = int(str(supply_df.iloc[0]['기관']).replace('.0','').replace(',',''))
        twin_b = (f_qty > 0 and i_qty > 0)
        whale_score = int(((f_qty + i_qty) * df.iloc[-1]['Close']) / 100000000)

        recent_df = df.tail(SCAN_DAYS)
        hits = []

        for curr_idx, row in recent_df.iterrows():
            raw_idx = df.index.get_loc(curr_idx)
            if raw_idx < 100: continue
            prev = df.iloc[raw_idx-1]
            
            # 1. 꼬리% 정밀 계산
            high_p, low_p, close_p, open_p = row['High'], row['Low'], row['Close'], row['Open']
            body_max = max(open_p, close_p)
            t_pct = int((high_p - body_max) / (high_p - low_p) * 100) if high_p != low_p else 0

            # 2. 핵심 전술 신호 판정
            is_cloud_brk = prev['Close'] <= prev['Cloud_Top'] and close_p > row['Cloud_Top']
            is_kijun_sup = close_p > row['Kijun_sen'] and prev['Close'] <= prev['Kijun_sen']
            is_diamond = is_cloud_brk and is_kijun_sup
            
            is_super_squeeze = row['BB20_Width'] < 10 and row['BB40_Width'] < 15
            is_yeok_mae = close_p > row['MA112'] and prev['Close'] <= row['MA112']
            is_vol_power = row['Volume'] > row['VMA20'] * 2.5 # 거래량 250% 폭발

            # 3. 점수 산출 및 태그 부여
            s_score = 100
            tags = []
            
            if is_diamond:
                s_score += 150
                tags.append("💎다이아몬드")
                # 💡 [신규] 폭발직전 필터: 다이아몬드인데 꼬리가 10% 미만일 때
                if t_pct < 10:
                    s_score += 50
                    tags.append("🔥폭발직전")
            
            elif is_cloud_brk:
                s_score += 40; tags.append("☁️구름돌파")

            if is_yeok_mae: s_score += 40; tags.append("🏆역매공파")
            if is_super_squeeze: s_score += 40; tags.append("🔋초강력응축")
            if is_vol_power: s_score += 30; tags.append("⚡거래폭발")
            
            # 꼬리 감점 로직 (다이아몬드가 아닐 때 더 엄격하게 적용)
            if t_pct > 40:
                s_score -= 25
                tags.append("⚠️윗꼬리")
            if row['BB40_Width'] < 15: tags.append("밴드(40)")

            # 기상도 및 과열(이격도) 감점
            storm_count = sum([1 for m in ['ixic', 'sp500'] if row[f'{m}_close'] <= row[f'{m}_ma5']])
            s_score -= (storm_count * 20)
            s_score -= max(0, int((row['Disparity']-108)*5)) 
            
            if not tags: continue

            # 4. 수익률 검증 데이터 생성
            h_df = df.iloc[raw_idx+1:]
            max_r = ((h_df['High'].max()-close_p)/close_p)*100 if not h_df.empty else 0
            curr_r = ((h_df['Close'].iloc[-1]-close_p)/close_p)*100 if not h_df.empty else 0

            hits.append({
                '날짜': curr_idx.strftime('%Y-%m-%d'),
                '기상': "☀️" * (2-storm_count) + "🌪️" * storm_count,
                '안전': int(max(0, s_score + whale_score)),
                '종목': name,
                '현재가': int(close_p),
                '꼬리%': t_pct,
                '이격': int(row['Disparity']),
                '🔺최고': f"{max_r:+.1f}%",
                '현재': f"{curr_r:+.1f}%",
                '현재_raw': curr_r, '최고_raw': max_r,
                '구분': " ".join(tags),
                '보유일': len(h_df)
            })
        return hits
    except: return [] #=================================================
# 🚀 [실행] 메인 컨트롤러
# #=================================================
if __name__ == "__main__":
    print(f"📡 [Ver 36.5] {TODAY_STR} 전술 사령부 통합 가동...")

    # 1. 매크로 데이터 수집 (get_safe_macro가 정의되어 있어야 함)
    m_ndx = get_safe_macro('^IXIC', '나스닥')
    m_sp5 = get_safe_macro('^GSPC', 'S&P500')
    m_vix = get_safe_macro('^VIX', 'VIX공포')
    m_fx  = get_safe_macro('USD/KRW', '달러환율')
    
    # KOSPI 수급 데이터
    kospi_supply = get_index_investor_data('KOSPI')
    macro_status = {'nasdaq': m_ndx, 'sp500': m_sp5, 'vix': m_vix, 'fx': m_fx, 'kospi': kospi_supply}

    print("\n" + "🌍 " * 5 + "[ 글로벌 사령부 통합 관제 센터 ]" + " 🌍" * 5)
    print(f"🇺🇸 {m_ndx['text']} | {m_sp5['text']} | ⚠️ {m_vix['text']}")
    print(f"💵 {m_fx['text']} | 🇰🇷 KOSPI 수급: {kospi_supply}")
    print("=" * 115)

    # 2. 전 종목 리스팅 및 기상도 준비
    df_krx = fdr.StockListing('KRX')
    # 💡 target_stocks 정의 (NameError 방지)
    target_stocks = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    # 💡 weather_data 준비 (analyze_final에 전달용)
    weather_data = prepare_historical_weather()
    
    # 3. 전술 스캔 (멀티스레딩)
    all_hits = []
    print(f"🔍 총 {len(target_stocks)}개 종목 💎다이아몬드 레이더 가동...")
    with ThreadPoolExecutor(max_workers=15) as executor:
        # lambda p에서 p[0]: Code, p[1]: Name, weather_data: 기상도 전달
        results = list(executor.map(
            lambda p: analyze_final(p[0], p[1], weather_data), 
            zip(target_stocks['Code'], target_stocks['Name'])
        ))
        for r in results:
            if r: all_hits.extend(r)

    if all_hits:
        df_total = pd.DataFrame(all_hits)
        # 💡 복합 전술 통계 산출
        stats_df = calculate_strategy_stats(all_hits)
        
        # 4. 결과 분류 및 리포트
        today = df_total[df_total['보유일'] == 0].sort_values(by='안전', ascending=False)
        print("\n" + "🔥 [오늘의 초정예 다이아몬드 타점] " + "="*50)
        print(today[['날짜', '안전', '종목', '꼬리%', '구분']].head(20))

        # 5. 구글 시트 전송
        try:
            update_commander_dashboard(df_total, macro_status, "사령부_통합_상황판", stats_df)
            print("\n✅ 구글 시트 및 전술 통계 업데이트 성공!")
        except Exception as e:
            print(f"\n❌ 시트 업데이트 실패: {e}")