# ------------------------------------------------------------------
# 💎 [Ultimate Masterpiece] 전천후 AI 전략 사령부 (Ver 36.7 엑셀저장+추천시스템)
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
from DNA_Analyzer import analyze_dna_sequences, find_winning_pattern

# 👇 구글 시트 매니저 연결 (파일명 확인 필수)
try:
    from google_sheet_managerEx import update_commander_dashboard
except ImportError:
    def update_commander_dashboard(*args, **kwargs): print("⚠️ 구글 시트 모듈 연결 실패")

warnings.filterwarnings('ignore')

# =================================================
# ⚙️ [1. 설정 및 글로벌 변수]
# =================================================
SCAN_DAYS = 20     # 최근 30일 내 타점 전수 조사
TOP_N = 2500        # 거래대금 상위 종목 수 (필요시 2500으로 확장 가능)
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')
START_DATE = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
END_DATE_STR = datetime.now().strftime('%Y%m%d')

print(f"📡 [Ver 36.7 엑셀저장+추천] 사령부 무결성 통합 가동... 💎다이아몬드 & 📊복합통계 엔진 탑재")


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
# 📊 [전술 통계] 복합 전술 통계 엔진 (상위 5개 추천)
# ---------------------------------------------------------
def calculate_strategy_stats(all_hits):
    past_hits = [h for h in all_hits if h['보유일'] > 0]
    if not past_hits: return pd.DataFrame(), None
    
    stats = {}
    for h in past_hits:
        raw_tags = h['구분'].split()
        if not raw_tags: continue
        
        # 개별 태그 및 복합 태그 생성
        combos = []
        for tag in raw_tags:
            combos.append(tag)
        
        # 2개 조합
        if len(raw_tags) >= 2:
            sorted_tags = sorted(raw_tags)
            for i in range(len(sorted_tags)):
                for j in range(i+1, len(sorted_tags)):
                    combos.append(f"{sorted_tags[i]} + {sorted_tags[j]}")
        
        # 전체 조합
        if len(raw_tags) > 1:
            combos.append(" + ".join(sorted(raw_tags)))
        
        for strategy in set(combos):
            if strategy not in stats: 
                stats[strategy] = {'total': 0, 'hits': 0, 'yields': [], 'min_yields': []}
            stats[strategy]['total'] += 1
            if h['최고수익률_raw'] >= 3.5: stats[strategy]['hits'] += 1
            stats[strategy]['yields'].append(h['최고수익률_raw'])
            stats[strategy]['min_yields'].append(h['최저수익률_raw'])

    report_data = []
    for strategy, data in stats.items():
        avg_max_yield = sum(data['yields']) / data['total']
        avg_min_yield = sum(data['min_yields']) / data['total']
        hit_rate = (data['hits'] / data['total']) * 100
        
        # 기대값 계산 (확률 * 수익률)
        expected_value = (hit_rate / 100) * avg_max_yield
        
        report_data.append({
            '전략명': strategy, 
            '포착건수': data['total'], 
            '타율(승률)': round(hit_rate, 1), 
            '평균최고수익': round(avg_max_yield, 1),
            '평균최저수익': round(avg_min_yield, 1),
            '기대값': round(expected_value, 2)
        })
    
    df_stats = pd.DataFrame(report_data).sort_values(
        by=['기대값', '평균최고수익', '타율(승률)'], 
        ascending=False
    )
    
    # 💡 상위 3~5개 패턴 추천
    top_recommendations = []
    if len(df_stats) > 0:
        # 최소 5건 이상 데이터 있는 패턴 우선
        reliable_patterns = df_stats[df_stats['포착건수'] >= 5]
        
        if len(reliable_patterns) >= 3:
            # 신뢰도 높은 패턴 중 상위 5개
            top_5 = reliable_patterns.head(5)
            for idx, row in top_5.iterrows():
                top_recommendations.append({
                    '순위': len(top_recommendations) + 1,
                    '패턴': row['전략명'],
                    '타율': row['타율(승률)'],
                    '평균수익': row['평균최고수익'],
                    '기대값': row['기대값'],
                    '건수': row['포착건수'],
                    '신뢰도': '⭐⭐⭐ 높음'
                })
        else:
            # 데이터 부족시 전체에서 상위 5개
            top_5 = df_stats.head(5)
            for idx, row in top_5.iterrows():
                reliability = '⭐⭐⭐ 높음' if row['포착건수'] >= 5 else '⭐⭐ 보통' if row['포착건수'] >= 3 else '⭐ 주의'
                top_recommendations.append({
                    '순위': len(top_recommendations) + 1,
                    '패턴': row['전략명'],
                    '타율': row['타율(승률)'],
                    '평균수익': row['평균최고수익'],
                    '기대값': row['기대값'],
                    '건수': row['포착건수'],
                    '신뢰도': reliability
                })
    
    return df_stats, top_recommendations

# ---------------------------------------------------------
# 📈 [데이터] 마스터 지표 엔진 (Ver 36.7)
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
    df['BB40_Lower'] = df['MA40'] - (std40 * 2)
    df['BB40_Width'] = (std40 * 4) / df['MA40'] * 100
    
    # 이평선 수렴도 계산
    df['MA_Convergence'] = abs(df['MA20'] - df['MA60']) / df['MA60'] * 100
    
    # 일목균형표
    df['Tenkan_sen'] = (df['High'].rolling(9).max() + df['Low'].rolling(9).min()) / 2
    df['Kijun_sen'] = (df['High'].rolling(26).max() + df['Low'].rolling(26).min()) / 2
    df['Span_A'] = ((df['Tenkan_sen'] + df['Kijun_sen']) / 2).shift(26)
    df['Span_B'] = ((df['High'].rolling(52).max() + df['Low'].rolling(52).min()) / 2).shift(26)
    df['Cloud_Top'] = df[['Span_A', 'Span_B']].max(axis=1)

    # 스토캐스틱
    l_min, h_max = df['Low'].rolling(12).min(), df['High'].rolling(12).max()
    df['Sto_K'] = ((df['Close'] - l_min) / (h_max - l_min)) * 100
    df['Sto_D'] = df['Sto_K'].rolling(5).mean()
    df['Sto_SD'] = df['Sto_D'].rolling(5).mean()
    
    # ADX
    high, low, close = df['High'], df['Low'], df['Close']
    tr = pd.concat([high - low, abs(high - close.shift(1)), abs(low - close.shift(1))], axis=1).max(axis=1)
    df['ADX'] = ((abs((high-high.shift(1)).clip(lower=0).rolling(14).sum() - (low.shift(1)-low).clip(lower=0).rolling(14).sum()) / 
                ((high-high.shift(1)).clip(lower=0).rolling(14).sum() + (low.shift(1)-low).clip(lower=0).rolling(14).sum())) * 100).rolling(14).mean()
    
    # MACD
    ema12 = df['Close'].ewm(span=12).mean()
    ema26 = df['Close'].ewm(span=26).mean()
    df['MACD'] = ema12 - ema26
    df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
    df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']
    
    # OBV
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
    df['OBV_Slope'] = (df['OBV'] - df['OBV'].shift(5)) / df['OBV'].shift(5).abs() * 100
    
    # RSI
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    df['Disparity'] = (df['Close'] / df['MA20']) * 100
    df['Box_Range'] = df['High'].rolling(10).max() / df['Low'].rolling(10).min()
    
    return df

# ---------------------------------------------------------
# 🕵️‍♂️ [분석] 정밀 분석 엔진 (Ver 36.7 최저수익률 추가)
# ---------------------------------------------------------
def analyze_final(ticker, name, historical_indices):
    try:
        df = fdr.DataReader(ticker, start=START_DATE)
        if len(df) < 100: return []
        df = get_indicators(df)
        df = df.join(historical_indices, how='left').fillna(method='ffill')
        
        # 💡 오늘의 현재가 저장 (나중에 사용)
        today_price = df.iloc[-1]['Close']
        
        # 최신 수급 데이터 수집
        try:
            url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            res.encoding = 'euc-kr'
            supply_df = pd.read_html(res.text)[2].dropna()
            f_qty = int(str(supply_df.iloc[0]['외국인']).replace('.0','').replace(',',''))
            i_qty = int(str(supply_df.iloc[0]['기관']).replace('.0','').replace(',',''))
            twin_b = (f_qty > 0 and i_qty > 0)
            whale_score = int(((f_qty + i_qty) * df.iloc[-1]['Close']) / 100000000)
        except:
            f_qty, i_qty, twin_b, whale_score = 0, 0, False, 0

        recent_df = df.tail(SCAN_DAYS)
        hits = []

        for curr_idx, row in recent_df.iterrows():
            raw_idx = df.index.get_loc(curr_idx)
            if raw_idx < 100: continue
            prev = df.iloc[raw_idx-1]
            prev_5 = df.iloc[max(0, raw_idx-5)]
            prev_10 = df.iloc[max(0, raw_idx-10)]
            
            # 1. 꼬리% 정밀 계산
            high_p, low_p, close_p, open_p = row['High'], row['Low'], row['Close'], row['Open']
            body_max = max(open_p, close_p)
            t_pct = int((high_p - body_max) / (high_p - low_p) * 100) if high_p != low_p else 0

            # 2. 기존 핵심 전술 신호 판정
            is_cloud_brk = prev['Close'] <= prev['Cloud_Top'] and close_p > row['Cloud_Top']
            is_kijun_sup = close_p > row['Kijun_sen'] and prev['Close'] <= prev['Kijun_sen']
            is_diamond = is_cloud_brk and is_kijun_sup
            
            is_super_squeeze = row['BB20_Width'] < 10 and row['BB40_Width'] < 15
            is_yeok_mae_old = close_p > row['MA112'] and prev['Close'] <= row['MA112']
            is_vol_power = row['Volume'] > row['VMA20'] * 2.5

            # 💡 역매공파 7가지 조건 체크
            yeok_1_ma_aligned = (row['MA5'] > row['MA20']) and (row['MA20'] > row['MA60'])
            yeok_2_ma_converged = row['MA_Convergence'] <= 3.0
            yeok_3_bb40_squeeze = row['BB40_Width'] <= 10.0
            yeok_4_red_candle = close_p < open_p
            day_change = ((close_p - prev['Close']) / prev['Close']) * 100
            yeok_5_pullback = -5.0 <= day_change <= -1.0
            yeok_6_volume_surge = row['Volume'] >= row['VMA5'] * 1.5
            yeok_7_ma5_support = close_p >= row['MA5'] * 0.97
            
            # 💡 매집 5가지 조건 체크
            acc_1_obv_rising = (row['OBV'] > prev_5['OBV']) and (row['OBV'] > prev_10['OBV'])
            acc_2_box_range = row['Box_Range'] <= 1.15
            acc_3_macd_golden = row['MACD'] > row['MACD_Signal']
            acc_4_rsi_healthy = 40 <= row['RSI'] <= 70
            acc_5_sto_golden = row['Sto_K'] > row['Sto_D']

            # 3. 점수 산출 및 태그 부여
            s_score = 100
            tags = []
            
            # 기존 시그널들
            if is_diamond:
                s_score += 150
                tags.append("💎다이아몬드")
                if t_pct < 10:
                    s_score += 50
                    tags.append("🔥폭발직전")
            elif is_cloud_brk:
                s_score += 40
                tags.append("☁️구름돌파")

            if is_yeok_mae_old: 
                s_score += 40
                tags.append("🏆역매공파")
                
            if is_super_squeeze: 
                s_score += 40
                tags.append("🔋초강력응축")
                
            if is_vol_power: 
                s_score += 30
                tags.append("⚡거래폭발")
            
            # 💡 역매공파 완전체 체크
            yeok_mae_count = sum([yeok_1_ma_aligned, yeok_2_ma_converged, yeok_3_bb40_squeeze,
                                 yeok_4_red_candle, yeok_5_pullback, yeok_6_volume_surge, yeok_7_ma5_support])
            
            if yeok_mae_count == 7:
                s_score += 100
                tags.append("🎯역매공파완전체")
            elif yeok_mae_count >= 5:
                s_score += 50
                tags.append("🎯역매공파강")
            elif yeok_mae_count >= 3:
                s_score += 20
                tags.append("🎯역매공파약")
            
            # 세부 태그
            if yeok_1_ma_aligned and yeok_2_ma_converged:
                tags.append("📐이평수렴")
            if yeok_3_bb40_squeeze:
                tags.append("🔋밴드(40)")
            
            # 💡 매집 시그널 체크
            acc_count = sum([acc_1_obv_rising, acc_2_box_range, acc_3_macd_golden,
                           acc_4_rsi_healthy, acc_5_sto_golden])
            
            if acc_count >= 4:
                s_score += 60
                tags.append("🐋세력매집")
            elif acc_count >= 3:
                s_score += 30
                tags.append("🐋매집징후")
                
            if acc_1_obv_rising:
                tags.append("📊OBV상승")

            # 기존 감점 로직
            if t_pct > 40:
                s_score -= 25
                tags.append("⚠️윗꼬리")

            # 기상도 감점
            storm_count = sum([1 for m in ['ixic', 'sp500'] if row[f'{m}_close'] <= row[f'{m}_ma5']])
            s_score -= (storm_count * 20)
            s_score -= max(0, int((row['Disparity']-108)*5)) 
            
            if not tags: continue

            # 4. 💡 수익률 검증 데이터 생성 (최고/최저 추가)
            h_df = df.iloc[raw_idx+1:]
            
            if not h_df.empty:
                max_r = ((h_df['High'].max() - close_p) / close_p) * 100
                min_r = ((h_df['Low'].min() - close_p) / close_p) * 100
                
                # 💡 오늘이면 현재가 = 오늘 종가, 아니면 해당 시점의 마지막 종가
                is_today = (len(h_df) == 0)  # 보유일 0이면 오늘
                current_price = today_price if not is_today else close_p
            else:
                max_r = 0
                min_r = 0
                current_price = close_p

            hits.append({
                '날짜': curr_idx.strftime('%Y-%m-%d'),
                '기상': "☀️" * (2-storm_count) + "🌪️" * storm_count,
                '안전점수': int(max(0, s_score + whale_score)),
                '종목': name,
                '매입가': int(close_p),
                '현재가': int(current_price),
                '꼬리%': t_pct,
                '이격': int(row['Disparity']),
                'BB40': f"{row['BB40_Width']:.1f}",
                'MA수렴': f"{row['MA_Convergence']:.1f}",
                '역매': f"{yeok_mae_count}/7",
                '매집': f"{acc_count}/5",
                '최고수익률%': f"{max_r:+.1f}%",
                '최저수익률%': f"{min_r:+.1f}%",
                '최고수익률_raw': max_r,
                '최저수익률_raw': min_r,
                '구분': " ".join(tags),
                '보유일': len(h_df)
            })
        return hits
    except: 
        return []

# ---------------------------------------------------------
# 💾 [엑셀 저장] 오늘의 추천종목 저장
# ---------------------------------------------------------
def save_today_recommendations(df_today, recommendation_info):
    """오늘의 추천종목을 엑셀로 저장"""
    try:
        filename = f"추천종목_{TODAY_STR}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # 시트1: 오늘의 추천 종목
            df_today.to_excel(writer, sheet_name='오늘의_추천', index=False)
            
            # 시트2: 추천 정보
            if recommendation_info:
                rec_df = pd.DataFrame([recommendation_info])
                rec_df.to_excel(writer, sheet_name='추천_패턴_정보', index=False)
        
        print(f"\n💾 엑셀 저장 완료: {filename}")
        return filename
    except Exception as e:
        print(f"\n❌ 엑셀 저장 실패: {e}")
        return None

# =================================================
# 🚀 [실행] 메인 컨트롤러 (수정 버전)
# =================================================
if __name__ == "__main__":
    print(f"📡 [Ver 36.7 구글시트 강화] {TODAY_STR} 전술 사령부 통합 가동...")

    # 1. 매크로 데이터 수집
    m_ndx = get_safe_macro('^IXIC', '나스닥')
    m_sp5 = get_safe_macro('^GSPC', 'S&P500')
    m_vix = get_safe_macro('^VIX', 'VIX공포')
    m_fx  = get_safe_macro('USD/KRW', '달러환율')
    
    kospi_supply = get_index_investor_data('KOSPI')
    macro_status = {'nasdaq': m_ndx, 'sp500': m_sp5, 'vix': m_vix, 'fx': m_fx, 'kospi': kospi_supply}

    print("\n" + "🌍 " * 5 + "[ 글로벌 사령부 통합 관제 센터 ]" + " 🌍" * 5)
    print(f"🇺🇸 {m_ndx['text']} | {m_sp5['text']} | ⚠️ {m_vix['text']}")
    print(f"💵 {m_fx['text']} | 🇰🇷 KOSPI 수급: {kospi_supply}")
    print("=" * 115)

    # 2. 전 종목 리스팅 및 기상도 준비
    df_krx = fdr.StockListing('KRX')
    target_stocks = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    weather_data = prepare_historical_weather()
    
    # 3. 전술 스캔 (멀티스레딩)
    all_hits = []
    print(f"🔍 총 {len(target_stocks)}개 종목 💎다이아몬드 & 🎯역매공파 레이더 가동...")
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = list(executor.map(
            lambda p: analyze_final(p[0], p[1], weather_data), 
            zip(target_stocks['Code'], target_stocks['Name'])
        ))
        for r in results:
            if r: all_hits.extend(r)

    if all_hits:
         # 1. 원재료(all_hits)를 연구소(DNA_Analyzer)로 송부
        print("🧬 [DNA Trace-Back] 성공 유전자 역추적 가동...")
        dna_results = analyze_dna_sequences(all_hits)
    
        # 2. 가장 승률 높은 패턴 랭킹 추출
        top_patterns = find_winning_pattern(dna_results)

        df_total = pd.DataFrame(all_hits)
        
        # 통계 계산 (상위 5개 추천 정보 포함)
        stats_df, top_recommendations = calculate_strategy_stats(all_hits)
        
        # 4. 결과 분류
        today = df_total[df_total['보유일'] == 0].sort_values(by='안전점수', ascending=False)
        
        # 추천 패턴 DataFrame 생성
        if top_recommendations:
            recommendation_df = pd.DataFrame(top_recommendations)
            recommendation_df['날짜'] = TODAY_STR
            recommendation_df = recommendation_df[['날짜', '순위', '패턴', '타율', '평균수익', '기대값', '건수', '신뢰도']]
        else:
            recommendation_df = pd.DataFrame()
        
        # 💡 추천 패턴 출력 (여러 개)
        if top_recommendations:
            print("\n" + "🏆 " * 10 + "[ AI 추천 TOP 5 패턴 ]" + " 🏆" * 10)
            for i, rec in enumerate(top_recommendations, 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}위"
                print(f"\n{medal} [{rec['패턴']}]")
                print(f"   📊 타율 {rec['타율']}% | 평균수익 {rec['평균수익']}% | 기대값 {rec['기대값']} | 건수 {rec['건수']}건")
                print(f"   {rec['신뢰도']}")
            print("=" * 100)
            
        if not top_patterns.empty:
    # 💡 1. 'top_patterns' 데이터프레임에서 1순위 패턴 문자열을 추출합니다.
    # DNA_시퀀스 컬럼의 첫 번째 행(iloc[0])을 가져옵니다.
            best_pattern_str = top_patterns.iloc[0]['DNA_시퀀스']
    
    # 💡 2. 패턴의 첫 번째 요소(예: '매집봉')만 떼어내어 오늘 종목을 필터링합니다.
    # 사령관님이 작성하신 split logic을 안전하게 처리합니다.
            target_tag = best_pattern_str.split(' ➔ ')[0] # '➔' 기호 기준 첫 태그 추출
    
            print(f"🎯 [DNA 필터] 오늘의 1순위 타겟 패턴: {target_tag}")
    
    # 💡 3. 오늘 데이터(today)에서 해당 태그가 포함된 종목만 추출
            recommended_today = today[today['구분'].str.contains(target_tag, na=False)]
        else:
            print("⚠️ [DNA 필터] 유효한 성공 패턴이 없어 전체 종목을 유지합니다.")
            recommended_today = today.copy()

            # 1위 패턴이 포함된 오늘의 종목 필터링
            top_pattern = top_recommendations[0]['패턴']
            recommended_today = today[today['구분'].str.contains(top_pattern.split(' + ')[0], na=False)]
            if not recommended_today.empty:
                print(f"\n✨ 오늘의 '{top_pattern}' 패턴 종목")
                print(recommended_today[['종목', '안전점수', '매입가', '역매', '매집', '구분']].head(10))
        
        # 💡 통합: 오늘의 추천종목 (역매공파 포함, 안전점수 순)
        print("\n" + "🎯 " * 10 + "[ 오늘의 추천종목 TOP 50 ]" + " 🎯" * 10)
        print("(역매공파, 다이아몬드, 세력매집 등 모든 패턴 포함 / 안전점수 순)")
        print("=" * 120)
        
        if not today.empty:
            display_cols = ['종목', '안전점수', '매입가', '현재가', '꼬리%', '역매', '매집', 'BB40', 'MA수렴', '구분']
            print(today[display_cols].head(50))
            
            # 💡 패턴별 집계 (참고용)
            diamond_count = len(today[today['구분'].str.contains('다이아몬드', na=False)])
            yeok_complete = len(today[today['구분'].str.contains('역매공파완전체', na=False)])
            yeok_strong = len(today[today['구분'].str.contains('역매공파강', na=False)])
            accumulation = len(today[today['구분'].str.contains('세력매집', na=False)])
            
            print("\n📊 [ 오늘의 패턴 분포 ]")
            print(f"   💎 다이아몬드: {diamond_count}개")
            print(f"   🎯 역매공파 완전체: {yeok_complete}개")
            print(f"   🎯 역매공파 강: {yeok_strong}개")
            print(f"   🐋 세력매집: {accumulation}개")
            print(f"   📈 전체 추천종목: {len(today)}개")
        else:
            print("오늘은 추천할 만한 종목이 없습니다.")

        print("\n" + "📊 [전략별 통계 (과거 30일)] " + "="*70)
        if not stats_df.empty:
            print(stats_df.head(20))

        # 5. 구글 시트 전송
        try:
            update_commander_dashboard(
                df_total,  # 메인 시트: 전체 30일 데이터
                macro_status, 
                "사령부_통합_상황판", 
                stats_df,
                today,  # 오늘의_추천종목 탭: 오늘만 (모든 패턴 통합)
                ai_recommendation=dna_results  # AI_추천패턴 탭: TOP 5
            )
            print("\n✅ 구글 시트 업데이트 성공!")
            print("   📋 메인 시트: 전체 30일 검증 데이터")
            print("   🎯 오늘의_추천종목 탭: 오늘 신호만 (TOP 50, 모든 패턴 통합)")
            print("   🏆 AI_추천패턴 탭: TOP 5 패턴 분석")
        except Exception as e:
            print(f"\n❌ 시트 업데이트 실패: {e}")
    else:
        print("\n⚠️ 검색 결과가 없습니다.")