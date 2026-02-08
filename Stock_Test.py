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
import pandas as pd
import json

# 👇 구글 시트
from google_sheet_managerEx import update_commander_dashboard
import io # 상단에 추가

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.width', 1000)

# =================================================
# ⚙️ [1. 글로벌 관제 및 수급 설정]
# =================================================
SCAN_DAYS = 30
TOP_N = 200 
START_DATE = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
END_DATE_STR = datetime.now().strftime('%Y%m%d')
START_DATE_STR = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

print(f"📡 [Ver 27.0] 사령부 퍼펙트 오버홀 가동... 스토캐스틱 레이더 및 전 지표 동기화")

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

# ---------------------------------------------------------
# 📈 [2] 마스터 지표 엔진 (스토캐스틱 포함)
# ---------------------------------------------------------
def get_indicators(df):
    df = df.copy()
    for n in [5, 20, 60]:
        df[f'MA{n}'] = df['Close'].rolling(n).mean()
        df[f'VMA{n}'] = df['Volume'].rolling(n).mean()
        df[f'Slope{n}'] = (df[f'MA{n}'] - df[f'MA{n}'].shift(3)) / df[f'MA{n}'].shift(3) * 100
    
    df['Disparity'] = (df['Close'] / df['MA20']) * 100
    std = df['Close'].rolling(20).std()
    df['BB_Upper'] = df['MA20'] + (std * 2)
    df['BB_Width'] = (df['BB_Upper'] - (df['MA20'] - (std * 2))) / df['MA20'] * 100
    df['BB40_Upper'] = df['Close'].rolling(window=40).mean() + (df['Close'].rolling(window=40).std() * 2)
    
    # 💡 [스토캐스틱 슬로우 12-5-5]
    l_min, h_max = df['Low'].rolling(12).min(), df['High'].rolling(12).max()
    df['Sto_K'] = ((df['Close'] - l_min) / (h_max - l_min)) * 100
    df['Sto_D'] = df['Sto_K'].rolling(5).mean()
    df['Sto_SD'] = df['Sto_D'].rolling(5).mean()
    
    # DMI/ADX
    high, low, close = df['High'], df['Low'], df['Close']
    tr = pd.concat([high - low, abs(high - close.shift(1)), abs(low - close.shift(1))], axis=1).max(axis=1)
    df['pDI'] = (pd.Series(np.where((high-high.shift(1) > low.shift(1)-low), (high-high.shift(1)).clip(lower=0), 0)).rolling(14).sum().values / tr.rolling(14).sum().values) * 100
    df['mDI'] = (pd.Series(np.where((low.shift(1)-low > high-high.shift(1)), (low.shift(1)-low).clip(lower=0), 0)).rolling(14).sum().values / tr.rolling(14).sum().values) * 100
    df['ADX'] = ((abs(df['pDI'] - df['mDI']) / (df['pDI'] + df['mDI'])) * 100).rolling(14).mean()
    
    df['MACD_Hist'] = (df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()) - (df['Close'].ewm(span=12).mean() - df['Close'].ewm(span=26).mean()).ewm(span=9).mean()
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
    df['OBV_Slope'] = (df['OBV'] - df['OBV'].shift(5)) / df['OBV'].shift(5).abs() * 100
    df['Base_Line'] = df['Close'].rolling(20).min().shift(5)
    return df

# ---------------------------------------------------------
# 🐳 [통합] 수급 & 고래 베팅액 분석 엔진
# ---------------------------------------------------------
def get_investor_data_stable(ticker, price):
    ticker = str(ticker).zfill(6)
    
    # 💡 [작전 1] 네이버 모바일 통합 API (가장 가벼운 루트)
    try:
        url = f"https://m.stock.naver.com/api/stock/{ticker}/integration/investor"
        headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1'
        }
        res = requests.get(url, headers=headers, timeout=5)
        
        if res.status_code == 200:
            data = res.json().get('data', {}).get('investor', [])
            if data:
                # 데이터가 있으면 즉시 가공 (오늘 데이터가 0이면 어제 데이터인 1번 인덱스 사용)
                idx = 0
                f_qty = int(data[idx]['foreignNetPurchaseVolume'].replace(',', ''))
                i_qty = int(data[idx]['institutionNetPurchaseVolume'].replace(',', ''))
                
                # 만약 오늘 장중이라 0이라면 어제(1) 데이터를 확인
                if f_qty == 0 and i_qty == 0 and len(data) > 1:
                    idx = 1
                    f_qty = int(data[idx]['foreignNetPurchaseVolume'].replace(',', ''))
                    i_qty = int(data[idx]['institutionNetPurchaseVolume'].replace(',', ''))

                return process_supply_data(data, idx, price)

    except Exception as e:
        print(f"📡 루트1(모바일) 차단됨: {e}")

    # 💡 [작전 2] 네이버 PC 금융 웹 스크래핑 (BeautifulSoup)
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        # 더 정교한 브라우저 위장
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.text, 'lxml')
        table = soup.find('table', {'class': 'type2'})
        
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) == 9 and cols[5].get_text(strip=True):
                    i_qty = int(cols[5].get_text(strip=True).replace(',', ''))
                    f_qty = int(cols[6].get_text(strip=True).replace(',', ''))
                    if i_qty == 0 and f_qty == 0: continue # 0인 날은 건너뜀 (유효 데이터 찾기)
                    
                    return finalize_supply_output(f_qty, i_qty, price)
    except Exception as e:
        print(f"📡 루트2(PC웹) 차단됨: {e}")

    return "외(0/0억)", "기(0/0억)", "❌", 0, False

def finalize_supply_output(f_qty, i_qty, price):
    # 공통 가공 로직
    f_money = (f_qty * price) / 100000000
    i_money = (i_qty * price) / 100000000
    total_m = f_money + i_money
    w_score = int(total_m * 2)
    return f"외({f_money:.1f}억)", f"기({i_money:.1f}억)", "✅", max(0, w_score), (f_qty > 0 and i_qty > 0)
        
# 🏛️ [역사적 지수 데이터 통합 로직]
def prepare_historical_weather():
    start_point = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
    
    # 3대 지수 호출
    ndx = fdr.DataReader('^IXIC', start=start_point)[['Close']]
    sp5 = fdr.DataReader('^GSPC', start=start_point)[['Close']]
    vix = fdr.DataReader('^VIX', start=start_point)[['Close']]
    
    # 각 지수별 MA5 계산
    ndx['ixic_ma5'] = ndx['Close'].rolling(5).mean()
    sp5['sp500_ma5'] = sp5['Close'].rolling(5).mean()
    vix['vix_ma5'] = vix['Close'].rolling(5).mean()
    
    # 컬럼명 변경 후 결합
    weather_df = pd.concat([
        ndx.rename(columns={'Close': 'ixic_close'}),
        sp5.rename(columns={'Close': 'sp500_close'}),
        vix.rename(columns={'Close': 'vix_close'})
    ], axis=1).fillna(method='ffill')
    
    return weather_df
    
# ---------------------------------------------------------
# 🕵️‍♂️ [3] 정밀 분석 엔진
# ---------------------------------------------------------
def analyze_final(ticker, name, historical_indices):
    """
    사령관님, 이 함수는 각 날짜별 '나스닥, S&P500, VIX' 상태를 실시간으로 대조하여 
    진정한 과거 수익률 검증 점수를 산출합니다.
    """
    try:
        # 1. 데이터 로드 및 지표 계산
        df = fdr.DataReader(ticker, start=START_DATE)
        if len(df) < 100: return []
        df = get_indicators(df)
        
        # 2. 💡 [핵심] 종목 데이터와 역사적 지수 데이터 동기화 (Join)
        # historical_indices에는 ixic_close, ixic_ma5, sp500_close, sp500_ma5, vix_close, vix_ma5가 들어있어야 함
        df = df.join(historical_indices, how='left').fillna(method='ffill')
        
        # 3. 수급 데이터 확보
        curr_price = df.iloc[-1]['Close']
        f_s, i_s, s_s, max_c, twin_b = get_investor_data_stable(ticker, curr_price)
        
        recent_df = df.tail(SCAN_DAYS)
        hits = []

        for curr_idx, row in recent_df.iterrows():
            raw_idx = df.index.get_loc(curr_idx)
            if raw_idx < 15: continue
            prev = df.iloc[raw_idx-1]
            
            # --- [A] 기술적 신호 판정 ---
            is_sto_gc = prev['Sto_D'] <= prev['Sto_SD'] and row['Sto_D'] > row['Sto_SD']
            is_vma_gc = prev['VMA5'] <= prev['VMA20'] and row['VMA5'] > row['VMA20']
            is_bb_brk = prev['Close'] <= prev['BB_Upper'] and row['Close'] > row['BB_Upper']
            is_melon = twin_b and row['OBV_Slope'] > 0 and row['ADX'] > 20 and row['MACD_Hist'] > 0
            is_nova = is_sto_gc and is_vma_gc and is_bb_brk and is_melon
            is_bb40_brk = prev['Close'] <= prev['BB40_Upper'] and row['Close'] > row['BB40_Upper']
            
            # --- [B] 💡 역사적 기상도 분석 (3대 지수) ---
            storm_count = 0
            weather_icons = []

            # --- [B-1] 🎯 재영솔루텍 패턴 매칭 (Legend Filter) ---
            # 1. 이격도가 바닥권인가? (98~104)
            is_bottom = 98 <= row['Disparity'] <= 104
            # 2. 거래량이 실리며 에너지가 도는가?
            is_energy = row['OBV_Slope'] > 0 and row['MACD_Hist'] > 0
            # 3. 고래가 입질을 시작했는가?
            is_whale = w_score > 5
            # 4. 볼린저밴드(40,2) 돌파했는가?
            if is_bb40_brk:
                s_score += 40  # 장기 추세 돌파는 매우 강력한 가점 대상!
    
            # 레전드 점수 계산 (재영솔루텍 조건 충족 시 폭등)
            legend_score = 0
            if is_bottom and is_energy and is_vma_gc:
                legend_score = 50 # 🏆 레전드 패턴 가산점
             
            # 1. 나스닥 판정
            if row['ixic_close'] > row['ixic_ma5']: weather_icons.append("☀️")
            else: weather_icons.append("🌪️"); storm_count += 1
            
            # 2. S&P500 판정
            if row['sp500_close'] > row['sp500_ma5']: weather_icons.append("☀️")
            else: weather_icons.append("🌪️"); storm_count += 1
            
            # 3. VIX 판정 (VIX는 낮을 때가 맑음)
            if row['vix_close'] < row['vix_ma5']: weather_icons.append("☀️")
            else: weather_icons.append("🌪️"); storm_count += 1
            
            # --- [C] 점수 산출 (당시 기상도 반영) ---
            s_score = int(90 + (30 if is_nova else 15 if is_melon else 0))
            s_score -= (storm_count * 10) # 🌪️ 1개당 10점 감점
            
            if row['OBV_Slope'] < 0: s_score -= 20
            s_score -= max(0, int((row['Disparity']-105)*4))
            
            # 꼬리% 계산
            t_pct = int((row['High']-max(row['Open'],row['Close']))/(row['High']-row['Low'])*100) if row['High']!=row['Low'] else 0
            if t_pct > 40: s_score -= 15

            # 태그 생성
            tags = [t for t, c in zip(["🚀슈퍼타점","🍉수박","Sto-GC","VMA-GC","BB-Break","5일선","🏆LEGEND","🚨장기돌파" ], 
                                      [is_nova, is_melon, is_sto_gc, is_vma_gc, is_bb_brk, row['Close']>row['MA5'], legend_score >= 50, is_bb40_brk]) if c]
            if not tags: continue

            # --- [D] 수익률 검증 ---
            h_df = df.iloc[raw_idx+1:]; buy_p = row['Close']
            max_r = curr_r = min_r = 0.0
            if not h_df.empty:
                max_r = ((h_df['High'].max()-buy_p)/buy_p)*100
                min_r = ((h_df['Low'].min()-buy_p)/buy_p)*100
                curr_r = ((h_df['Close'].iloc[-1]-buy_p)/buy_p)*100

            # --- [E] 결과 기록 ---
            hits.append({
                '날짜': curr_idx.strftime('%Y-%m-%d'), 
                '기상': "".join(weather_icons), # 💡 기상도 컬럼 추가
                '안전': int(max(0, s_score)), 
                '종목': name,
                '외인': f_s, '기관': i_s, '쌍끌이': s_s, 
                '에너지': "🔋" if row['MACD_Hist']>0 else "🪫",
                'OBV기울기': int(row['OBV_Slope']), 
                '🔺최고': f"🔴{max_r:+.1f}%" if max_r>=0 else f"🔵{max_r:+.1f}%",
                '💧최저': f"🔴{min_r:+.1f}%" if min_r>=0 else f"🔵{min_r:+.1f}%",
                '현재': f"🔴{curr_r:+.1f}%" if curr_r>=0 else f"🔵{curr_r:+.1f}%",
                '현재_raw': curr_r, '최고_raw': max_r, '꼬리%': t_pct, 
                '이격': int(row['Disparity']), '구분': " ".join(tags), '보유일': len(h_df)
            })
        return hits
    except Exception as e:
        print(f"❌ {name} 분석 실패: {e}")
        return []

# 🚀 [4] 실행부 및 통합 관제 리포트
# ---------------------------------------------------------
if __name__ == "__main__":
    m_ndx = get_safe_macro('^IXIC', '나스닥')
    m_sp5 = get_safe_macro('^GSPC', 'S&P500')
    m_vix = get_safe_macro('^VIX', 'VIX공포')
    m_fx  = get_safe_macro('USD/KRW', '달러환율')
    macro_status = {'nasdaq': m_ndx, 'sp500': m_sp5, 'vix': m_vix, 'fx': m_fx , 'kospi': {get_index_investor_data('KOSPI')}}

    print("\n" + "🌍 " * 5 + "[ 글로벌 사령부 통합 관제 센터 ]" + " 🌍" * 5)
    print(f"🇺🇸 {m_ndx['text']} | {m_sp5['text']} | ⚠️ {m_vix['text']}")
    print(f"💵 {m_fx['text']} | 🇰🇷 KOSPI 수급: {get_index_investor_data('KOSPI')}")
    print("=" * 115)

    df_krx = fdr.StockListing('KRX').copy()
    target_stocks = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N).copy()
    weather_data = prepare_historical_weather()
    
    all_hits = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(lambda p: analyze_final(p[0], p[1], weather_data), zip(target_stocks['Code'], target_stocks['Name'])))
        for r in results: all_hits.extend(r)

    if all_hits:
        df_total = pd.DataFrame(all_hits)
        past = df_total[df_total['보유일'] > 0].copy()
        today = df_total[df_total['보유일'] == 0].sort_values(by='안전', ascending=False).copy()
        
        low_perf = past[(past['최고_raw'] <= 0) & (past['현재_raw'] <= -5.0)].sort_values(by=['안전', '현재_raw'], ascending=[False, True])
        high_perf = past.drop(low_perf.index).sort_values(by=['안전', '현재_raw'], ascending=[False, False])

        display_cols = ['날짜', '기상', '안전', '종목', '외인', '기관', '쌍끌이', '에너지', 'OBV기울기', '🔺최고', '💧최저', '현재', '꼬리%', '이격', '구분']
        print("\n" + "💎" * 15 + " [사령부 수익/반등 정예군 (Sto-GC 포함)] " + "💎" * 15)
        print(high_perf[display_cols].head(40))
        print("\n" + "💀" * 15 + " [배신자 색출 리포트 (최고수익 <= 0 & 현재 <= -5%)] " + "💀" * 15)
        print(low_perf[display_cols].head(60))
        print("\n" + "🔥" * 15 + " [오늘의 신규 정예군 (0일차)] " + "🔥" * 15)
        print(today[['날짜', '기상', '안전', '종목', '외인', '기관', '쌍끌이', '에너지', 'OBV기울기', '꼬리%', '이격', '구분']].head(20))

    # 7. 구글 시트 업데이트 (별도 관리)
    try:
        final_df = pd.concat([high_perf, low_perf]) # 수익조와 배신자조 합치기
        update_commander_dashboard(final_df, macro_status,"사령부_통합_상황판")
    except:
        pass


    
    else: print("❌ 데이터 분석 실패.")
