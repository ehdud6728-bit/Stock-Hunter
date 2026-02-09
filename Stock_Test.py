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
SCAN_DAYS = 30
TOP_N = 200 
START_DATE = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
END_DATE_STR = datetime.now().strftime('%Y%m%d')

print(f"📡 [Ver 28.0] 사령부 무결성 통합 가동... 10회 검수 완료 및 초강력 응축 레이더 장착")

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
# 📈 [2] 마스터 지표 엔진 (40일 BB 및 Width 포함)
# ---------------------------------------------------------
def get_indicators(df):
    df = df.copy()
    for n in [5, 20, 40, 60]:
        df[f'MA{n}'] = df['Close'].rolling(n).mean()
        df[f'VMA{n}'] = df['Volume'].rolling(n).mean()
    
    df['Disparity'] = (df['Close'] / df['MA20']) * 100
    
    # 20일 BB 및 Width
    std20 = df['Close'].rolling(20).std()
    df['BB_Upper'] = df['MA20'] + (std20 * 2)
    df['BB20_Width'] = (df['BB_Upper'] - (df['MA20'] - (std20 * 2))) / df['MA20'] * 100
    
    # 40일 BB 및 Width (응축 측정 핵심)
    std40 = df['Close'].rolling(40).std()
    df['BB40_Upper'] = df['MA40'] + (std40 * 2)
    df['BB40_Width'] = (df['BB40_Upper'] - (df['MA40'] - (std40 * 2))) / df['MA40'] * 100
        
    # 스토캐스틱 슬로우 12-5-5
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
    return df

# ---------------------------------------------------------
# 🐳 [3] 수급 분석 엔진 (쌍끌이 twin_b 판정 포함)
# ---------------------------------------------------------
def get_investor_data_stable(ticker, price):
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        res = requests.get(url, headers=headers, timeout=5)
        res.encoding = 'euc-kr'
        df_list = pd.read_html(res.text)
        df = df_list[2].dropna()
        
        i_qty = int(str(df.iloc[0]['기관']).replace('.0','').replace(',',''))
        f_qty = int(str(df.iloc[0]['외국인']).replace('.0','').replace(',',''))
        
        f_money = (f_qty * price) / 100000000
        i_money = (i_qty * price) / 100000000
        total_m = f_money + i_money
        
        twin_b = (f_qty > 0 and i_qty > 0)
        w_score = int(total_m * 2)
        
        return f"외({f_money:.1f}억)", f"기({i_money:.1f}억)", "✅" if twin_b else "❌", max(0, w_score), twin_b
    except:
        return "외(0억)", "기(0억)", "❌", 0, False

# ---------------------------------------------------------
# 🕵️‍♂️ [4] 정밀 분석 엔진 (모든 필터링 및 점수화 통합)
# ---------------------------------------------------------
def prepare_historical_weather():
    start_point = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
    ndx = fdr.DataReader('^IXIC', start=start_point)[['Close']]
    sp5 = fdr.DataReader('^GSPC', start=start_point)[['Close']]
    vix = fdr.DataReader('^VIX', start=start_point)[['Close']]
    
    ndx['ixic_ma5'] = ndx['Close'].rolling(5).mean()
    sp5['sp500_ma5'] = sp5['Close'].rolling(5).mean()
    vix['vix_ma5'] = vix['Close'].rolling(5).mean()
    
    weather_df = pd.concat([
        ndx.rename(columns={'Close': 'ixic_close'}),
        sp5.rename(columns={'Close': 'sp500_close'}),
        vix.rename(columns={'Close': 'vix_close'})
    ], axis=1).fillna(method='ffill')
    return weather_df

def analyze_final(ticker, name, historical_indices):
    try:
        df = fdr.DataReader(ticker, start=START_DATE)
        if len(df) < 100: return []
        df = get_indicators(df)
        df = df.join(historical_indices, how='left').fillna(method='ffill')
        
        curr_price = df.iloc[-1]['Close']
        f_s, i_s, s_s, whale_score, twin_b = get_investor_data_stable(ticker, curr_price)
        
        recent_df = df.tail(SCAN_DAYS)
        hits = []

        for curr_idx, row in recent_df.iterrows():
            raw_idx = df.index.get_loc(curr_idx)
            if raw_idx < 100: continue
            prev = df.iloc[raw_idx-1]
            
            # --- [A] 기술적 신호 판정 ---
            is_sto_gc = prev['Sto_D'] <= prev['Sto_SD'] and row['Sto_D'] > row['Sto_SD']
            is_vma_gc = prev['VMA5'] <= prev['VMA20'] and row['VMA5'] > row['VMA20']
            is_bb_brk = prev['Close'] <= prev['BB_Upper'] and row['Close'] > row['BB_Upper']
            is_bb40_brk = prev['Close'] <= prev['BB40_Upper'] and row['Close'] > row['BB40_Upper']
            
            # --- [B] 🔋 초강력 응축(Double Squeeze) 판정 ---
            min_w20 = df['BB20_Width'].iloc[raw_idx-100:raw_idx+1].min()
            is_min_width20 = row['BB20_Width'] <= min_w20 * 1.15
            is_tight_width40 = row['BB40_Width'] < 15
            is_super_squeeze = is_min_width20 and is_tight_width40 and row['ADX'] < 18 and row['Disparity'] < 103

            # --- [C] 🏆 LEGEND (재영솔루텍 역매공파) 판정 ---
            is_bottom = 98 <= row['Disparity'] <= 104
            is_energy = row['OBV_Slope'] > 0 and row['MACD_Hist'] > 0
            is_legend = is_bottom and is_energy and is_vma_gc

            # --- [D] 점수 산출 및 기상도 ---
            s_score = 90
            is_melon = twin_b and row['OBV_Slope'] > 0 and row['ADX'] > 20 and row['MACD_Hist'] > 0
            is_nova = is_sto_gc and is_vma_gc and is_bb_brk and is_melon
            
            if is_nova: s_score += 30
            elif is_melon: s_score += 15
            if is_legend: s_score += 50
            if is_super_squeeze: s_score += 40
            if is_bb40_brk: s_score += 30
            s_score += whale_score

            # 🌪️ 기상도 감점 로직
            storm_count = 0
            weather_icons = []
            for k in ['ixic', 'sp500']:
                if row[f'{k}_close'] > row[f'{k}_ma5']: weather_icons.append("☀️")
                else: weather_icons.append("🌪️"); storm_count += 1
            if row['vix_close'] < row['vix_ma5']: weather_icons.append("☀️")
            else: weather_icons.append("🌪️"); storm_count += 1
            
            s_score -= (storm_count * 10)
            if row['OBV_Slope'] < 0: s_score -= 20
            
            # 꼬리 감점
            t_pct = int((row['High']-max(row['Open'],row['Close']))/(row['High']-row['Low'])*100) if row['High']!=row['Low'] else 0
            if t_pct > 40: s_score -= 15

            # 태그 생성
            tag_list = []
            if is_nova: tag_list.append("🚀슈퍼타점")
            if is_melon: tag_list.append("🍉수박")
            if is_legend: tag_list.append("🏆LEGEND")
            if is_super_squeeze: tag_list.append("🔋초강력응축")
            if is_bb40_brk: tag_list.append("🚨장기돌파")
            if is_sto_gc: tag_list.append("Sto-GC")
            if row['Close'] > row['MA5']: tag_list.append("5일선")
            
            if not tag_list: continue

            # --- [E] 수익률 검증 ---
            h_df = df.iloc[raw_idx+1:]
            max_r = curr_r = min_r = 0.0
            if not h_df.empty:
                max_r = ((h_df['High'].max()-row['Close'])/row['Close'])*100
                min_r = ((h_df['Low'].min()-row['Close'])/row['Close'])*100
                curr_r = ((h_df['Close'].iloc[-1]-row['Close'])/row['Close'])*100

            hits.append({
                '날짜': curr_idx.strftime('%Y-%m-%d'), 
                '기상': "".join(weather_icons),
                '안전': int(max(0, s_score)), 
                '종목': name,
                '외인': f_s, '기관': i_s, '쌍끌이': s_s, 
                '에너지': "🔋" if row['MACD_Hist']>0 else "🪫",
                'OBV기울기': int(row['OBV_Slope']), 
                '🔺최고': f"{max_r:+.1f}%", '현재': f"{curr_r:+.1f}%", '💧최저': f"{min_r:+.1f}%",
                '현재_raw': curr_r, '최고_raw': max_r, 
                '꼬리%': t_pct, '이격': int(row['Disparity']), 
                '구분': " ".join(tag_list), '보유일': len(h_df)
            })
        return hits
    except Exception as e:
        print(f"❌ {name} 분석 오류: {e}")
        return []

# =================================================
# 🚀 [5] 메인 실행부
# =================================================
if __name__ == "__main__":
    # 매크로 수집
    m_ndx = get_safe_macro('^IXIC', '나스닥')
    m_sp5 = get_safe_macro('^GSPC', 'S&P500')
    m_vix = get_safe_macro('^VIX', 'VIX공포')
    m_fx  = get_safe_macro('USD/KRW', '달러환율')
    macro_status = {'nasdaq': m_ndx, 'sp500': m_sp5, 'vix': m_vix, 'fx': m_fx , 'kospi': get_index_investor_data('KOSPI')}

    print("\n" + "🌍 [글로벌 통합 관제 센터] " + "="*50)
    print(f"🇺🇸 {m_ndx['text']} | {m_sp5['text']} | {m_vix['text']}")
    print(f"💵 {m_fx['text']} | 🇰🇷 KOSPI 수급: {macro_status['kospi']}")
    
    # 종목 리스팅
    df_krx = fdr.StockListing('KRX')
    target_stocks = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    weather_data = prepare_historical_weather()
    
    all_hits = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(lambda p: analyze_final(p[0], p[1], weather_data), zip(target_stocks['Code'], target_stocks['Name'])))
        for r in results: all_hits.extend(r)

    if all_hits:
        df_total = pd.DataFrame(all_hits)
        today = df_total[df_total['보유일'] == 0].sort_values(by='안전', ascending=False)
        past = df_total[df_total['보유일'] > 0]
        
        low_perf = past[(past['최고_raw'] <= 0) & (past['현재_raw'] <= -5.0)].sort_values(by=['안전', '현재_raw'], ascending=[False, True])
        high_perf = past.drop(low_perf.index).sort_values(by=['안전', '현재_raw'], ascending=[False, False])

        display_cols = ['날짜', '기상', '안전', '종목', '쌍끌이', '에너지', 'OBV기울기', '🔺최고', '💧최저', '현재', '꼬리%', '이격', '구분']
        print("\n" + "💎" * 15 + " [사령부 수익/반등 정예군] " + "💎" * 15)
        print(high_perf[display_cols].head(40))
        print("\n" + "🔥" * 15 + " [오늘의 신규 정예군] " + "🔥" * 15)
        print(today[display_cols].head(20))

        # 구글 시트 전송
        try:
            final_to_sheet = pd.concat([today, high_perf, low_perf])
            update_commander_dashboard(final_to_sheet, macro_status, "사령부_통합_상황판")
            print("\n✅ 구글 시트 업데이트 완료!")
        except Exception as e:
            print(f"\n❌ 시트 업데이트 실패: {e}")
    else:
        print("❌ 탐지된 종목이 없습니다.")