import FinanceDataReader as fdr
import os, re, time, pytz
from pykrx import stock
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import warnings

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

def get_investor_data_stable(ticker):
    try:
        ticker = str(ticker).zfill(6)
        df_inv = stock.get_market_net_purchases_of_equities_by_ticker(START_DATE_STR, END_DATE_STR, ticker)
        if df_inv.empty: return "외(0)", "기(0)", "❌", 0, False
        df_valid = df_inv[(df_inv['외국인'] != 0) | (df_inv['기관합계'] != 0)]
        if df_valid.empty: df_valid = df_inv # 전부 0인 경우 대비
        last_row = df_valid.iloc[-1]
        f_net, i_net = last_row['외국인'], last_row['기관합계']
        f_days = i_days = s_days = 0
        for val in reversed(df_inv['외국인']):
            if val > 0: f_days += 1
            elif val < 0: break
        for val in reversed(df_inv['기관합계']):
            if val > 0: i_days += 1
            elif val < 0: break
        if f_net > 0 and i_net > 0:
            for f_v, i_v in zip(reversed(df_inv['외국인']), reversed(df_inv['기관합계'])):
                if f_v > 0 and i_v > 0: s_days += 1
                elif f_v < 0 or i_v < 0: break
        return f"외({f_days})", f"기({i_days})", (f"쌍({s_days})" if s_days > 0 else "❌"), max(f_days, i_days), (f_net > 0 and i_net > 0)
    except: return "외(0)", "기(0)", "❌", 0, False

# ---------------------------------------------------------
# 🕵️‍♂️ [3] 정밀 분석 엔진
# ---------------------------------------------------------
def analyze_final(ticker, name, macro_status, is_kospi=True):
    try:
        df = fdr.DataReader(ticker, start=START_DATE)
        if len(df) < 100: return []
        df = get_indicators(df)
        f_s, i_s, s_s, max_c, twin_b = get_investor_data_stable(ticker)
        
        recent_df = df.tail(SCAN_DAYS)
        hits = []

        for curr_idx, row in recent_df.iterrows():
            raw_idx = df.index.get_loc(curr_idx)
            if raw_idx < 15: continue
            prev = df.iloc[raw_idx-1]
            
            # 💡 신호 판정 (스토캐스틱 골든크로스 복구)
            is_sto_gc = prev['Sto_D'] <= prev['Sto_SD'] and row['Sto_D'] > row['Sto_SD']
            is_vma_gc = prev['VMA5'] <= prev['VMA20'] and row['VMA5'] > row['VMA20']
            is_bb_brk = prev['Close'] <= prev['BB_Upper'] and row['Close'] > row['BB_Upper']
            is_melon = twin_b and row['OBV_Slope'] > 0 and row['ADX'] > 20 and row['MACD_Hist'] > 0
            is_nova = is_sto_gc and is_vma_gc and is_bb_brk and is_melon
            
            # 점수 산출
            storm_count = [macro_status['nasdaq']['status'], macro_status['sp500']['status']].count("🌪️폭풍우")
            s_score = int(90 + (30 if is_nova else 15 if is_melon else 0) - (storm_count * 10))
            if row['OBV_Slope'] < 0: s_score -= 20
            s_score -= max(0, int((row['Disparity']-105)*4))
            
            t_pct = int((row['High']-max(row['Open'],row['Close']))/(row['High']-row['Low'])*100) if row['High']!=row['Low'] else 0
            if t_pct > 40: s_score -= 15

            tags = [t for t, c in zip(["🚀슈퍼타점","🍉수박","Sto-GC","VMA-GC","BB-Break","5일선"], 
                                      [is_nova, is_melon, is_sto_gc, is_vma_gc, is_bb_brk, row['Close']>row['MA5']]) if c]
            if not tags: continue

            h_df = df.iloc[raw_idx+1:]; buy_p = row['Close']
            max_r = curr_r = min_r = 0.0
            if not h_df.empty:
                max_r = ((h_df['High'].max()-buy_p)/buy_p)*100
                min_r = ((h_df['Low'].min()-buy_p)/buy_p)*100
                curr_r = ((h_df['Close'].iloc[-1]-buy_p)/buy_p)*100

            hits.append({
                '날짜': curr_idx.strftime('%m-%d'), '안전': int(max(0, s_score)), '종목': name,
                '외인': f_s, '기관': i_s, '쌍끌이': s_s, '에너지': "🔋" if row['MACD_Hist']>0 else "🪫",
                'OBV기울기': int(row['OBV_Slope']), '🔺최고': f"🔴{max_r:+.1f}%" if max_r>=0 else f"🔵{max_r:+.1f}%",
                '💧최저': f"🔴{min_r:+.1f}%" if min_r>=0 else f"🔵{min_r:+.1f}%",
                '현재': f"🔴{curr_r:+.1f}%" if curr_r>=0 else f"🔵{curr_r:+.1f}%",
                '현재_raw': curr_r, '최고_raw': max_r, '꼬리%': t_pct, '이격': int(row['Disparity']), '구분': " ".join(tags), '보유일': len(h_df)
            })
        return hits
    except: return []

# 🚀 [4] 실행부 및 통합 관제 리포트
# ---------------------------------------------------------
if __name__ == "__main__":
    m_ndx = get_safe_macro('^IXIC', '나스닥')
    m_sp5 = get_safe_macro('^GSPC', 'S&P500')
    m_vix = get_safe_macro('^VIX', 'VIX공포')
    m_fx  = get_safe_macro('USD/KRW', '달러환율')
    macro_status = {'nasdaq': m_ndx, 'sp500': m_sp5, 'vix': m_vix}

    print("\n" + "🌍 " * 5 + "[ 글로벌 사령부 통합 관제 센터 ]" + " 🌍" * 5)
    print(f"🇺🇸 {m_ndx['text']} | {m_sp5['text']} | ⚠️ {m_vix['text']}")
    print(f"💵 {m_fx['text']} | 🇰🇷 KOSPI 수급: {get_index_investor_data('KOSPI')}")
    print("=" * 115)

    df_krx = fdr.StockListing('KRX').copy()
    target_stocks = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N).copy()
    
    all_hits = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(lambda p: analyze_final(p[0], p[1], macro_status), zip(target_stocks['Code'], target_stocks['Name'])))
        for r in results: all_hits.extend(r)

    if all_hits:
        df_total = pd.DataFrame(all_hits)
        past = df_total[df_total['보유일'] > 0].copy()
        today = df_total[df_total['보유일'] == 0].sort_values(by='안전', ascending=False).copy()
        
        low_perf = past[(past['최고_raw'] <= 0) & (past['현재_raw'] <= -5.0)].sort_values(by=['안전', '현재_raw'], ascending=[False, True])
        high_perf = past.drop(low_perf.index).sort_values(by=['안전', '현재_raw'], ascending=[False, False])

        display_cols = ['날짜', '안전', '종목', '외인', '기관', '쌍끌이', '에너지', 'OBV기울기', '🔺최고', '💧최저', '현재', '꼬리%', '이격', '구분']
        print("\n" + "💎" * 15 + " [사령부 수익/반등 정예군 (Sto-GC 포함)] " + "💎" * 15)
        print(high_perf[display_cols].head(40))
        print("\n" + "💀" * 15 + " [배신자 색출 리포트 (최고수익 <= 0 & 현재 <= -5%)] " + "💀" * 15)
        print(low_perf[display_cols].head(60))
        print("\n" + "🔥" * 15 + " [오늘의 신규 정예군 (0일차)] " + "🔥" * 15)
        print(today[['날짜', '안전', '종목', '외인', '기관', '쌍끌이', '에너지', 'OBV기울기', '꼬리%', '이격', '구분']].head(20))

    # 7. 구글 시트 업데이트 (별도 관리)
    try:
        final_df = pd.concat([high_perf, low_perf]) # 수익조와 배신자조 합치기
        update_commander_dashboard(macro_status, final_df, "사령부_통합_상황판")
    except:
        pass


    
    else: print("❌ 데이터 분석 실패.")
