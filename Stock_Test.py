# ------------------------------------------------------------------
# 💎 [Ultimate Masterpiece] 구글 시트 자동 저장 및 수익률 검증 통합판 
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# =================================================
# ⚙️ [1. 설정] API 및 구글 시트 정보
# =================================================
# 구글 API 인증용 JSON 파일 이름 (파일을 미리 업로드해야 합니다)
JSON_KEY_FILE = os.environ.get('GOOGLE_JSON_KEY')

# 구글 시트 파일의 제목
SHEET_NAME = '나의_주식_스캐너_리포트' 

SCAN_DAYS = 7             # 성과 검증을 위해 스캔 범위를 7일로 확장
TOP_N = 400               
MIN_MARCAP = 100000000000 
STOP_LOSS_PCT = -5.0      
WHALE_THRESHOLD = 50      
STREAK_THRESHOLD = 3      

HEADERS = {'User-Agent': 'Mozilla/5.0'}

# ---------------------------------------------------------
# 🏥 [2] 재무 건전성 및 수급 분석 로직
# ---------------------------------------------------------
def get_financial_health(code):
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = requests.get(url, headers=HEADERS, timeout=5)
        dfs = pd.read_html(res.text)
        df_fin = dfs[3]; df_fin.columns = df_fin.columns.get_level_values(1)
        latest_profit = df_fin.iloc[1, -2] 
        latest_debt = df_fin.iloc[6, -2]   
        f_score = (1 if float(latest_profit) > 0 else 0) + (1 if float(latest_debt) < 100 else 0)
        tag = "S(우량)" if f_score == 2 else ("A(양호)" if f_score == 1 else "C(주의)")
        return tag, f_score
    except: return "N(미비)", 0

def get_supply_and_score(code, price):
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        res = requests.get(url, headers=HEADERS, timeout=5); res.encoding = 'euc-kr'
        df = pd.read_html(res.text, match='날짜')[0].dropna().head(10)
        df.columns = ['_'.join(col) if isinstance(col, tuple) else col for col in df.columns]
        inst_col = [c for c in df.columns if '기관' in c and '순매매' in c][0]
        frgn_col = [c for c in df.columns if '외국인' in c and '순매매' in c][0]
        inst_qty = [int(float(str(v).replace(',', ''))) for v in df[inst_col].values]
        frgn_qty = [int(float(str(v).replace(',', ''))) for v in df[frgn_col].values]
        
        total_m = round((abs(inst_qty[0]) + abs(frgn_qty[0])) * price / 10000000)
        leader = "🤝쌍끌" if inst_qty[0] > 0 and frgn_qty[0] > 0 else ("🔴기관" if inst_qty[0] > frgn_qty[0] else "🔵외인")
        
        w_streak = 0
        for k in range(len(inst_qty)):
            if (abs(inst_qty[k]) + abs(frgn_qty[k])) * price / 10000000 >= WHALE_THRESHOLD: w_streak += 1
            else: break
        
        bonus = (total_m // 50) + (3 if w_streak >= STREAK_THRESHOLD else 0)
        return f"{leader}({w_streak}일)", total_m, w_streak, bonus
    except: return "⚠️오류", 0, 0, 0

# ---------------------------------------------------------
# 📊 [3] 구글 시트 저장 함수
# ---------------------------------------------------------
def save_to_google_sheets(df_today, df_past):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SHEET_NAME)
        
        # 1. '오늘의추천' 탭 저장 (데이터 갱신)
        ws_today = spreadsheet.get_worksheet(0)
        ws_today.clear()
        ws_today.update([df_today.columns.tolist()] + df_today.fillna('').values.tolist())
        
        # 2. '성과복기' 탭 저장 (데이터 갱신)
        ws_past = spreadsheet.get_worksheet(1)
        ws_past.clear()
        ws_past.update([df_past.columns.tolist()] + df_past.fillna('').values.tolist())
        
        print(f"✅ 구글 시트 '{SHEET_NAME}' 업데이트 완료!")
    except Exception as e:
        print(f"❌ 구글 시트 오류: {e}")

# ---------------------------------------------------------
# 🕵️‍♂️ [4] 분석 엔진
# ---------------------------------------------------------
def analyze_final(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now()-timedelta(days=730)).strftime('%Y-%m-%d'))
        if len(df) < 120: return []
        
        # 지표 계산
        for n in [5, 20, 60]: df[f'MA{n}'] = df['Close'].rolling(n).mean()
        for n in [5, 20]: df[f'VMA{n}'] = df['Volume'].rolling(n).mean()
        df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
        df['OBV_MA20'] = df['OBV'].rolling(20).mean()
        l, h = df['Low'].rolling(5).min(), df['High'].rolling(5).max()
        df['Slow_K'] = ((df['Close'] - l) / (h - l)).rolling(3).mean() * 100
        df['Slow_D'] = df['Slow_K'].rolling(3).mean()
        
        recent_df = df.iloc[-SCAN_DAYS:]
        hits = []
        sector = sector_dict.get(ticker, "미분류")

        for i in range(len(recent_df)):
            curr_idx = recent_df.index[i]; raw_idx = df.index.get_loc(curr_idx); row, prev = df.iloc[raw_idx], df.iloc[raw_idx-1]
            score, tags = 0, []
            
            # 전략 체크
            is_p_gc = (prev['MA5'] <= prev['MA20']) and (row['MA5'] > row['MA20'])
            is_v_gc = (prev['VMA5'] <= prev['VMA20']) and (row['VMA5'] > row['VMA20'])
            if is_p_gc and is_v_gc: tags.append("✨Double-GC"); score += 5
            if row['OBV'] > row['OBV_MA20']: tags.append("🌊OBV매집"); score += 2
            box_h = df['High'].iloc[raw_idx-25:raw_idx].max()
            if row['Close'] > box_h: tags.append("🔨공구리"); score += 4
            if prev['Slow_K'] <= prev['Slow_D'] and row['Slow_K'] > row['Slow_D'] and row['Slow_K'] < 75:
                tags.append("🍉수박"); score += 2

            if not tags: continue

            # 수급/재무/성과 분석
            s_tag, total_m, w_streak, w_score = get_supply_and_score(ticker, row['Close'])
            f_tag, f_score = get_financial_health(ticker)
            score += (w_score + f_score)
            
            buy_p = row['Close']; holding = df.iloc[raw_idx+1:]; sl_date = "유지중"
            max_r = min_r = curr_r = 0.0
            if not holding.empty:
                for h_idx, h_row in holding.iterrows():
                    if ((h_row['Low'] - buy_p)/buy_p)*100 <= STOP_LOSS_PCT:
                        sl_date = h_idx.strftime('%m-%d'); break
                max_r = ((holding['High'].max()-buy_p)/buy_p)*100
                min_r = ((holding['Low'].min()-buy_p)/buy_p)*100
                curr_r = ((holding['Close'].iloc[-1]-buy_p)/buy_p)*100

            hits.append({
                '날짜': curr_idx.strftime('%Y-%m-%d'), '점수': score, '종목': name, '구분': " ".join(tags),
                '재무': f_tag, '🔥베팅': f"{total_m}천", '🔺최고%': round(max_r, 1), '💧최저%': round(min_r, 1),
                '현재%': round(curr_r, 1), '🛑손절': sl_date, '수급': s_tag, '산업': str(sector)[:10], '보유': len(holding)
            })
        return hits
    except: return []

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [The Masterpiece] 스캔 및 구글 시트 전송 시작...")
    df_krx = fdr.StockListing('KRX')
    found_col = next((c for c in ['Sector', 'Industry', 'Dept'] if c in df_krx.columns), 'Market')
    sector_dict = dict(zip(df_krx['Code'], df_krx[found_col]))
    target_stocks = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    
    all_hits = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(lambda p: analyze_final(*p), zip(target_stocks['Code'], target_stocks['Name'])))
        for r in results: all_hits.extend(r)

    if all_hits:
        df_total = pd.DataFrame(all_hits)
        
        # 1. 오늘의 추천 (보유 0일차)
        today = df_total[df_total['보유'] == 0].sort_values(by='점수', ascending=False)
        # 2. 성과 복기 (보유 1일차 이상)
        past = df_total[df_total['보유'] > 0].sort_values(by=['날짜', '현재%'], ascending=[False, False])
        
        # 화면 출력
        print("\n📢 [오늘의 추천 종목]")
        print(today[['날짜', '점수', '종목', '구분', '🔥베팅', '수급']].head(10))
        
        # 구글 시트 저장
        save_to_google_sheets(today, past)
    else:
        print("❌ 포착된 종목이 없습니다.")
