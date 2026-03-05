import os
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
from pykrx import stock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import openai
from google_sheet_manager import update_google_sheet

# ──────────────────────────────
# 전역 변수 (조건값 조정 가능)
# ──────────────────────────────
ROSS_BAND_TOLERANCE = 1.05
RSI_LOW_TOLERANCE   = 1.05
WATERMELON_VOLUME_MULTIPLIER = 2
WATERMELON_BODY_RATIO = 0.05
MAX_WORKERS = 20
TOP_N = 20

# OpenAI API
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

# ──────────────────────────────
# KRX 상장 종목 안전 로드
# ──────────────────────────────
def load_krx_listing_safe():
    try:
        SHEET_ID = "13Esd11iwgzLN7opMYobQ3ee6huHs1FDEbyeb3Djnu6o"
        GID = "1238448456"
        url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
        df_krx = pd.read_csv(url, encoding="utf-8", engine="python")
        if df_krx.empty:
            print("📡 FDR KRX 시도...")
            import FinanceDataReader as fdr
            df_krx = fdr.StockListing('KRX')
        if df_krx.empty:
            raise ValueError("빈 데이터")
        print("✅ FDR 성공")
        return df_krx
    except Exception as e:
        print(f"⚠️ FDR 실패 → pykrx 대체 사용 ({e})")
        tickers = stock.get_market_ticker_list(datetime.today().strftime("%Y%m%d"), market="ALL")
        df_krx = pd.DataFrame({
            'Code': tickers,
            'Name': [stock.get_market_ticker_name(c) for c in tickers],
            'Sector': ['일반']*len(tickers)
        })
        return df_krx

# ──────────────────────────────
# 유틸 함수
# ──────────────────────────────
def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(span=period, adjust=False).mean()
    roll_down = down.ewm(span=period, adjust=False).mean()
    rs = roll_up / roll_down
    rsi = 100 - 100 / (1 + rs)
    return rsi

def ai_comment_summary(name: str, pattern_info: str) -> str:
    try:
        if not OPENAI_API_KEY:
            return "API Key 없음"
        prompt = f"한국 주식 종목 {name}이 다음 패턴을 보였습니다: {pattern_info}. 투자자에게 짧게 코멘트 해주세요."
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role":"user","content":prompt}],
            max_tokens=50,
            temperature=0.5
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"AI 코멘트 실패: {e}"

# ──────────────────────────────
# 패턴 체크
# ──────────────────────────────
def check_watermelon(curr: pd.Series, past: pd.DataFrame):
    cond1 = curr['종가'] > curr['BB_UP']
    cond2 = curr['거래량'] > past['거래량'].mean() * WATERMELON_VOLUME_MULTIPLIER
    body_ratio = (curr['종가'] - curr['시가']) / curr['시가']
    cond3 = body_ratio > WATERMELON_BODY_RATIO
    return cond1 and cond2 and cond3, f"종가:{curr['종가']:.0f}, 거래량:{curr['거래량']}, 몸통:{body_ratio:.2f}"

def check_ross(curr: pd.Series, past: pd.DataFrame):
    if past.empty or past['BB_LOW'].isna().all():
        return False, "과거 데이터 부족"
    bb_low = past['BB_LOW']
    outside_mask = past['저가'] < bb_low
    if not outside_mask.any():
        return False, "1차 저점 없음"
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx + 1:]
    rebound = (after_first['종가'] > after_first['BB_LOW']).any()
    near_band = curr['저가'] <= curr['BB_LOW'] * ROSS_BAND_TOLERANCE
    close_above = curr['종가'] > curr['BB_LOW']
    passed = rebound and near_band and close_above
    return passed, f"반등:{rebound}, 저가밴드근접:{near_band}, 종가밴드위:{close_above}"

def check_rsi_div(curr: pd.Series, past: pd.DataFrame):
    if past['RSI'].isna().all() or pd.isna(curr['RSI']):
        return False, "RSI 데이터 부족"
    min_price_past = past['저가'].min()
    min_rsi_past = past['RSI'].min()
    price_similar = curr['저가'] <= min_price_past * RSI_LOW_TOLERANCE
    rsi_higher = curr['RSI'] > min_rsi_past
    return price_similar and rsi_higher, f"주가저점:{curr['저가']:.0f}(과거:{min_price_past:.0f}), RSI:{curr['RSI']:.1f}(과거:{min_rsi_past:.1f})"

# ──────────────────────────────
# 단일 종목 분석
# ──────────────────────────────
def analyze_stock(name: str, code: str):
    try:
        today = datetime.today().strftime("%Y%m%d")
        df = stock.get_market_ohlcv(today, code, "200d")
        if df.empty or len(df)<60:
            return None
        df = flatten_df(df)
        df['MA20'] = df['종가'].rolling(20).mean()
        df['MA40'] = df['종가'].rolling(40).mean()
        df['BB_UP'] = df['MA40'] + 2*df['종가'].rolling(40).std()
        df['BB_LOW'] = df['MA20'] - 2*df['종가'].rolling(20).std()
        df['RSI'] = compute_rsi(df['종가'], 14)
        df.dropna(subset=['BB_UP','BB_LOW','RSI'], inplace=True)
        curr = df.iloc[-1]
        past = df.iloc[-21:-1]
        wm, _ = check_watermelon(curr, past)
        ross, _ = check_ross(curr, past)
        rsi_div, _ = check_rsi_div(curr, past)
        score = (50 if wm else 0) + (30 if ross else 0) + (20 if rsi_div else 0)
        grade = 'S' if score>=80 else 'A' if score>=50 else 'B' if score>=30 else 'C'
        pattern_info = []
        if wm: pattern_info.append("수박")
        if ross: pattern_info.append("로스쌍바닥")
        if rsi_div: pattern_info.append("RSI다이버전스")
        ai_comment = ai_comment_summary(name, ",".join(pattern_info)) if pattern_info else "패턴없음"
        return {
            "종목명": name,
            "코드": code,
            "점수": score,
            "등급": grade,
            "패턴": ",".join(pattern_info),
            "수박": "✅" if wm else "❌",
            "로스쌍바닥": "✅" if ross else "❌",
            "RSI다이버전스": "✅" if rsi_div else "❌",
            "현재가": f"{curr['종가']:.0f}원",
            "패턴점수": score,
            "패턴정보": ",".join(pattern_info),
            "AI코멘트": ai_comment,
            "뉴스점수": 0
        }
    except Exception as e:
        print(f"❌ {name}({code}) 분석 실패: {e}")
        return None

# ──────────────────────────────
# 전체 스캔
# ──────────────────────────────
def scan_market():
    print("📊 Scanning all KOSPI & KOSDAQ stocks...")
    today = datetime.today().strftime("%Y%m%d")
    try:
        tickers = load_krx_listing_safe()
        tickers['Code'] = (
            tickers['Code']
            .fillna('')
            .astype(str)
            .str.replace('.0', '', regex=False)
            .str.zfill(6)
        )
        print(f"✅ 총 종목 로드: {len(tickers)}개")
    except Exception as e:
        print(f"🚨 종목 로드 실패: {e}")
        return

    results = []
    done = 0
    start_ts = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_stock,row['Name'],row['Code']): row['Code'] for _, row in tickers.iterrows()}
        for future in as_completed(futures):
            done +=1
            r = future.result()
            if r:
                results.append(r)
            if done%50==0 or done==len(futures):
                elapsed = time.time()-start_ts
                eta = (elapsed/done)*(len(futures)-done)
                print(f"진행 {done}/{len(futures)}, 후보:{len(results)}, 경과:{elapsed:.0f}s, 남은:{eta:.0f}s")

    if not results:
        print("조건 만족 종목 없음")
        return

    df_result = pd.DataFrame(results)
    df_result = df_result.sort_values("점수",ascending=False).reset_index(drop=True)
    df_result.index +=1
    print(f"\n🔥 TOP {min(TOP_N,len(df_result))} 후보\n")
    print(df_result.head(TOP_N).to_string())
    out_path = "watermelon_candidates.csv"
    df_result.to_csv(out_path,index=False,encoding="utf-8-sig")
    print(f"\n✅ CSV 저장 완료 → {out_path}")
    print(f"⏱️ 총 소요시간: {time.time()-start_ts:.0f}초")

# ──────────────────────────────
# 실행
# ──────────────────────────────
if __name__=="__main__":
    scan_market()