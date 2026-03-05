# ─────────────────────────────────────────────
# Stock Hunter Ultimate Version
# ─────────────────────────────────────────────
import sys
import subprocess
import warnings
warnings.filterwarnings("ignore")

# ── 라이브러리 설치
def install_libs():
    for lib in ['yfinance', 'pandas_ta', 'pykrx', 'numpy', 'pandas']:
        try:
            __import__(lib.replace('-', '_'))
        except ImportError:
            print(f"🚀 Installing {lib} ...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

install_libs()

import pandas as pd
import numpy as np
import yfinance as yf
import pandas_ta as ta
from pykrx import stock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ── 유틸: MultiIndex 컬럼 정리
def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

# ── 유틸: 티커 선택 (KS 먼저, 없으면 KQ)
def get_ticker(code: str) -> tuple[str, pd.DataFrame]:
    df = yf.download(f"{code}.KS", period="200d", interval="1d", progress=False)
    df = flatten_df(df)
    if not df.empty:
        return f"{code}.KS", df
    df = yf.download(f"{code}.KQ", period="200d", interval="1d", progress=False)
    df = flatten_df(df)
    return f"{code}.KQ", df

# ── 로직 1: 수박 BB40 돌파
def check_watermelon(curr: pd.Series, past: pd.DataFrame) -> tuple[bool, str]:
    if pd.isna(curr.get('BB_UP_40')):
        return False, "BB40 데이터 부족"

    cond1 = curr['Close'] > curr['BB_UP_40'] * 0.99  # ±1% 허용
    cond2 = curr['Volume'] > past['Volume'].mean() * 1.2  # 거래량 1.2배
    body  = (curr['Close'] - curr['Open']) / curr['Open']
    cond3 = body > 0.03  # 3% 이상 양봉
    passed = bool(cond1 and cond2 and cond3)
    detail = f"{curr['Close']:.0f} / BB40:{curr['BB_UP_40']:.0f} | Volume:{curr['Volume']:.0f} | Body:{body*100:.1f}%"
    return passed, detail

# ── 로직 2: 로스 쌍바닥
def check_ross(curr: pd.Series, past: pd.DataFrame) -> tuple[bool, str]:
    if past.empty or past['BB_LOW_20'].isna().all() or pd.isna(curr.get('BB_LOW_20')):
        return False, "BB20 데이터 부족"
    bb_low = past['BB_LOW_20']
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any():
        return False, "1차 저점 없음"
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx + 1:]
    rebound = (after_first['Close'] > after_first['BB_LOW_20']).any() if not after_first.empty else False
    near_band = curr['Low'] <= curr['BB_LOW_20'] * 1.03
    close_above = curr['Close'] > curr['BB_LOW_20']
    passed = bool(rebound and near_band and close_above)
    detail = f"2차저점:{curr['Low']:.0f} / BB20:{curr['BB_LOW_20']:.0f}"
    return passed, detail

# ── 로직 3: RSI 다이버전스
def check_divergence(curr: pd.Series, past: pd.DataFrame) -> tuple[bool, str]:
    if pd.isna(curr.get('RSI')) or past['RSI'].isna().all():
        return False, "RSI 데이터 부족"
    price_low = past['Low'].min()
    rsi_low = past['RSI'].min()
    cond1 = curr['Low'] <= price_low * 1.03
    cond2 = curr['RSI'] > rsi_low
    passed = bool(cond1 and cond2)
    detail = f"Low:{curr['Low']:.0f}(과거:{price_low:.0f}) / RSI:{curr['RSI']:.1f}(과거:{rsi_low:.1f})"
    return passed, detail

# ── 단일 종목 분석
def analyze_stock(name: str, code: str) -> dict | None:
    try:
        ticker, df = get_ticker(code)
        if df.empty or len(df) < 60:
            return None
        # 지표 계산
        close = df['Close'].squeeze()
        bb40 = ta.bbands(close, length=40, std=2)
        bb20 = ta.bbands(close, length=20, std=2)
        df['BB_UP_40']  = bb40['BBU_40_2.0']
        df['BB_LOW_20'] = bb20['BBL_20_2.0']
        df['RSI'] = ta.rsi(close, length=14)
        df.dropna(subset=['BB_UP_40','BB_LOW_20','RSI'], inplace=True)
        if len(df) < 22:
            return None
        curr = df.iloc[-1]
        past = df.iloc[-21:-1]
        # 패턴 체크
        wm_pass, wm_detail = check_watermelon(curr, past)
        ross_pass, ross_detail = check_ross(curr, past)
        div_pass, div_detail = check_divergence(curr, past)
        score = (50 if wm_pass else 0) + (30 if ross_pass else 0) + (20 if div_pass else 0)
        if score == 0:
            return None
        grade = 'S' if score >= 80 else 'A' if score >= 50 else 'B'
        return {
            "종목명": name,
            "코드": code,
            "등급": grade,
            "점수": score,
            "수박": "✅" if wm_pass else "❌",
            "수박_패턴": wm_detail if wm_pass else "",
            "로스쌍바닥": "✅" if ross_pass else "❌",
            "로스_패턴": ross_detail if ross_pass else "",
            "RSI다이버전스": "✅" if div_pass else "❌",
            "RSI_패턴": div_detail if div_pass else "",
            "현재가": f"{curr['Close']:.0f}원"
        }
    except Exception:
        return None

# ── 전체 종목 스캔
def scan_market(max_workers: int = 20):
    print("📊 Scanning all KOSPI & KOSDAQ stocks...")
    today = datetime.today().strftime("%Y%m%d")
    kospi  = stock.get_market_ticker_list(today, market="KOSPI")
    kosdaq = stock.get_market_ticker_list(today, market="KOSDAQ")
    tickers = [(stock.get_market_ticker_name(c), c) for c in kospi + kosdaq]
    results = []
    done = 0
    start_ts = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(analyze_stock, name, code): code for name, code in tickers}
        for future in as_completed(futures):
            done += 1
            r = future.result()
            if r:
                results.append(r)
            if done % 100 == 0 or done == len(futures):
                elapsed = time.time() - start_ts
                eta = (elapsed / done) * (len(futures) - done)
                print(f"Progress {done}/{len(futures)} | Candidates {len(results)} | ETA ~{eta:.0f}s")
    if not results:
        print("No candidates found.")
        return
    df_result = pd.DataFrame(results)
    df_result = df_result.sort_values("점수", ascending=False).reset_index(drop=True)
    df_result.index += 1
    print("\n🔥 Top Candidates\n")
    print(df_result.head(20).to_string())
    df_result.to_csv("ultimate_candidates.csv", index=False, encoding="utf-8-sig")
    print("\n✅ CSV saved → ultimate_candidates.csv")
    total_time = time.time() - start_ts
    print(f"⏱️  Total elapsed time: {total_time:.0f}s")

# ── 실행
if __name__ == "__main__":
    scan_market(max_workers=20)