# ─────────────────────────────────────────────
# 📌 완전체 주식 스캔 코드 (Python 3.10 호환, pandas만)
# ─────────────────────────────────────────────
import sys
import subprocess
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 전역변수 (조건 수정 가능)
# ─────────────────────────────────────────────
BB40_STD           = 2      # BB40 상단 표준편차
BB20_STD           = 2      # BB20 하단 표준편차
WATERMELON_BODY_PCT = 0.05 # 수박 캔들 몸통 최소 비율
WATERMELON_VOL_MULT = 2    # 수박 거래량 배수
ROSS_LOW_TOL        = 0.03 # 쌍바닥 ±3% 허용
DIV_LOW_TOL         = 0.03 # RSI 다이버전스 ±3% 허용
RSI_PERIOD          = 14
BB40_PERIOD         = 40
BB20_PERIOD         = 20
PAST_PERIOD         = 20   # 과거봉 수

# ─────────────────────────────────────────────
# 라이브러리 설치
# ─────────────────────────────────────────────
def install_libs():
    for lib in ['yfinance', 'pykrx', 'pandas', 'numpy']:
        try:
            __import__(lib.replace('-', '_'))
        except ImportError:
            print(f"🚀 설치 중: {lib}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

install_libs()

# ─────────────────────────────────────────────
# 라이브러리 임포트
# ─────────────────────────────────────────────
import pandas as pd
import numpy as np
import yfinance as yf
from pykrx import stock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ─────────────────────────────────────────────
# 유틸리티
# ─────────────────────────────────────────────
def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

# ─────────────────────────────────────────────
# 지표 계산
# ─────────────────────────────────────────────
def calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    # BB40
    df['MA40'] = df['Close'].rolling(BB40_PERIOD).mean()
    df['STD40'] = df['Close'].rolling(BB40_PERIOD).std()
    df['BB_UP_40'] = df['MA40'] + BB40_STD * df['STD40']
    # BB20
    df['MA20'] = df['Close'].rolling(BB20_PERIOD).mean()
    df['STD20'] = df['Close'].rolling(BB20_PERIOD).std()
    df['BB_LOW_20'] = df['MA20'] - BB20_STD * df['STD20']
    # RSI
    delta = df['Close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(RSI_PERIOD).mean()
    avg_loss = loss.rolling(RSI_PERIOD).mean()
    rs = avg_gain / avg_loss
    df['RSI'] = 100 - (100 / (1 + rs))
    df.dropna(subset=['BB_UP_40', 'BB_LOW_20', 'RSI'], inplace=True)
    return df

# ─────────────────────────────────────────────
# 패턴 체크
# ─────────────────────────────────────────────
def check_watermelon(curr: pd.Series, past: pd.DataFrame) -> tuple[bool, str]:
    cond1 = curr['Close'] > curr['BB_UP_40']
    cond2 = curr['Volume'] > past['Volume'].mean() * WATERMELON_VOL_MULT
    body  = (curr['Close'] - curr['Open']) / curr['Open']
    cond3 = body > WATERMELON_BODY_PCT
    if cond1 and cond2 and cond3:
        return True, "수박 BB40 상단 돌파"
    return False, "❌ 수박 조건 미충족"

def check_ross(curr: pd.Series, past: pd.DataFrame) -> tuple[bool, str]:
    if past.empty or past['BB_LOW_20'].isna().all() or pd.isna(curr['BB_LOW_20']):
        return False, "BB20 데이터 부족"
    bb_low = past['BB_LOW_20']
    # 1차 저점
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any():
        return False, "1차 저점 없음"
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx + 1:]
    if after_first.empty:
        return False, "반등 구간 없음"
    rebound = (after_first['Close'] > after_first['BB_LOW_20']).any()
    if not rebound:
        return False, "반등 없음"
    # 2차 저점 & 안착
    near_band = curr['Low'] <= curr['BB_LOW_20'] * (1 + ROSS_LOW_TOL)
    close_above = curr['Close'] > curr['BB_LOW_20']
    if near_band and close_above:
        return True, "로스 쌍바닥 안착"
    return False, "❌ 쌍바닥 조건 미충족"

def check_divergence(curr: pd.Series, past: pd.DataFrame) -> tuple[bool, str]:
    if pd.isna(curr['RSI']) or past['RSI'].isna().all():
        return False, "RSI 데이터 부족"
    price_low = past['Low'].min()
    rsi_low   = past['RSI'].min()
    cond1 = curr['Low'] <= price_low * (1 + DIV_LOW_TOL)
    cond2 = curr['RSI'] > rsi_low
    if cond1 and cond2:
        return True, "RSI 강세 다이버전스"
    return False, "❌ 다이버전스 조건 미충족"

# ─────────────────────────────────────────────
# 종목 분석
# ─────────────────────────────────────────────
def analyze_stock(name: str, code: str) -> dict | None:
    try:
        df = yf.download(f"{code}.KS", period="200d", interval="1d", progress=False)
        df = flatten_df(df)
        if df.empty or len(df) < 60:
            df = yf.download(f"{code}.KQ", period="200d", interval="1d", progress=False)
            df = flatten_df(df)
        if len(df) < 60:
            return None
        df = calc_indicators(df)
        if len(df) < PAST_PERIOD + 1:
            return None
        curr = df.iloc[-1]
        past = df.iloc[-(PAST_PERIOD+1):-1]
        wm, wm_name = check_watermelon(curr, past)
        ross, ross_name = check_ross(curr, past)
        div, div_name = check_divergence(curr, past)
        score = (50 if wm else 0) + (30 if ross else 0) + (20 if div else 0)
        if score == 0:
            return None
        grade = 'S' if score >= 80 else 'A' if score >= 50 else 'B'
        patterns = []
        if wm: patterns.append(wm_name)
        if ross: patterns.append(ross_name)
        if div: patterns.append(div_name)
        return {
            "종목명": name,
            "코드": code,
            "점수": score,
            "등급": grade,
            "패턴종류": ", ".join(patterns),
            "수박": "✅" if wm else "❌",
            "로스쌍바닥": "✅" if ross else "❌",
            "RSI다이버전스": "✅" if div else "❌",
            "현재가": f"{curr['Close']:.0f}원"
        }
    except Exception as e:
        return None

# ─────────────────────────────────────────────
# 전체 시장 스캔
# ─────────────────────────────────────────────
def scan_market(max_workers: int = 20):
    print("📊 KOSPI & KOSDAQ 전체 종목 스캔 중...")
    today = datetime.today().strftime("%Y%m%d")
    try:
        kospi  = stock.get_market_ticker_list(today, market="KOSPI")
        kosdaq = stock.get_market_ticker_list(today, market="KOSDAQ")
    except:
        print("❌ PyKRX 종목 불러오기 실패")
        return
    tickers = [(stock.get_market_ticker_name(c), c) for c in kospi + kosdaq]
    results, done, total = [], 0, len(tickers)
    start_ts = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(analyze_stock, name, code): code for name, code in tickers}
        for future in as_completed(futures):
            done += 1
            r = future.result()
            if r:
                results.append(r)
            if done % 100 == 0 or done == total:
                elapsed = time.time() - start_ts
                eta = (elapsed / done) * (total - done)
                print(f"진행 {done}/{total} | 후보 {len(results)} | 경과 {elapsed:.0f}s | 남은 ~{eta:.0f}s")
    if not results:
        print("❌ 조건 만족 종목 없음")
        return
    df_result = pd.DataFrame(results).sort_values("점수", ascending=False).reset_index(drop=True)
    df_result.index += 1
    print(f"\n🔥 오늘의 후보 TOP {min(20,len(df_result))}\n")
    print(df_result.head(20).to_string())
    out_path = "candidates.csv"
    df_result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ CSV 저장 완료 → {out_path}")
    total_time = time.time() - start_ts
    print(f"⏱️ 총 소요시간: {total_time:.0f}s")

# ─────────────────────────────────────────────
# 실행
# ─────────────────────────────────────────────
if __name__ == "__main__":
    scan_market(max_workers=20)