# ─────────────────────────────────────────────
# Stock Hunter 끝판왕 버전
# ─────────────────────────────────────────────
import os
import sys
import warnings
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import yfinance as yf
from pykrx import stock
import requests

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 전역 변수: 조건값 조정 가능
# ─────────────────────────────────────────────
BB40_STD = 2
BB20_STD = 2
ROSS_BAND_TOLERANCE = 1.05   # 로스 쌍바닥 ±5%
RSI_LOW_TOLERANCE = 1.05     # RSI 저점 ±5%
WATERMELON_BODY_MIN = 0.05   # 5% 이상 양봉
VOLUME_MULT = 2               # 20일 평균 대비 거래량 배수
TOP_N = 20                    # TOP 후보 수
MAX_WORKERS = 20              # 병렬 스레드 수
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# ─────────────────────────────────────────────
# 유틸 함수
# ─────────────────────────────────────────────
def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

def calculate_bb(close: pd.Series, length: int, std: float) -> tuple[pd.Series, pd.Series]:
    ma = close.rolling(length).mean()
    sd = close.rolling(length).std()
    upper = ma + (sd * std)
    lower = ma - (sd * std)
    return upper, lower

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.rolling(period).mean()
    ma_down = down.rolling(period).mean()
    rsi = 100 - (100 / (1 + ma_up / ma_down))
    return rsi

# ─────────────────────────────────────────────
# 패턴 로직
# ─────────────────────────────────────────────
def check_watermelon(curr: pd.Series, past: pd.DataFrame) -> tuple[bool, str]:
    cond1 = curr['Close'] > curr['BB_UP_40']
    cond2 = curr['Volume'] > past['Volume'].mean() * VOLUME_MULT
    body = (curr['Close'] - curr['Open']) / curr['Open']
    cond3 = body > WATERMELON_BODY_MIN
    detail = f"종가 {curr['Close']:.0f} / BB40 상단 {curr['BB_UP_40']:.0f} / 몸통 {body:.2%} / 거래량 {curr['Volume']}"
    return cond1 and cond2 and cond3, detail

def check_ross(curr: pd.Series, past: pd.DataFrame) -> tuple[bool, str]:
    bb_low = past['BB_LOW_20']
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any():
        return False, "1차 저점 없음"
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx + 1:]
    rebound = (after_first['Close'] > after_first['BB_LOW_20']).any()
    near_band = curr['Low'] <= curr['BB_LOW_20'] * ROSS_BAND_TOLERANCE
    close_above = curr['Close'] > curr['BB_LOW_20']
    detail = f"현재 저가 {curr['Low']:.0f} / BB20 {curr['BB_LOW_20']:.0f}"
    return bool(rebound and near_band and close_above), detail

def check_divergence(curr: pd.Series, past: pd.DataFrame) -> tuple[bool, str]:
    price_low = past['Low'].min()
    rsi_low = past['RSI'].min()
    cond1 = curr['Low'] <= price_low * RSI_LOW_TOLERANCE
    cond2 = curr['RSI'] > rsi_low
    detail = f"현재 저가 {curr['Low']:.0f} / 과거 최저 {price_low:.0f} / RSI {curr['RSI']:.1f} vs {rsi_low:.1f}"
    return cond1 and cond2, detail

# ─────────────────────────────────────────────
# AI 코멘트 (간단 샘플)
# ─────────────────────────────────────────────
def generate_ai_comment(stock_name: str, patterns: list[str]) -> str:
    return f"{stock_name} 패턴: {', '.join(patterns)} 분석 완료. 상승 가능성 확인 필요."

# ─────────────────────────────────────────────
# 뉴스 점수 (한국 뉴스 간단 샘플)
# ─────────────────────────────────────────────
def fetch_news_score(stock_name: str) -> int:
    # 단순 샘플: 실제 API 연결 필요
    return np.random.randint(0, 11)

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

        # 지표 계산
        close = df['Close']
        df['BB_UP_40'], _ = calculate_bb(close, 40, BB40_STD)
        _, df['BB_LOW_20'] = calculate_bb(close, 20, BB20_STD)
        df['RSI'] = calculate_rsi(close, 14)
        df.dropna(subset=['BB_UP_40', 'BB_LOW_20', 'RSI'], inplace=True)

        curr = df.iloc[-1]
        past = df.iloc[-21:-1]

        patterns = []
        score = 0

        wm, wm_detail = check_watermelon(curr, past)
        if wm:
            score += 50
            patterns.append("수박")
        ross, ross_detail = check_ross(curr, past)
        if ross:
            score += 30
            patterns.append("로스쌍바닥")
        div, div_detail = check_divergence(curr, past)
        if div:
            score += 20
            patterns.append("RSI다이버전스")

        news_score = fetch_news_score(name)
        final_score = score + news_score
        grade = 'S' if final_score >= 80 else 'A' if final_score >= 50 else 'B'

        return {
            "종목명": name,
            "코드": code,
            "패턴점수": score,
            "패턴종류": ",".join(patterns) if patterns else "없음",
            "뉴스점수": news_score,
            "최종점수": final_score,
            "등급": grade,
            "AI코멘트": generate_ai_comment(name, patterns),
            "현재가": f"{curr['Close']:.0f}원",
        }

    except Exception as e:
        print(f"[ERROR] {name} ({code}) 분석 실패: {e}")
        return None

# ─────────────────────────────────────────────
# 전체 스캔
# ─────────────────────────────────────────────
def scan_market():
    print("📊 Scanning all KOSPI & KOSDAQ stocks...")
    today = datetime.today().strftime("%Y%m%d")
    kospi = stock.get_market_ticker_list(today, market="KOSPI")
    kosdaq = stock.get_market_ticker_list(today, market="KOSDAQ")
    tickers = [(stock.get_market_ticker_name(c), c) for c in kospi + kosdaq]
    results = []
    done = 0
    start_ts = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_stock, name, code): code for name, code in tickers}
        for future in as_completed(futures):
            done += 1
            r = future.result()
            if r:
                results.append(r)
            if done % 50 == 0 or done == len(futures):
                elapsed = time.time() - start_ts
                eta = (elapsed / done) * (len(futures) - done)
                print(f"진행 {done}/{len(futures)} | 후보 {len(results)} | 경과 {elapsed:.0f}s | 남은시간 ~{eta:.0f}s")

    if not results:
        print("조건 만족 종목 없음")
        return

    df_result = pd.DataFrame(results)
    df_result = df_result.sort_values("최종점수", ascending=False).reset_index(drop=True)
    df_result.index += 1
    print(f"\n🔥 오늘의 TOP {min(TOP_N, len(df_result))} 후보\n")
    print(df_result.head(TOP_N).to_string())
    df_result.to_csv("final_candidates.csv", index=False, encoding="utf-8-sig")
    print("\n✅ CSV 저장 완료 → final_candidates.csv")

# ─────────────────────────────────────────────
# 실행
# ─────────────────────────────────────────────
if __name__ == "__main__":
    scan_market()