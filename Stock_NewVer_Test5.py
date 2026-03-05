import sys
import subprocess
import warnings
warnings.filterwarnings("ignore")

import os
import pandas as pd
import numpy as np
import yfinance as yf
from pykrx import stock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import openai

# OPENAI_API_KEY 환경변수 사용
openai.api_key = os.environ.get("OPENAI_API_KEY")

# ────────────── 설정: 조절 가능한 전역변수 ──────────────
ROSS_BB_ALLOW = 1.07    # 로스 쌍바닥 BB 하단 허용 ±7%
RSI_ALLOW     = 1.07    # RSI 저점 허용 ±7%
WATERMELON_VOLUME_MULT = 2    # 수박 거래량 기준 곱
WATERMELON_BODY_PCT     = 0.05 # 수박 캔들 몸통 기준

MAX_WORKERS = 20
TOP_N_RESULTS = 20

# ────────────── 지표 계산 함수 ──────────────
def bbands(series, length=20, std=2):
    ma = series.rolling(length).mean()
    sd = series.rolling(length).std()
    upper = ma + std * sd
    lower = ma - std * sd
    return upper, lower

def rsi(series, length=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(length).mean()
    avg_loss = loss.rolling(length).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# ────────────── 지표 체크 ──────────────
def check_watermelon(curr, past):
    cond1 = curr['Close'] > curr['BB_UP']
    cond2 = curr['Volume'] > past['Volume'].mean() * WATERMELON_VOLUME_MULT
    body  = (curr['Close'] - curr['Open']) / curr['Open']
    cond3 = body > WATERMELON_BODY_PCT
    passed = cond1 and cond2 and cond3
    detail = f"Close:{curr['Close']}, BB_UP:{curr['BB_UP']}, Volume:{curr['Volume']}, Body:{body:.2f}"
    return passed, detail, "수박 BB40 돌파"

def check_ross(curr, past):
    if past.empty or past['BB_LOW'].isna().all() or pd.isna(curr['BB_LOW']):
        return False, "데이터 부족", "로스 쌍바닥"
    bb_low = past['BB_LOW']
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any():
        return False, "1차 저점 없음", "로스 쌍바닥"
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx + 1:]
    if after_first.empty or not (after_first['Close'] > after_first['BB_LOW']).any():
        return False, "반등 없음", "로스 쌍바닥"
    near_band = curr['Low'] <= curr['BB_LOW'] * ROSS_BB_ALLOW
    close_above = curr['Close'] > curr['BB_LOW']
    passed = near_band and close_above
    detail = f"Low:{curr['Low']}, BB_LOW:{curr['BB_LOW']}, Close:{curr['Close']}"
    return passed, detail, "로스 쌍바닥"

def check_rsi_div(curr, past):
    if past['RSI'].isna().all() or pd.isna(curr['RSI']):
        return False, "RSI 데이터 부족", "RSI 다이버전스"
    price_low = past['Low'].min()
    rsi_low = past['RSI'].min()
    cond1 = curr['Low'] <= price_low * RSI_ALLOW
    cond2 = curr['RSI'] > rsi_low
    passed = cond1 and cond2
    detail = f"Low:{curr['Low']}, Price_low:{price_low}, RSI:{curr['RSI']}, RSI_low:{rsi_low}"
    return passed, detail, "RSI 다이버전스"

# ────────────── AI 뉴스 코멘트 ──────────────
def get_ai_comment(stock_name, code):
    try:
        prompt = f"한국 주식 {stock_name}({code}) 최근 뉴스와 이슈를 요약하고, 매수 관점 코멘트를 50자 내외로 작성해줘."
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role":"user","content":prompt}],
            max_tokens=150
        )
        return response['choices'][0]['message']['content'].strip()
    except Exception as e:
        return f"AI 코멘트 오류: {e}"

# ────────────── 단일 종목 분석 ──────────────
def analyze_stock(name, code):
    try:
        df = yf.download(f"{code}.KS", period="200d", interval="1d", progress=False)
        if df.empty or len(df) < 60:
            df = yf.download(f"{code}.KQ", period="200d", interval="1d", progress=False)
        if len(df) < 60:
            return None
        df['BB_UP'], df['BB_LOW'] = bbands(df['Close'], length=40, std=2), bbands(df['Close'], length=20, std=2)[1]
        df['RSI'] = rsi(df['Close'], length=14)
        df.dropna(subset=['BB_UP','BB_LOW','RSI'], inplace=True)
        if len(df) < 22:
            return None
        curr = df.iloc[-1]
        past = df.iloc[-21:-1]

        patterns = []
        score = 0

        wm_pass, wm_detail, wm_name = check_watermelon(curr, past)
        ross_pass, ross_detail, ross_name = check_ross(curr, past)
        rsi_pass, rsi_detail, rsi_name = check_rsi_div(curr, past)

        if wm_pass:
            score += 50
            patterns.append((wm_name, wm_detail))
        if ross_pass:
            score += 30
            patterns.append((ross_name, ross_detail))
        if rsi_pass:
            score += 20
            patterns.append((rsi_name, rsi_detail))

        if score == 0:
            return None

        grade = 'S' if score >= 80 else 'A' if score >=50 else 'B'
        ai_comment = get_ai_comment(name, code)

        return {
            "종목명": name,
            "코드": code,
            "점수": score,
            "등급": grade,
            "패턴": patterns,
            "AI코멘트": ai_comment,
            "현재가": f"{curr['Close']:.0f}원"
        }
    except Exception as e:
        print(f"[오류] {name}({code}): {e}")
        return None

# ────────────── 전체 시장 스캔 ──────────────
def scan_market():
    today = datetime.today().strftime("%Y%m%d")
    kospi  = stock.get_market_ticker_list(today, market="KOSPI")
    kosdaq = stock.get_market_ticker_list(today, market="KOSDAQ")
    tickers = [(stock.get_market_ticker_name(c), c) for c in kospi + kosdaq]
    total = len(tickers)
    print(f"📊 Scanning {total} stocks with {MAX_WORKERS} workers...")

    results = []
    done = 0
    start_ts = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_stock, name, code): code for name, code in tickers}
        for future in as_completed(futures):
            done +=1
            res = future.result()
            if res:
                results.append(res)
            if done % 100 == 0 or done == total:
                elapsed = time.time() - start_ts
                eta = (elapsed / done)*(total-done)
                print(f"  진행 {done}/{total} | 후보 {len(results)}개 | 경과 {elapsed:.0f}s | 남은시간 ~{eta:.0f}s")

    if not results:
        print("No candidates found.")
        return

    df_result = pd.DataFrame(results).sort_values("점수", ascending=False).reset_index(drop=True)
    df_result.index +=1
    print(f"\n🔥 TOP {min(TOP_N_RESULTS,len(df_result))} candidates\n")
    print(df_result.head(TOP_N_RESULTS).to_string())
    df_result.to_csv("final_candidates.csv", index=False, encoding="utf-8-sig")
    print("✅ CSV saved as final_candidates.csv")

# ────────────── 실행 ──────────────
if __name__=="__main__":
    scan_market()