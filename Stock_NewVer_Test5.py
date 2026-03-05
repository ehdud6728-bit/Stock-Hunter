# stock_master_scanner_final.py
import sys
import subprocess
import warnings
warnings.filterwarnings("ignore")
import os

# ────────────── 설치 확인 ──────────────
def install_libs():
    for lib in ['yfinance', 'pandas', 'numpy', 'pykrx', 'beautifulsoup4', 'requests', 'openai', 'pandas_ta']:
        try:
            __import__(lib.replace('-', '_'))
        except ImportError:
            print(f"🚀 설치 중: {lib}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

install_libs()

# ────────────── 라이브러리 임포트 ──────────────
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_ta as ta
from pykrx import stock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
from openai import OpenAI
import json
import time

# ────────────── 전역 설정 ──────────────
LOGBUF = []

# 패턴 조건 (전역변수로 쉽게 조정)
ROSS_BB_MARGIN = 1.05    # 로스 쌍바닥 ±5%
RSI_MARGIN      = 1.05    # RSI 저점 ±5%
WATERMELON_VOLUME_MULT = 2  # 거래량 2배
WATERMELON_BODY_PCT = 0.05  # 캔들 몸통 5% 이상

# AI 뉴스 분석 설정
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# ────────────── 유틸 ──────────────
def log(msg):
    print(msg)
    LOGBUF.append(msg)

def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

def get_news_headlines(ticker, n=5):
    url = f"https://news.google.com/rss/search?q={ticker}+when:7d&hl=en-US&gl=US&ceid=US:en"
    res = requests.get(url, headers={'User-Agent':'Mozilla/5.0'})
    soup = BeautifulSoup(res.content, "xml")
    items = soup.find_all("item")
    return [item.title.text for item in items[:n]]

def get_news_score_and_comment(ticker):
    headlines = get_news_headlines(ticker)
    if not headlines:
        return 50, "최근 뉴스 없음, 중립"
    prompt = f"""
    아래 {ticker} 관련 최신 뉴스 {len(headlines)}개를 분석해줘.
    - 치명적 부정적 이슈는 점수 낮게
    - 긍정적 뉴스는 점수 높게
    - 점수는 0~100
    - 한줄 코멘트
    뉴스: {headlines}
    JSON으로 {{"score":0~100, "comment":"..."}} 형태로 출력
    """
    if not OPENAI_API_KEY:
        return 50, "OPENAI_API_KEY 없음, 중립"
    client = OpenAI(api_key=OPENAI_API_KEY)
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role":"system","content":"You are a financial analyst."},
                {"role":"user","content":prompt}
            ]
        )
        text = res.choices[0].message.content
        data = json.loads(text)
        score = int(data.get("score",50))
        comment = data.get("comment","코멘트 없음")
    except Exception as e:
        log(f"AI 뉴스 분석 실패: {e}")
        score = 50
        comment = "분석 실패, 중립"
    return score, comment

# ────────────── 패턴 로직 ──────────────
def check_watermelon(curr, past):
    if pd.isna(curr.get('BB_UP')):
        return False, ""
    cond1 = curr['Close'] > curr['BB_UP']
    cond2 = curr['Volume'] > past['Volume'].mean() * WATERMELON_VOLUME_MULT
    body = (curr['Close'] - curr['Open']) / curr['Open']
    cond3 = body > WATERMELON_BODY_PCT
    return bool(cond1 and cond2 and cond3), "수박 돌파"

def check_ross(curr, past):
    if past.empty or past['BB_LOW'].isna().all(): return False, ""
    if pd.isna(curr.get('BB_LOW')): return False, ""
    bb_low = past['BB_LOW']
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any(): return False, ""
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx+1:]
    if after_first.empty: return False, ""
    rebound = (after_first['Close'] > after_first['BB_LOW']).any()
    near_band = curr['Low'] <= curr['BB_LOW'] * ROSS_BB_MARGIN
    close_above = curr['Close'] > curr['BB_LOW']
    return bool(rebound and near_band and close_above), "로스 쌍바닥"

def check_divergence(curr, past):
    if past['RSI'].isna().all() or pd.isna(curr.get('RSI')): return False, ""
    price_low = past['Low'].min()
    rsi_low = past['RSI'].min()
    cond1 = curr['Low'] <= price_low * RSI_MARGIN
    cond2 = curr['RSI'] > rsi_low
    return bool(cond1 and cond2), "RSI 다이버전스"

# ────────────── 단일 종목 분석 ──────────────
def analyze_stock(name, code):
    try:
        df = yf.download(f"{code}.KS", period="200d", interval="1d", progress=False)
        df = flatten_df(df)
        if df.empty or len(df)<60:
            df = yf.download(f"{code}.KQ", period="200d", interval="1d", progress=False)
            df = flatten_df(df)
        if len(df)<60: return None

        bb40 = ta.bbands(df['Close'], length=40, std=2)
        bb20 = ta.bbands(df['Close'], length=20, std=2)
        df['BB_UP'] = bb40['BBU_40_2.0']
        df['BB_LOW'] = bb20['BBL_20_2.0']
        df['RSI'] = ta.rsi(df['Close'],14)
        df.dropna(subset=['BB_UP','BB_LOW','RSI'], inplace=True)
        if len(df)<22: return None

        curr = df.iloc[-1]
        past = df.iloc[-21:-1]

        wm_flag, wm_name = check_watermelon(curr,past)
        ross_flag, ross_name = check_ross(curr,past)
        div_flag, div_name = check_divergence(curr,past)
        pattern_score = (50 if wm_flag else 0) + (30 if ross_flag else 0) + (20 if div_flag else 0)
        patterns = []
        if wm_flag: patterns.append(wm_name)
        if ross_flag: patterns.append(ross_name)
        if div_flag: patterns.append(div_name)

        news_score, news_comment = get_news_score_and_comment(code)

        final_score = pattern_score * 0.5 + news_score * 0.5
        grade = 'S' if final_score>=80 else 'A' if final_score>=60 else 'B' if final_score>=40 else 'C'

        return {
            "종목명": name,
            "코드": code,
            "패턴점수": pattern_score,
            "패턴종류": ",".join(patterns) if patterns else "없음",
            "뉴스점수": news_score,
            "AI코멘트": news_comment,
            "최종점수": final_score,
            "등급": grade
        }
    except Exception as e:
        log(f"종목 분석 실패 {code}: {e}")
        return None

# ────────────── 전체 종목 스캔 ──────────────
def scan_market(max_workers=10):
    log("📊 한국 전체 종목 스캔 시작")
    today = datetime.today().strftime("%Y%m%d")
    kospi = stock.get_market_ticker_list(today, market="KOSPI")
    kosdaq = stock.get_market_ticker_list(today, market="KOSDAQ")
    tickers = [(stock.get_market_ticker_name(c), c) for c in kospi+kosdaq]
    log(f"총 {len(tickers)}개 종목 / 병렬 {max_workers}스레드")

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
            if done % 50 == 0 or done == len(futures):
                elapsed = time.time() - start_ts
                log(f"  진행 {done}/{len(futures)} | 후보 {len(results)}개 | 경과 {elapsed:.0f}s")

    if not results:
        log("\n조건 만족 종목 없음")
        return

    df_result = pd.DataFrame(results).sort_values("최종점수",ascending=False).reset_index(drop=True)
    df_result.index += 1
    print("\n🔥 오늘의 후보 TOP 20\n")
    print(df_result.head(20).to_string())
    df_result.to_csv("stock_candidates.csv", index=False, encoding="utf-8-sig")
    log("\n✅ CSV 저장 완료 → stock_candidates.csv")
    log(f"⏱ 총 소요시간: {time.time()-start_ts:.0f}s")

# ────────────── 실행 ──────────────
if __name__=="__main__":
    scan_market(max_workers=10)