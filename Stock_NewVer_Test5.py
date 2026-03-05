# ─────────────────────────────────────────────
# Stock Hunter - 끝판왕 통합 버전
# ─────────────────────────────────────────────

import os
import sys
import subprocess
import warnings
warnings.filterwarnings("ignore")

def install_libs():
    for lib in ['yfinance', 'pandas', 'numpy', 'pandas_ta', 'pykrx', 'requests', 'beautifulsoup4', 'openai']:
        try:
            __import__(lib.replace('-', '_'))
        except ImportError:
            print(f"🚀 설치 중: {lib}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

install_libs()

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

# ─────────────────────────────────────────────
# 환경 변수 / 설정
# ─────────────────────────────────────────────
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")  # 필수

# 패턴 조건
ROSS_ALLOW_PCT = 0.07
RSI_ALLOW_PCT = 0.07
WATERMELON_VOLUME_MULT = 2
WATERMELON_BODY_PCT = 0.05

# 점수 배분
SCORE_WATERMELON = 50
SCORE_ROSS       = 30
SCORE_RSI        = 20
SCORE_NEWS       = 20

# ─────────────────────────────────────────────
# 유틸 함수
# ─────────────────────────────────────────────
def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

def get_news_headlines(ticker, n=5):
    url = f"https://news.google.com/rss/search?q={ticker}+when:7d&hl=en-US&gl=US&ceid=US:en"
    res = requests.get(url, headers={'User-Agent':'Mozilla/5.0'})
    soup = BeautifulSoup(res.content, "xml")
    items = soup.find_all("item")
    headlines = [item.title.text for item in items[:n]]
    return headlines

def get_news_score(ticker):
    headlines = get_news_headlines(ticker)
    if not headlines:
        return 50, "최근 뉴스 없음, 중립"

    prompt = f"""
    아래 {ticker} 관련 최신 뉴스 {len(headlines)}개를 분석해줘.
    - 부정적 뉴스는 점수 낮게, 긍정적 뉴스는 높게
    - 점수 0~100
    - 한줄 코멘트 작성
    뉴스: {headlines}
    JSON으로 {{'score':0~100, 'comment':'...'}} 형태로 출력
    """
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
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
        return score, comment
    except Exception:
        return 50, "AI 분석 실패, 중립"

# ─────────────────────────────────────────────
# 패턴 분석 함수
# ─────────────────────────────────────────────
def check_watermelon(curr: pd.Series, past: pd.DataFrame) -> tuple[bool,str]:
    cond1 = curr['Close'] > curr['BB_UP_40']
    cond2 = curr['Volume'] > past['Volume'].mean()*WATERMELON_VOLUME_MULT
    body  = (curr['Close']-curr['Open'])/curr['Open']
    cond3 = body > WATERMELON_BODY_PCT
    return cond1 and cond2 and cond3, "수박돌파"

def check_ross(curr: pd.Series, past: pd.DataFrame) -> tuple[bool,str]:
    if past.empty or past['BB_LOW_20'].isna().all():
        return False, "로스쌍바닥"
    bb_low = past['BB_LOW_20']
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any():
        return False, "로스쌍바닥"
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx+1:]
    if (after_first['Close'] > after_first['BB_LOW_20']).any() and curr['Low'] <= curr['BB_LOW_20']*(1+ROSS_ALLOW_PCT) and curr['Close']>curr['BB_LOW_20']:
        return True, "로스쌍바닥"
    return False, "로스쌍바닥"

def check_rsi(curr: pd.Series, past: pd.DataFrame) -> tuple[bool,str]:
    if past['RSI'].isna().all() or pd.isna(curr['RSI']):
        return False, "RSI다이버전스"
    min_price_past = past['Low'].min()
    min_rsi_past   = past['RSI'].min()
    cond1 = curr['Low'] <= min_price_past*(1+RSI_ALLOW_PCT)
    cond2 = curr['RSI'] > min_rsi_past
    return cond1 and cond2, "RSI다이버전스"

# ─────────────────────────────────────────────
# 단일 종목 분석
# ─────────────────────────────────────────────
def analyze_stock(name:str, code:str) -> dict|None:
    try:
        df = yf.download(f"{code}.KS", period="200d", interval="1d", progress=False)
        df = flatten_df(df)
        if df.empty or len(df)<60:
            df = yf.download(f"{code}.KQ", period="200d", interval="1d", progress=False)
            df = flatten_df(df)
        if len(df)<60:
            return None
        df['BB_UP_40']  = ta.bbands(df['Close'], length=40,std=2)['BBU_40_2.0']
        df['BB_LOW_20'] = ta.bbands(df['Close'], length=20,std=2)['BBL_20_2.0']
        df['RSI']       = ta.rsi(df['Close'], length=14)
        df.dropna(subset=['BB_UP_40','BB_LOW_20','RSI'], inplace=True)
        if len(df)<22:
            return None
        curr = df.iloc[-1]
        past = df.iloc[-21:-1]

        wm, wm_name = check_watermelon(curr,past)
        ross, ross_name = check_ross(curr,past)
        rsi, rsi_name = check_rsi(curr,past)
        news_score, news_comment = get_news_score(name)

        pattern_score = (SCORE_WATERMELON if wm else 0) + (SCORE_ROSS if ross else 0) + (SCORE_RSI if rsi else 0)
        final_score   = pattern_score + news_score
        grade = 'S' if final_score>=80 else 'A' if final_score>=60 else 'B' if final_score>=40 else 'C'
        patterns = [p for flag,p in [(wm,wm_name),(ross,ross_name),(rsi,rsi_name)] if flag]

        return {
            "종목명": name,
            "코드": code,
            "패턴점수": pattern_score,
            "패턴종류": ",".join(patterns),
            "뉴스점수": news_score,
            "AI코멘트": news_comment,
            "최종점수": final_score,
            "등급": grade,
            "현재가": f"{curr['Close']:.0f}원"
        }
    except Exception:
        return None

# ─────────────────────────────────────────────
# 전체 종목 스캔
# ─────────────────────────────────────────────
def scan_market(max_workers:int=10):
    print("📊 Scanning all KOSPI & KOSDAQ stocks...")
    today = datetime.today().strftime("%Y%m%d")
    kospi  = stock.get_market_ticker_list(today,"KOSPI")
    kosdaq = stock.get_market_ticker_list(today,"KOSDAQ")
    tickers = [(stock.get_market_ticker_name(c),c) for c in kospi+kosdaq]
    results=[]
    start_ts=time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(analyze_stock,name,code): code for name,code in tickers}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)
    if not results:
        print("No candidates found.")
        return
    df_result = pd.DataFrame(results).sort_values("최종점수",ascending=False)
    df_result.index += 1
    print("\n🔥 TOP 후보\n")
    print(df_result.head(20).to_string())
    df_result.to_csv("stock_candidates.csv",index=False,encoding="utf-8-sig")
    print("\n✅ CSV 저장 완료 → stock_candidates.csv")
    print(f"⏱ 총 소요시간: {time.time()-start_ts:.0f}s")

# ─────────────────────────────────────────────
# 실행
# ─────────────────────────────────────────────
if __name__=="__main__":
    scan_market(max_workers=10)