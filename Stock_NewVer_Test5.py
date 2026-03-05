# ─────────────────────────────────────────────
# Stock Hunter 끝판왕 스캔 코드 (Python 3.10 호환)
# ─────────────────────────────────────────────
import sys
import warnings
import subprocess
import pandas as pd
import numpy as np
import yfinance as yf
from pykrx import stock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
import json
import os
from openai import OpenAI

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 환경 변수
# ─────────────────────────────────────────────
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# ─────────────────────────────────────────────
# 전역 변수 / 설정 (여기서 수정 가능)
# ─────────────────────────────────────────────
WATERMELON_VOLUME_MULTIPLIER = 1.5      # 거래량 기준
WATERMELON_BODY_PCT = 0.05            # 캔들 몸통 기준

ROSS_BAND_MARGIN = 0.05                # ±5% 허용
RSI_MARGIN = 0.05                       # ±5% 허용

TOP_N = 20                             # 결과 TOP N
MAX_WORKERS = 20                        # 병렬 스레드 수
NEWS_COUNT = 10                          # 뉴스 개수

# ─────────────────────────────────────────────
# 유틸 함수
# ─────────────────────────────────────────────
def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

def get_ticker(code: str) -> str:
    """KS 먼저 시도 → 없으면 KQ"""
    test = yf.download(f"{code}.KS", period="5d", interval="1d", progress=False)
    if not test.empty:
        return f"{code}.KS"
    return f"{code}.KQ"

# ─────────────────────────────────────────────
# 지표 계산 (ta 대체)
# ─────────────────────────────────────────────
def bbands(close: pd.Series, length=20, std=2):
    ma = close.rolling(length).mean()
    sd = close.rolling(length).std()
    upper = ma + std * sd
    lower = ma - std * sd
    return upper, lower

def rsi_calc(close: pd.Series, length=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(length).mean()
    avg_loss = loss.rolling(length).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# ─────────────────────────────────────────────
# 뉴스 & AI 분석
# ─────────────────────────────────────────────
def get_news_headlines(ticker: str, n=NEWS_COUNT):
    url = f"https://news.google.com/rss/search?q={ticker}+when:7d&hl=en-US&gl=US&ceid=US:en"
    res = requests.get(url, headers={'User-Agent':'Mozilla/5.0'})
    soup = BeautifulSoup(res.content, "xml")
    items = soup.find_all("item")
    return [item.title.text for item in items[:n]]

def get_news_sentiment(ticker: str):
    headlines = get_news_headlines(ticker)
    if not headlines:
        return 50, "최근 뉴스 없음, 중립"

    prompt = f"""
    아래 {ticker} 관련 최신 뉴스 {len(headlines)}개를 분석해줘.
    - 부정적 이슈는 점수 낮게, 긍정적 뉴스는 점수 높게
    - 점수 0~100
    - 한줄 코멘트 작성
    뉴스: {headlines}
    JSON으로 {{ "score":0~100, "comment":"..." }} 형태 출력
    """
    client = OpenAI(api_key=OPENAI_API_KEY)
    res = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role":"system","content":"You are a financial analyst."},
            {"role":"user","content":prompt}
        ]
    )
    try:
        text = res.choices[0].message.content
        data = json.loads(text)
        score = int(data.get("score",50))
        comment = data.get("comment","코멘트 없음")
    except Exception:
        score, comment = 50, "분석 실패, 중립"
    return score, comment

# ─────────────────────────────────────────────
# 패턴 분석
# ─────────────────────────────────────────────
def check_watermelon(curr: pd.Series, past: pd.DataFrame) -> tuple[int,str]:
    cond1 = curr['Close'] > curr['BB_UP_40']
    cond2 = curr['Volume'] > past['Volume'].mean() * WATERMELON_VOLUME_MULTIPLIER
    body = (curr['Close'] - curr['Open'])/curr['Open']
    cond3 = body > WATERMELON_BODY_PCT
    if cond1 and cond2 and cond3:
        return 50, "수박 BB40 상단 돌파"
    return 0, "수박 신호 없음"

def check_ross(curr: pd.Series, past: pd.DataFrame) -> tuple[int,str]:
    if past.empty or past['BB_LOW_20'].isna().all() or pd.isna(curr['BB_LOW_20']):
        return 0, "로스 데이터 부족"
    bb_low = past['BB_LOW_20']
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any():
        return 0, "1차 저점 없음"
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx+1:]
    rebound = (after_first['Close'] > after_first['BB_LOW_20']).any()
    near_band = curr['Low'] <= curr['BB_LOW_20']*(1+ROSS_BAND_MARGIN)
    close_above = curr['Close'] > curr['BB_LOW_20']
    if rebound and near_band and close_above:
        return 30, "로스 쌍바닥 안착"
    return 0, "로스 조건 미충족"

def check_divergence(curr: pd.Series, past: pd.DataFrame) -> tuple[int,str]:
    if pd.isna(curr['RSI']) or past['RSI'].isna().all():
        return 0, "RSI 데이터 부족"
    price_low = past['Low'].min()
    rsi_low = past['RSI'].min()
    cond1 = curr['Low'] <= price_low*(1+RSI_MARGIN)
    cond2 = curr['RSI'] > rsi_low
    if cond1 and cond2:
        return 20, "RSI 강세 다이버전스"
    return 0, "RSI 조건 미충족"

# ─────────────────────────────────────────────
# 단일 종목 분석
# ─────────────────────────────────────────────
def analyze_stock(name: str, code: str) -> dict|None:
    try:
        ticker = get_ticker(code)
        df = yf.download(ticker, period="200d", interval="1d", progress=False)
        df = flatten_df(df)
        if len(df)<50:
            return None

        # 지표 계산
        df['BB_UP_40'], _ = bbands(df['Close'], length=40,std=2)
        _, df['BB_LOW_20'] = bbands(df['Close'], length=20,std=2)
        df['RSI'] = rsi_calc(df['Close'], length=14)
        df.dropna(subset=['BB_UP_40','BB_LOW_20','RSI'], inplace=True)
        curr = df.iloc[-1]
        past = df.iloc[-21:-1]

        # 패턴 점수
        wm_score, wm_detail = check_watermelon(curr,past)
        ross_score, ross_detail = check_ross(curr,past)
        div_score, div_detail = check_divergence(curr,past)

        pattern_score = wm_score + ross_score + div_score
        pattern_detail = ", ".join([d for d in [wm_detail, ross_detail, div_detail]])

        # 뉴스 점수
        news_score, news_comment = get_news_sentiment(code)

        # 최종 점수
        final_score = pattern_score + news_score
        grade = 'S' if final_score>=80 else 'A' if final_score>=50 else 'B' if final_score>=30 else 'C'

        return {
            "종목명": name,
            "코드": code,
            "패턴점수": pattern_score,
            "패턴종류": pattern_detail,
            "뉴스점수": news_score,
            "AI코멘트": news_comment,
            "최종점수": final_score,
            "등급": grade,
            "현재가": f"{curr['Close']:.0f}원"
        }
    except:
        return None

# ─────────────────────────────────────────────
# 전체 시장 스캔
# ─────────────────────────────────────────────
def scan_market():
    print("📊 Scanning all KOSPI & KOSDAQ stocks...")
    today = datetime.today().strftime("%Y%m%d")
    kospi = stock.get_market_ticker_list(today,"KOSPI")
    kosdaq = stock.get_market_ticker_list(today,"KOSDAQ")
    tickers = [(stock.get_market_ticker_name(c), c) for c in kospi+kosdaq]

    results=[]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_stock,name,code): code for name,code in tickers}
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    if not results:
        print("No candidates found.")
        return

    df_result = pd.DataFrame(results)
    df_result = df_result.sort_values("최종점수",ascending=False).reset_index(drop=True)
    df_result.index += 1
    print(df_result.head(TOP_N).to_string())

    df_result.to_csv("final_candidates.csv",index=False,encoding="utf-8-sig")
    print(f"✅ CSV 저장 완료 → final_candidates.csv")

# ─────────────────────────────────────────────
# 실행
# ─────────────────────────────────────────────
if __name__=="__main__":
    scan_market()