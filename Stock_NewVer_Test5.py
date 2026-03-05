import os
import sys
import subprocess
import warnings
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf
import pandas_ta as ta
from openai import OpenAI

warnings.filterwarnings("ignore")

# ───────────────────────────────
# 전역변수: 조건값
# ───────────────────────────────
BB40_STD = 2
BB20_STD = 2
ROSS_BAND_TOLERANCE = 0.05   # ±5%
RSI_LOW_TOLERANCE = 0.05     # ±5%
WATERMELON_VOLUME_MULTIPLIER = 2
WATERMELON_BODY_RATIO = 0.05
NEWS_HEADLINE_COUNT = 10
MAX_WORKERS = 10

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# ───────────────────────────────
# 라이브러리 설치
# ───────────────────────────────
def install_libs():
    for lib in ['yfinance', 'pandas_ta', 'beautifulsoup4', 'openai']:
        try:
            __import__(lib.replace('-', '_'))
        except ImportError:
            print(f"🚀 설치 중: {lib}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

install_libs()

# ───────────────────────────────
# 유틸: MultiIndex 컬럼 정리
# ───────────────────────────────
def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

# ───────────────────────────────
# 뉴스 스크래핑 (네이버)
# ───────────────────────────────
def get_news_headlines_kor(ticker: str, n=NEWS_HEADLINE_COUNT):
    query = f"{ticker}"
    url = f"https://search.naver.com/search.naver?&where=news&query={query}"
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(res.text, "html.parser")
        items = soup.select("a.news_tit")
        headlines = [item.get_text().strip() for item in items[:n]]
        return headlines
    except Exception as e:
        print(f"❌ 뉴스 스크래핑 실패: {e}")
        return []

# ───────────────────────────────
# 뉴스 점수 + AI 코멘트
# ───────────────────────────────
def get_news_sentiment(ticker: str):
    headlines = get_news_headlines_kor(ticker)
    if not headlines:
        return 50, "최근 뉴스 없음, 중립"

    prompt = f"""
    아래 {ticker} 관련 최신 뉴스 {len(headlines)}개를 분석해주세요.
    - 부정적 뉴스는 점수를 낮게, 긍정적 뉴스는 점수를 높게
    - 점수 0~100
    - 한줄 코멘트 작성
    뉴스: {headlines}
    결과를 JSON으로 {"score":0~100, "comment":"..."} 형태로 출력
    """

    client = OpenAI(api_key=OPENAI_API_KEY)
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role":"system","content":"You are a financial analyst."},
                {"role":"user","content":prompt}
            ]
        )
        import json
        text = res.choices[0].message.content
        data = json.loads(text)
        score = int(data.get("score", 50))
        comment = data.get("comment", "코멘트 없음")
    except Exception:
        score = 50
        comment = "분석 실패, 중립"
    return score, comment

# ───────────────────────────────
# 패턴 로직
# ───────────────────────────────
def check_watermelon(curr: pd.Series, past: pd.DataFrame):
    if pd.isna(curr.get('BB_UP')):
        return False, None
    cond1 = curr['Close'] > curr['BB_UP']
    cond2 = curr['Volume'] > past['Volume'].mean() * WATERMELON_VOLUME_MULTIPLIER
    body = (curr['Close'] - curr['Open']) / curr['Open']
    cond3 = body > WATERMELON_BODY_RATIO
    return bool(cond1 and cond2 and cond3), "수박 BB40 상단 돌파" if cond1 and cond2 and cond3 else None

def check_ross(curr: pd.Series, past: pd.DataFrame):
    if past.empty or past['BB_LOW'].isna().all() or pd.isna(curr.get('BB_LOW')):
        return False, None
    bb_low = past['BB_LOW']
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any():
        return False, None
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx + 1:]
    if after_first.empty or not (after_first['Close'] > after_first['BB_LOW']).any():
        return False, None
    near_band = curr['Low'] <= curr['BB_LOW'] * (1 + ROSS_BAND_TOLERANCE)
    close_above = curr['Close'] > curr['BB_LOW']
    passed = bool(near_band and close_above)
    return passed, "로스 쌍바닥" if passed else None

def check_divergence(curr: pd.Series, past: pd.DataFrame):
    if pd.isna(curr.get('RSI')) or past['RSI'].isna().all():
        return False, None
    price_low = past['Low'].min()
    rsi_low = past['RSI'].min()
    cond1 = curr['Low'] <= price_low * (1 + RSI_LOW_TOLERANCE)
    cond2 = curr['RSI'] > rsi_low
    passed = cond1 and cond2
    return passed, "RSI 다이버전스" if passed else None

# ───────────────────────────────
# 종목 분석
# ───────────────────────────────
def analyze_stock(name: str, code: str) -> dict | None:
    try:
        df = yf.download(f"{code}.KS", period="200d", interval="1d", progress=False)
        df = flatten_df(df)
        if df.empty or len(df) < 60:
            df = yf.download(f"{code}.KQ", period="200d", interval="1d", progress=False)
            df = flatten_df(df)
        if len(df) < 60:
            return None
        close = df['Close'].squeeze()
        df['BB_UP'] = ta.bbands(close, length=40, std=BB40_STD)['BBU_40_2.0']
        df['BB_LOW'] = ta.bbands(close, length=20, std=BB20_STD)['BBL_20_2.0']
        df['RSI'] = ta.rsi(close, length=14)
        df.dropna(subset=['BB_UP','BB_LOW','RSI'], inplace=True)
        if len(df) < 22:
            return None
        curr = df.iloc[-1]
        past = df.iloc[-21:-1]

        # 패턴
        wm_pass, wm_name = check_watermelon(curr, past)
        ross_pass, ross_name = check_ross(curr, past)
        div_pass, div_name = check_divergence(curr, past)
        pattern_score = (50 if wm_pass else 0) + (30 if ross_pass else 0) + (20 if div_pass else 0)
        patterns = [p for p in [wm_name, ross_name, div_name] if p]

        # 뉴스
        news_score, news_comment = get_news_sentiment(name)

        # 세력매집 (거래량)
        volume_score = int(min(50, curr['Volume']/past['Volume'].mean()*10))  # 최대 50점

        final_score = pattern_score + news_score + volume_score
        grade = 'S' if final_score >= 150 else 'A' if final_score >= 100 else 'B' if final_score >= 70 else 'C'

        return {
            "종목명": name,
            "코드": code,
            "현재가": f"{curr['Close']:.0f}원",
            "패턴점수": pattern_score,
            "패턴종류": ", ".join(patterns),
            "뉴스점수": news_score,
            "AI 코멘트": news_comment,
            "세력매집": volume_score,
            "최종점수": final_score,
            "등급": grade
        }
    except Exception as e:
        print(f"❌ 분석 실패 {name}: {e}")
        return None

# ───────────────────────────────
# 시장 전체 스캔
# ───────────────────────────────
def scan_market(max_workers=MAX_WORKERS):
    from pykrx import stock
    today = datetime.today().strftime("%Y%m%d")
    kospi = stock.get_market_ticker_list(today, market="KOSPI")
    kosdaq = stock.get_market_ticker_list(today, market="KOSDAQ")
    tickers = [(stock.get_market_ticker_name(c), c) for c in kospi + kosdaq]
    total = len(tickers)
    results = []
    done = 0
    start_ts = datetime.now()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(analyze_stock, name, code): code for name, code in tickers}
        for future in as_completed(futures):
            done += 1
            r = future.result()
            if r:
                results.append(r)
            if done % 50 == 0 or done == total:
                print(f"진행 {done}/{total} | 후보 {len(results)}개 | 경과 {datetime.now()-start_ts}")

    if not results:
        print("조건 만족 종목 없음")
        return

    df_result = pd.DataFrame(results)
    df_result = df_result.sort_values("최종점수", ascending=False).reset_index(drop=True)
    df_result.index += 1
    print("\n🔥 오늘의 TOP 후보\n")
    print(df_result.head(20).to_string())
    df_result.to_csv("final_candidates.csv", index=False, encoding="utf-8-sig")
    print("\n✅ CSV 저장 완료 → final_candidates.csv")

# ───────────────────────────────
# 실행
# ───────────────────────────────
if __name__ == "__main__":
    scan_market()