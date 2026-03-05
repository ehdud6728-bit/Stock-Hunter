# ===========================================
# 📊 AI 종목 스캔 + 뉴스 분석 + 패턴종류 표시
# ===========================================

import sys
import subprocess
import warnings
warnings.filterwarnings("ignore")

def install_libs():
    for lib in ['yfinance','pykrx','requests','beautifulsoup4','openai']:
        try:
            __import__(lib.replace('-', '_'))
        except ImportError:
            subprocess.check_call([sys.executable,"-m","pip","install",lib])

install_libs()

import pandas as pd
import numpy as np
import yfinance as yf
from pykrx import stock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
import time
import os
import json
from openai import OpenAI

# ───────────────────────────────────────────
# OPENAI API SETUP
# ───────────────────────────────────────────
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise Exception("❌ OPENAI_API_KEY 환경 변수 미설정")

client = OpenAI(api_key=OPENAI_API_KEY)

# ───────────────────────────────────────────
# UTILS
# ───────────────────────────────────────────
def flatten_df(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

# ───────────────────────────────────────────
# INDICATORS
# ───────────────────────────────────────────
def add_indicators(df):
    df['MA40'] = df['Close'].rolling(40).mean()
    df['STD40']= df['Close'].rolling(40).std()
    df['BB_UP']= df['MA40'] + 2*df['STD40']

    df['MA20'] = df['Close'].rolling(20).mean()
    df['STD20']= df['Close'].rolling(20).std()
    df['BB_LOW']= df['MA20'] - 2*df['STD20']

    delta = df['Close'].diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain/avg_loss
    df['RSI'] = 100 - 100/(1+rs)

    df.dropna(subset=['BB_UP','BB_LOW','RSI'], inplace=True)
    return df

# ───────────────────────────────────────────
# PATTERN CHECKS
# ───────────────────────────────────────────
def check_watermelon(curr, past):
    cond1 = curr['Close'] > curr['BB_UP']
    cond2 = curr['Volume'] > past['Volume'].mean()*2
    body  = (curr['Close']-curr['Open'])/curr['Open']
    cond3 = body > 0.05
    return bool(cond1 and cond2 and cond3)

def check_ross(curr, past):
    if past['BB_LOW'].isna().all(): return False
    outside = past['Low'] < past['BB_LOW']
    if not outside.any(): return False
    idx = outside.values.argmax()
    after = past.iloc[idx+1:]
    if after.empty: return False
    rebound = (after['Close']>after['BB_LOW']).any()
    near  = curr['Low'] <= past['BB_LOW'].iloc[-1]*1.03
    above = curr['Close'] > past['BB_LOW'].iloc[-1]
    return bool(rebound and near and above)

def check_divergence(curr, past):
    price_low = past['Low'].min()
    rsi_low   = past['RSI'].min()
    cond1 = curr['Low'] <= price_low*1.03
    cond2 = curr['RSI'] > rsi_low
    return bool(cond1 and cond2)

# ───────────────────────────────────────────
# SMART MONEY SCORE
# ───────────────────────────────────────────
def smart_money_score(df):
    score = 0
    vol_ratio = df['Volume'].iloc[-5:].mean()/df['Volume'].iloc[-60:].mean()
    if vol_ratio>1.5: score+=30
    up_vol   = df[df['Close']>df['Open']]['Volume'].tail(20).mean()
    down_vol = df[df['Close']<df['Open']]['Volume'].tail(20).mean()
    if up_vol > down_vol*1.2: score+=30
    volatility = (df['High']-df['Low'])/df['Close']
    if volatility.tail(10).mean() < volatility.tail(60).mean(): score+=20
    if df['Close'].iloc[-1] > df['Close'].rolling(60).mean().iloc[-1]: score+=20
    return min(score,100)

# ───────────────────────────────────────────
# NEWS SCORE
# ───────────────────────────────────────────
def get_news_score(code, n=5):
    try:
        url = f"https://news.google.com/rss/search?q={code}+when:7d&hl=en-US&gl=US&ceid=US:en"
        res = requests.get(url, headers={'User-Agent':'Mozilla/5.0'})
        soup = BeautifulSoup(res.content,"xml")
        items = soup.find_all("item")
        headlines = [item.title.text for item in items[:n]]
        if not headlines:
            return 50, "No recent news"

        prompt = f"""
        Analyze these {len(headlines)} news items for stock {code}.
        Score sentiment 0-100 and give one-line comment.
        Headlines: {headlines}
        Return JSON: {{"score":xx, "comment":"..."}} 
        """

        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role":"system","content":"You are a stock sentiment analyst."},
                {"role":"user","content":prompt}
            ]
        )
        text = res.choices[0].message.content
        data = json.loads(text)
        return int(data.get("score",50)), data.get("comment","")
    except:
        return 50, "News parse failed"

# ───────────────────────────────────────────
# SINGLE STOCK ANALYSIS
# ───────────────────────────────────────────
def analyze_stock(name, code):
    try:
        df = yf.download(f"{code}.KS", period="200d", interval="1d", progress=False)
        df = flatten_df(df)
        if df.empty or len(df)<60:
            df = yf.download(f"{code}.KQ", period="200d", interval="1d", progress=False)
            df = flatten_df(df)
        if len(df)<60: return None

        df = add_indicators(df)
        curr = df.iloc[-1]; past = df.iloc[-21:-1]

        wm   = check_watermelon(curr,past)
        ross = check_ross(curr,past)
        div  = check_divergence(curr,past)

        # 패턴종류
        patterns = []
        if wm: patterns.append("수박")
        if ross: patterns.append("로스")
        if div: patterns.append("RSI")

        pattern_score = (50 if wm else 0) + (30 if ross else 0) + (20 if div else 0)
        smart_score   = smart_money_score(df)
        news_score, news_comment = get_news_score(code)
        final_score = round(pattern_score*0.3 + smart_score*0.3 + news_score*0.4,1)
        grade = 'S' if final_score>=80 else 'A' if final_score>=50 else 'B'

        return {
            "종목명": name,
            "코드": code,
            "패턴종류": ",".join(patterns) if patterns else "없음",
            "패턴점수": pattern_score,
            "세력매집": smart_score,
            "뉴스점수": news_score,
            "AI코멘트": news_comment,
            "최종점수": final_score,
            "등급": grade
        }
    except:
        return None

# ───────────────────────────────────────────
# MARKET SCAN
# ───────────────────────────────────────────
def scan_market(max_workers=20):
    print("📊 Scanning all KOSPI & KOSDAQ stocks...")
    today = datetime.today().strftime("%Y%m%d")
    kospi = stock.get_market_ticker_list(today, market="KOSPI")
    kosdaq = stock.get_market_ticker_list(today, market="KOSDAQ")
    tickers = [(stock.get_market_ticker_name(c),c) for c in kospi+kosdaq]

    results = []
    start_ts = time.time()
    done=0

    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = [exe.submit(analyze_stock,name,code) for name,code in tickers]
        for f in as_completed(futures):
            done+=1
            r = f.result()
            if r: results.append(r)
            if done % 100 == 0 or done==len(tickers):
                print(f"Progress {done}/{len(tickers)}")

    if not results:
        print("No candidates found.")
        return

    df = pd.DataFrame(results).sort_values("최종점수", ascending=False)
    df.index += 1
    print("\n🔥 TOP Stock Candidates\n")
    print(df.head(20).to_string())
    df.to_csv("ai_stock_full_results_patterns.csv", index=False, encoding="utf-8-sig")
    print("\n✅ CSV saved -> ai_stock_full_results_patterns.csv")
    print(f"⏱ Total time: {round(time.time()-start_ts,1)}s")

# ───────────────────────────────────────────
# RUN
# ───────────────────────────────────────────
if __name__ == "__main__":
    scan_market(max_workers=20)