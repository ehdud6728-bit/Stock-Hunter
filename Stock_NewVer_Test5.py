# ─────────────────────────────────────────────
# 끝판왕 종목 분석기 (한국 전체)
# ─────────────────────────────────────────────
import sys
import subprocess
import warnings
warnings.filterwarnings("ignore")

# ── 라이브러리 설치 ──
def install_libs():
    for lib in ['yfinance', 'pandas_ta', 'pykrx', 'openai', 'requests', 'beautifulsoup4']:
        try:
            __import__(lib.replace('-', '_'))
        except ImportError:
            print(f"🚀 설치 중: {lib}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

install_libs()

# ── 라이브러리 임포트 ──
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_ta as ta
from pykrx import stock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os, requests, json
from bs4 import BeautifulSoup
from openai import OpenAI

# ── 환경 변수 (OpenAI API Key) ──
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

# ─────────────────────────────────────────────
# 유틸리티
# ─────────────────────────────────────────────
def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

def get_ticker(code: str) -> str:
    """KS 먼저, 없으면 KQ"""
    for suffix in ['.KS', '.KQ']:
        ticker = f"{code}{suffix}"
        df = yf.download(ticker, period="5d", interval="1d", progress=False)
        if not df.empty:
            return ticker
    return f"{code}.KS"

# ─────────────────────────────────────────────
# 뉴스 점수/코멘트
# ─────────────────────────────────────────────
def get_news_score(stock_code, n=5):
    url = f"https://news.google.com/rss/search?q={stock_code}+when:7d&hl=ko&gl=KR&ceid=KR:ko"
    res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(res.content, "xml")
    items = soup.find_all("item")
    headlines = [item.title.text for item in items[:n]]
    if not headlines:
        return 50, "뉴스 없음, 중립"
    prompt = f"""
    {stock_code} 관련 최신 뉴스 {len(headlines)}개를 분석
    - 0~100점, 0 매우 부정, 100 매우 긍정
    - 한줄 코멘트도 작성
    뉴스: {headlines}
    결과 JSON: {{"score":0~100, "comment":"..."}} 출력
    """
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
        return int(data.get("score",50)), data.get("comment","코멘트 없음")
    except Exception:
        return 50, "분석 실패, 중립"

# ─────────────────────────────────────────────
# 패턴 로직
# ─────────────────────────────────────────────
def check_watermelon(curr, past):
    if pd.isna(curr.get('BB_UP')):
        return False
    cond1 = curr['Close'] > curr['BB_UP']
    cond2 = curr['Volume'] > past['Volume'].mean()*2
    body = (curr['Close'] - curr['Open'])/curr['Open']
    cond3 = body > 0.05
    return bool(cond1 and cond2 and cond3)

def check_ross(curr, past):
    if past.empty or past['BB_LOW'].isna().all() or pd.isna(curr.get('BB_LOW')):
        return False
    bb_low = past['BB_LOW']
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any():
        return False
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx+1:]
    if after_first.empty or not (after_first['Close'] > after_first['BB_LOW']).any():
        return False
    near_band = curr['Low'] <= curr['BB_LOW']*1.03
    close_above = curr['Close'] > curr['BB_LOW']
    return bool(near_band and close_above)

def check_divergence(curr, past):
    if pd.isna(curr.get('RSI')) or past['RSI'].isna().all():
        return False
    price_low = past['Low'].min()
    rsi_low = past['RSI'].min()
    cond1 = curr['Low'] <= price_low*1.03
    cond2 = curr['RSI'] > rsi_low
    return bool(cond1 and cond2)

# ─────────────────────────────────────────────
# 세력 매집 점수
# ─────────────────────────────────────────────
def smart_money_score(df):
    score = 0
    vol_ratio = df["Volume"].iloc[-5:].mean()/df["Volume"].iloc[-60:].mean()
    if vol_ratio > 1.5: score +=30
    up_vol = df[df["Close"]>df["Open"]]["Volume"].tail(20).mean()
    down_vol = df[df["Close"]<df["Open"]]["Volume"].tail(20).mean()
    if up_vol > down_vol*1.2: score+=30
    volatility = (df["High"]-df["Low"])/df["Close"]
    if volatility.tail(10).mean() < volatility.tail(60).mean(): score+=20
    if df["Close"].iloc[-1] > df["Close"].rolling(60).mean().iloc[-1]: score+=20
    return min(score,100)

# ─────────────────────────────────────────────
# 단일 종목 분석
# ─────────────────────────────────────────────
def analyze_stock(name, code):
    try:
        df = yf.download(f"{code}.KS", period="200d", interval="1d", progress=False)
        if df.empty: df = yf.download(f"{code}.KQ", period="200d", interval="1d", progress=False)
        df = flatten_df(df)
        if len(df)<60: return None
        close = df['Close']
        bb40 = ta.bbands(close,length=40,std=2)
        bb20 = ta.bbands(close,length=20,std=2)
        df['BB_UP']=bb40['BBU_40_2.0']
        df['BB_LOW']=bb20['BBL_20_2.0']
        df['RSI']=ta.rsi(close,length=14)
        df.dropna(subset=['BB_UP','BB_LOW','RSI'],inplace=True)
        if len(df)<22: return None
        curr = df.iloc[-1]; past = df.iloc[-21:-1]
        wm = check_watermelon(curr,past)
        ross = check_ross(curr,past)
        div = check_divergence(curr,past)
        pattern_score = (50 if wm else 0)+(30 if ross else 0)+(20 if div else 0)
        smart = smart_money_score(df)
        news_score, news_comment = get_news_score(code)
        final_score = pattern_score*0.3 + smart*0.3 + news_score*0.2 + 0 # breakouts 제외
        grade = 'S' if final_score>=80 else 'A' if final_score>=50 else 'B'
        return {
            "종목명":name,
            "코드":code,
            "패턴점수":pattern_score,
            "세력매집":smart,
            "뉴스점수":news_score,
            "최종점수":round(final_score,1),
            "등급":grade,
            "패턴": f"수박:{'✅' if wm else '❌'} 로스:{'✅' if ross else '❌'} RSI:{'✅' if div else '❌'}",
            "뉴스코멘트":news_comment,
            "현재가":f"{curr['Close']:.0f}원"
        }
    except Exception:
        return None

# ─────────────────────────────────────────────
# 전체 시장 스캔
# ─────────────────────────────────────────────
def scan_market(max_workers=20):
    print("📊 한국 전체 종목 스캔 시작")
    today=datetime.today().strftime("%Y%m%d")
    kospi = stock.get_market_ticker_list(today,"KOSPI")
    kosdaq = stock.get_market_ticker_list(today,"KOSDAQ")
    tickers = [(stock.get_market_ticker_name(c),c) for c in kospi+kosdaq]
    total=len(tickers); results=[]; done=0; start_ts=time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures={executor.submit(analyze_stock,name,code):code for name,code in tickers}
        for future in as_completed(futures):
            done+=1
            r=future.result()
            if r: results.append(r)
            if done%50==0 or done==total:
                elapsed=time.time()-start_ts
                eta=(elapsed/done)*(total-done)
                print(f"진행 {done}/{total} | 후보 {len(results)} | 경과 {elapsed:.0f}s | 남은 ~{eta:.0f}s")
    if not results:
        print("조건 만족 종목 없음")
        return
    df_result=pd.DataFrame(results).sort_values("최종점수",ascending=False).reset_index(drop=True)
    df_result.index+=1
    print(f"\n🔥 오늘의 TOP {min(20,len(df_result))} 종목\n")
    print(df_result.head(20).to_string())
    df_result.to_csv("final_candidates.csv",index=False,encoding="utf-8-sig")
    print("\n✅ CSV 저장 완료 → final_candidates.csv")
    print(f"⏱️ 총 소요시간 {time.time()-start_ts:.0f}초")

# ─────────────────────────────────────────────
# 실행
# ─────────────────────────────────────────────
if __name__=="__main__":
    scan_market(max_workers=20)