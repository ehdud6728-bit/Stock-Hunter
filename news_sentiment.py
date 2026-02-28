# news_sentiment.py
import os
import requests
from bs4 import BeautifulSoup
from openai import OpenAI

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

def get_news_headlines(ticker, n=10):
    """
    Google News RSS에서 ticker 관련 최신 뉴스 n개 가져오기
    """
    url = f"https://news.google.com/rss/search?q={ticker}+when:7d&hl=en-US&gl=US&ceid=US:en"
    res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(res.content, "xml")
    items = soup.find_all("item")
    headlines = [item.title.text for item in items[:n]]
    return headlines

def get_news_sentiment(ticker):
    """
    뉴스 기반 점수(0~100)와 한줄 코멘트 반환
    """
    headlines = get_news_headlines(ticker)
    if not headlines:
        return 50, "최근 뉴스 없음, 중립"

    # ChatGPT 프롬프트
    prompt = f"""
    아래 {ticker} 관련 최신 뉴스 {len(headlines)}개를 분석해줘.
    - 치명적 부정적 이슈(공급망, 규제, 경영진 문제 등)는 점수 낮게
    - 긍정적 뉴스는 점수 높게
    - 점수는 0(매우 부정) ~ 100(매우 긍정)
    - 한줄 코멘트도 작성
    뉴스: {headlines}
    결과를 JSON으로 {"score":0~100, "comment":"..."} 형태로 출력
    """

    client = OpenAI(api_key=OPENAI_API_KEY)
    res = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role":"system","content":"You are a financial analyst."},
            {"role":"user","content":prompt}
        ]
    )

    # GPT 응답에서 JSON 파싱
    import json
    try:
        text = res.choices[0].message.content
        data = json.loads(text)
        score = int(data.get("score", 50))
        comment = data.get("comment", "코멘트 없음")
    except Exception:
        score = 50
        comment = "분석 실패, 중립"
    
    return score, comment
