# news_sentiment.py
from pygooglenews import GoogleNews
import openai
import json

def get_news_sentiment(ticker):
    """
    ticker: 종목 코드나 이름
    return: (score:int, comment:str)
    """
    sentiment_score = 50
    sentiment_comment = "뉴스 없음 - 중립"
    
    try:
        gn = GoogleNews(lang='en')
        search = gn.search(f'{ticker} when:7d')
        entries = search.get('entries', [])
        if entries:
            headlines = [entry.title for entry in entries[:10]]
            prompt = f"""
            다음 {ticker} 뉴스 10개를 분석하세요.
            1. 뉴스가 긍정적이면 점수 높게, 부정적이면 낮게, 0~100 숫자로 점수화 (50은 중립)
            2. 핵심 리스크/긍정 포인트를 한 줄로 요약
            헤드라인:
            {headlines}
            JSON 형식으로 응답:
            {{
                "score": 숫자,
                "comment": "한 줄 코멘트"
            }}
            """
            response = openai.ChatCompletion.create(
                model="gpt-5-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            content = response.choices[0].message['content']
            data = json.loads(content)
            sentiment_score = int(data.get('score', 50))
            sentiment_comment = data.get('comment', "코멘트 없음")
    except Exception as e:
        print(f"⚠️ 뉴스 점수 분석 실패 ({ticker}): {e}")
    
    return sentiment_score, sentiment_comment
