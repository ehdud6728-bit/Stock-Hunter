# theme_news_sentiment.py
import os
import requests
from bs4 import BeautifulSoup
from openai import OpenAI
import json

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")


# ─────────────────────────────
# 1️⃣ 테마별 검색어 정의
# ─────────────────────────────
THEME_KEYWORDS = {
    "전쟁": "war OR military conflict OR missile OR attack",
    "원유": "oil price OR crude oil OR OPEC",
    "반도체": "semiconductor OR chip industry OR NVIDIA",
    "AI": "artificial intelligence OR AI industry",
    "금리": "interest rate OR Federal Reserve OR inflation",
    "환율": "currency OR dollar OR exchange rate",
}


# ─────────────────────────────
# 2️⃣ 뉴스 수집
# ─────────────────────────────
def get_theme_news(theme, n=10):
    if theme not in THEME_KEYWORDS:
        raise ValueError(f"정의되지 않은 테마: {theme}")

    query = THEME_KEYWORDS[theme]
    url = f"https://news.google.com/rss/search?q={query}+when:7d&hl=en-US&gl=US&ceid=US:en"

    res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(res.content, "xml")
    items = soup.find_all("item")

    headlines = [item.title.text for item in items[:n]]
    return headlines


# ─────────────────────────────
# 3️⃣ 테마 감성 분석
# ─────────────────────────────
def get_theme_sentiment(theme):
    headlines = get_theme_news(theme)

    if not headlines:
        return 50, "최근 뉴스 없음, 중립"

    prompt = f"""
    다음은 "{theme}" 테마 관련 최근 뉴스 제목들이다.

    1. 시장 관점에서 리스크인지 호재인지 평가
    2. 점수: 0(매우 부정) ~ 100(매우 긍정)
    3. 반드시 과장 표현 금지 (전쟁 → 전쟁급 리스크 등으로 완화 표현)
    4. 한 줄 요약 코멘트 작성

    뉴스 목록:
    {headlines}

    JSON으로만 출력:
    {{
      "score": 0~100,
      "comment": "한줄 코멘트"
    }}
    """

    client = OpenAI(api_key=OPENAI_API_KEY)
    res = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a financial market analyst."},
            {"role": "user", "content": prompt}
        ]
    )

    try:
        text = res.choices[0].message.content.strip()
        data = json.loads(text)
        return int(data["score"]), data["comment"]
    except Exception:
        return 50, "뉴스 해석 실패 (중립)"


# ─────────────────────────────
# 4️⃣ 테스트
# ─────────────────────────────
if __name__ == "__main__":
    for theme in ["전쟁", "원유", "반도체"]:
        score, comment = get_theme_sentiment(theme)
        print(f"\n📌 테마: {theme}")
        print("점수:", score)
        print("코멘트:", comment)
