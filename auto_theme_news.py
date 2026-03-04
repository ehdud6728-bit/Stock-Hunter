import requests
from bs4 import BeautifulSoup
from openai import OpenAI
import json
import os

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")


# ─────────────────────────────
# 1️⃣ 최신 글로벌 증시 뉴스 수집
# ─────────────────────────────
def get_market_news(n=20):
    url = "https://news.google.com/rss/search?q=stock+market+global+economy+geopolitical+inflation&hl=en-US&gl=US&ceid=US:en"
    res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(res.content, "xml")
    items = soup.find_all("item")
    return [item.title.text for item in items[:n]]


# ─────────────────────────────
# 2️⃣ AI가 이슈 자동 추출 + 점수화
# ─────────────────────────────
def analyze_market_issues():
    headlines = get_market_news()

    prompt = f"""
    다음은 최근 글로벌 증시 관련 뉴스 제목들이다.

    1. 공통적으로 나타나는 "시장 이슈" 3~5개를 추출
    2. 각 이슈별로:
       - 이름
       - 시장 영향 점수 (0 매우 부정 ~ 100 매우 긍정)
       - 투자자 관점 한줄 코멘트
    3. 과장 금지:
       - "전쟁" → "전쟁급 지정학 리스크"
       - "붕괴" → "시장 충격 우려"
    4. 실제 투자 판단용으로 작성

    뉴스 목록:
    {headlines}

    반드시 JSON 배열로 출력:
    [
      {{
        "issue": "이슈명",
        "score": 0~100,
        "comment": "한줄 코멘트"
      }}
    ]
    """

    client = OpenAI(api_key=OPENAI_API_KEY)
    res = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a professional market analyst."},
            {"role": "user", "content": prompt}
        ]
    )

    try:
        text = res.choices[0].message.content.strip()
        issues = json.loads(text)
        print("\n" + "🌍 " * 5 + "[ 뉴스정보 ]" + " 🌍" * 5)
        print(f"💵 {issues}") 
        return issues
    except Exception as e:
        return [{"issue": "분석 실패", "score": 50, "comment": "이슈 분석 실패"}]


# ─────────────────────────────
# 3️⃣ 실행 테스트
# ─────────────────────────────
if __name__ == "__main__":
    issues = analyze_market_issues()
    for i in issues:
        print(f"\n📌 이슈: {i['issue']}")
        print("점수:", i['score'])
        print("코멘트:", i['comment'])
