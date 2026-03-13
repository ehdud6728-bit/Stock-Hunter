import re
import json
import time
import feedparser
import pandas as pd
from openai import OpenAI


NEWS_FEEDS = {
    "global_google": "https://news.google.com/rss/search?q=oil+OR+federal+reserve+OR+china+economy+OR+semiconductor+OR+shipping+OR+war&hl=en-US&gl=US&ceid=US:en",
    "yahoo_finance": "https://finance.yahoo.com/news/rssindex",
    "mk_headline": "https://www.mk.co.kr/rss/30000001/",
    "mk_economy": "https://www.mk.co.kr/rss/30100041/",
}


def _clean_title(title: str) -> str:
    title = re.sub(r"\s+", " ", str(title)).strip()
    title = title.replace(" - Yahoo Finance", "").strip()
    return title


def fetch_rss_titles(url: str, limit: int = 8):
    feed = feedparser.parse(url)
    items = []

    for entry in feed.entries[:limit]:
        title = _clean_title(getattr(entry, "title", ""))
        link = getattr(entry, "link", "")
        published = getattr(entry, "published", "")

        if not title:
            continue

        items.append({
            "title": title,
            "link": link,
            "published": published,
        })

    return items


def collect_market_news():
    result = {}
    for key, url in NEWS_FEEDS.items():
        try:
            result[key] = fetch_rss_titles(url, limit=8)
            time.sleep(0.2)
        except Exception:
            result[key] = []
    return result


def flatten_news_titles(news_map, max_items: int = 20):
    seen = set()
    merged = []

    for _, items in news_map.items():
        for item in items:
            title = item["title"]
            norm = title.lower().strip()
            if norm in seen:
                continue
            seen.add(norm)
            merged.append(title)

    return merged[:max_items]


def analyze_news_to_korea_theme(news_titles, openai_api_key: str):
    client = OpenAI(api_key=openai_api_key)

    news_text = "\n".join(f"- {x}" for x in news_titles[:20])

    system_prompt = """
당신은 한국 테마주/뉴스 모멘텀 트레이딩에 특화된 프로 트레이더다.
뉴스를 설명하지 말고, 반드시 한국 증시의 테마와 종목으로 연결하라.

규칙:
1. 거시 일반론 금지.
2. 반드시 한국 시장의 섹터와 종목까지 연결.
3. 대장주 / 후발주 / 연동주를 구분.
4. 과거 유사 이슈 때 자주 움직였던 종목군을 우선 제시.
5. 실전 체크포인트를 제시.
6. JSON으로만 답변.
"""

    user_prompt = f"""
다음 뉴스들을 바탕으로 오늘 한국 증시에서 실제로 움직일 가능성이 높은 테마와 종목을 분석해줘.

반드시 아래 JSON 형식으로만 답해라:

{{
  "key_events": [
    {{
      "event": "",
      "market_view": "",
      "leaders": [{{"name": "", "reason": ""}}],
      "followers": [{{"name": "", "reason": ""}}],
      "related": [{{"name": "", "reason": ""}}],
      "checkpoints": ["", "", ""]
    }}
  ],
  "today_focus_keywords": ["", "", ""]
}}

뉴스:
{news_text}
"""

    res = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.4,
    )

    text = res.choices[0].message.content.strip()
    # 1. 마크다운 코드 블록 제거 (LLM이 ```json ... ``` 이렇게 보낼 때 대비)
    clean_text = re.sub(r'```json|```', '', text).strip()

    if not clean_text:
        print("⚠️ API 응답이 비어있습니다.")
        return [] # 혹은 적절한 기본값

    try:
        return json.loads(clean_text)
    except json.JSONDecodeError as e:
        print(f"❌ JSON 파싱 실패! 응답 내용: {response_text}")
        # 파싱 실패 시 빈 리스트나 에러를 담은 객체 반환
        return []

def apply_news_theme_bonus(candidates_df, news_analysis):
    if candidates_df is None or candidates_df.empty:
        return candidates_df

    df = candidates_df.copy()
    df["뉴스보너스"] = 0
    df["뉴스키워드"] = ""

    theme_stock_map = {}

    for event in news_analysis.get("key_events", []):
        event_name = event.get("event", "")
        for bucket in ["leaders", "followers", "related"]:
            for item in event.get(bucket, []):
                name = item.get("name", "").strip()
                reason = item.get("reason", "").strip()
                if name:
                    theme_stock_map[name] = {
                        "event": event_name,
                        "reason": reason,
                        "bucket": bucket
                    }

    for idx, row in df.iterrows():
        stock_name = str(row.get("종목명", "")).strip()

        if stock_name in theme_stock_map:
            info = theme_stock_map[stock_name]

            if info["bucket"] == "leaders":
                bonus = 40
            elif info["bucket"] == "followers":
                bonus = 25
            else:
                bonus = 10

            df.at[idx, "뉴스보너스"] = bonus
            df.at[idx, "뉴스키워드"] = f"{info['event']} | {info['reason']}"

            if "안전점수" in df.columns:
                df.at[idx, "안전점수"] = int(df.at[idx, "안전점수"]) + bonus

    if "안전점수" in df.columns:
        df = df.sort_values(by="안전점수", ascending=False)

    return df


def format_news_theme_for_telegram(news_analysis):
    if not news_analysis:
        return "📰 [뉴스-테마 분석]\n분석 결과 없음"

    lines = ["📰 [뉴스-테마 분석]"]

    for event in news_analysis.get("key_events", [])[:3]:
        lines.append(f"\n[이슈] {event.get('event', '')}")
        lines.append(f"- 해석: {event.get('market_view', '')}")

        leaders = ", ".join([x.get("name", "") for x in event.get("leaders", [])[:3] if x.get("name")])
        followers = ", ".join([x.get("name", "") for x in event.get("followers", [])[:3] if x.get("name")])
        related = ", ".join([x.get("name", "") for x in event.get("related", [])[:3] if x.get("name")])

        if leaders:
            lines.append(f"- 대장주: {leaders}")
        if followers:
            lines.append(f"- 후발주: {followers}")
        if related:
            lines.append(f"- 연동주: {related}")

        checkpoints = event.get("checkpoints", [])
        if checkpoints:
            lines.append(f"- 체크: {' / '.join(checkpoints[:3])}")

    keywords = news_analysis.get("today_focus_keywords", [])
    if keywords:
        lines.append(f"\n[오늘 키워드] {', '.join(keywords[:5])}")

    return "\n".join(lines)
