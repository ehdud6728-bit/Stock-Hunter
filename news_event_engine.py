import re
import time
import json
from typing import List, Dict, Tuple

import feedparser
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


def fetch_rss_titles(url: str, limit: int = 8) -> List[Dict]:
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


def collect_market_news() -> Dict[str, List[Dict]]:
    result = {}
    for key, url in NEWS_FEEDS.items():
        try:
            result[key] = fetch_rss_titles(url, limit=8)
            time.sleep(0.2)
        except Exception:
            result[key] = []
    return result


def flatten_news_titles(news_map: Dict[str, List[Dict]], max_items: int = 20) -> List[str]:
    seen = set()
    merged = []

    for source_name, items in news_map.items():
        for item in items:
            title = item["title"]
            norm = title.lower().strip()
            if norm in seen:
                continue
            seen.add(norm)
            merged.append(title)

    return merged[:max_items]