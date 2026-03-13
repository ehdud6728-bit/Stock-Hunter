import re
from theme_stock_map import THEME_STOCK_MAP


KEYWORD_ALIASES = {
    "oil": ["oil", "crude", "wti", "brent"],
    "shipping": ["shipping", "freight", "container"],
    "war": ["war", "conflict", "military", "missile"],
    "semiconductor": ["chip", "semiconductor"],
    "ai": ["ai", "artificial intelligence"],
    "china stimulus": ["china stimulus", "china economy"],
    "defense": ["defense", "military spending"],
    "battery": ["battery", "ev battery"]
}


def detect_news_themes(news_titles):

    detected = []

    for title in news_titles:

        text = title.lower()

        for key, aliases in KEYWORD_ALIASES.items():

            for alias in aliases:

                if alias in text:

                    detected.append(key)

    return list(set(detected))