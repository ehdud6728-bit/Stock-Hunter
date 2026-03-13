from theme_stock_map import THEME_STOCK_MAP

KEYWORD_ALIASES = {
    "oil": ["oil", "crude", "wti", "brent"],
    "shipping": ["shipping", "freight", "container"],
    "war": ["war", "conflict", "military", "missile"],
    "semiconductor": ["chip", "semiconductor"],
    "ai": ["ai", "artificial intelligence"],
    "china stimulus": ["china stimulus", "china economy", "stimulus"],
    "defense": ["defense", "military spending"],
    "battery": ["battery", "ev battery"]
}

def detect_news_themes(news_titles):
    detected = []

    for title in news_titles:
        text = str(title).lower()

        for key, aliases in KEYWORD_ALIASES.items():
            for alias in aliases:
                if alias in text:
                    detected.append(key)
                    break

    return list(set(detected))

def map_theme_to_stocks(themes):
    events = []

    for theme in themes:
        if theme not in THEME_STOCK_MAP:
            continue

        data = THEME_STOCK_MAP[theme]

        events.append({
            "event": data["theme"],
            "leaders": data["leaders"],
            "followers": data["followers"],
            "related": data["related"]
        })

    return events

def analyze_news_rule_based(news_titles):
    themes = detect_news_themes(news_titles)
    events = map_theme_to_stocks(themes)

    return {
        "themes": themes,
        "events": events
    }