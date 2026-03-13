import json
from typing import Dict, List

import yfinance as yf
from openai import OpenAI


US_SYMBOLS = {
    "SPY": "미국 대형주",
    "QQQ": "미국 기술주",
    "SOXX": "미국 반도체",
    "SMH": "미국 반도체(대형)",
    "XLE": "미국 에너지",
    "XLF": "미국 금융",
    "XLI": "미국 산업재",
    "XLV": "미국 헬스케어",
    "ARKK": "고베타 성장주",
    "URA": "우라늄/원전",
    "SLX": "철강/소재",
    "BTC-USD": "비트코인",
    "CL=F": "WTI 원유",
    "GC=F": "금",
    "^VIX": "VIX"
}


KOR_THEME_STOCK_MAP = {
    "반도체": {
        "leaders": ["한미반도체", "주성엔지니어링", "테크윙", "디아이"],
        "followers": ["이수페타시스", "하나마이크론", "리노공업"],
        "related": ["삼성전자", "SK하이닉스"]
    },
    "에너지": {
        "leaders": ["흥구석유", "중앙에너비스"],
        "followers": ["한국석유", "대성에너지"],
        "related": ["흥아해운", "STX그린로지스"]
    },
    "원전": {
        "leaders": ["두산에너빌리티", "한전기술"],
        "followers": ["한전산업", "우리기술"],
        "related": ["비에이치아이"]
    },
    "방산": {
        "leaders": ["한화에어로스페이스", "LIG넥스원"],
        "followers": ["현대로템", "한국항공우주"],
        "related": ["풍산"]
    },
    "2차전지": {
        "leaders": ["에코프로", "포스코퓨처엠"],
        "followers": ["엘앤에프", "대주전자재료"],
        "related": ["금양"]
    },
    "바이오": {
        "leaders": ["알테오젠", "삼천당제약"],
        "followers": ["HLB", "리가켐바이오"],
        "related": ["셀트리온"]
    },
    "비트코인": {
        "leaders": ["우리기술투자", "위지트"],
        "followers": ["한화투자증권"],
        "related": ["갤럭시아머니트리"]
    },
    "조선/해운": {
        "leaders": ["흥아해운", "STX그린로지스"],
        "followers": ["대한해운", "팬오션"],
        "related": ["HMM"]
    },
    "금융": {
        "leaders": ["KB금융", "신한지주"],
        "followers": ["하나금융지주"],
        "related": ["메리츠금융지주"]
    },
    "산업재": {
        "leaders": ["현대로템", "두산밥캣"],
        "followers": ["HD현대일렉트릭"],
        "related": ["LS ELECTRIC"]
    }
}


def _pct_change_from_hist(hist) -> float:
    if hist is None or hist.empty or len(hist) < 2:
        return 0.0
    prev_close = float(hist["Close"].dropna().iloc[-2])
    last_close = float(hist["Close"].dropna().iloc[-1])
    if prev_close == 0:
        return 0.0
    return round((last_close - prev_close) / prev_close * 100, 2)


def fetch_us_market_snapshot() -> Dict:
    snapshot = {}

    for symbol, label in US_SYMBOLS.items():
        try:
            hist = yf.Ticker(symbol).history(period="5d", auto_adjust=False)
            chg = _pct_change_from_hist(hist)
            close = float(hist["Close"].dropna().iloc[-1]) if len(hist["Close"].dropna()) > 0 else None

            snapshot[symbol] = {
                "label": label,
                "close": round(close, 4) if close is not None else None,
                "change_pct": chg
            }
        except Exception:
            snapshot[symbol] = {
                "label": label,
                "close": None,
                "change_pct": 0.0
            }

    return snapshot


def infer_kor_themes_rule_based(snapshot: Dict) -> List[Dict]:
    events = []

    soxx = snapshot.get("SOXX", {}).get("change_pct", 0)
    smh = snapshot.get("SMH", {}).get("change_pct", 0)
    xle = snapshot.get("XLE", {}).get("change_pct", 0)
    ura = snapshot.get("URA", {}).get("change_pct", 0)
    arkk = snapshot.get("ARKK", {}).get("change_pct", 0)
    btc = snapshot.get("BTC-USD", {}).get("change_pct", 0)
    oil = snapshot.get("CL=F", {}).get("change_pct", 0)
    vix = snapshot.get("^VIX", {}).get("change_pct", 0)
    xlf = snapshot.get("XLF", {}).get("change_pct", 0)
    xli = snapshot.get("XLI", {}).get("change_pct", 0)
    xlv = snapshot.get("XLV", {}).get("change_pct", 0)

    if max(soxx, smh) >= 2.0:
        events.append({
            "theme": "반도체",
            "reason": f"미국 반도체 ETF 강세(SOXX {soxx:+.2f}%, SMH {smh:+.2f}%)"
        })
    elif min(soxx, smh) <= -2.0:
        events.append({
            "theme": "반도체",
            "reason": f"미국 반도체 ETF 약세(SOXX {soxx:+.2f}%, SMH {smh:+.2f}%)"
        })

    if xle >= 1.5 or oil >= 2.0:
        events.append({
            "theme": "에너지",
            "reason": f"에너지/유가 강세(XLE {xle:+.2f}%, WTI {oil:+.2f}%)"
        })

    if ura >= 1.5:
        events.append({
            "theme": "원전",
            "reason": f"미국 우라늄/원전 강세(URA {ura:+.2f}%)"
        })

    if arkk >= 2.0:
        events.append({
            "theme": "고베타 성장주",
            "reason": f"고베타 성장주 강세(ARKK {arkk:+.2f}%)"
        })
    elif arkk <= -2.0:
        events.append({
            "theme": "고베타 성장주",
            "reason": f"고베타 성장주 약세(ARKK {arkk:+.2f}%)"
        })

    if btc >= 2.5:
        events.append({
            "theme": "비트코인",
            "reason": f"비트코인 강세(BTC {btc:+.2f}%)"
        })

    if xlf >= 1.2:
        events.append({
            "theme": "금융",
            "reason": f"미국 금융주 강세(XLF {xlf:+.2f}%)"
        })

    if xli >= 1.2:
        events.append({
            "theme": "산업재",
            "reason": f"미국 산업재 강세(XLI {xli:+.2f}%)"
        })

    if xlv >= 1.2:
        events.append({
            "theme": "바이오",
            "reason": f"미국 헬스케어 강세(XLV {xlv:+.2f}%)"
        })

    if vix >= 10:
        events.append({
            "theme": "리스크오프",
            "reason": f"변동성 급등(VIX {vix:+.2f}%)"
        })

    return events


def analyze_us_to_kor_with_gpt(snapshot: Dict, openai_api_key: str) -> Dict:
    client = OpenAI(api_key=openai_api_key)

    prompt = f"""
당신은 미국 시장 데이터를 한국 증시 종목으로 연결하는 단기 트레이딩 보조 AI다.

중요 규칙:
1. 미국 시장 해설로 끝내지 말고 반드시 한국 테마와 종목으로 연결할 것.
2. 대장주 / 후발주 / 연동주를 구분할 것.
3. "주의" 같은 일반론보다 어떤 종목을 먼저 볼지 구체적으로 쓸 것.
4. JSON으로만 답할 것.

출력 형식:
{{
  "us_market_view": "",
  "key_links": [
    {{
      "theme": "",
      "reason": "",
      "leaders": [{{"name": "", "reason": ""}}],
      "followers": [{{"name": "", "reason": ""}}],
      "related": [{{"name": "", "reason": ""}}],
      "checkpoints": ["", "", ""]
    }}
  ]
}}

미국 시장 데이터:
{json.dumps(snapshot, ensure_ascii=False, indent=2)}
"""

    res = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "너는 한국 증시 테마/대장주 연결형 단기 트레이딩 보조 AI다."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3
    )

    return json.loads(res.choices[0].message.content.strip())


def merge_rule_and_gpt_us_mapping(rule_events: List[Dict], gpt_result: Dict) -> List[Dict]:
    merged = []

    for ev in rule_events:
        theme = ev["theme"]
        stocks = KOR_THEME_STOCK_MAP.get(theme, {"leaders": [], "followers": [], "related": []})
        merged.append({
            "theme": theme,
            "reason": ev["reason"],
            "leaders": stocks["leaders"],
            "followers": stocks["followers"],
            "related": stocks["related"]
        })

    for ev in gpt_result.get("key_links", []):
        merged.append({
            "theme": ev.get("theme", ""),
            "reason": ev.get("reason", ""),
            "leaders": [x.get("name", "") for x in ev.get("leaders", []) if x.get("name")],
            "followers": [x.get("name", "") for x in ev.get("followers", []) if x.get("name")],
            "related": [x.get("name", "") for x in ev.get("related", []) if x.get("name")]
        })

    return merged


def apply_us_theme_bonus(candidates_df, us_events: List[Dict]):
    if candidates_df is None or candidates_df.empty:
        return candidates_df

    df = candidates_df.copy()
    df["미국연결보너스"] = 0
    df["미국연결키워드"] = ""

    stock_map = {}

    for ev in us_events:
        theme = ev.get("theme", "")
        reason = ev.get("reason", "")

        for name in ev.get("leaders", []):
            stock_map[name] = {"theme": theme, "reason": reason, "bucket": "leader"}

        for name in ev.get("followers", []):
            if name not in stock_map:
                stock_map[name] = {"theme": theme, "reason": reason, "bucket": "follower"}

        for name in ev.get("related", []):
            if name not in stock_map:
                stock_map[name] = {"theme": theme, "reason": reason, "bucket": "related"}

    for idx, row in df.iterrows():
        name = str(row.get("종목명", "")).strip()
        if name not in stock_map:
            continue

        info = stock_map[name]
        if info["bucket"] == "leader":
            bonus = 35
        elif info["bucket"] == "follower":
            bonus = 20
        else:
            bonus = 10

        df.at[idx, "미국연결보너스"] = bonus
        df.at[idx, "미국연결키워드"] = f"{info['theme']} | {info['reason']}"

        if "안전점수" in df.columns:
            df.at[idx, "안전점수"] = int(df.at[idx, "안전점수"]) + bonus

    if "안전점수" in df.columns:
        df = df.sort_values(by="안전점수", ascending=False)

    return df


def format_us_mapping_for_telegram(gpt_result: Dict, merged_events: List[Dict]) -> str:
    lines = ["🇺🇸➡🇰🇷 [미국시장 → 한국종목 연결]"]

    us_view = gpt_result.get("us_market_view", "")
    if us_view:
        lines.append(f"- 미국시장 해석: {us_view}")

    for ev in merged_events[:4]:
        lines.append(f"\n[테마] {ev.get('theme', '')}")
        lines.append(f"- 이유: {ev.get('reason', '')}")

        leaders = ", ".join(ev.get("leaders", [])[:3])
        followers = ", ".join(ev.get("followers", [])[:3])
        related = ", ".join(ev.get("related", [])[:3])

        if leaders:
            lines.append(f"- 대장주: {leaders}")
        if followers:
            lines.append(f"- 후발주: {followers}")
        if related:
            lines.append(f"- 연동주: {related}")

    return "\n".join(lines)