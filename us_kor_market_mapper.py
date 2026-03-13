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
    "