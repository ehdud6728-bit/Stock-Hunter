# -*- coding: utf-8 -*-
"""
quick_open_checker.py
──────────────────────────────────────────────────────────────────────────────
Stock-Hunter 정규장 초반 빠른 확인용 미니 검색기

목적
- main7_bugfix_2.py 같은 전체 검색기가 15~18분 걸릴 때,
  정규장 09:03~09:10 사이에는 전 종목 재검색 대신 "오늘 후보군"만 빠르게 확인합니다.
- NXT/프리마켓성 움직임은 "예비 알람"으로만 보고,
  정규장 거래량·가격 유지·재돌파 여부로 속임수를 걸러냅니다.

입력 파일
1) today_candidates.json  권장
   - list[dict] 또는 dict[str, list[dict]] 모두 지원
   - code / 종목코드 / ticker / Code 중 아무 키나 사용 가능
   - name / 종목명 / Name 중 아무 키나 사용 가능

2) nxt_snapshot.json 선택
   - list[dict] 또는 dict[code, dict]
   - code, name, nxt_price, nxt_high, nxt_volume, nxt_amount_b 지원

3) --codes "005930,000660,042700" 로 직접 실행 가능

출력
- quick_open_result.txt
- quick_open_result.json
- 콘솔 출력
- 선택 시 텔레그램 전송: --send-telegram

실행 예시
python quick_open_checker.py
python quick_open_checker.py --candidate-file today_candidates.json
python quick_open_checker.py --codes "277810,049950,448280" --send-telegram

필수 패키지
- requests, beautifulsoup4, pandas
- FinanceDataReader는 있으면 사용, 없으면 Naver 일봉 페이지로 일부 대체
"""

from __future__ import annotations

import argparse
import ast
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    import pytz
except Exception:  # pragma: no cover
    pytz = None

try:
    import FinanceDataReader as fdr
except Exception:  # pragma: no cover
    fdr = None

KST_NAME = "Asia/Seoul"
REAL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://finance.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

SEP = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
THIN = "────────────────────────────"


# ─────────────────────────────────────────────────────────────────────────────
# 기본 유틸
# ─────────────────────────────────────────────────────────────────────────────

def now_kst() -> datetime:
    if pytz:
        return datetime.now(pytz.timezone(KST_NAME))
    return datetime.utcnow() + timedelta(hours=9)


def to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        if isinstance(value, float) and math.isnan(value):
            return default
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return default
    text = re.sub(r"[^0-9\-]", "", text)
    if text in ("", "-", "+"):
        return default
    try:
        return int(text)
    except Exception:
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        if isinstance(value, float) and math.isnan(value):
            return default
    except Exception:
        pass
    text = str(value).strip().replace(",", "")
    if not text:
        return default
    text = re.sub(r"[^0-9\.\-]", "", text)
    if text in ("", "-", "+", "."):
        return default
    try:
        return float(text)
    except Exception:
        return default


def pct(a: float, b: float) -> float:
    if not b:
        return 0.0
    return (a / b - 1.0) * 100.0


def ratio(a: float, b: float) -> float:
    if not b:
        return 0.0
    return a / b


def normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    # V9.1: 0015G0 같은 문자 포함 특수코드는 일반 빠른검색에서 제외
    if re.search(r"[A-Za-z가-힣]", text):
        return ""
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".")[0]
    text = re.sub(r"[^0-9]", "", text)
    if not text or len(text) > 6:
        return ""
    return text.zfill(6)


def compact_name(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text if text else default


def read_json_file(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc) as f:
                return json.load(f)
        except UnicodeDecodeError:
            continue
        except Exception:
            return default
    return default


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8-sig")


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8-sig")


# ─────────────────────────────────────────────────────────────────────────────
# 데이터 구조
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Candidate:
    code: str
    name: str = ""
    source: str = ""
    status: str = ""
    grade: str = ""
    decision: str = ""
    tags: str = ""
    raw: Optional[Dict[str, Any]] = None


@dataclass
class NxtInfo:
    code: str
    name: str = ""
    nxt_price: int = 0
    nxt_high: int = 0
    nxt_low: int = 0
    nxt_volume: int = 0
    nxt_amount_b: float = 0.0
    raw: Optional[Dict[str, Any]] = None


@dataclass
class DailyInfo:
    code: str
    prev_close: int = 0
    prev_high: int = 0
    prev_low: int = 0
    ma5: float = 0.0
    ma20: float = 0.0
    vma20: float = 0.0
    high20: int = 0
    source: str = ""


@dataclass
class QuoteInfo:
    code: str
    name: str = ""
    price: int = 0
    open_price: int = 0
    high: int = 0
    low: int = 0
    volume: int = 0
    change_rate: float = 0.0
    amount_b: float = 0.0
    source: str = ""
    ok: bool = False
    error: str = ""


@dataclass
class MinuteInfo:
    code: str
    start: str = ""
    end: str = ""
    open_price: int = 0
    high: int = 0
    low: int = 0
    close: int = 0
    volume: int = 0
    amount_b: float = 0.0
    candle: str = ""
    upper_wick_pct: float = 0.0
    ok: bool = False
    source: str = ""
    error: str = ""


@dataclass
class CheckResult:
    code: str
    name: str
    status: str
    grade: str
    source: str
    price: int
    change_rate: float
    amount_b: float
    prev_close: int
    prev_high: int
    ma5: float
    vma20: float
    volume_ratio_daily: float
    early_amount_b: float
    early_volume_ratio: float
    nxt_price: int
    nxt_high: int
    structure: str
    trade_decision: str
    action: str
    score: int
    reasons: List[str]
    warnings: List[str]
    conditions: List[str]
    meaning: str
    raw: Dict[str, Any]


# ─────────────────────────────────────────────────────────────────────────────
# 후보 파일 로딩
# ─────────────────────────────────────────────────────────────────────────────

CODE_KEYS = ["code", "종목코드", "ticker", "Ticker", "Code", "symbol", "Symbol"]
NAME_KEYS = ["name", "종목명", "Name", "stock_name", "종목"]
STATUS_KEYS = ["status", "상태", "state", "최종상태"]
GRADE_KEYS = ["grade", "등급"]
DECISION_KEYS = ["decision", "판정", "최종판정", "매매판정"]
TAG_KEYS = ["tags", "태그", "tag"]


def pick(d: Dict[str, Any], keys: Iterable[str], default: Any = "") -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def flatten_candidates(data: Any, source_name: str = "") -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                item = dict(item)
                if source_name and "source" not in item:
                    item["source"] = source_name
                out.append(item)
        return out
    if isinstance(data, dict):
        # dict[code] = name/info 형태도 지원
        if any(normalize_code(k) for k in data.keys()):
            for k, v in data.items():
                code = normalize_code(k)
                if not code:
                    continue
                if isinstance(v, dict):
                    item = dict(v)
                    item.setdefault("code", code)
                else:
                    item = {"code": code, "name": str(v)}
                if source_name and "source" not in item:
                    item["source"] = source_name
                out.append(item)
            return out
        # dict[category] = list 형태
        for category, value in data.items():
            nested = flatten_candidates(value, str(category))
            out.extend(nested)
    return out


def load_candidates(candidate_file: Path, codes_arg: str = "") -> List[Candidate]:
    items: List[Dict[str, Any]] = []

    if codes_arg.strip():
        for raw_code in re.split(r"[,\s]+", codes_arg.strip()):
            code = normalize_code(raw_code)
            if code:
                items.append({"code": code, "source": "직접입력"})
    else:
        data = read_json_file(candidate_file, default=None)
        if data is None:
            # txt 후보 파일도 간단 지원: 005930 삼성전자 형태
            txt_path = candidate_file.with_suffix(".txt")
            if txt_path.exists():
                lines = txt_path.read_text(encoding="utf-8-sig").splitlines()
                for line in lines:
                    m = re.search(r"(\d{6})", line)
                    if m:
                        code = m.group(1)
                        name = re.sub(r"\d{6}", "", line).strip(" -_/|\t")
                        items.append({"code": code, "name": name, "source": "txt"})
            else:
                # ✅ V9.0 fallback: today_candidates.json이 없어도 amount_top60.json만 있으면 빠른검색 실행
                fallback = candidate_file.with_name("amount_top60.json")
                if fallback.exists():
                    data2 = read_json_file(fallback, default=None)
                    items = flatten_candidates(data2) if data2 is not None else []
                else:
                    return []
        else:
            items = flatten_candidates(data)

    seen: set[str] = set()
    candidates: List[Candidate] = []
    for item in items:
        code = normalize_code(pick(item, CODE_KEYS, ""))
        if not code or code in seen:
            continue
        seen.add(code)
        name = compact_name(pick(item, NAME_KEYS, ""), default=code)
        candidates.append(
            Candidate(
                code=code,
                name=name,
                source=compact_name(item.get("source", "후보군")),
                status=compact_name(pick(item, STATUS_KEYS, "")),
                grade=compact_name(pick(item, GRADE_KEYS, "")),
                decision=compact_name(pick(item, DECISION_KEYS, "")),
                tags=compact_name(pick(item, TAG_KEYS, "")),
                raw=item,
            )
        )
    return candidates


def load_nxt_snapshot(path: Path) -> Dict[str, NxtInfo]:
    data = read_json_file(path, default=None)
    if data is None:
        return {}
    items = flatten_candidates(data)
    out: Dict[str, NxtInfo] = {}
    for item in items:
        code = normalize_code(pick(item, CODE_KEYS, ""))
        if not code:
            continue
        name = compact_name(pick(item, NAME_KEYS, ""))
        nxt_price = to_int(item.get("nxt_price", item.get("NXT현재가", item.get("price", 0))))
        nxt_high = to_int(item.get("nxt_high", item.get("NXT고가", item.get("high", nxt_price))))
        nxt_low = to_int(item.get("nxt_low", item.get("NXT저가", item.get("low", 0))))
        nxt_volume = to_int(item.get("nxt_volume", item.get("NXT거래량", item.get("volume", 0))))
        nxt_amount_b = to_float(item.get("nxt_amount_b", item.get("NXT거래대금억", 0.0)))
        out[code] = NxtInfo(
            code=code,
            name=name,
            nxt_price=nxt_price,
            nxt_high=nxt_high or nxt_price,
            nxt_low=nxt_low,
            nxt_volume=nxt_volume,
            nxt_amount_b=nxt_amount_b,
            raw=item,
        )
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 시세/일봉 조회
# ─────────────────────────────────────────────────────────────────────────────

def request_get(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 7) -> requests.Response:
    res = requests.get(url, params=params, headers=REAL_HEADERS, timeout=timeout)
    res.raise_for_status()
    return res


def fetch_quote_naver(code: str) -> QuoteInfo:
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    q = QuoteInfo(code=code, source="naver")
    try:
        res = request_get(url, timeout=7)
        res.encoding = "euc-kr"
        html = res.text
        soup = BeautifulSoup(html, "html.parser")

        # 현재가
        today = soup.select_one("p.no_today span.blind")
        price = to_int(today.get_text(" ", strip=True) if today else 0)

        # 종목명
        name_node = soup.select_one("div.wrap_company h2")
        name = compact_name(name_node.get_text(" ", strip=True) if name_node else "")

        # 등락률
        text = soup.get_text(" ", strip=True)
        rate = 0.0
        m_rate = re.search(r"등락률\s*([+\-]?[0-9\.]+)%", text)
        if not m_rate:
            m_rate = re.search(r"전일대비.*?([+\-]?[0-9\.]+)%", text)
        if not m_rate:
            # blind span 중 % 근처가 안 잡힐 수 있어 fallback
            m_rate = re.search(r"([+\-]?[0-9\.]+)%", text)
        if m_rate:
            rate = to_float(m_rate.group(1))
            # Naver는 부호가 텍스트로 빠질 때가 있어 약식 보정은 하지 않음

        # 거래량/거래대금/시고저는 페이지 텍스트에서 최대한 추출
        volume = 0
        amount_b = 0.0
        open_price = high = low = 0

        # blind dl에 들어있는 경우가 많음
        patterns = {
            "volume": [r"거래량\s*([0-9,]+)", r"거래량\(천주\)\s*([0-9,]+)"],
            "amount": [r"거래대금\s*([0-9,]+)\s*백만", r"거래대금\s*([0-9,]+)"],
            "open": [r"시가\s*([0-9,]+)"],
            "high": [r"고가\s*([0-9,]+)"],
            "low": [r"저가\s*([0-9,]+)"],
        }
        for p in patterns["volume"]:
            m = re.search(p, text)
            if m:
                volume = to_int(m.group(1)); break
        for p in patterns["amount"]:
            m = re.search(p, text)
            if m:
                # '백만'이면 억 환산: 백만원 / 100 = 억원
                raw = to_float(m.group(1))
                amount_b = raw / 100.0 if "백만" in p else raw / 100000000.0
                break
        for p in patterns["open"]:
            m = re.search(p, text)
            if m:
                open_price = to_int(m.group(1)); break
        for p in patterns["high"]:
            m = re.search(p, text)
            if m:
                high = to_int(m.group(1)); break
        for p in patterns["low"]:
            m = re.search(p, text)
            if m:
                low = to_int(m.group(1)); break

        # 거래대금이 없으면 현재가×거래량으로 대략 계산
        if price and volume and amount_b <= 0:
            amount_b = price * volume / 100000000.0

        q.name = name
        q.price = price
        q.open_price = open_price
        q.high = high
        q.low = low
        q.volume = volume
        q.change_rate = round(rate, 2)
        q.amount_b = round(amount_b, 2)
        q.ok = bool(price)
        if not q.ok:
            q.error = "현재가 파싱 실패"
        return q
    except Exception as e:
        q.error = str(e)
        return q


def fetch_daily_fdr(code: str, lookback_days: int = 80) -> DailyInfo:
    d = DailyInfo(code=code, source="")
    if fdr is None:
        return d
    try:
        end = now_kst().date()
        start = end - timedelta(days=lookback_days * 2)
        df = fdr.DataReader(code, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        if df is None or df.empty:
            return d
        df = df.rename(columns={c: c.capitalize() for c in df.columns})
        # 당일 데이터가 포함되어도 정규장 초반에는 전일 확정봉 기준이 필요하므로 마지막 1개 제외 시도
        if len(df) >= 2:
            base = df.iloc[:-1].copy()
            if base.empty:
                base = df.copy()
        else:
            base = df.copy()
        last = base.iloc[-1]
        d.prev_close = int(last.get("Close", 0))
        d.prev_high = int(last.get("High", 0))
        d.prev_low = int(last.get("Low", 0))
        d.ma5 = float(base["Close"].tail(5).mean()) if "Close" in base else 0.0
        d.ma20 = float(base["Close"].tail(20).mean()) if "Close" in base else 0.0
        d.vma20 = float(base["Volume"].tail(20).mean()) if "Volume" in base else 0.0
        d.high20 = int(base["High"].tail(20).max()) if "High" in base else 0
        d.source = "fdr"
        return d
    except Exception:
        return d


def fetch_daily_naver(code: str) -> DailyInfo:
    """FDR 실패 시 Naver 일별시세 2~3페이지에서 전일/평균 거래량 대체."""
    d = DailyInfo(code=code, source="")
    try:
        rows: List[pd.DataFrame] = []
        for page in range(1, 4):
            url = f"https://finance.naver.com/item/sise_day.naver?code={code}&page={page}"
            res = request_get(url, timeout=7)
            res.encoding = "euc-kr"
            tables = pd.read_html(res.text)
            if tables:
                rows.append(tables[0])
            time.sleep(0.08)
        if not rows:
            return d
        df = pd.concat(rows, ignore_index=True).dropna()
        if df.empty:
            return d
        df.columns = [str(c).strip() for c in df.columns]
        # 날짜 최신순. 오늘 행이 있더라도 정규장 초반엔 전일 기준을 위해 최신 1행이 오늘이면 제외하는 로직
        today_str = now_kst().strftime("%Y.%m.%d")
        if "날짜" in df.columns:
            df["날짜"] = df["날짜"].astype(str)
            if str(df.iloc[0]["날짜"]) == today_str and len(df) >= 2:
                base = df.iloc[1:].copy()
            else:
                base = df.copy()
        else:
            base = df.copy()
        if base.empty:
            return d
        last = base.iloc[0]
        d.prev_close = to_int(last.get("종가", 0))
        d.prev_high = to_int(last.get("고가", 0))
        d.prev_low = to_int(last.get("저가", 0))
        closes = [to_int(x) for x in base.get("종가", pd.Series(dtype=object)).tolist()]
        highs = [to_int(x) for x in base.get("고가", pd.Series(dtype=object)).tolist()]
        vols = [to_int(x) for x in base.get("거래량", pd.Series(dtype=object)).tolist()]
        d.ma5 = sum(closes[:5]) / max(1, len(closes[:5])) if closes else 0.0
        d.ma20 = sum(closes[:20]) / max(1, len(closes[:20])) if closes else 0.0
        d.vma20 = sum(vols[:20]) / max(1, len(vols[:20])) if vols else 0.0
        d.high20 = max(highs[:20]) if highs else 0
        d.source = "naver_day"
        return d
    except Exception:
        return d


def fetch_daily_info(code: str) -> DailyInfo:
    d = fetch_daily_fdr(code)
    if d.prev_close:
        return d
    return fetch_daily_naver(code)


def fetch_minute_naver(code: str, window_min: int = 5) -> MinuteInfo:
    """
    Naver 비공식 분봉 endpoint 시도.
    환경에 따라 실패할 수 있으므로 실패해도 전체 판정은 quote/daily 기준으로 진행.
    """
    m = MinuteInfo(code=code, source="naver_minute")
    try:
        now = now_kst()
        date_str = now.strftime("%Y%m%d")
        start = f"{date_str}090000"
        end = now.strftime("%Y%m%d%H%M00")
        url = "https://api.finance.naver.com/siseJson.naver"
        params = {
            "symbol": code,
            "requestType": 1,
            "startTime": start,
            "endTime": end,
            "timeframe": "minute",
        }
        res = request_get(url, params=params, timeout=7)
        text = res.text.strip()
        if not text or text in ("[]", "null"):
            m.error = "분봉 데이터 없음"
            return m
        # JS 배열 형태를 ast로 최대한 변환
        text = text.replace("null", "None")
        data = ast.literal_eval(text)
        if not isinstance(data, list) or len(data) < 2:
            m.error = "분봉 파싱 실패"
            return m
        header = [str(x) for x in data[0]]
        rows = data[1:]
        # 보통 [날짜, 시가, 고가, 저가, 종가, 거래량]
        valid = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 6:
                continue
            ts = str(row[0])
            if not ts.startswith(date_str):
                continue
            valid.append(row)
        if not valid:
            m.error = "오늘 분봉 없음"
            return m
        # 09:00부터 window_min 안쪽만 우선 사용. 실행 시간이 09:08이면 09:00~09:08 누적 확인.
        cutoff = datetime(now.year, now.month, now.day, 9, 0) + timedelta(minutes=max(1, window_min))
        if pytz:
            cutoff = pytz.timezone(KST_NAME).localize(cutoff.replace(tzinfo=None))
        selected = []
        for row in valid:
            ts = str(row[0])[:12]
            try:
                dt = datetime.strptime(ts, "%Y%m%d%H%M")
                if pytz:
                    dt = pytz.timezone(KST_NAME).localize(dt)
                if dt <= now and dt <= cutoff + timedelta(minutes=3):
                    selected.append(row)
            except Exception:
                selected.append(row)
        if not selected:
            selected = valid[:max(1, window_min)]

        open_price = to_int(selected[0][1])
        high = max(to_int(r[2]) for r in selected)
        low = min(to_int(r[3]) for r in selected)
        close = to_int(selected[-1][4])
        volume = sum(to_int(r[5]) for r in selected)
        amount_b = sum(to_int(r[4]) * to_int(r[5]) for r in selected) / 100000000.0
        body_high = max(open_price, close)
        body_low = min(open_price, close)
        rng = max(1, high - low)
        upper_wick_pct = max(0.0, (high - body_high) / rng * 100.0)
        candle = "양봉" if close >= open_price else "음봉"

        m.start = start
        m.end = end
        m.open_price = open_price
        m.high = high
        m.low = low
        m.close = close
        m.volume = volume
        m.amount_b = round(amount_b, 2)
        m.candle = candle
        m.upper_wick_pct = round(upper_wick_pct, 1)
        m.ok = bool(close)
        return m
    except Exception as e:
        m.error = str(e)
        return m


# ─────────────────────────────────────────────────────────────────────────────
# 판정 로직
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Thresholds:
    min_amount_b: float = 3.0              # 정규장 누적 거래대금 억원 최소
    min_early_amount_b: float = 1.0        # 분봉 데이터 있을 때 초반 거래대금 억원 최소
    min_early_vratio: float = 0.04         # 초반 거래량 / 20일평균거래량
    min_daily_vratio: float = 0.06         # 현재 누적 거래량 / 20일평균거래량
    overheat_disparity: float = 116.0      # 현재가/전일 MA20 대신 prev_close 기준 보조 과열
    upper_wick_warn: float = 45.0          # 초반 분봉 윗꼬리 경고
    quick_execute_score: int = 70
    conditional_score: int = 55


def candidate_bias(c: Candidate) -> int:
    text = " ".join([c.status, c.grade, c.decision, c.tags, c.source])
    score = 0
    if any(k in text for k in ["Blue-2", "파란점", "재점화", "즉시 대응"]):
        score += 12
    if any(k in text for k in ["정제수박", "PASS_B", "PASS_A"]):
        score += 8
    if any(k in text for k in ["후행", "추격 금지", "과열"]):
        score -= 14
    if any(k in text for k in ["가짜수박", "주의"]):
        score -= 10
    if "S" in c.grade:
        score += 4
    return score


def judge_candidate(
    c: Candidate,
    q: QuoteInfo,
    d: DailyInfo,
    minute: MinuteInfo,
    nxt: Optional[NxtInfo],
    th: Thresholds,
) -> CheckResult:
    reasons: List[str] = []
    warnings: List[str] = []
    conditions: List[str] = []
    score = 50 + candidate_bias(c)

    price = q.price
    prev_close = d.prev_close or 0
    prev_high = d.prev_high or 0
    ma5 = d.ma5 or 0.0
    vma20 = d.vma20 or 0.0
    volume_ratio_daily = ratio(q.volume, vma20) if vma20 else 0.0

    early_amount_b = minute.amount_b if minute.ok else q.amount_b
    early_volume_ratio = ratio(minute.volume, vma20) if (minute.ok and vma20) else volume_ratio_daily

    nxt_price = nxt.nxt_price if nxt else 0
    nxt_high = nxt.nxt_high if nxt else 0
    ref_break = max(prev_high, nxt_high or 0)

    # 가격 위치
    if price and prev_close:
        chg = pct(price, prev_close)
        if chg > 0:
            score += 8
            reasons.append(f"전일종가 대비 +{chg:.1f}%")
        else:
            score -= 20
            warnings.append(f"전일종가 대비 {chg:.1f}% 약세")

    if ref_break and price >= ref_break:
        score += 18
        reasons.append("전일고가/NXT고가 재돌파")
    elif prev_high and price >= prev_high * 0.995:
        score += 8
        reasons.append("전일고가 근접")
        conditions.append("전일고가 재돌파 확인")
    elif nxt_high and price < nxt_high:
        score -= 8
        warnings.append("NXT고가 재돌파 실패")
        conditions.append("NXT고가 재돌파 필요")

    if ma5 and price >= ma5:
        score += 8
        reasons.append("5일선 위 유지")
    elif ma5:
        score -= 14
        warnings.append("5일선 아래")
        conditions.append("5일선 재안착 필요")

    # 거래량/거래대금
    if q.amount_b >= th.min_amount_b:
        score += 10
        reasons.append(f"정규장 누적 거래대금 {q.amount_b:.1f}억")
    else:
        score -= 8
        warnings.append(f"정규장 거래대금 부족 {q.amount_b:.1f}억")
        conditions.append(f"거래대금 {th.min_amount_b:.1f}억 이상")

    if early_amount_b >= th.min_early_amount_b:
        score += 6
        reasons.append(f"초반 거래대금 {early_amount_b:.1f}억")
    else:
        score -= 4
        conditions.append(f"초반 거래대금 {th.min_early_amount_b:.1f}억 이상")

    if early_volume_ratio >= th.min_early_vratio:
        score += 10
        reasons.append(f"초반 거래량/VMA20 {early_volume_ratio:.1%}")
    elif volume_ratio_daily >= th.min_daily_vratio:
        score += 5
        reasons.append(f"누적 거래량/VMA20 {volume_ratio_daily:.1%}")
    else:
        score -= 10
        warnings.append("정규장 거래량 확인 부족")
        conditions.append("거래량 재유입")

    # 분봉 캔들
    if minute.ok:
        if minute.candle == "양봉":
            score += 7
            reasons.append("초반 분봉 양봉")
        else:
            score -= 10
            warnings.append("초반 분봉 음봉")
            conditions.append("재양봉 확인")
        if minute.upper_wick_pct >= th.upper_wick_warn:
            score -= 8
            warnings.append(f"초반 윗꼬리 부담 {minute.upper_wick_pct:.0f}%")
            conditions.append("윗꼬리 축소")
    else:
        conditions.append("1~5분봉 양봉 확인")

    # NXT 속임수 판단
    if nxt and (nxt.nxt_price or nxt.nxt_high):
        if price >= max(nxt.nxt_price, nxt.nxt_high or 0):
            score += 8
            reasons.append("NXT 가격대 정규장 재확인")
        elif price >= prev_close:
            score -= 3
            warnings.append("NXT 고점은 미회복, 전일종가는 유지")
        else:
            score -= 18
            warnings.append("NXT 상승 후 정규장 이탈 가능성")

    # 과열/추격 부담
    if prev_close and price / prev_close * 100 >= th.overheat_disparity:
        score -= 12
        warnings.append(f"전일종가 대비 이격 {price / prev_close * 100:.0f} 추격부담")
        conditions.append("눌림 후 재양봉")

    text = " ".join([c.status, c.decision, c.tags, c.source])
    is_late = any(k in text for k in ["후행", "추격 금지", "과열후행"])
    is_fake = any(k in text for k in ["가짜수박", "정제주의"])
    is_blue = any(k in text for k in ["Blue-2", "파란점", "재점화"])

    # 구조판정
    if is_blue and price >= ref_break and score >= th.conditional_score:
        structure = "저항구름 돌파 재점화형"
    elif price >= ref_break and ref_break:
        structure = "저항구름 돌파확인형"
    elif (price >= prev_high * 0.99) if prev_high else False:
        structure = "저항구름 돌파테스트형"
    elif (price >= ma5) if ma5 else False:
        structure = "정규장 재안착 관찰형"
    else:
        structure = "NXT/초반 속임수 확인형"

    # 매매판정
    if is_late or is_fake:
        if score >= 76 and not is_fake:
            trade_decision = "보유자 대응"
            action = "신규 추격 금지 / 보유자는 5일선·구름상단 이탈 관리"
            meaning = "구조는 살아 있지만 후행·추격 부담이 있어 신규보다 보유자의 관리가 우선입니다."
        else:
            trade_decision = "추격 금지"
            action = "신규 매수 금지 / 재안착 전까지 관찰"
            meaning = "좋아 보여도 타점이 늦었거나 위험 신호가 있어 신규 추격은 피하는 구간입니다."
    elif score >= th.quick_execute_score and price >= ref_break and early_volume_ratio >= th.min_early_vratio:
        trade_decision = "조건부 실행"
        action = "분할 대응 / 5일선 훼손 없음 / 장중 눌림 후 재양봉 확인"
        meaning = "정규장 거래량과 가격 재확인이 붙어 실행 후보로 올릴 수 있지만, 분할과 손절 기준이 전제입니다."
    elif score >= th.conditional_score:
        trade_decision = "확인 대기"
        action = "구름상단 또는 5일선 지지 + 재양봉 + 거래량 재유입 확인"
        meaning = "관심 구조는 맞지만 아직 매수 확정이 아니라 정규장 거래량과 지지 확인이 먼저입니다."
    elif price and prev_close and price < prev_close:
        trade_decision = "제외"
        action = "NXT 단독 상승 가능성 / 정규장 회복 전 제외"
        meaning = "NXT나 장전 기대와 달리 정규장 가격 유지가 되지 않아 속임수 가능성이 큽니다."
    else:
        trade_decision = "눌림 대기"
        action = "눌림 후 5일선 재안착·양봉·거래량 보강 확인"
        meaning = "구조는 일부 보이지만 정규장 확인이 부족해 눌림과 재안착을 기다리는 자리입니다."

    # 실행조건 기본 보강
    if not conditions:
        if trade_decision == "조건부 실행":
            conditions = ["구름상단 위 종가 유지", "5일선 훼손 없음", "장중 눌림 후 재양봉", "거래량 재유입"]
        elif trade_decision in ("확인 대기", "눌림 대기"):
            conditions = ["구름상단 또는 5일선 지지", "재양봉", "거래량 20일평균 대비 보강"]
        elif trade_decision == "추격 금지":
            conditions = ["신규 매수 금지", "재안착 전까지 관찰"]
        else:
            conditions = ["5일선/구름상단 이탈 관리"]

    score = max(0, min(100, int(round(score))))

    return CheckResult(
        code=c.code,
        name=q.name or c.name or c.code,
        status=c.status,
        grade=c.grade,
        source=c.source,
        price=price,
        change_rate=q.change_rate,
        amount_b=q.amount_b,
        prev_close=prev_close,
        prev_high=prev_high,
        ma5=round(ma5, 2),
        vma20=round(vma20, 2),
        volume_ratio_daily=round(volume_ratio_daily, 4),
        early_amount_b=round(early_amount_b, 2),
        early_volume_ratio=round(early_volume_ratio, 4),
        nxt_price=nxt_price,
        nxt_high=nxt_high,
        structure=structure,
        trade_decision=trade_decision,
        action=action,
        score=score,
        reasons=reasons[:6],
        warnings=warnings[:6],
        conditions=conditions[:6],
        meaning=meaning,
        raw={
            "candidate": c.raw or {},
            "quote": asdict(q),
            "daily": asdict(d),
            "minute": asdict(minute),
            "nxt": asdict(nxt) if nxt else None,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# 리포트
# ─────────────────────────────────────────────────────────────────────────────

def money(n: int) -> str:
    return f"{n:,}원" if n else "-"


def format_result_block(i: int, r: CheckResult) -> str:
    icon = {
        "조건부 실행": "✅",
        "확인 대기": "⏳",
        "돌파확인 대기": "⏳",
        "눌림 대기": "🧭",
        "보유자 대응": "🛡️",
        "추격 금지": "⛔",
        "제외": "❌",
    }.get(r.trade_decision, "👀")

    status = f" | 상태:{r.status}" if r.status else ""
    grade = f" | 등급:{r.grade}" if r.grade else ""
    nxt_line = ""
    if r.nxt_price or r.nxt_high:
        nxt_line = f"\n- 🌅 NXT: 현재 {money(r.nxt_price)} / 고가 {money(r.nxt_high)}"

    reasons = " / ".join(r.reasons) if r.reasons else "특이 강점 없음"
    warnings = " / ".join(r.warnings) if r.warnings else "큰 경고 없음"
    conditions = " / ".join(r.conditions) if r.conditions else "기본 확인"

    return (
        f"{i}) {r.name}({r.code}){status}{grade}\n"
        f"- 현재가:{money(r.price)} | 등락:{r.change_rate:+.2f}% | 점수:{r.score}\n"
        f"- 거래대금:{r.amount_b:.1f}억 | 초반거래:{r.early_amount_b:.1f}억 | "
        f"거래량/VMA20:{r.early_volume_ratio:.1%}\n"
        f"- 전일종가:{money(r.prev_close)} | 전일고가:{money(r.prev_high)} | 5일선:{money(int(r.ma5))}"
        f"{nxt_line}\n"
        f"✅ 구조판정: {r.structure}\n"
        f"{icon} 매매판정: {r.trade_decision}\n"
        f"📌 실행조건: {conditions}\n"
        f"⚠️ 주의: {warnings}\n"
        f"🧠 의미: {r.meaning}\n"
        f"🧩 근거: {reasons}"
    )


def format_report(results: List[CheckResult], title: str = "정규장 초반 미니 확인") -> str:
    now = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    valid = [r for r in results if r.price]
    by_decision: Dict[str, int] = {}
    for r in valid:
        by_decision[r.trade_decision] = by_decision.get(r.trade_decision, 0) + 1

    order = {"조건부 실행": 0, "확인 대기": 1, "보유자 대응": 2, "눌림 대기": 3, "추격 금지": 4, "제외": 5}
    sorted_results = sorted(valid, key=lambda x: (order.get(x.trade_decision, 9), -x.score, -x.amount_b))

    lines = [
        "[QUICK]",
        SEP,
        f"⚡ [{title}]",
        f"- 기준시각: {now}",
        f"- 확인종목: {len(results)}개 / 시세성공: {len(valid)}개",
        "- 판정요약: " + (" / ".join([f"{k} {v}개" for k, v in by_decision.items()]) if by_decision else "없음"),
        SEP,
    ]

    execs = [r for r in sorted_results if r.trade_decision == "조건부 실행"]
    waits = [r for r in sorted_results if r.trade_decision == "확인 대기"]
    holders = [r for r in sorted_results if r.trade_decision == "보유자 대응"]
    pullbacks = [r for r in sorted_results if r.trade_decision == "눌림 대기"]
    bans = [r for r in sorted_results if r.trade_decision in ("추격 금지", "제외")]

    groups = [
        ("✅ 조건부 실행 후보", execs),
        ("⏳ 확인 대기", waits),
        ("🛡️ 보유자 대응", holders),
        ("🧭 눌림 대기", pullbacks),
        ("⛔ 제외/추격 금지", bans),
    ]

    for group_title, group in groups:
        lines.append(f"\n{SEP}\n{group_title}")
        if not group:
            lines.append("- 해당 종목 없음")
            continue
        for i, r in enumerate(group[:10], 1):
            lines.append("\n" + format_result_block(i, r))
            lines.append(THIN)

    failed = [r for r in results if not r.price]
    if failed:
        lines.append(f"\n{SEP}\n⚙️ 시세 조회 실패")
        for r in failed[:10]:
            err = r.raw.get("quote", {}).get("error", "") if isinstance(r.raw, dict) else ""
            lines.append(f"- {r.name}({r.code}) {err}")

    lines.append("\n" + SEP)
    lines.append("📌 해석 기준")
    lines.append("- NXT 강세는 예비 알람입니다. 정규장 거래량과 가격 유지가 확인되어야 합니다.")
    lines.append("- 조건부 실행도 추격 확정이 아니라 분할·손절 기준을 전제로 한 실행 후보입니다.")
    lines.append("- 확인 대기/눌림 대기는 구름상단·5일선 지지와 재양봉을 기다리는 구간입니다.")
    lines.append(SEP)
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 텔레그램
# ─────────────────────────────────────────────────────────────────────────────

def split_message(text: str, limit: int = 3600) -> List[str]:
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    buf = ""
    for line in text.splitlines():
        if len(buf) + len(line) + 1 > limit:
            chunks.append(buf)
            buf = line
        else:
            buf = buf + "\n" + line if buf else line
    if buf:
        chunks.append(buf)
    return chunks


def send_telegram(text: str) -> bool:
    token = os.getenv("TELEGRAM_TOKEN") or os.getenv("CLOSING_BET_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CLOSING_BET_CHAT_ID")
    if not token or not chat_id:
        print("⚠️ TELEGRAM_TOKEN/TELEGRAM_CHAT_ID 없음: 텔레그램 전송 생략")
        return False
    ok = True
    for chunk in split_message(text):
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            res = requests.post(url, data={"chat_id": chat_id, "text": chunk}, timeout=10)
            if res.status_code >= 300:
                ok = False
                print(f"⚠️ 텔레그램 전송 실패: {res.status_code} {res.text[:120]}")
            time.sleep(0.25)
        except Exception as e:
            ok = False
            print(f"⚠️ 텔레그램 전송 예외: {e}")
    return ok


# ─────────────────────────────────────────────────────────────────────────────
# 실행부
# ─────────────────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> Tuple[List[CheckResult], str]:
    th = Thresholds(
        min_amount_b=args.min_amount_b,
        min_early_amount_b=args.min_early_amount_b,
        min_early_vratio=args.min_early_vratio,
        min_daily_vratio=args.min_daily_vratio,
        overheat_disparity=args.overheat_disparity,
        upper_wick_warn=args.upper_wick_warn,
    )

    candidate_file = Path(args.candidate_file)
    nxt_file = Path(args.nxt_file)
    candidates = load_candidates(candidate_file, args.codes)
    nxt_map = load_nxt_snapshot(nxt_file) if nxt_file.exists() else {}

    if not candidates:
        msg = (
            "[QUICK]\n"
            f"{SEP}\n"
            "⚠️ 후보군이 없습니다.\n"
            f"- 찾은 파일: {candidate_file}\n"
            "- today_candidates.json을 만들거나 --codes \"005930,000660\" 형식으로 실행하세요.\n"
            f"{SEP}"
        )
        return [], msg

    if args.limit and args.limit > 0:
        candidates = candidates[: args.limit]

    results: List[CheckResult] = []
    for idx, c in enumerate(candidates, 1):
        if not args.quiet:
            print(f"[{idx}/{len(candidates)}] {c.name}({c.code}) 확인 중...")
        q = fetch_quote_naver(c.code)
        d = fetch_daily_info(c.code)
        minute = fetch_minute_naver(c.code, window_min=args.window_min) if args.use_minute else MinuteInfo(code=c.code)
        nxt = nxt_map.get(c.code)

        # 시세 실패해도 결과에 남김
        if not q.ok:
            r = CheckResult(
                code=c.code,
                name=c.name or c.code,
                status=c.status,
                grade=c.grade,
                source=c.source,
                price=0,
                change_rate=0.0,
                amount_b=0.0,
                prev_close=d.prev_close,
                prev_high=d.prev_high,
                ma5=round(d.ma5, 2),
                vma20=round(d.vma20, 2),
                volume_ratio_daily=0.0,
                early_amount_b=0.0,
                early_volume_ratio=0.0,
                nxt_price=nxt.nxt_price if nxt else 0,
                nxt_high=nxt.nxt_high if nxt else 0,
                structure="시세조회실패",
                trade_decision="제외",
                action="시세 확인 실패",
                score=0,
                reasons=[],
                warnings=[q.error or "시세 확인 실패"],
                conditions=["재조회 필요"],
                meaning="현재가를 확인하지 못해 매매 판단에서 제외합니다.",
                raw={"candidate": c.raw or {}, "quote": asdict(q), "daily": asdict(d), "minute": asdict(minute), "nxt": asdict(nxt) if nxt else None},
            )
        else:
            r = judge_candidate(c, q, d, minute, nxt, th)
        results.append(r)
        time.sleep(args.sleep)

    report = format_report(results)
    return results, report


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Stock-Hunter 정규장 초반 후보군 미니 확인기")
    p.add_argument("--candidate-file", default="today_candidates.json", help="후보군 JSON 파일")
    p.add_argument("--nxt-file", default="nxt_snapshot.json", help="NXT 스냅샷 JSON 파일 선택")
    p.add_argument("--codes", default="", help="직접 종목코드 입력: '005930,000660'")
    p.add_argument("--limit", type=int, default=0, help="확인 종목 수 제한")
    p.add_argument("--window-min", type=int, default=5, help="정규장 초반 확인 분 단위")
    p.add_argument("--no-minute", dest="use_minute", action="store_false", help="Naver 분봉 조회 생략")
    p.set_defaults(use_minute=True)
    p.add_argument("--min-amount-b", type=float, default=float(os.getenv("QUICK_MIN_AMOUNT_B", "3.0")), help="정규장 누적 거래대금 최소 억원")
    p.add_argument("--min-early-amount-b", type=float, default=float(os.getenv("QUICK_MIN_EARLY_AMOUNT_B", "1.0")), help="초반 거래대금 최소 억원")
    p.add_argument("--min-early-vratio", type=float, default=float(os.getenv("QUICK_MIN_EARLY_VRATIO", "0.04")), help="초반 거래량/VMA20 최소 비율")
    p.add_argument("--min-daily-vratio", type=float, default=float(os.getenv("QUICK_MIN_DAILY_VRATIO", "0.06")), help="누적 거래량/VMA20 최소 비율")
    p.add_argument("--overheat-disparity", type=float, default=float(os.getenv("QUICK_OVERHEAT_DISP", "116")), help="전일종가 대비 과열 기준")
    p.add_argument("--upper-wick-warn", type=float, default=float(os.getenv("QUICK_UPPER_WICK_WARN", "45")), help="초반 윗꼬리 경고 기준")
    p.add_argument("--sleep", type=float, default=float(os.getenv("QUICK_SLEEP", "0.12")), help="종목별 조회 간격")
    p.add_argument("--out-text", default="quick_open_result.txt", help="텍스트 결과 저장 파일")
    p.add_argument("--out-json", default="quick_open_result.json", help="JSON 결과 저장 파일")
    p.add_argument("--send-telegram", action="store_true", help="텔레그램 전송")
    p.add_argument("--quiet", action="store_true", help="진행 로그 최소화")
    return p


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    results, report = run(args)

    print(report)
    write_text(Path(args.out_text), report)
    write_json(Path(args.out_json), [asdict(r) for r in results])

    if args.send_telegram:
        send_telegram(report)

    # 조건부 실행 후보가 있으면 exit code 0, 후보가 없어도 오류는 아님
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
