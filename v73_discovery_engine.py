from __future__ import annotations

import csv
import json
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import requests

V73_VERSION = "V73.2.1"
V73_POLICY = "LISTING_MULTISOURCE_CACHE_DTYPE_EMPTY_GUARD"


# One stock can belong to multiple active themes.  This is deliberately explicit
# for names whose official industry label hides their actual trading theme.
EXPLICIT_MULTI_SECTOR: Dict[str, Tuple[str, ...]] = {
    "119850": ("발전솔루션/비상전력", "AI전력/전력설비", "데이터센터 인프라", "저탄소/신재생", "에너지 통합"),
    "475150": ("신재생/전력개발", "데이터센터 전력", "에너지 통합"),
    "389260": ("신재생/전력개발", "에너지 통합"),
    "100130": ("신재생/전력개발", "에너지 통합"),
    "112610": ("풍력/전력개발", "신재생/전력개발", "에너지 통합"),
    "322000": ("태양광/전력개발", "신재생/전력개발", "에너지 통합"),
    "010950": ("정유/석유/가스", "에너지 통합"),
    "096770": ("정유/석유/가스", "에너지 통합"),
    "034020": ("원전/전력설비", "에너지 통합"),
    "052690": ("원전/전력설비", "에너지 통합"),
    "130660": ("원전/전력설비", "AI전력/전력설비", "에너지 통합"),
    "015760": ("원전/전력설비", "AI전력/전력설비", "에너지 통합"),
    "010120": ("AI전력/전력설비", "전력기기/배전"),
    "267260": ("AI전력/전력설비", "전력기기/배전"),
    "298040": ("AI전력/전력설비", "전력기기/배전"),
    "001440": ("전선/전력망", "AI전력/전력설비"),
    "006260": ("전선/전력망", "AI전력/전력설비"),
    # V73.1: frequently blank/ambiguous listing industries are fixed explicitly.
    "333430": ("조선기자재", "환경설비"),
    "460930": ("조선기자재",),
    "078930": ("지주/복합산업", "정유/석유/가스", "에너지 통합"),
    "413630": ("산업자동화/부품", "로봇/자동화"),
    "011200": ("해운",),
    "282330": ("유통/편의점",),
    "298000": ("화학",),
    "001450": ("보험", "금융/은행"),
    "017940": ("LPG/에너지", "정유/석유/가스", "에너지 통합"),
}

PROFILE_LABELS = {
    "DISCOVERY_A": "바닥 거래량 첫 유입",
    "DISCOVERY_B": "60일 박스 첫 돌파",
    "DISCOVERY_C": "장기이평 밀집 회복",
    "DISCOVERY_D": "1파 후 첫눌림 재출발",
    "DISCOVERY_E": "섹터 상대강도 급상승",
    "DISCOVERY_F": "10·20일 지역박스 첫 돌파",
    "DISCOVERY_G": "견고한 종가·피벗 저항 회복",
}


def _env_int(name: str, default: int, low: int = 0, high: int = 100000) -> int:
    try:
        return max(low, min(high, int(float(str(os.getenv(name, default)).strip()))))
    except Exception:
        return default


def _env_float(name: str, default: float, low: float = -1e12, high: float = 1e12) -> float:
    try:
        return max(low, min(high, float(str(os.getenv(name, default)).strip())))
    except Exception:
        return default


def _num(v, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, str):
            v = v.replace(",", "").replace("%", "").replace("원", "").strip()
            if not v or v.lower() == "nan":
                return default
        x = float(v)
        return default if not math.isfinite(x) else x
    except Exception:
        return default


def _code(v) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s[-6:].zfill(6) if s else ""


def _pick_col(df: pd.DataFrame, names: Sequence[str]) -> Optional[str]:
    return next((x for x in names if x in df.columns), None)


def _amount_to_eok(series: pd.Series) -> pd.Series:
    x = _to_numeric_clean(series)
    nz = x[x > 0]
    if nz.empty:
        return x
    # KRX/FDR listing Amount is normally KRW.  Some cached sources already use 억원.
    return x / 1e8 if float(nz.median()) > 1e5 else x


def _empty_listing_frame() -> pd.DataFrame:
    """Return an empty normalized listing with stable dtypes.

    pandas creates object columns for ``DataFrame(columns=[...])``.  That is
    enough to make ``nlargest`` raise even when the frame has zero rows.
    """
    return pd.DataFrame({
        "code": pd.Series(dtype="string"),
        "name": pd.Series(dtype="string"),
        "market": pd.Series(dtype="string"),
        "industry": pd.Series(dtype="string"),
        "price": pd.Series(dtype="float64"),
        "amount_eok": pd.Series(dtype="float64"),
        "marcap_eok": pd.Series(dtype="float64"),
        "change_pct": pd.Series(dtype="float64"),
    })


def _to_numeric_clean(series: pd.Series) -> pd.Series:
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    if series.dtype == object or pd.api.types.is_string_dtype(series):
        series = (series.astype(str)
                  .str.replace(",", "", regex=False)
                  .str.replace("%", "", regex=False)
                  .str.strip())
    return pd.to_numeric(series, errors="coerce").fillna(0.0).astype("float64")


def normalize_listing(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return _empty_listing_frame()
    df = raw.copy()
    c_code = _pick_col(df, ["Code", "code", "Symbol", "종목코드", "티커"])
    c_name = _pick_col(df, ["Name", "name", "종목명"])
    c_market = _pick_col(df, ["Market", "market", "시장"])
    c_sector = _pick_col(df, ["Sector", "Industry", "업종", "산업"])
    c_price = _pick_col(df, ["Close", "Price", "현재가", "종가"])
    c_amount = _pick_col(df, ["Amount", "amount_eok", "거래대금", "TradingValue"])
    c_marcap = _pick_col(df, ["Marcap", "MarCap", "marcap_eok", "MarketCap", "시가총액"])
    c_change = _pick_col(df, ["ChangeRate", "change_pct", "ChagesRatio", "Change", "등락률"])

    out = pd.DataFrame(index=df.index)
    out["code"] = df[c_code].map(_code) if c_code else ""
    out["name"] = df[c_name].fillna("").astype(str).str.strip() if c_name else out["code"]
    out["market"] = df[c_market].fillna("").astype(str).str.strip() if c_market else ""
    out["industry"] = df[c_sector].fillna("").astype(str).str.strip() if c_sector else ""
    out["price"] = _to_numeric_clean(df[c_price]) if c_price else pd.Series(0.0, index=df.index, dtype="float64")
    out["amount_eok"] = _amount_to_eok(df[c_amount]) if c_amount else 0.0
    if c_marcap:
        m = _to_numeric_clean(df[c_marcap])
        nz = m[m > 0]
        out["marcap_eok"] = m / 1e8 if (not nz.empty and float(nz.median()) > 1e6) else m
    else:
        out["marcap_eok"] = pd.Series(0.0, index=df.index, dtype="float64")
    out["change_pct"] = _to_numeric_clean(df[c_change]) if c_change else pd.Series(0.0, index=df.index, dtype="float64")
    # Some providers expose Change as a decimal ratio.
    nz = out.loc[out["change_pct"].abs() > 0, "change_pct"]
    if not nz.empty and float(nz.abs().median()) < 0.5:
        out["change_pct"] *= 100.0
    for _col in ("price", "amount_eok", "marcap_eok", "change_pct"):
        out[_col] = _to_numeric_clean(out[_col])
    out = out[out["code"].str.len().eq(6)].copy()
    out = out[~out["name"].str.contains(r"ETF|ETN|스팩|제\d+호|우$|우A$|우B$|우C$", regex=True, na=False)]
    return out.drop_duplicates("code", keep="first").reset_index(drop=True)


def multi_sector_tags(code: str, name: str, industry: str) -> Tuple[str, ...]:
    c = _code(code)
    if c in EXPLICIT_MULTI_SECTOR:
        return EXPLICIT_MULTI_SECTOR[c]
    text = f"{name} {industry}".lower()
    tags: List[str] = []

    rules = [
        (("비상발전", "발전기", "가스터빈", "바이오가스"), "발전솔루션/비상전력"),
        (("데이터센터", "전력설비", "전기장비", "변압기", "배전"), "AI전력/전력설비"),
        (("전선", "케이블"), "전선/전력망"),
        (("태양광", "풍력", "신재생", "연료전지"), "신재생/전력개발"),
        (("원자력", "원전", "우라늄"), "원전/전력설비"),
        (("정유", "석유", "가스"), "정유/석유/가스"),
        (("반도체",), "반도체"),
        (("로봇", "자동화"), "로봇/자동화"),
        (("2차전지", "배터리"), "2차전지/EV"),
        (("조선", "해운"), "조선/해운"),
        (("바이오", "제약"), "바이오"),
        (("금융", "은행", "보험"), "금융/은행"),
        (("건설", "토목"), "건설/인프라"),
    ]
    for keys, tag in rules:
        if any(k in text for k in keys):
            tags.append(tag)
    if any(t in tags for t in ("발전솔루션/비상전력", "AI전력/전력설비", "신재생/전력개발", "원전/전력설비", "정유/석유/가스")):
        tags.append("에너지 통합")
    if not tags:
        tags.append(industry.strip() or "미분류")
    return tuple(dict.fromkeys(tags))


def _valid_sector_tag(tag: str) -> bool:
    t = str(tag or "").strip()
    return bool(t) and t not in {"미분류", "에너지 통합", "기타", "기타서비스"}


def _signal_phase(m: Mapping[str, float], profiles: Sequence[str]) -> str:
    # V73.3.2: overextension is a higher-order safety state.  A robust pivot
    # recovery must never relabel a +20~30% chase bar as a first ignition.
    ret = _num(m.get("ret1"))
    high = _num(m.get("high")); close = _num(m.get("close"))
    giveback = ((high - close) / high * 100.0) if high > 0 and close > 0 else 0.0
    supports = [_num(m.get("ma5")), _num(m.get("ma20")), _num(m.get("robust20_resistance")) * 0.98]
    supports = [x for x in supports if 0 < x < close]
    preview_risk = (1.0 - max(supports) / close) * 100.0 if supports and close > 0 else 999.0
    if ret >= 12.0 or preview_risk > 8.0 or giveback >= 8.0:
        return "LATE_SPIKE"
    if bool(m.get("restart")) or "DISCOVERY_D" in profiles:
        return "RESTART_CONFIRMED"
    if bool(m.get("pullback_setup")) and not profiles:
        return "PULLBACK_SETUP"
    # A robust close/pivot ignition below the raw max-high is a discovery
    # state, not a completed breakout.  Keep it separate from chaseable labels.
    if "DISCOVERY_G" in profiles and max(_num(m.get("break10_pct"), -999), _num(m.get("break20_pct"), -999)) < 0:
        return "BASE_IGNITION"
    vr = _num(m.get("vol_ratio20"))
    if ret >= 8.0:
        return "CONFIRMED_BREAKOUT"
    if profiles and (vr >= 1.2 or any(p in profiles for p in ("DISCOVERY_B", "DISCOVERY_C", "DISCOVERY_F", "DISCOVERY_G"))):
        return "EARLY_DISCOVERY"
    if profiles:
        return "LOW_CONFIRMATION"
    return "NO_SIGNAL"


def _business_gap(start, end) -> int:
    try:
        a = np.datetime64(pd.Timestamp(start).date(), "D")
        b = np.datetime64(pd.Timestamp(end).date(), "D")
        if b <= a:
            return 0
        return int(np.busday_count(a + np.timedelta64(1, "D"), b + np.timedelta64(1, "D")))
    except Exception:
        return max(0, int((pd.Timestamp(end) - pd.Timestamp(start)).days))


def _naver_daily(code: str, count: int = 330, timeout: int = 8) -> pd.DataFrame:
    url = "https://fchart.stock.naver.com/sise.nhn"
    params = {"symbol": _code(code), "timeframe": "day", "count": int(count), "requestType": "0"}
    headers = {"User-Agent": "Mozilla/5.0 StockHunter-V73"}
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    rows = re.findall(r'<item\s+data="([^"]+)"', r.text)
    parsed = []
    for item in rows:
        p = item.split("|")
        if len(p) < 6:
            continue
        try:
            parsed.append((pd.to_datetime(p[0]), float(p[1]), float(p[2]), float(p[3]), float(p[4]), float(p[5])))
        except Exception:
            continue
    if not parsed:
        return pd.DataFrame()
    return pd.DataFrame(parsed, columns=["Date", "Open", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()


def fetch_daily(code: str, count: int = 330, end_date: Optional[pd.Timestamp] = None) -> Tuple[pd.DataFrame, str]:
    timeout = _env_int("V73_HTTP_TIMEOUT", 8, 2, 30)
    try:
        df = _naver_daily(code, count=count, timeout=timeout)
        if end_date is not None and not df.empty:
            df = df[df.index.normalize() <= pd.Timestamp(end_date).normalize()]
        if len(df) >= 80:
            return df, "NAVER_CHART"
    except Exception:
        pass
    try:
        import FinanceDataReader as fdr
        end = pd.Timestamp(end_date).normalize() + pd.Timedelta(days=1) if end_date is not None else pd.Timestamp.now().normalize() + pd.Timedelta(days=1)
        start = end - pd.Timedelta(days=max(500, count * 2))
        df = fdr.DataReader(_code(code), start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        if df is not None:
            df = df.rename(columns={c: c.title() for c in df.columns})
            if end_date is not None:
                df = df[df.index.normalize() <= pd.Timestamp(end_date).normalize()]
            return df[["Open", "High", "Low", "Close", "Volume"]].dropna(), "FDR_FALLBACK"
    except Exception:
        pass
    return pd.DataFrame(), "FETCH_FAIL"


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff()).fillna(0.0)
    return (direction * volume.fillna(0.0)).cumsum()


def _metrics(df: pd.DataFrame) -> Optional[dict]:
    if df is None or len(df) < 80:
        return None
    d = df.copy().sort_index()
    for c in ("Open", "High", "Low", "Close", "Volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    if len(d) < 80:
        return None
    c, o, h, l, v = (float(d[x].iloc[-1]) for x in ("Close", "Open", "High", "Low", "Volume"))
    pc = float(d["Close"].iloc[-2])
    if min(c, o, h, l, pc) <= 0:
        return None
    ma = {n: float(d["Close"].rolling(n).mean().iloc[-1]) if len(d) >= n else np.nan for n in (5, 10, 20, 60, 112, 224)}
    prev_ma = {n: float(d["Close"].rolling(n).mean().iloc[-2]) if len(d) > n else np.nan for n in (20, 60, 112, 224)}
    vma20 = float(d["Volume"].tail(20).mean())
    vma5 = float(d["Volume"].tail(5).mean())
    amount_eok = c * v / 1e8
    amount5 = float((d["Close"] * d["Volume"]).tail(5).mean() / 1e8)
    amount20 = float((d["Close"] * d["Volume"]).tail(20).mean() / 1e8)
    vol_ratio = v / vma20 if vma20 > 0 else 0.0
    ret1 = (c / pc - 1.0) * 100.0
    body = (c / o - 1.0) * 100.0
    range_pos = (c - l) / max(h - l, 1e-9)
    prior5 = float(d["High"].iloc[-6:-1].max()) if len(d) >= 6 else np.nan
    prior10 = float(d["High"].iloc[-11:-1].max()) if len(d) >= 11 else np.nan
    prior20 = float(d["High"].iloc[-21:-1].max()) if len(d) >= 21 else np.nan
    prior60 = float(d["High"].iloc[-61:-1].max()) if len(d) >= 61 else np.nan
    prior5_close = float(d["Close"].iloc[-6:-1].max()) if len(d) >= 6 else np.nan
    prior10_close = float(d["Close"].iloc[-11:-1].max()) if len(d) >= 11 else np.nan
    prior20_close = float(d["Close"].iloc[-21:-1].max()) if len(d) >= 21 else np.nan
    prior10_high_q80 = float(d["High"].iloc[-11:-1].quantile(0.80)) if len(d) >= 11 else np.nan
    prior20_high_q80 = float(d["High"].iloc[-21:-1].quantile(0.80)) if len(d) >= 21 else np.nan
    # A single historical wick must not define the whole local box.  Robust
    # resistance uses repeated closes plus the upper body of the high
    # distribution, while the raw max-high is retained for overhead supply.
    robust10 = max(prior10_close, prior10_high_q80) if prior10_close > 0 and prior10_high_q80 > 0 else prior10
    robust20 = max(prior20_close, prior20_high_q80) if prior20_close > 0 and prior20_high_q80 > 0 else prior20
    box_low10 = float(d["Low"].iloc[-11:-1].min()) if len(d) >= 11 else np.nan
    box_low20 = float(d["Low"].iloc[-21:-1].min()) if len(d) >= 21 else np.nan
    box_low60 = float(d["Low"].iloc[-61:-1].min()) if len(d) >= 61 else np.nan
    base10_width = (robust10 / box_low10 - 1.0) * 100.0 if robust10 > 0 and box_low10 > 0 else np.nan
    base20_width = (robust20 / box_low20 - 1.0) * 100.0 if robust20 > 0 and box_low20 > 0 else np.nan
    obv = _obv(d["Close"], d["Volume"])
    obv_now = float(obv.iloc[-1])
    obv_5max = float(obv.iloc[-6:-1].max()) if len(obv) >= 6 else obv_now

    long_vals = [ma[n] for n in (20, 60, 112, 224) if math.isfinite(ma[n]) and ma[n] > 0]
    cluster_pct = ((max(long_vals) - min(long_vals)) / c * 100.0) if len(long_vals) >= 3 else np.nan
    long_top = max([ma[n] for n in (112, 224) if math.isfinite(ma[n]) and ma[n] > 0] or [np.nan])
    prev_long_top = max([prev_ma[n] for n in (112, 224) if math.isfinite(prev_ma[n]) and prev_ma[n] > 0] or [np.nan])

    impulse = None
    # An impulse can be a moderate-volume local-box ignition.  Requiring 1.7x
    # volume discarded valid first moves such as 119850 on 2026-06-29.
    for ago in range(2, 13):
        if len(d) <= ago + 21:
            continue
        i = len(d) - 1 - ago
        ic = float(d["Close"].iloc[i]); ipc = float(d["Close"].iloc[i - 1]); io = float(d["Open"].iloc[i])
        ih = float(d["High"].iloc[i]); il = float(d["Low"].iloc[i]); iv = float(d["Volume"].iloc[i])
        ivma = float(d["Volume"].iloc[max(0, i - 20):i].mean())
        iret = (ic / ipc - 1.0) * 100.0 if ipc > 0 else 0.0
        ipos = (ic - il) / max(ih - il, 1e-9)
        ivr = iv / ivma if ivma > 0 else 0.0
        ihist = d.iloc[max(0, i - 20):i]
        iprior20 = float(ihist["High"].max())
        iprior20_close = float(ihist["Close"].max())
        iprior20_q80 = float(ihist["High"].quantile(0.80))
        irobust20 = max(iprior20_close, iprior20_q80) if iprior20_close > 0 and iprior20_q80 > 0 else iprior20
        ibreak20 = (ic / iprior20 - 1.0) * 100.0 if iprior20 > 0 else -999.0
        irobust_break20 = (ic / irobust20 - 1.0) * 100.0 if irobust20 > 0 else -999.0
        strong_impulse = (iret >= 5.0 and ivr >= 1.50) or (iret >= 8.0 and ivr >= 1.20 and irobust_break20 >= -2.0)
        if strong_impulse and ipos >= 0.62 and ic >= io:
            impulse = {"ago": ago, "idx": i, "high": ih, "low": il, "close": ic, "volume": iv,
                       "ret": iret, "vol_ratio": ivr, "break20_pct": ibreak20,
                       "robust_break20_pct": irobust_break20}
            break

    restart = False
    pullback_setup = False
    restart_debug = ""
    pullback_debug = ""
    pullback_pct = np.nan
    if impulse:
        after = d.iloc[impulse["idx"] + 1:]
        pullback_pct = (c / impulse["high"] - 1.0) * 100.0
        min_after = float(after["Low"].min()) if not after.empty else c
        middle = after["Volume"].iloc[:-1] if len(after) > 1 else pd.Series(dtype=float)
        med_vol_after = float(middle.median()) if not middle.empty else v
        support = min_after >= impulse["low"] * 0.94 and c >= min(ma[5], ma[20]) * 0.96 and c >= impulse["low"] * 0.96
        compressed = med_vol_after <= impulse["volume"] * 0.80 and v <= impulse["volume"] * 0.90
        obv_hold = obv_now >= float(obv.iloc[impulse["idx"]]) - abs(impulse["volume"]) * 0.65
        in_pullback_zone = -15.0 <= pullback_pct <= 3.0
        pullback_setup = in_pullback_zone and support and compressed and obv_hold
        reentry = ret1 >= 1.2 and c >= o and v >= max(float(d["Volume"].iloc[-2]) * 1.05, vma5 * 0.9)
        restart = pullback_setup and reentry
        common = (f"impulseD-{impulse['ago']} {impulse['ret']:+.1f}%/V{impulse['vol_ratio']:.1f} "
                  f"raw20 {impulse['break20_pct']:+.1f}%/robust20 {impulse.get('robust_break20_pct', impulse['break20_pct']):+.1f}% · pullback {pullback_pct:+.1f}% · "
                  f"compressed={compressed} support={support} obv={obv_hold}")
        pullback_debug = common
        restart_debug = common + f" reentry={reentry}"

    return {
        "date": d.index[-1].strftime("%Y-%m-%d"), "close": c, "open": o, "high": h, "low": l,
        "ret1": ret1, "body_pct": body, "range_pos": range_pos,
        "high_giveback_pct": ((h - c) / h * 100.0) if h > 0 else 0.0, "volume": v,
        "vol_ratio20": vol_ratio, "amount_eok": amount_eok, "amount5_eok": amount5, "amount20_eok": amount20,
        "prior5_high": prior5, "prior10_high": prior10, "prior20_high": prior20, "prior60_high": prior60,
        "prior5_close_high": prior5_close, "prior10_close_high": prior10_close, "prior20_close_high": prior20_close,
        "robust10_resistance": robust10, "robust20_resistance": robust20,
        "box10_low": box_low10, "box20_low": box_low20, "box60_low": box_low60,
        "base10_width_pct": base10_width, "base20_width_pct": base20_width,
        "break5_pct": (c / prior5 - 1.0) * 100.0 if prior5 > 0 else np.nan,
        "break5_close_pct": (c / prior5_close - 1.0) * 100.0 if prior5_close > 0 else np.nan,
        "break10_pct": (c / prior10 - 1.0) * 100.0 if prior10 > 0 else np.nan,
        "break20_pct": (c / prior20 - 1.0) * 100.0 if prior20 > 0 else np.nan,
        "robust_break10_pct": (c / robust10 - 1.0) * 100.0 if robust10 > 0 else np.nan,
        "robust_break20_pct": (c / robust20 - 1.0) * 100.0 if robust20 > 0 else np.nan,
        "wick_overhang20_pct": (prior20 / robust20 - 1.0) * 100.0 if prior20 > 0 and robust20 > 0 else np.nan,
        "break60_pct": (c / prior60 - 1.0) * 100.0 if prior60 > 0 else np.nan,
        "ma5": ma[5], "ma10": ma[10], "ma20": ma[20], "ma60": ma[60], "ma112": ma[112], "ma224": ma[224],
        "cluster_pct": cluster_pct, "long_top": long_top, "prev_long_top": prev_long_top,
        "obv_now": obv_now, "obv_5max": obv_5max, "obv_break": obv_now >= obv_5max,
        "impulse_found": bool(impulse), "impulse_ago": impulse["ago"] if impulse else np.nan,
        "impulse_ret": impulse["ret"] if impulse else np.nan, "impulse_vol_ratio": impulse["vol_ratio"] if impulse else np.nan,
        "pullback_setup": pullback_setup, "pullback_debug": pullback_debug,
        "restart": restart, "restart_debug": restart_debug, "pullback_pct": pullback_pct,
    }

def detect_profiles(m: Mapping[str, float]) -> Tuple[List[str], List[str]]:
    profiles: List[str] = []
    reasons: List[str] = []
    amount = _num(m.get("amount_eok")); vr = _num(m.get("vol_ratio20")); ret = _num(m.get("ret1"))
    pos = _num(m.get("range_pos")); body = _num(m.get("body_pct")); close = _num(m.get("close")); ma20 = _num(m.get("ma20"))
    b5c = _num(m.get("break5_close_pct"), -999)
    b10 = _num(m.get("break10_pct"), -999); b20 = _num(m.get("break20_pct"), -999); b60 = _num(m.get("break60_pct"), -999)
    rb10 = _num(m.get("robust_break10_pct"), b10); rb20 = _num(m.get("robust_break20_pct"), b20)
    wick20 = _num(m.get("wick_overhang20_pct"), 0.0)
    obv_break = bool(m.get("obv_break"))

    # A: first ignition.  Moderate volume is allowed only with a powerful local
    # breakout and OBV confirmation, preventing a blanket threshold reduction.
    robust_local_break = max(b5c, rb10, rb20)
    volume_ok = vr >= 1.45 or (vr >= 1.20 and ret >= 8.0 and obv_break)
    if amount >= 10 and volume_ok and ret >= 4.0 and body >= 2.5 and pos >= 0.62 and close >= ma20 * 0.97 and robust_local_break >= -2.0:
        profiles.append("DISCOVERY_A")
        reasons.append(f"거래량 {vr:.1f}배·당일 {ret:+.1f}%·견고저항 {robust_local_break:+.1f}%·OBV {'돌파' if obv_break else '유지'}")

    # F: local base breakout is independent from a distant 60-day overhead high.
    local_break = max(rb10, rb20)
    base_width = min(_num(m.get("base10_width_pct"), 999), _num(m.get("base20_width_pct"), 999))
    if amount >= 8 and vr >= 1.15 and ret >= 3.0 and local_break >= -0.8 and pos >= 0.62 and body >= 1.5 and base_width <= 35.0 and obv_break:
        profiles.append("DISCOVERY_F")
        reasons.append(f"견고한 지역박스 돌파 {local_break:+.1f}%·폭 {base_width:.1f}%·거래량 {vr:.1f}배")

    # G: a high-energy candle may start a new cycle while still below a single
    # old wick.  This profile requires a robust close/pivot reclaim, strong
    # candle quality and OBV confirmation, so it does not simply lower the
    # max-high breakout threshold.
    strong_price_confirm = ret >= 12.0 and body >= 5.0 and pos >= 0.65 and vr >= 1.25
    obv_or_price_confirm = obv_break or strong_price_confirm
    raw_local_break = max(b10, b20)
    resistance_ok = robust_local_break >= -1.5 or (strong_price_confirm and raw_local_break >= -12.0)
    if amount >= 8 and vr >= 1.20 and ret >= 8.0 and body >= 5.0 and pos >= 0.65 and obv_or_price_confirm and close >= ma20 * 0.95 and resistance_ok:
        profiles.append("DISCOVERY_G")
        confirm = "OBV돌파" if obv_break else "강봉가격확정"
        reasons.append(f"강봉 {ret:+.1f}%·V{vr:.2f}·원고점 {raw_local_break:+.1f}%·견고저항 {robust_local_break:+.1f}%·윗꼬리초과 {wick20:.1f}%·{confirm}")

    if amount >= 18 and vr >= 1.45 and b60 >= 0.2 and pos >= 0.65 and body >= 1.0:
        profiles.append("DISCOVERY_B")
        reasons.append(f"60일박스 {b60:+.1f}% 돌파·고가유지 {pos*100:.0f}%")
    cluster = _num(m.get("cluster_pct"), 999); long_top = _num(m.get("long_top")); prev_long = _num(m.get("prev_long_top")); pc_est = close / max(1.0 + ret / 100.0, 1e-9)
    if amount >= 10 and vr >= 1.15 and long_top > 0 and close >= long_top and pc_est <= max(prev_long, long_top) * 1.035 and cluster <= 16.0 and pos >= 0.55:
        profiles.append("DISCOVERY_C")
        reasons.append(f"장기선 회복·이평밀집 {cluster:.1f}%·거래량 {vr:.1f}배")
    if bool(m.get("restart")) and amount >= 8:
        profiles.append("DISCOVERY_D")
        reasons.append(str(m.get("restart_debug") or "첫눌림 재출발"))
    return list(dict.fromkeys(profiles)), reasons


def _profile_score(m: Mapping[str, float], profiles: Sequence[str], sector_excess: float = 0.0) -> float:
    weights = {"DISCOVERY_A": 22, "DISCOVERY_B": 24, "DISCOVERY_C": 18, "DISCOVERY_D": 30, "DISCOVERY_E": 12, "DISCOVERY_F": 24, "DISCOVERY_G": 26}
    s = sum(weights.get(p, 0) for p in set(profiles))
    vr = _num(m.get("vol_ratio20")); ret = _num(m.get("ret1")); pos = _num(m.get("range_pos"))
    s += min(10.0, max(0.0, vr - 1.0) * 3.5)
    s += min(10.0, math.log10(max(_num(m.get("amount_eok")), 1.0)) * 4.0)
    s += max(0.0, min(7.0, (pos - 0.5) * 18.0))
    s += max(0.0, min(7.0, sector_excess * 1.0))
    if 2.0 <= ret < 8.0:
        s += 8.0
    elif 8.0 <= ret < 12.0:
        s += 5.0
    elif 12.0 <= ret < 18.0:
        s += 2.0
    elif ret >= 18.0:
        s -= 12.0
    if pos < 0.45:
        s -= 10.0
    if _num(m.get("close")) < _num(m.get("ma20")):
        s -= 8.0
    if set(profiles) == {"DISCOVERY_E"} and vr < 1.0:
        s = min(s, 35.0)
    return round(max(0.0, min(100.0, s)), 1)

def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype={"code": str}) if path.exists() else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _append_dedup(path: Path, rows: pd.DataFrame, keys: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    old = _read_csv(path)
    out = pd.concat([old, rows], ignore_index=True, sort=False) if not old.empty else rows.copy()
    if not out.empty:
        for k in keys:
            if k in out.columns:
                out[k] = out[k].astype(str)
        out = out.drop_duplicates(list(keys), keep="last")
    out.to_csv(path, index=False, encoding="utf-8-sig")


def _prior_lifecycle(path: Path) -> pd.DataFrame:
    df = _read_csv(path)
    if not df.empty and "signal_date" in df.columns:
        df["signal_date"] = pd.to_datetime(df["signal_date"], errors="coerce")
    return df


def _lifecycle_state(code: str, signal_date: str, profiles: Sequence[str], prior: pd.DataFrame) -> Tuple[str, str, int]:
    now = pd.Timestamp(signal_date)
    if prior.empty or "code" not in prior.columns:
        return "NEW_DISCOVERY", signal_date, 0
    p = prior[prior["code"].astype(str).str.zfill(6).eq(_code(code))].dropna(subset=["signal_date"]).sort_values("signal_date")
    if p.empty:
        return "NEW_DISCOVERY", signal_date, 0
    p_signal = p[p.get("profiles", pd.Series(index=p.index, dtype=str)).fillna("").astype(str).str.len() > 0] if "profiles" in p.columns else p
    if p_signal.empty:
        p_signal = p
    last = pd.Timestamp(p_signal.iloc[-1]["signal_date"])
    first = pd.Timestamp(p_signal.iloc[0]["signal_date"])
    gap = _business_gap(last, now)
    if "DISCOVERY_D" in profiles:
        state = "RESTART_CONFIRMED"
    elif gap <= 2:
        state = "TRACKING_CONTINUE"
    elif gap <= 15:
        state = "REACTIVATED"
    else:
        state = "NEW_CYCLE"
        first = now
    return state, first.strftime("%Y-%m-%d"), gap

def build_dynamic_universe(listing: pd.DataFrame, prior_lifecycle: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    min_price = _env_float("V73_MIN_PRICE", 3000, 100, 1e7)
    min_marcap = _env_float("V73_MIN_MARCAP_EOK", 300, 0, 1e8)
    limit = _env_int("V73_DISCOVERY_UNIVERSE_LIMIT", 900, 50, 2500)
    top_amount_n = _env_int("V73_TOP_AMOUNT_POOL", 750, 50, 2500)

    _normalized_cols = {"code", "name", "market", "industry", "price", "amount_eok", "marcap_eok", "change_pct"}
    if isinstance(listing, pd.DataFrame) and _normalized_cols.issubset(set(listing.columns)):
        x = listing.copy()
    else:
        x = normalize_listing(listing) if listing is not None else _empty_listing_frame()
    for _col in ("price", "amount_eok", "marcap_eok", "change_pct"):
        if _col not in x.columns:
            x[_col] = pd.Series(0.0, index=x.index, dtype="float64")
        else:
            x[_col] = _to_numeric_clean(x[_col])
    if x.empty:
        empty = x.copy()
        empty["basic_pass"] = pd.Series(dtype="bool")
        empty["universe_reason"] = pd.Series(dtype="string")
        return empty, pd.DataFrame({
            "code": pd.Series(dtype="string"), "name": pd.Series(dtype="string"),
            "market": pd.Series(dtype="string"), "industry": pd.Series(dtype="string"),
            "price": pd.Series(dtype="float64"), "amount_eok": pd.Series(dtype="float64"),
            "marcap_eok": pd.Series(dtype="float64"), "change_pct": pd.Series(dtype="float64"),
            "stage": pd.Series(dtype="string"), "result": pd.Series(dtype="string"),
            "reason": pd.Series(dtype="string"),
        })
    x["basic_pass"] = (x["price"] >= min_price) & ((x["marcap_eok"] <= 0) | (x["marcap_eok"] >= min_marcap))
    basic = x[x["basic_pass"]].copy()
    prior_codes = set()
    if prior_lifecycle is not None and not prior_lifecycle.empty and "code" in prior_lifecycle.columns:
        pl = prior_lifecycle.copy()
        pl["code"] = pl["code"].astype(str).str.zfill(6)
        if "signal_date" in pl.columns:
            pl["signal_date"] = pd.to_datetime(pl["signal_date"], errors="coerce")
            pl = pl.dropna(subset=["signal_date"]).sort_values("signal_date").groupby("code", as_index=False).tail(1)
            now_d = pd.Timestamp.now().normalize()
            pl["_age"] = pl["signal_date"].map(lambda x: _business_gap(x, now_d))
            active_states = {"NEW_DISCOVERY", "NEW_CYCLE", "TRACKING_CONTINUE", "TRACKING_NO_NEW_SIGNAL", "REACTIVATED", "PULLBACK_SETUP", "RESTART_CONFIRMED"}
            if "state" in pl.columns:
                pl = pl[pl["state"].astype(str).isin(active_states)]
            pl = pl[pl["_age"] <= 15]
        prior_codes = set(pl["code"].tail(5000))
    explicit = set(EXPLICIT_MULTI_SECTOR)

    basic["universe_reason"] = ""
    top_codes = set(basic.nlargest(min(top_amount_n, len(basic)), "amount_eok")["code"]) if not basic.empty else set()
    momentum_codes = set(basic[(basic["change_pct"] >= 2.0) & (basic["amount_eok"] >= 3.0)]["code"])
    strategic_codes = set(basic[basic["code"].isin(explicit)]["code"])
    tracked_codes = set(basic[basic["code"].isin(prior_codes)]["code"])
    selected = top_codes | momentum_codes | strategic_codes | tracked_codes

    def reason(c: str) -> str:
        r = []
        if c in top_codes: r.append("AMOUNT_POOL")
        if c in momentum_codes: r.append("MOMENTUM_ESCAPE")
        if c in strategic_codes: r.append("STRATEGIC_MULTI_SECTOR")
        if c in tracked_codes: r.append("LIFECYCLE_TRACKED")
        return "+".join(r)

    cand = basic[basic["code"].isin(selected)].copy()
    cand["universe_reason"] = cand["code"].map(reason)
    cand["priority"] = (
        cand["amount_eok"].rank(pct=True) * 45
        + cand["change_pct"].clip(-10, 30) * 1.5
        + cand["code"].isin(strategic_codes).astype(int) * 30
        + cand["code"].isin(tracked_codes).astype(int) * 20
    )
    cand = cand.sort_values(["priority", "amount_eok"], ascending=False).head(limit).reset_index(drop=True)

    selected_final = set(cand["code"])
    rej = x[["code", "name", "market", "industry", "price", "amount_eok", "marcap_eok", "change_pct"]].copy()
    rej["stage"] = np.where(~x["basic_pass"], "BASIC_FILTER", np.where(rej["code"].isin(selected_final), "DISCOVERY_UNIVERSE", "DYNAMIC_UNIVERSE"))
    rej["result"] = np.where(rej["code"].isin(selected_final), "PASS", "FAIL")
    rej["reason"] = np.where(~x["basic_pass"], "PRICE_OR_MARCAP", np.where(rej["code"].isin(selected_final), "SELECTED", "NOT_AMOUNT_MOMENTUM_TRACKED_OR_STRATEGIC"))
    return cand, rej


def run_discovery(
    listing_raw: pd.DataFrame,
    output_dir: str = "reports",
    fetcher: Callable[[str, int, Optional[pd.Timestamp]], Tuple[pd.DataFrame, str]] = fetch_daily,
    now: Optional[datetime] = None,
) -> Tuple[str, pd.DataFrame, dict]:
    started = time.monotonic()
    now = now or datetime.now()
    today = now.strftime("%Y-%m-%d")
    outdir = Path(output_dir)
    paths = {
        "stage": outdir / "v73_discovery_stage_ledger.csv",
        "reject": outdir / "v73_filter_rejection_ledger.csv",
        "sector": outdir / "v73_sector_mapping_audit.csv",
        "life": outdir / "v73_signal_lifecycle_ledger.csv",
        "snapshot": outdir / "v73_latest_discovery_snapshot.csv",
    }
    listing = normalize_listing(listing_raw)
    prior = _prior_lifecycle(paths["life"])
    if listing.empty:
        source = os.environ.get("V73_LISTING_SOURCE", "UNAVAILABLE")
        report = (
            "🧭 [V73 Discovery → Tracking → Execution]\n"
            f"📌 {V73_VERSION} | {today} | SAFE STOP / 기존 V72.29 STRICT 변경 없음\n"
            f"🚨 종목목록 사용 불가 | source={source}\n"
            "- KRX/FDR·pykrx·보존 캐시가 모두 비어 있어 Discovery 계산을 실행하지 않았습니다.\n"
            "- 빈 유니버스를 전일 데이터나 임의 종목으로 대체하지 않았으며, 다음 실행에서 재시도합니다."
        )
        fail = pd.DataFrame([{
            "scan_date": today, "code": "", "name": "", "stage": "LISTING_LOAD",
            "result": "FAIL", "reason": "LISTING_UNAVAILABLE", "policy_version": V73_VERSION,
        }])
        _append_dedup(paths["reject"], fail, ["scan_date", "code", "stage"])
        return report, listing, {"listing": 0, "universe": 0, "signals": 0, "fetch_fail": 0, "safe_stop": True}
    universe, rejection = build_dynamic_universe(listing, prior)

    workers = _env_int("V73_DISCOVERY_WORKERS", 10, 1, 24)
    count = _env_int("V73_HISTORY_COUNT", 330, 120, 600)
    records: List[dict] = []
    fetch_fail: List[dict] = []

    def task(row: dict):
        df, source = fetcher(row["code"], count, None)
        m = _metrics(df)
        return row, m, source

    rows = universe.to_dict("records")
    executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="v73disc")
    futures = [executor.submit(task, r) for r in rows]
    try:
        for fut in as_completed(futures):
            try:
                row, m, source = fut.result()
            except Exception as e:
                fetch_fail.append({"scan_date": today, "stage": "HISTORY_FETCH", "result": "FAIL", "reason": type(e).__name__})
                continue
            tags = multi_sector_tags(row["code"], row["name"], row["industry"])
            if not m:
                fetch_fail.append({"scan_date": today, "code": row["code"], "name": row["name"], "stage": "HISTORY_METRICS", "result": "FAIL", "reason": source})
                continue
            profiles, reasons = detect_profiles(m)
            rec = dict(row)
            rec.update(m)
            rec.update({
                "scan_date": today, "source": source, "sector_tags": "|".join(tags),
                "profiles": "|".join(profiles), "profile_labels": "|".join(PROFILE_LABELS[p] for p in profiles),
                "reasons": " / ".join(reasons),
            })
            records.append(rec)
    finally:
        executor.shutdown(wait=True, cancel_futures=True)

    frame = pd.DataFrame(records)
    if frame.empty:
        report = (
            "🧭 [V73 Discovery → Tracking → Execution]\n"
            f"📌 {V73_VERSION} | {today} | RESEARCH_ONLY / 기존 STRICT 변경 없음\n"
            f"- 동적 발견 유니버스 {len(universe)}개를 검사했지만 계산 가능한 신호가 없습니다.\n"
            f"- 데이터 실패 {len(fetch_fail)}개 · 전일 데이터로 대체하지 않았습니다."
        )
        rejection["scan_date"] = today
        _append_dedup(paths["reject"], pd.concat([rejection, pd.DataFrame(fetch_fail)], ignore_index=True, sort=False), ["scan_date", "code", "stage"])
        return report, frame, {"universe": len(universe), "signals": 0, "fetch_fail": len(fetch_fail)}

    # Sector-relative profile is evaluated only after all same-date metrics exist.
    expanded = []
    for idx, r in frame.iterrows():
        for tag in str(r["sector_tags"]).split("|"):
            if _valid_sector_tag(tag):
                expanded.append({"idx": idx, "sector": tag, "ret1": _num(r["ret1"]), "amount_eok": _num(r["amount_eok"])})
    ex = pd.DataFrame(expanded)
    sector_med = ex.groupby("sector")["ret1"].median().to_dict() if not ex.empty else {}
    sector_n = ex.groupby("sector").size().to_dict() if not ex.empty else {}

    scores, excesses, final_profiles, final_reasons = [], [], [], []
    for _, r in frame.iterrows():
        tags = str(r["sector_tags"]).split("|")
        valid = [t for t in tags if _valid_sector_tag(t) and sector_n.get(t, 0) >= 3]
        med = float(np.median([sector_med[t] for t in valid])) if valid else np.nan
        excess = _num(r["ret1"]) - med if math.isfinite(med) else 0.0
        profiles = [p for p in str(r["profiles"]).split("|") if p]
        reasons = [x for x in str(r["reasons"]).split(" / ") if x]
        if valid and _num(r["ret1"]) >= 3.0 and excess >= 3.0 and _num(r["amount_eok"]) >= 20 and _num(r["range_pos"]) >= 0.65 and _num(r["vol_ratio20"]) >= 0.9:
            profiles.append("DISCOVERY_E")
            reasons.append(f"유효섹터 중앙 대비 {excess:+.1f}%p")
        profiles = list(dict.fromkeys(profiles))
        scores.append(_profile_score(r, profiles, excess))
        excesses.append(round(excess, 2))
        final_profiles.append("|".join(profiles))
        final_reasons.append(" / ".join(reasons))
    frame["sector_excess_pctp"] = excesses
    frame["profiles"] = final_profiles
    frame["profile_labels"] = frame["profiles"].map(lambda x: "|".join(PROFILE_LABELS[p] for p in x.split("|") if p))
    frame["reasons"] = final_reasons
    frame["discovery_score"] = scores
    frame["signal_phase"] = [
        _signal_phase(r, [p for p in str(r["profiles"]).split("|") if p]) for _, r in frame.iterrows()
    ]
    signal = frame[frame["profiles"].str.len() > 0].copy()

    lifecycle_rows = []
    signal_codes = set(signal["code"].astype(str)) if not signal.empty else set()
    for _, r in signal.iterrows():
        profiles = [p for p in str(r["profiles"]).split("|") if p]
        state, first, gap = _lifecycle_state(r["code"], today, profiles, prior)
        execution_status = (
            "PROMOTION_RESEARCH_READY"
            if state == "RESTART_CONFIRMED" and "DISCOVERY_D" in profiles and _num(r["discovery_score"]) >= 70
            else ("TRACKING_ONLY" if state in ("TRACKING_CONTINUE", "REACTIVATED") else "DISCOVERY_ONLY")
        )
        lifecycle_rows.append({
            "signal_date": today, "code": r["code"], "name": r["name"], "state": state,
            "first_signal_date": first, "days_from_prior_signal": gap, "profiles": r["profiles"],
            "profile_labels": r["profile_labels"], "discovery_score": r["discovery_score"],
            "close": r["close"], "ret1": r["ret1"], "amount_eok": r["amount_eok"],
            "sector_tags": r["sector_tags"], "signal_phase": r.get("signal_phase", ""), "execution_status": execution_status,
            "policy_version": V73_VERSION,
        })

    # A discovered name remains in the tracking ledger even on days with no fresh trigger.
    # This is the core separation between Discovery and Execution.
    if prior is not None and not prior.empty and "code" in prior.columns and "signal_date" in prior.columns:
        prior_valid = prior.dropna(subset=["signal_date"]).copy()
        for code, grp in prior_valid.groupby(prior_valid["code"].astype(str).str.zfill(6)):
            if code in signal_codes:
                continue
            grp = grp.sort_values("signal_date")
            sig_grp = grp[grp.get("profiles", pd.Series(index=grp.index, dtype=str)).fillna("").astype(str).str.len() > 0] if "profiles" in grp.columns else grp
            if sig_grp.empty:
                sig_grp = grp
            last = sig_grp.iloc[-1]
            last_dt = pd.Timestamp(last["signal_date"])
            age = _business_gap(last_dt, pd.Timestamp(today))
            if age > 30:
                continue
            cur = frame[frame["code"].astype(str).eq(code)]
            if cur.empty:
                continue
            r = cur.iloc[0]
            first = str(last.get("first_signal_date", "") or "")
            if not first:
                first = grp.sort_values("signal_date").iloc[0]["signal_date"].strftime("%Y-%m-%d")
            if age <= 15 and bool(r.get("pullback_setup")):
                state = "PULLBACK_SETUP"
                execution_status = "TRACKING_ONLY"
            elif age <= 15:
                state = "TRACKING_NO_NEW_SIGNAL"
                execution_status = "TRACKING_ONLY"
            else:
                state = "CYCLE_EXPIRED"
                execution_status = "ARCHIVED"
            lifecycle_rows.append({
                "signal_date": today, "code": code, "name": r["name"], "state": state,
                "first_signal_date": first if age <= 15 else "", "previous_cycle_first_signal_date": first if age > 15 else "", "days_from_prior_signal": age, "profiles": "",
                "profile_labels": "", "discovery_score": r["discovery_score"],
                "close": r["close"], "ret1": r["ret1"], "amount_eok": r["amount_eok"],
                "sector_tags": r["sector_tags"], "signal_phase": "PULLBACK_SETUP" if state == "PULLBACK_SETUP" else "NO_SIGNAL",
                "pullback_debug": r.get("pullback_debug", ""), "execution_status": execution_status,
                "policy_version": V73_VERSION,
            })
    life = pd.DataFrame(lifecycle_rows)
    if not life.empty and not signal.empty:
        signal_states = life[life["code"].astype(str).isin(signal_codes)][["code", "state", "first_signal_date", "days_from_prior_signal"]]
        signal = signal.merge(signal_states, on="code", how="left")

    # Full stage ledger includes no-signal rows, which is essential for missed-stock audits.
    frame["stage_result"] = np.select(
        [frame["profiles"].str.len() > 0, frame["pullback_setup"].fillna(False).astype(bool)],
        ["SIGNAL", "PULLBACK_SETUP_CANDIDATE"],
        default="NO_SIGNAL",
    )
    frame["execution_status"] = "DISCOVERY_ONLY"
    frame["policy_version"] = V73_VERSION
    frame["scan_timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
    rejection["scan_date"] = today
    fetch_rej = pd.DataFrame(fetch_fail)
    if not fetch_rej.empty and "scan_date" not in fetch_rej.columns:
        fetch_rej["scan_date"] = today

    sector_audit = listing[["code", "name", "market", "industry"]].copy()
    sector_audit["sector_tags"] = ["|".join(multi_sector_tags(r.code, r.name, r.industry)) for r in sector_audit.itertuples()]
    sector_audit["scan_date"] = today

    _append_dedup(paths["stage"], frame, ["scan_date", "code"])
    _append_dedup(paths["reject"], pd.concat([rejection, fetch_rej], ignore_index=True, sort=False), ["scan_date", "code", "stage"])
    _append_dedup(paths["sector"], sector_audit, ["scan_date", "code"])
    if not life.empty:
        _append_dedup(paths["life"], life, ["signal_date", "code"])
    signal.sort_values(["discovery_score", "amount_eok"], ascending=False).to_csv(paths["snapshot"], index=False, encoding="utf-8-sig")

    top_n = _env_int("V73_TELEGRAM_TOP_N", 10, 3, 20)
    phase_order = {"BASE_IGNITION": 0, "EARLY_DISCOVERY": 1, "CONFIRMED_BREAKOUT": 2, "RESTART_CONFIRMED": 3, "LOW_CONFIRMATION": 4, "LATE_SPIKE": 5}
    signal["_phase_order"] = signal["signal_phase"].map(phase_order).fillna(9)
    top = signal.sort_values(["_phase_order", "discovery_score", "amount_eok"], ascending=[True, False, False]).head(top_n)
    lines = [
        "🧭 [V73 Discovery → Tracking → Execution]",
        f"📌 {V73_VERSION} | {today} | DISCOVERY_ONLY / 기존 V72.29 STRICT 변경 없음",
        f"📌 종목목록: {os.environ.get('V73_LISTING_SOURCE', 'UNKNOWN')}",
        f"📌 유니버스: 상장 {len(listing)} → 기본통과 {int(((listing['price'] >= _env_float('V73_MIN_PRICE',3000)) & ((listing['marcap_eok'] <= 0) | (listing['marcap_eok'] >= _env_float('V73_MIN_MARCAP_EOK',300)))).sum())} → 동적발견 {len(universe)}",
        f"📌 결과: 계산 {len(frame)} | 발견신호 {len(signal)} | 데이터실패 {len(fetch_fail)}",
        "📌 원칙: 넓게 발견 → 15일 추적 → OOS 검증 전 실행 승격 금지",
        "",
    ]
    if top.empty:
        lines += ["- 오늘 V73 최소 발견 조건을 통과한 종목이 없습니다.", "- 억지로 후보를 채우지 않습니다."]
    else:
        for rank, (_, r) in enumerate(top.iterrows(), 1):
            profiles = ", ".join(PROFILE_LABELS[p] for p in str(r["profiles"]).split("|") if p)
            phase = str(r.get("signal_phase", "EARLY_DISCOVERY"))
            chase = {
                "BASE_IGNITION": "🟣저항전 첫시동·추적등록",
                "EARLY_DISCOVERY": "🟣초동관찰",
                "CONFIRMED_BREAKOUT": "🔵확정돌파·첫눌림등록",
                "RESTART_CONFIRMED": "🟢첫눌림 재출발",
                "LATE_SPIKE": "⚠️급등연장·첫눌림대기",
                "LOW_CONFIRMATION": "👀저확신 추적",
            }.get(phase, "🟣초동관찰")
            state = str(r.get("state", "NEW_DISCOVERY"))
            support = max(_num(r.get("ma5")), _num(r.get("ma20")))
            lines += [
                f"{rank}) {r['name']}({r['code']}) | 점수 {r['discovery_score']:.1f} | {state} | {phase}",
                f"   🧬 {profiles}",
                f"   현재 {int(round(_num(r['close']))):,}원 ({_num(r['ret1']):+.2f}%) | 거래대금 {_num(r['amount_eok']):,.0f}억 | 거래량 {_num(r['vol_ratio20']):.1f}배",
                f"   고가유지 {_num(r['range_pos'])*100:.0f}% | 섹터초과 {_num(r['sector_excess_pctp']):+.1f}%p | {chase}",
                f"   추적선: MA5/20 상단 {int(round(support)):,}원 | 사유: {str(r['reasons'])[:180]}",
                f"   섹터: {str(r['sector_tags']).replace('|',' · ')}",
                "",
            ]
    pullback_life = life[life["state"].astype(str).eq("PULLBACK_SETUP")].copy() if not life.empty and "state" in life.columns else pd.DataFrame()
    if not pullback_life.empty:
        pullback_life = pullback_life.sort_values(["discovery_score", "amount_eok"], ascending=False).head(5)
        lines += ["🔄 [기존 발견종목 첫눌림 진행]", "📌 새 매수신호가 아니라 재출발 전 추적 상태입니다."]
        for _, r in pullback_life.iterrows():
            lines += [
                f"- {r.get('name')}({r.get('code')}) | PULLBACK_SETUP | 최초발견 {r.get('first_signal_date')}",
                f"  현재 {int(round(_num(r.get('close')))):,}원 ({_num(r.get('ret1')):+.2f}%) | 거래대금 {_num(r.get('amount_eok')):,.0f}억",
                f"  {str(r.get('pullback_debug',''))[:200] or '강봉 후 거래량 감소·지지선 유지 여부 추적'}",
            ]
        lines.append("")

    lines += [
        "📁 [감사 원장]",
        "- v73_discovery_stage_ledger.csv: 계산값·신호·NO_SIGNAL 전부",
        "- v73_filter_rejection_ledger.csv: 단계별 탈락 사유",
        "- v73_sector_mapping_audit.csv: 다중 섹터 분류",
        "- v73_signal_lifecycle_ledger.csv: 발견→추적→재출발 상태",
        f"⏱️ 실행 {time.monotonic()-started:.1f}초",
    ]
    return "\n".join(lines).strip(), signal, {"universe": len(universe), "calculated": len(frame), "signals": len(signal), "fetch_fail": len(fetch_fail)}


def run_case_audit(
    codes: Sequence[str], dates: Sequence[str], output_dir: str = "reports",
    fetcher: Callable[[str, int, Optional[pd.Timestamp]], Tuple[pd.DataFrame, str]] = fetch_daily,
) -> Tuple[str, pd.DataFrame]:
    """Causal sequential replay with active-cycle reset.

    Historical discoveries are useful evidence, but after 15 trading days
    without a fresh trigger they must not remain the active first discovery.
    V73.2 archives the old cycle, detects a later robust close/pivot ignition as
    NEW_CYCLE, and then carries that active cycle into pullback tracking.
    """
    rows: List[dict] = []
    target_dates = sorted({pd.Timestamp(d).normalize() for d in dates})
    for raw_code in codes:
        code = _code(raw_code)
        if not code or not target_dates:
            continue
        max_date = max(target_dates)
        full, source = fetcher(code, 520, max_date)
        if full is None or full.empty:
            for dt in target_dates:
                rows.append({"code": code, "audit_date": dt.strftime("%Y-%m-%d"), "result": "DATA_FAIL", "state": "DATA_FAIL", "source": source})
            continue
        full = full.sort_index()
        active_first: Optional[pd.Timestamp] = None
        active_last: Optional[pd.Timestamp] = None
        previous_cycle_first: Optional[pd.Timestamp] = None
        previous_cycle_last: Optional[pd.Timestamp] = None
        ever_had_cycle = False
        last_signal_profiles: List[str] = []
        daily_states: Dict[pd.Timestamp, dict] = {}
        for i in range(79, len(full)):
            d = full.iloc[: i + 1]
            dt = pd.Timestamp(d.index[-1]).normalize()
            if dt > max_date:
                break
            m = _cycle_anchored_metrics(d, _metrics(d), active_first)
            if not m:
                continue
            profiles, reasons = detect_profiles(m)
            phase = _signal_phase(m, profiles)

            # Expire the active cycle before evaluating today's new signal.
            if active_last is not None and _business_gap(active_last, dt) > 15:
                previous_cycle_first = active_first
                previous_cycle_last = active_last
                active_first = None
                active_last = None
                last_signal_profiles = []

            if profiles:
                if active_first is None:
                    state = "NEW_CYCLE" if ever_had_cycle else "NEW_DISCOVERY"
                    active_first = dt
                    ever_had_cycle = True
                elif "DISCOVERY_D" in profiles:
                    state = "RESTART_CONFIRMED"
                else:
                    state = "REACTIVATED"
                active_last = dt
                last_signal_profiles = list(profiles)
                result = "SIGNAL"
            elif active_first is not None and active_last is not None:
                gap = _business_gap(active_last, dt)
                if bool(m.get("pullback_setup")):
                    state = "PULLBACK_SETUP"
                    phase = "PULLBACK_SETUP"
                    result = "TRACKING"
                    reasons = [str(m.get("pullback_debug") or "첫 강봉 후 저거래량 눌림 진행")]
                else:
                    state = "TRACKING_NO_NEW_SIGNAL"
                    phase = "NO_SIGNAL"
                    result = "TRACKING"
                    reasons = [f"활성 사이클 최근신호 후 {gap}거래일 추적 유지"]
            else:
                state = "UNTRACKED"
                result = "NO_SIGNAL"
                phase = "NO_SIGNAL"
                reasons = reasons or (["이전 사이클 만료·새 신호 없음"] if ever_had_cycle else ["최소 조건 미충족"])

            daily_states[dt] = {
                "result": result, "state": state, "phase": phase, "profiles": profiles,
                "reasons": reasons, "metrics": m,
                "active_first": active_first, "active_last": active_last,
                "previous_cycle_first": previous_cycle_first, "previous_cycle_last": previous_cycle_last,
                "last_signal_profiles": last_signal_profiles,
            }

        available = sorted(daily_states)
        for target in target_dates:
            eligible = [d for d in available if d <= target]
            if not eligible:
                rows.append({"code": code, "audit_date": target.strftime("%Y-%m-%d"), "result": "DATA_FAIL", "state": "INSUFFICIENT_HISTORY", "source": source})
                continue
            asof = eligible[-1]
            st = daily_states[asof]
            m = st["metrics"]
            profiles = st["profiles"]
            reasons = st["reasons"]
            af = st["active_first"]; al = st["active_last"]
            pf = st["previous_cycle_first"]; pl = st["previous_cycle_last"]
            row = {
                "code": code, "audit_date": target.strftime("%Y-%m-%d"), "asof_market_date": asof.strftime("%Y-%m-%d"),
                "result": st["result"], "state": st["state"], "signal_phase": st["phase"], "source": source,
                "profiles": "|".join(profiles), "profile_labels": "|".join(PROFILE_LABELS[p] for p in profiles),
                "reasons": " / ".join(reasons), "discovery_score": _profile_score(m, profiles, 0.0),
                "first_signal_date": af.strftime("%Y-%m-%d") if af is not None else "",
                "last_signal_date": al.strftime("%Y-%m-%d") if al is not None else "",
                "previous_cycle_first_signal_date": pf.strftime("%Y-%m-%d") if pf is not None else "",
                "previous_cycle_last_signal_date": pl.strftime("%Y-%m-%d") if pl is not None else "",
            }
            row.update(m)
            rows.append(row)
    out = pd.DataFrame(rows)
    path = Path(output_dir) / "v73_case_audit.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False, encoding="utf-8-sig")
    lines = [
        "🧪 [V73 과거 사례 순차 인과감사]",
        f"📌 {V73_VERSION} | 미래 데이터 미사용 · 견고한 종가·피벗 저항 + 활성 사이클 순차 재생",
        "📌 판정: 15거래일 무신호면 과거 사이클 종료 · 이후 신호는 NEW_CYCLE로 재등록",
        "",
    ]
    for _, r in out.iterrows():
        labels = str(r.get("profile_labels", "")).replace("|", " · ")
        state = str(r.get("state", r.get("result", "")))
        suffix = f" · {labels}" if labels else ""
        active = f"활성최초 {r.get('first_signal_date') or '-'} | 활성최근 {r.get('last_signal_date') or '-'}"
        previous = f"이전사이클최근 {r.get('previous_cycle_last_signal_date') or '-'}"
        lines += [
            f"- {r.get('code')} | {r.get('audit_date')} | {r.get('result')} | {state}{suffix}",
            f"  기준거래일 {r.get('asof_market_date','-')} | 종가 {int(round(_num(r.get('close')))):,}원 | 일간 {_num(r.get('ret1')):+.2f}% | 거래량 {_num(r.get('vol_ratio20')):.2f}배",
            f"  원고점10/20 {_num(r.get('break10_pct'),-999):+.1f}% / {_num(r.get('break20_pct'),-999):+.1f}% | 견고저항10/20 {_num(r.get('robust_break10_pct'),-999):+.1f}% / {_num(r.get('robust_break20_pct'),-999):+.1f}% | 60일 {_num(r.get('break60_pct'),-999):+.1f}%",
            f"  {active} | {previous} | {str(r.get('reasons','')) or '최소 조건 미충족'}",
        ]
    return "\n".join(lines).strip(), out


# ============================================================
# V73.3 COMPLETE LIFECYCLE / SHARED SECTOR / REGIME / EXECUTION CONTRACT
# Discovery thresholds above are deliberately frozen from V73.2.1.
# This section replaces only state persistence, cross-layer metadata,
# output grouping and the research-only execution bridge.
# ============================================================
V73_VERSION = "V73.3.6.2"
V73_POLICY = "AUX_FORCE_STATE_RESTORE_GUARD"

EXPLICIT_MULTI_SECTOR.update({
    "251970": ("화장품용기/포장재", "화장품"),
    "002810": ("화학소재", "산업재유통"),
    "010130": ("비철금속/원자재",),
    "079160": ("콘텐츠/미디어", "영화관"),
    "014940": ("조선기자재", "조선/해운"),
    "002960": ("정유/석유/가스", "에너지 통합"),
    "138040": ("금융지주", "금융/은행"),
    "068760": ("바이오", "제약"),
    "207940": ("바이오",),
    "068270": ("바이오",),
    "047810": ("방산", "항공우주"),
    "012450": ("방산",),
    "064350": ("방산",),
    "005490": ("철강", "2차전지소재"),
    "086790": ("금융지주", "금융/은행"),
    "316140": ("금융지주", "금융/은행"),
    "055550": ("금융지주", "금융/은행"),
    "000810": ("보험", "금융/은행"),
    "319400": ("물류자동화", "로봇/자동화"),
    "264850": ("로봇부품", "로봇/자동화"),
    "059120": ("모션제어", "산업자동화/부품", "로봇/자동화"),
    "399720": ("반도체 설계솔루션", "반도체"),
    "192820": ("화장품", "화장품 ODM"),
    "377300": ("핀테크", "결제", "소프트웨어/클라우드"),
    "000640": ("제약", "바이오", "제약지주"),
    "126700": ("머신비전", "검사장비"),
    "082640": ("보험", "금융/은행"),
    "099190": ("의료기기", "진단"),
    "018260": ("IT서비스", "소프트웨어/클라우드"),
    "028050": ("플랜트/엔지니어링", "건설"),
    "054540": ("산업기계", "조선기자재", "발전기자재"),
})

_V733_PREV_MULTI_SECTOR_TAGS = multi_sector_tags


def multi_sector_tags(code: str, name: str, industry: str) -> Tuple[str, ...]:
    c = _code(code)
    if c in EXPLICIT_MULTI_SECTOR:
        return EXPLICIT_MULTI_SECTOR[c]
    base = list(_V733_PREV_MULTI_SECTOR_TAGS(c, name, industry))
    text = f"{name} {industry}".lower()
    extra_rules = [
        (("화장품", "코스메틱"), "화장품"),
        (("용기", "포장", "패키징"), "화장품용기/포장재"),
        (("비철", "아연", "동제련", "제련"), "비철금속/원자재"),
        (("영화", "극장", "콘텐츠", "미디어"), "콘텐츠/미디어"),
        (("편의점", "유통"), "유통/편의점"),
        (("해운", "운송"), "해운"),
        (("조선기자재", "선박부품"), "조선기자재"),
        (("항공우주", "방위", "방산"), "방산"),
        (("화학소재", "화학제품"), "화학소재"),
        (("음식료", "식품"), "음식료"),
        (("통신",), "통신"),
        (("리츠", "부동산투자"), "리츠/부동산"),
    ]
    for keys, tag in extra_rules:
        if any(k in text for k in keys):
            base.append(tag)
    specific = [x for x in base if x and x not in {industry.strip(), "미분류"}]
    if specific:
        base = specific
    return tuple(dict.fromkeys(x for x in base if str(x).strip())) or ("미분류",)


SECTOR_FAMILY_ALIASES: Dict[str, str] = {
    "조선기자재": "조선/해운", "해운": "조선/해운", "조선/해운": "조선/해운",
    "방산": "방산", "항공우주": "방산",
    "AI전력/전력설비": "AI전력/전력설비", "전력기기/배전": "AI전력/전력설비",
    "전선/전력망": "AI전력/전력설비", "데이터센터 전력": "AI전력/전력설비",
    "데이터센터 인프라": "AI전력/전력설비", "발전솔루션/비상전력": "AI전력/전력설비",
    "원전/전력설비": "원전/우라늄", "원전/우라늄": "원전/우라늄",
    "정유/석유/가스": "정유/에너지", "LPG/에너지": "정유/에너지",
    "정유/에너지": "정유/에너지", "신재생/전력개발": "신재생/전력개발",
    "풍력/전력개발": "신재생/전력개발", "태양광/전력개발": "신재생/전력개발",
    "바이오": "바이오", "제약": "바이오",
    "보험": "금융/은행", "금융지주": "금융/은행", "금융/은행": "금융/은행",
    "로봇/자동화": "로봇/자동화", "산업자동화/부품": "로봇/자동화",
    "2차전지/EV": "2차전지/EV", "2차전지소재": "2차전지/EV",
    "화장품": "화장품", "화장품용기/포장재": "화장품",
    "비철금속/원자재": "비철금속/원자재", "철강": "철강",
    "콘텐츠/미디어": "콘텐츠/미디어", "영화관": "콘텐츠/미디어",
    "유통/편의점": "유통/편의점", "화학": "화학", "화학소재": "화학",
    "건설/인프라": "건설/인프라", "통신": "통신", "음식료": "음식료",
    "리츠/부동산": "리츠/부동산", "반도체": "반도체",
    "물류자동화": "로봇/자동화", "로봇부품": "로봇/자동화", "모션제어": "로봇/자동화",
    "반도체 설계솔루션": "반도체", "화장품 ODM": "화장품",
    "핀테크": "소프트웨어/클라우드", "결제": "소프트웨어/클라우드", "소프트웨어/클라우드": "소프트웨어/클라우드",
    "제약지주": "바이오", "의료기기": "바이오", "진단": "바이오",
    "머신비전": "산업자동화/검사", "검사장비": "산업자동화/검사",
}


def _sector_family(tag: str) -> str:
    t = str(tag or "").strip()
    return SECTOR_FAMILY_ALIASES.get(t, t)


def _sector_families(tags: str) -> List[str]:
    out: List[str] = []
    for tag in str(tags or "").split("|"):
        if _valid_sector_tag(tag):
            fam = _sector_family(tag)
            if fam and fam not in out:
                out.append(fam)
    return out


def _atomic_write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    os.replace(tmp, path)


def _append_dedup(path: Path, rows: pd.DataFrame, keys: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    old = _read_csv(path)
    if rows is None:
        rows = pd.DataFrame()
    out = pd.concat([old, rows], ignore_index=True, sort=False) if not old.empty else rows.copy()
    if not out.empty:
        for k in keys:
            if k not in out.columns:
                out[k] = ""
            out[k] = out[k].fillna("").astype(str)
        out = out.drop_duplicates(list(keys), keep="last")
    _atomic_write_csv(path, out)


def _active_state_columns() -> List[str]:
    return [
        "code", "name", "is_active", "active_first_signal_date", "active_last_signal_date",
        "previous_cycle_first_signal_date", "previous_cycle_last_signal_date",
        "last_state", "last_profiles", "last_phase", "last_score", "last_close",
        "last_amount_eok", "sector_tags", "updated_date", "policy_version",
    ]


def _normalize_active_state(df: pd.DataFrame) -> pd.DataFrame:
    cols = _active_state_columns()
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    x = df.copy()
    for c in cols:
        if c not in x.columns:
            x[c] = ""
    x["code"] = x["code"].map(_code)
    x = x[x["code"].str.len().eq(6)].copy()
    x["is_active"] = pd.to_numeric(x["is_active"], errors="coerce").fillna(0).astype(int)
    for c in ("last_score", "last_close", "last_amount_eok"):
        x[c] = pd.to_numeric(x[c], errors="coerce").fillna(0.0)
    return x[cols].drop_duplicates("code", keep="last").reset_index(drop=True)


def _migrate_active_state(lifecycle: pd.DataFrame, today: str) -> pd.DataFrame:
    if lifecycle is None or lifecycle.empty or "code" not in lifecycle.columns:
        return _normalize_active_state(pd.DataFrame())
    life = lifecycle.copy()
    life["code"] = life["code"].map(_code)
    life["signal_date"] = pd.to_datetime(life.get("signal_date"), errors="coerce")
    life = life.dropna(subset=["signal_date"])
    rows: List[dict] = []
    asof = pd.Timestamp(today)
    for code, grp in life.groupby("code"):
        grp = grp.sort_values("signal_date")
        sig = grp[grp.get("profiles", pd.Series(index=grp.index, dtype=str)).fillna("").astype(str).str.len().gt(0)] if "profiles" in grp.columns else grp
        if sig.empty:
            continue
        last_sig = sig.iloc[-1]
        last_dt = pd.Timestamp(last_sig["signal_date"])
        age = _business_gap(last_dt, asof)
        last_row = grp.iloc[-1]
        first = str(last_sig.get("first_signal_date", "") or "")
        if not first:
            first = pd.Timestamp(sig.iloc[0]["signal_date"]).strftime("%Y-%m-%d")
        active = int(age <= 15 and str(last_row.get("state", "")) not in {"CYCLE_EXPIRED", "ARCHIVED"})
        rows.append({
            "code": code, "name": str(last_row.get("name", "") or ""), "is_active": active,
            "active_first_signal_date": first if active else "",
            "active_last_signal_date": last_dt.strftime("%Y-%m-%d") if active else "",
            "previous_cycle_first_signal_date": "" if active else first,
            "previous_cycle_last_signal_date": "" if active else last_dt.strftime("%Y-%m-%d"),
            "last_state": str(last_row.get("state", "") or ""),
            "last_profiles": str(last_sig.get("profiles", "") or ""),
            "last_phase": str(last_sig.get("signal_phase", "") or ""),
            "last_score": _num(last_sig.get("discovery_score")), "last_close": _num(last_row.get("close")),
            "last_amount_eok": _num(last_row.get("amount_eok")), "sector_tags": str(last_row.get("sector_tags", "") or ""),
            "updated_date": pd.Timestamp(last_row["signal_date"]).strftime("%Y-%m-%d"), "policy_version": V73_VERSION,
        })
    return _normalize_active_state(pd.DataFrame(rows))


def _v73362_on(name: str, default: str = "0") -> bool:
    try:
        return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return False


def _load_active_state(state_path: Path, lifecycle_path: Path, today: str) -> Tuple[pd.DataFrame, str, int]:
    state_file_exists = state_path.exists()
    lifecycle_file_exists = lifecycle_path.exists()
    state = _normalize_active_state(_read_csv(state_path))
    if not state.empty:
        return state, "ACTIVE_STATE_CACHE", 0
    migrated = _migrate_active_state(_prior_lifecycle(lifecycle_path), today)
    if not migrated.empty:
        return migrated, "LIFECYCLE_MIGRATION", len(migrated)

    # V73.3.6.2: GitHub cache miss/corruption must never silently become a new lifecycle.
    # The guard is enabled by the workflow only; local/synthetic runs keep legacy behavior.
    guard = _v73362_on("V73_STATE_RESTORE_GUARD", "0")
    allow_fresh = _v73362_on("V73_ALLOW_FRESH_START", "0")
    matched = _v73362_on("V73_CACHE_RESTORE_MATCHED", "0")
    claimed_active = _v73362_on("V73_CACHE_ACTIVE_PRESENT", "0")
    claimed_lifecycle = _v73362_on("V73_CACHE_LIFECYCLE_PRESENT", "0")
    if guard and not allow_fresh:
        if matched and (claimed_active or claimed_lifecycle or state_file_exists or lifecycle_file_exists):
            return migrated, "STATE_RESTORE_FAILURE_EMPTY_RESTORED_STATE", 0
        if not matched and not state_file_exists and not lifecycle_file_exists:
            return migrated, "STATE_RESTORE_UNVERIFIED_CACHE_MISS", 0
    return migrated, "EMPTY_FRESH_START", 0


def _v73362_write_state_manifest(path: Path, payload: Mapping[str, object]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, path)
    except Exception:
        pass


def build_dynamic_universe(listing: pd.DataFrame, prior_lifecycle: pd.DataFrame, asof_date: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    min_price = _env_float("V73_MIN_PRICE", 3000, 100, 1e7)
    min_marcap = _env_float("V73_MIN_MARCAP_EOK", 300, 0, 1e8)
    limit = _env_int("V73_DISCOVERY_UNIVERSE_LIMIT", 900, 50, 2500)
    top_amount_n = _env_int("V73_TOP_AMOUNT_POOL", 750, 50, 2500)
    normalized_cols = {"code", "name", "market", "industry", "price", "amount_eok", "marcap_eok", "change_pct"}
    x = listing.copy() if isinstance(listing, pd.DataFrame) and normalized_cols.issubset(set(listing.columns)) else normalize_listing(listing)
    for col in ("price", "amount_eok", "marcap_eok", "change_pct"):
        x[col] = _to_numeric_clean(x[col]) if col in x.columns else pd.Series(0.0, index=x.index, dtype="float64")
    if x.empty:
        empty = x.copy(); empty["basic_pass"] = pd.Series(dtype="bool"); empty["universe_reason"] = pd.Series(dtype="string")
        return empty, pd.DataFrame(columns=["code", "name", "market", "industry", "price", "amount_eok", "marcap_eok", "change_pct", "stage", "result", "reason"])
    x["basic_pass"] = (x["price"] >= min_price) & ((x["marcap_eok"] <= 0) | (x["marcap_eok"] >= min_marcap))
    basic = x[x["basic_pass"]].copy()
    tracked_codes: set = set()
    if prior_lifecycle is not None and not prior_lifecycle.empty and "code" in prior_lifecycle.columns:
        pl = prior_lifecycle.copy(); pl["code"] = pl["code"].map(_code)
        if "is_active" in pl.columns:
            pl = pl[pd.to_numeric(pl["is_active"], errors="coerce").fillna(0).astype(int).eq(1)]
            if "active_last_signal_date" in pl.columns:
                asof = pd.Timestamp(asof_date or pd.Timestamp.now().strftime("%Y-%m-%d"))
                pl["_last"] = pd.to_datetime(pl["active_last_signal_date"], errors="coerce")
                pl = pl.dropna(subset=["_last"])
                pl = pl[pl["_last"].map(lambda d: _business_gap(d, asof) <= 15)]
        tracked_codes = set(pl["code"])
    explicit = set(EXPLICIT_MULTI_SECTOR)
    top_codes = set(basic.nlargest(min(top_amount_n, len(basic)), "amount_eok")["code"]) if not basic.empty else set()
    momentum_codes = set(basic[(basic["change_pct"] >= 2.0) & (basic["amount_eok"] >= 3.0)]["code"])
    strategic_codes = set(basic[basic["code"].isin(explicit)]["code"])
    tracked_codes = set(basic[basic["code"].isin(tracked_codes)]["code"])
    selected = top_codes | momentum_codes | strategic_codes | tracked_codes
    def reason(c: str) -> str:
        r: List[str] = []
        if c in top_codes: r.append("AMOUNT_POOL")
        if c in momentum_codes: r.append("MOMENTUM_ESCAPE")
        if c in strategic_codes: r.append("STRATEGIC_MULTI_SECTOR")
        if c in tracked_codes: r.append("ACTIVE_CYCLE_TRACKED")
        return "+".join(r)
    cand = basic[basic["code"].isin(selected)].copy(); cand["universe_reason"] = cand["code"].map(reason)
    cand["priority"] = (cand["amount_eok"].rank(pct=True) * 45 + cand["change_pct"].clip(-10, 30) * 1.5 + cand["code"].isin(strategic_codes).astype(int) * 30 + cand["code"].isin(tracked_codes).astype(int) * 25)
    cand = cand.sort_values(["priority", "amount_eok"], ascending=False).head(limit).reset_index(drop=True)
    selected_final = set(cand["code"])
    rej = x[["code", "name", "market", "industry", "price", "amount_eok", "marcap_eok", "change_pct"]].copy()
    rej["stage"] = np.where(~x["basic_pass"], "BASIC_FILTER", np.where(rej["code"].isin(selected_final), "DISCOVERY_UNIVERSE", "DYNAMIC_UNIVERSE"))
    rej["result"] = np.where(rej["code"].isin(selected_final), "PASS", "FAIL")
    rej["reason"] = np.where(~x["basic_pass"], "PRICE_OR_MARCAP", np.where(rej["code"].isin(selected_final), "SELECTED", "NOT_AMOUNT_MOMENTUM_ACTIVE_OR_STRATEGIC"))
    return cand, rej


def _macro_regime_from_sources(output_dir: Path, today: str) -> Tuple[Dict[str, str], str]:
    mapping: Dict[str, str] = {}
    source_bits: List[str] = []
    env_map = {
        "STRONG": os.getenv("V73_STRONG_SECTORS", ""),
        "WATCH": os.getenv("V73_WATCH_SECTORS", ""),
        "WEAK": os.getenv("V73_WEAK_SECTORS", ""),
    }
    for regime, raw in env_map.items():
        for sector in re.split(r"[,|]", str(raw or "")):
            sector = sector.strip()
            if sector:
                mapping[_sector_family(sector)] = regime
                source_bits.append("ENV")
    path = output_dir / "v73_daily_sector_regime.csv"
    df = _read_csv(path)
    if not df.empty and {"sector", "macro_regime"}.issubset(df.columns):
        if "regime_date" in df.columns:
            df["regime_date"] = pd.to_datetime(df["regime_date"], errors="coerce")
            valid = df.dropna(subset=["regime_date"]).copy()
            if not valid.empty:
                valid["_gap"] = valid["regime_date"].map(lambda d: _business_gap(d, pd.Timestamp(today)))
                valid = valid[(valid["_gap"] >= 0) & (valid["_gap"] <= 1)]
                if not valid.empty:
                    latest = valid["regime_date"].max(); valid = valid[valid["regime_date"].eq(latest)]
                df = valid
        for _, r in df.iterrows():
            sec = _sector_family(str(r.get("sector", "")))
            reg = str(r.get("macro_regime", "")).upper()
            if sec and reg in {"STRONG", "WATCH", "WEAK"}:
                mapping[sec] = reg
                source_bits.append(str(r.get("source", "FILE") or "FILE"))
    source = "+".join(dict.fromkeys(source_bits)) if source_bits else "NO_MACRO_REGIME_WAIT_V72_PAYLOAD"
    return mapping, source


def _aggregate_regimes(values: Sequence[str]) -> str:
    vals = {str(v).upper() for v in values if str(v).upper() in {"STRONG", "WATCH", "WEAK"}}
    if "STRONG" in vals and "WEAK" in vals:
        return "CONFLICT"
    if "WEAK" in vals:
        return "WEAK"
    if "STRONG" in vals:
        return "STRONG"
    if "WATCH" in vals:
        return "WATCH"
    return "UNKNOWN"


def _tape_sector_table(frame: pd.DataFrame) -> pd.DataFrame:
    rows: List[dict] = []
    for _, r in frame.iterrows():
        for fam in _sector_families(str(r.get("sector_tags", ""))):
            rows.append({"sector": fam, "code": r.get("code"), "ret1": _num(r.get("ret1")), "amount_eok": _num(r.get("amount_eok"))})
    ex = pd.DataFrame(rows)
    if ex.empty:
        return pd.DataFrame(columns=["sector", "n", "median_ret", "breadth", "amount_eok", "tape_regime"])
    grp = ex.groupby("sector", as_index=False).agg(n=("code", "nunique"), median_ret=("ret1", "median"), amount_eok=("amount_eok", "sum"))
    breadth = ex.assign(up=ex["ret1"].gt(0).astype(float)).groupby("sector", as_index=False)["up"].mean().rename(columns={"up": "breadth"})
    grp = grp.merge(breadth, on="sector", how="left")
    def classify(r) -> str:
        if int(r.n) >= 3 and float(r.median_ret) >= 1.5 and float(r.breadth) >= 0.60:
            return "STRONG"
        if int(r.n) >= 3 and float(r.median_ret) <= -1.5 and float(r.breadth) <= 0.40:
            return "WEAK"
        return "WATCH"
    grp["tape_regime"] = [classify(r) for r in grp.itertuples()]
    return grp


def _final_sector_regime(macro: str, tape: str) -> str:
    macro = str(macro or "UNKNOWN").upper(); tape = str(tape or "UNKNOWN").upper()
    if macro == "CONFLICT" or tape == "CONFLICT":
        return "CONFLICT"
    if {macro, tape} == {"STRONG", "WEAK"}:
        return "CONFLICT"
    if macro == "WEAK":
        return "WEAK"
    if macro == "STRONG":
        return "STRONG" if tape != "WEAK" else "CONFLICT"
    if tape in {"STRONG", "WEAK", "WATCH"}:
        return tape
    return macro if macro in {"STRONG", "WATCH", "WEAK"} else "UNKNOWN"


def _liquidity_tier(amount_eok: float) -> str:
    a = _num(amount_eok)
    if a >= 300: return "L1_INSTITUTIONAL"
    if a >= 50: return "L2_EXECUTION_REVIEW"
    if a >= 10: return "L3_TRACKING_ONLY"
    return "L4_LOW_LIQUIDITY"


def _research_risk_contract(r: Mapping[str, object]) -> Tuple[float, float, float, str]:
    entry = _num(r.get("close"))
    supports = [_num(r.get("ma5")), _num(r.get("ma20")), _num(r.get("robust20_resistance")) * 0.98]
    supports = [x for x in supports if 0 < x < entry]
    if entry <= 0 or not supports:
        return entry, 0.0, 999.0, "NO_VALID_SUPPORT"
    stop = max(supports)
    risk = (1.0 - stop / entry) * 100.0
    status = "PASS" if 0.0 < risk <= 5.0 else "FAIL_OVER_5PCT"
    return entry, stop, risk, status


def _apply_sector_execution_metadata(frame: pd.DataFrame, output_dir: Path, today: str) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    x = frame.copy()
    macro_map, macro_source = _macro_regime_from_sources(output_dir, today)
    tape = _tape_sector_table(x)
    tape_map = dict(zip(tape.get("sector", pd.Series(dtype=str)), tape.get("tape_regime", pd.Series(dtype=str))))
    macros: List[str] = []; tapes: List[str] = []; finals: List[str] = []; liqs: List[str] = []
    entries: List[float] = []; stops: List[float] = []; risks: List[float] = []; risk_states: List[str] = []
    gates: List[str] = []; blockers_all: List[str] = []; readiness_all: List[str] = []
    for _, r in x.iterrows():
        families = _sector_families(str(r.get("sector_tags", "")))
        macro = _aggregate_regimes([macro_map.get(f, "UNKNOWN") for f in families])
        tape_reg = _aggregate_regimes([tape_map.get(f, "UNKNOWN") for f in families])
        final = _final_sector_regime(macro, tape_reg)
        liq = _liquidity_tier(_num(r.get("amount_eok")))
        entry, stop, risk, risk_status = _research_risk_contract(r)
        phase = str(r.get("signal_phase", "")); score = _num(r.get("discovery_score"))
        blockers: List[str] = []
        precheck: List[str] = []
        # Promotion blockers are stage-aware. Before a confirmed restart there is
        # no executable entry contract yet, so score/liquidity/sector/risk are
        # diagnostics only and must not be presented as actual rejection causes.
        if phase != "RESTART_CONFIRMED":
            blockers.append("NOT_RESTART")
            precheck.append("SCORE_OK" if score >= 70 else "SCORE_LT70")
            precheck.append("LIQ_OK" if liq in {"L1_INSTITUTIONAL", "L2_EXECUTION_REVIEW"} else "LIQ_TRACK_ONLY")
            precheck.append("SECTOR_OK" if final not in {"WEAK", "CONFLICT"} else "SECTOR_HEADWIND")
            if risk_status == "PASS":
                precheck.append(f"RISK_PREVIEW_OK_{risk:.1f}PCT")
            elif risk_status == "NO_VALID_SUPPORT":
                precheck.append("RISK_WAIT_RESTART_SUPPORT")
            else:
                precheck.append(f"RISK_PREVIEW_OVER5_{risk:.1f}PCT")
        else:
            if score < 70: blockers.append("SCORE_LT70")
            if liq not in {"L1_INSTITUTIONAL", "L2_EXECUTION_REVIEW"}: blockers.append("LIQUIDITY_TRACK_ONLY")
            if final in {"WEAK", "CONFLICT"}: blockers.append("SECTOR_HEADWIND")
            if risk_status != "PASS": blockers.append("ENTRY_RISK_CONTRACT")
            precheck.append("RESTART_EXECUTION_CONTRACT")
            if risk_status == "PASS":
                precheck.append(f"RISK_OK_{risk:.1f}PCT")
            elif risk_status == "NO_VALID_SUPPORT":
                precheck.append("RISK_UNVERIFIED")
            else:
                precheck.append(f"RISK_OVER5_{risk:.1f}PCT")
        gate = "PROMOTION_RESEARCH_READY" if phase == "RESTART_CONFIRMED" and not blockers else "TRACKING_ONLY"
        macros.append(macro); tapes.append(tape_reg); finals.append(final); liqs.append(liq)
        entries.append(round(entry, 4)); stops.append(round(stop, 4)); risks.append(round(risk, 2)); risk_states.append(risk_status)
        gates.append(gate); blockers_all.append("|".join(blockers)); readiness_all.append("|".join(precheck))
    x["macro_sector_regime"] = macros; x["tape_sector_regime"] = tapes; x["final_sector_regime"] = finals
    x["liquidity_tier"] = liqs; x["research_entry"] = entries; x["research_stop"] = stops
    x["entry_risk_pct"] = risks; x["entry_risk_status"] = risk_states
    x["execution_gate"] = gates; x["promotion_blockers"] = blockers_all; x["readiness_precheck"] = readiness_all
    x["macro_regime_source"] = macro_source
    return x, tape, macro_source


def _state_row_dict(state_df: pd.DataFrame) -> Dict[str, dict]:
    if state_df is None or state_df.empty:
        return {}
    return {str(r["code"]): r.to_dict() for _, r in state_df.iterrows()}


def _safe_date(v) -> Optional[pd.Timestamp]:
    try:
        x = pd.to_datetime(v, errors="coerce")
        return None if pd.isna(x) else pd.Timestamp(x).normalize()
    except Exception:
        return None


def _same_day_monotonic_state(old_state: str, signal_phase: str, fallback_state: str) -> Tuple[str, str]:
    """Preserve same-day idempotency while allowing forward lifecycle transitions."""
    rank = {
        "NEW_DISCOVERY": 0, "NEW_CYCLE": 0, "TRACKING_CONTINUE": 0, "TRACKING_NO_NEW_SIGNAL": 0, "REACTIVATED": 0,
        "PULLBACK_SETUP": 1, "RESTART_CONFIRMED": 2,
    }
    old = str(old_state or fallback_state or "TRACKING_CONTINUE")
    computed = "RESTART_CONFIRMED" if str(signal_phase or "") == "RESTART_CONFIRMED" else ("PULLBACK_SETUP" if str(signal_phase or "") == "PULLBACK_SETUP" else str(fallback_state or old))
    if rank.get(computed, 0) > rank.get(old, 0):
        return computed, "SAME_DAY_FORWARD"
    return old, "SAME_DAY_REFRESH"


def _transition_signal(code: str, today: str, profiles: Sequence[str], active: Optional[dict]) -> Tuple[str, str, int, str, dict]:
    now = pd.Timestamp(today)
    rec = dict(active or {})
    active_first = _safe_date(rec.get("active_first_signal_date")); active_last = _safe_date(rec.get("active_last_signal_date"))
    is_active = int(_num(rec.get("is_active"))) == 1 and active_first is not None and active_last is not None
    ever = bool(active_first is not None or _safe_date(rec.get("previous_cycle_last_signal_date")) is not None or rec)
    if is_active and active_last == now:
        state = str(rec.get("last_state") or "TRACKING_CONTINUE")
        first = active_first.strftime("%Y-%m-%d")
        return state, first, 0, "SAME_DAY_REFRESH", rec
    gap = _business_gap(active_last, now) if is_active else 999
    if is_active and gap <= 15:
        state = "RESTART_CONFIRMED" if "DISCOVERY_D" in profiles else ("TRACKING_CONTINUE" if gap <= 2 else "REACTIVATED")
        first = active_first.strftime("%Y-%m-%d")
        event = "FRESH_SIGNAL"
    else:
        if is_active:
            rec["previous_cycle_first_signal_date"] = active_first.strftime("%Y-%m-%d")
            rec["previous_cycle_last_signal_date"] = active_last.strftime("%Y-%m-%d")
        state = "NEW_CYCLE" if ever else "NEW_DISCOVERY"
        first = today; event = "NEW_ACTIVE_CYCLE"
    return state, first, (0 if gap == 999 else gap), event, rec


def _state_upsert_frame(state_map: Dict[str, dict]) -> pd.DataFrame:
    return _normalize_active_state(pd.DataFrame(list(state_map.values())))


def _cycle_anchored_metrics(df: pd.DataFrame, metrics: Optional[dict], active_first) -> Optional[dict]:
    """Re-evaluate pullback/restart only inside the current active cycle.

    Discovery profiles remain unchanged.  Tracking states require an impulse on
    or after ``active_first_signal_date`` and a real trough of at least -2%.
    """
    if not metrics:
        return metrics
    out = dict(metrics)
    out.update({"pullback_setup": False, "restart": False, "pullback_pct": np.nan,
                "pullback_debug": "CYCLE_ANCHOR_WAIT", "restart_debug": "CYCLE_ANCHOR_WAIT",
                "impulse_found": False, "impulse_ago": np.nan, "impulse_ret": np.nan,
                "impulse_vol_ratio": np.nan})
    first = _safe_date(active_first)
    if first is None or df is None or len(df) < 25:
        return out
    d = df.copy().sort_index()
    for col in ("Open", "High", "Low", "Close", "Volume"):
        d[col] = pd.to_numeric(d[col], errors="coerce")
    d = d.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    if len(d) < 25:
        return out
    idx_dates = pd.to_datetime(d.index, errors="coerce").normalize()
    current_i = len(d) - 1
    c = float(d["Close"].iloc[-1]); o = float(d["Open"].iloc[-1]); v = float(d["Volume"].iloc[-1])
    pc = float(d["Close"].iloc[-2]); ret1 = (c / pc - 1.0) * 100.0 if pc > 0 else 0.0
    vma5 = float(d["Volume"].tail(5).mean())
    ma5 = float(d["Close"].rolling(5).mean().iloc[-1]); ma20 = float(d["Close"].rolling(20).mean().iloc[-1])
    obv = _obv(d["Close"], d["Volume"]); obv_now = float(obv.iloc[-1])
    # Most recent qualifying impulse wins, but it must belong to this cycle and
    # cannot be today's unfinished tracking bar.
    for i in range(current_i - 1, -1, -1):
        dt = pd.Timestamp(idx_dates[i]).normalize() if not pd.isna(idx_dates[i]) else None
        if dt is None or dt < first:
            break
        if i < 21:
            continue
        ago = current_i - i
        if ago > 15:
            break
        ic = float(d["Close"].iloc[i]); ipc = float(d["Close"].iloc[i - 1]); io = float(d["Open"].iloc[i])
        ih = float(d["High"].iloc[i]); il = float(d["Low"].iloc[i]); iv = float(d["Volume"].iloc[i])
        ivma = float(d["Volume"].iloc[max(0, i - 20):i].mean())
        iret = (ic / ipc - 1.0) * 100.0 if ipc > 0 else 0.0
        ipos = (ic - il) / max(ih - il, 1e-9); ivr = iv / ivma if ivma > 0 else 0.0
        hist = d.iloc[max(0, i - 20):i]
        prior20 = float(hist["High"].max()); prior20_close = float(hist["Close"].max()); prior20_q80 = float(hist["High"].quantile(0.80))
        robust20 = max(prior20_close, prior20_q80) if prior20_close > 0 and prior20_q80 > 0 else prior20
        break20 = (ic / prior20 - 1.0) * 100.0 if prior20 > 0 else -999.0
        robust_break20 = (ic / robust20 - 1.0) * 100.0 if robust20 > 0 else -999.0
        strong = (iret >= 5.0 and ivr >= 1.50) or (iret >= 8.0 and ivr >= 1.20 and robust_break20 >= -2.0)
        if not (strong and ipos >= 0.62 and ic >= io):
            continue
        after = d.iloc[i + 1:]
        if after.empty:
            continue
        trough_pct = (float(after["Low"].min()) / ih - 1.0) * 100.0
        close_pct = (c / ih - 1.0) * 100.0
        middle = after["Volume"].iloc[:-1] if len(after) > 1 else pd.Series(dtype=float)
        med_vol_after = float(middle.median()) if not middle.empty else v
        min_after = float(after["Low"].min())
        support = min_after >= il * 0.94 and c >= min(ma5, ma20) * 0.96 and c >= il * 0.96
        compressed = med_vol_after <= iv * 0.80 and v <= iv * 0.90
        obv_hold = obv_now >= float(obv.iloc[i]) - abs(iv) * 0.65
        real_pullback = -15.0 <= trough_pct <= -2.0
        setup_zone = -15.0 <= close_pct <= -2.0
        pullback_setup = real_pullback and setup_zone and support and compressed and obv_hold
        reentry = ago >= 2 and ret1 >= 1.2 and c >= o and v >= max(float(d["Volume"].iloc[-2]) * 1.05, vma5 * 0.9)
        restart = real_pullback and close_pct <= 3.0 and support and compressed and obv_hold and reentry
        common = (f"activeFirst {first.strftime('%Y-%m-%d')} · impulse {dt.strftime('%Y-%m-%d')} D-{ago} "
                  f"{iret:+.1f}%/V{ivr:.1f} raw20 {break20:+.1f}%/robust20 {robust_break20:+.1f}% · "
                  f"trough {trough_pct:+.1f}% · closePullback {close_pct:+.1f}% · compressed={compressed} support={support} obv={obv_hold}")
        out.update({"impulse_found": True, "impulse_ago": ago, "impulse_ret": iret, "impulse_vol_ratio": ivr,
                    "pullback_pct": close_pct, "pullback_setup": pullback_setup, "restart": restart,
                    "pullback_debug": common, "restart_debug": common + f" reentry={reentry}"})
        return out
    out["pullback_debug"] = f"CYCLE_ANCHORED_NO_VALID_IMPULSE activeFirst={first.strftime('%Y-%m-%d')}"
    out["restart_debug"] = out["pullback_debug"]
    return out


def run_discovery(
    listing_raw: pd.DataFrame,
    output_dir: str = "reports",
    fetcher: Callable[[str, int, Optional[pd.Timestamp]], Tuple[pd.DataFrame, str]] = fetch_daily,
    now: Optional[datetime] = None,
) -> Tuple[str, pd.DataFrame, dict]:
    started = time.monotonic(); now = now or datetime.now(); today = now.strftime("%Y-%m-%d")
    outdir = Path(output_dir)
    paths = {
        "stage": outdir / "v73_discovery_stage_ledger.csv", "reject": outdir / "v73_filter_rejection_ledger.csv",
        "sector": outdir / "v73_sector_mapping_audit.csv", "life": outdir / "v73_signal_lifecycle_ledger.csv",
        "snapshot": outdir / "v73_latest_discovery_snapshot.csv", "listing": outdir / "v73_listing_cache.csv",
        "active": outdir / "v73_active_cycle_state.csv", "registry": outdir / "v73_shared_sector_registry.csv",
        "regime": outdir / "v73_daily_sector_regime.csv", "cross": outdir / "v73_cross_layer_audit.csv",
        "bridge": outdir / "v73_execution_bridge.csv", "health": outdir / "v73_state_health_audit.csv",
        "tape": outdir / "v73_sector_tape_snapshot.csv", "manifest": outdir / "v73_state_persistence_manifest.json",
    }
    listing = normalize_listing(listing_raw)
    active_state, state_source, migrated_count = _load_active_state(paths["active"], paths["life"], today)

    # Cross-check the restored state against the prior health ledger when available.
    # State rows are version-independent and are not normally deleted, so a row-count
    # regression is a persistence failure rather than a fresh discovery cycle.
    if _v73362_on("V73_STATE_RESTORE_GUARD", "0") and not _v73362_on("V73_ALLOW_FRESH_START", "0"):
        try:
            hist = _read_csv(paths["health"])
            if hist is not None and not hist.empty:
                h = hist.copy()
                h["scan_date"] = pd.to_datetime(h.get("scan_date"), errors="coerce")
                h = h.dropna(subset=["scan_date"])
                h = h[h["scan_date"] < pd.Timestamp(today)].sort_values("scan_date")
                if not h.empty:
                    prev = h.iloc[-1]
                    expected_rows = int(_num(prev.get("restored_rows")))
                    expected_after = int(_num(prev.get("active_after")))
                    if not active_state.empty and expected_rows > 0 and len(active_state) < expected_rows:
                        state_source = f"STATE_RESTORE_FAILURE_ROWCOUNT_REGRESSION_{len(active_state)}LT{expected_rows}"
                    elif active_state.empty and expected_after > 0:
                        state_source = f"STATE_RESTORE_FAILURE_HISTORY_EXPECTED_{expected_after}"
        except Exception:
            pass

    if str(state_source).startswith("STATE_RESTORE_"):
        reason = str(state_source)
        report = ("🧭 [V73 Discovery → Tracking → Execution]\n"
                  f"📌 {V73_VERSION} | {today} | SAFE STOP / 기존 V72.29 STRICT 변경 없음\n"
                  f"🚨 상태원장 복원 보호 작동: {reason}\n"
                  f"📌 cache matched={os.getenv('V73_CACHE_RESTORE_MATCHED','0')} · active_file={os.getenv('V73_CACHE_ACTIVE_PRESENT','0')} · lifecycle_file={os.getenv('V73_CACHE_LIFECYCLE_PRESENT','0')}\n"
                  "- 기존 활성 사이클을 잃은 상태에서 EMPTY_FRESH_START로 재시작하지 않습니다.\n"
                  "- 오늘 V73 lifecycle 계산/연구승격/상태원장 덮어쓰기를 모두 중단합니다.\n"
                  "- GitHub Actions의 V73 restored state diagnostics와 cache matched-key를 확인하세요.")
        health = pd.DataFrame([{"scan_date": today, "scan_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "state_source": reason, "migrated_count": migrated_count, "restored_rows": len(active_state),
            "active_before": int(pd.to_numeric(active_state.get("is_active", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not active_state.empty else 0,
            "active_after": 0, "same_day_refresh": 0, "same_day_forward": 0, "expired_count": 0,
            "signals": 0, "calculated": 0, "fetch_fail": 0, "policy_version": V73_VERSION, "state_guard_blocked": 1}])
        _append_dedup(paths["health"], health, ["scan_date"])
        return report, pd.DataFrame(), {"listing": len(listing), "universe": 0, "signals": 0, "safe_stop": True, "state_source": reason, "state_guard_blocked": True}

    if listing.empty:
        source = os.environ.get("V73_LISTING_SOURCE", "UNAVAILABLE")
        report = ("🧭 [V73 Discovery → Tracking → Execution]\n"
                  f"📌 {V73_VERSION} | {today} | SAFE STOP / 기존 V72.29 STRICT 변경 없음\n"
                  f"🚨 종목목록 사용 불가 | source={source}\n"
                  f"📌 상태원장: {state_source} · 활성 {int(active_state.get('is_active', pd.Series(dtype=int)).sum()) if not active_state.empty else 0}개 보존\n"
                  "- 종목목록이 없어 계산만 중단했으며 기존 활성 사이클은 삭제하지 않았습니다.")
        fail = pd.DataFrame([{"scan_date": today, "code": "", "name": "", "stage": "LISTING_LOAD", "result": "FAIL", "reason": "LISTING_UNAVAILABLE", "policy_version": V73_VERSION}])
        _append_dedup(paths["reject"], fail, ["scan_date", "code", "stage"])
        return report, listing, {"listing": 0, "universe": 0, "signals": 0, "safe_stop": True, "state_source": state_source}

    universe, rejection = build_dynamic_universe(listing, active_state, asof_date=today)
    workers = _env_int("V73_DISCOVERY_WORKERS", 10, 1, 24); count = _env_int("V73_HISTORY_COUNT", 330, 120, 600)
    records: List[dict] = []; fetch_fail: List[dict] = []
    _active_lookup = _state_row_dict(active_state)
    def task(row: dict):
        df, source = fetcher(row["code"], count, None)
        active = _active_lookup.get(str(row["code"]), {})
        metrics = _cycle_anchored_metrics(df, _metrics(df), active.get("active_first_signal_date"))
        return row, metrics, source
    executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="v73disc")
    futures = [executor.submit(task, r) for r in universe.to_dict("records")]
    try:
        for fut in as_completed(futures):
            try: row, m, source = fut.result()
            except Exception as e:
                fetch_fail.append({"scan_date": today, "code": "", "name": "", "stage": "HISTORY_FETCH", "result": "FAIL", "reason": type(e).__name__}); continue
            tags = multi_sector_tags(row["code"], row["name"], row["industry"])
            if not m:
                fetch_fail.append({"scan_date": today, "code": row["code"], "name": row["name"], "stage": "HISTORY_METRICS", "result": "FAIL", "reason": source}); continue
            profiles, reasons = detect_profiles(m)
            rec = dict(row); rec.update(m); rec.update({"scan_date": today, "source": source, "sector_tags": "|".join(tags), "profiles": "|".join(profiles), "profile_labels": "|".join(PROFILE_LABELS[p] for p in profiles), "reasons": " / ".join(reasons)})
            records.append(rec)
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    frame = pd.DataFrame(records)
    if frame.empty:
        rejection["scan_date"] = today
        _append_dedup(paths["reject"], pd.concat([rejection, pd.DataFrame(fetch_fail)], ignore_index=True, sort=False), ["scan_date", "code", "stage"])
        return f"🧭 [V73 Discovery → Tracking → Execution]\n📌 {V73_VERSION} | {today}\n- 계산 가능한 데이터가 없습니다. 활성 사이클은 보존했습니다.", frame, {"universe": len(universe), "signals": 0, "state_source": state_source}

    expanded: List[dict] = []
    for idx, r in frame.iterrows():
        for tag in str(r["sector_tags"]).split("|"):
            if _valid_sector_tag(tag): expanded.append({"idx": idx, "sector": tag, "ret1": _num(r["ret1"]), "amount_eok": _num(r["amount_eok"])})
    ex = pd.DataFrame(expanded); sector_med = ex.groupby("sector")["ret1"].median().to_dict() if not ex.empty else {}; sector_n = ex.groupby("sector").size().to_dict() if not ex.empty else {}
    scores: List[float] = []; excesses: List[float] = []; final_profiles: List[str] = []; final_reasons: List[str] = []
    for _, r in frame.iterrows():
        tags = str(r["sector_tags"]).split("|"); valid = [t for t in tags if _valid_sector_tag(t) and sector_n.get(t, 0) >= 3]
        med = float(np.median([sector_med[t] for t in valid])) if valid else np.nan; excess = _num(r["ret1"]) - med if math.isfinite(med) else 0.0
        profiles = [p for p in str(r["profiles"]).split("|") if p]; reasons = [x for x in str(r["reasons"]).split(" / ") if x]
        if valid and _num(r["ret1"]) >= 3.0 and excess >= 3.0 and _num(r["amount_eok"]) >= 20 and _num(r["range_pos"]) >= 0.65 and _num(r["vol_ratio20"]) >= 0.9:
            profiles.append("DISCOVERY_E"); reasons.append(f"유효섹터 중앙 대비 {excess:+.1f}%p")
        profiles = list(dict.fromkeys(profiles)); scores.append(_profile_score(r, profiles, excess)); excesses.append(round(excess, 2)); final_profiles.append("|".join(profiles)); final_reasons.append(" / ".join(reasons))
    frame["sector_excess_pctp"] = excesses; frame["profiles"] = final_profiles
    frame["profile_labels"] = frame["profiles"].map(lambda x: "|".join(PROFILE_LABELS[p] for p in x.split("|") if p)); frame["reasons"] = final_reasons; frame["discovery_score"] = scores
    frame["signal_phase"] = [_signal_phase(r, [p for p in str(r["profiles"]).split("|") if p]) for _, r in frame.iterrows()]
    frame, tape_table, macro_source = _apply_sector_execution_metadata(frame, outdir, today)
    signal = frame[frame["profiles"].str.len().gt(0)].copy()

    prior_life = _prior_lifecycle(paths["life"]); today_existing: Dict[str, dict] = {}
    if not prior_life.empty and "signal_date" in prior_life.columns:
        same = prior_life[prior_life["signal_date"].dt.strftime("%Y-%m-%d").eq(today)]
        today_existing = {str(r.get("code", "")).zfill(6): r.to_dict() for _, r in same.iterrows()}
    state_map = _state_row_dict(active_state); lifecycle_rows: List[dict] = []; same_day_refresh = 0; same_day_forward = 0; expired_count = 0
    signal_codes = set(signal["code"].astype(str)) if not signal.empty else set()
    for _, r in signal.iterrows():
        code = str(r["code"]); profiles = [p for p in str(r["profiles"]).split("|") if p]
        state, first, gap, event, base_state = _transition_signal(code, today, profiles, state_map.get(code))
        if code in today_existing or event == "SAME_DAY_REFRESH":
            old_state = str(today_existing.get(code, {}).get("state", "") or base_state.get("last_state", "") or state)
            # Same-day idempotency blocks duplicate cycles, not monotonic lifecycle progress.
            # PULLBACK_SETUP -> RESTART_CONFIRMED is allowed and persisted atomically.
            state, event = _same_day_monotonic_state(old_state, str(r.get("signal_phase", "")), state)
            if event == "SAME_DAY_FORWARD": same_day_forward += 1
            same_day_refresh += 1
        previous_first = str(base_state.get("previous_cycle_first_signal_date", "") or ""); previous_last = str(base_state.get("previous_cycle_last_signal_date", "") or "")
        persisted_phase = "RESTART_CONFIRMED" if state == "RESTART_CONFIRMED" else ("PULLBACK_SETUP" if state == "PULLBACK_SETUP" else str(r.get("signal_phase", "") or "NO_SIGNAL"))
        execution_status = str(r.get("execution_gate", "TRACKING_ONLY"))
        promotion_blockers = str(r.get("promotion_blockers", "") or "")
        readiness_precheck = str(r.get("readiness_precheck", "") or "")
        if execution_status == "PROMOTION_RESEARCH_READY" and state != "RESTART_CONFIRMED":
            execution_status = "TRACKING_ONLY"
            promotion_blockers = "|".join(x for x in [promotion_blockers, "PHASE_STATE_MISMATCH"] if x)
            readiness_precheck = "|".join(x for x in [readiness_precheck, "PERSISTED_PHASE_NOT_RESTART"] if x)
        lifecycle_rows.append({"signal_date": today, "code": code, "name": r["name"], "state": state, "run_event": event, "first_signal_date": first,
            "previous_cycle_first_signal_date": previous_first, "previous_cycle_last_signal_date": previous_last, "days_from_prior_signal": gap,
            "profiles": r["profiles"], "profile_labels": r["profile_labels"], "discovery_score": r["discovery_score"], "close": r["close"], "ret1": r["ret1"],
            "amount_eok": r["amount_eok"], "sector_tags": r["sector_tags"], "signal_phase": persisted_phase, "pullback_debug": r.get("pullback_debug", ""),
            "liquidity_tier": r["liquidity_tier"], "final_sector_regime": r["final_sector_regime"], "research_entry": r.get("research_entry", 0), "research_stop": r.get("research_stop", 0), "entry_risk_pct": r["entry_risk_pct"],
            "entry_risk_status": r.get("entry_risk_status", ""), "execution_status": execution_status, "promotion_blockers": promotion_blockers, "readiness_precheck": readiness_precheck, "policy_version": V73_VERSION, "scan_timestamp": now.strftime("%Y-%m-%d %H:%M:%S")})
        state_map[code] = {"code": code, "name": r["name"], "is_active": 1, "active_first_signal_date": first, "active_last_signal_date": today,
            "previous_cycle_first_signal_date": previous_first, "previous_cycle_last_signal_date": previous_last, "last_state": state,
            "last_profiles": r["profiles"], "last_phase": persisted_phase, "last_score": r["discovery_score"], "last_close": r["close"],
            "last_amount_eok": r["amount_eok"], "sector_tags": r["sector_tags"], "updated_date": today, "policy_version": V73_VERSION}

    for code, active in list(state_map.items()):
        if code in signal_codes or int(_num(active.get("is_active"))) != 1: continue
        last_dt = _safe_date(active.get("active_last_signal_date")); first_dt = _safe_date(active.get("active_first_signal_date"))
        if last_dt is None: continue
        age = _business_gap(last_dt, pd.Timestamp(today)); cur = frame[frame["code"].astype(str).eq(code)]
        if cur.empty: continue
        r = cur.iloc[0]
        preserve_same_day_signal = last_dt == pd.Timestamp(today) and bool(str(active.get("last_profiles", "") or ""))
        if preserve_same_day_signal:
            state = str(active.get("last_state", "") or "TRACKING_CONTINUE")
            phase = str(active.get("last_phase", "") or "NO_SIGNAL")
            event = "SAME_DAY_PRESERVE_SIGNAL"
            execution_status = str(today_existing.get(code, {}).get("execution_status", "") or "DISCOVERY_ONLY")
        elif age <= 15 and bool(r.get("pullback_setup")):
            state = "PULLBACK_SETUP"; phase = "PULLBACK_SETUP"; event = "DAILY_TRACK"; execution_status = "TRACKING_ONLY"
        elif age <= 15:
            state = "TRACKING_NO_NEW_SIGNAL"; phase = "NO_SIGNAL"; event = "DAILY_TRACK"; execution_status = "TRACKING_ONLY"
        else:
            state = "CYCLE_EXPIRED"; phase = "NO_SIGNAL"; event = "CYCLE_ARCHIVE"; execution_status = "ARCHIVED"; expired_count += 1
        first = first_dt.strftime("%Y-%m-%d") if first_dt is not None and age <= 15 else ""
        previous_first = str(active.get("previous_cycle_first_signal_date", "") or ""); previous_last = str(active.get("previous_cycle_last_signal_date", "") or "")
        if age > 15:
            previous_first = first_dt.strftime("%Y-%m-%d") if first_dt is not None else previous_first; previous_last = last_dt.strftime("%Y-%m-%d")
        lifecycle_rows.append({"signal_date": today, "code": code, "name": r["name"], "state": state, "run_event": event, "first_signal_date": first,
            "previous_cycle_first_signal_date": previous_first, "previous_cycle_last_signal_date": previous_last, "days_from_prior_signal": age,
            "profiles": str(active.get("last_profiles", "") or "") if preserve_same_day_signal else "", "profile_labels": "|".join(PROFILE_LABELS[p] for p in str(active.get("last_profiles", "") or "").split("|") if p) if preserve_same_day_signal else "", "discovery_score": r["discovery_score"], "close": r["close"], "ret1": r["ret1"], "amount_eok": r["amount_eok"],
            "sector_tags": r["sector_tags"], "signal_phase": phase, "pullback_debug": r.get("pullback_debug", ""), "liquidity_tier": r["liquidity_tier"],
            "final_sector_regime": r["final_sector_regime"], "entry_risk_pct": r["entry_risk_pct"], "execution_status": execution_status,
            "promotion_blockers": "NO_FRESH_RESTART", "readiness_precheck": "TRACKING_NO_FRESH_RESTART", "policy_version": V73_VERSION, "scan_timestamp": now.strftime("%Y-%m-%d %H:%M:%S")})
        if age > 15:
            active.update({"is_active": 0, "active_first_signal_date": "", "active_last_signal_date": "", "previous_cycle_first_signal_date": previous_first,
                           "previous_cycle_last_signal_date": previous_last, "last_state": state, "updated_date": today, "policy_version": V73_VERSION})
        else:
            active.update({"last_state": state, "last_close": r["close"], "last_amount_eok": r["amount_eok"], "sector_tags": r["sector_tags"], "updated_date": today, "policy_version": V73_VERSION})
        state_map[code] = active

    life = pd.DataFrame(lifecycle_rows)
    if not life.empty and not signal.empty:
        sig_states = life[life["code"].astype(str).isin(signal_codes)][["code", "state", "run_event", "first_signal_date", "days_from_prior_signal", "signal_phase", "execution_status", "promotion_blockers", "readiness_precheck"]].copy()
        sig_states = sig_states.rename(columns={"signal_phase": "persisted_phase", "execution_status": "persisted_execution_status", "promotion_blockers": "persisted_promotion_blockers", "readiness_precheck": "persisted_readiness_precheck"})
        signal = signal.merge(sig_states, on="code", how="left")
        signal["signal_phase"] = signal["persisted_phase"].where(signal["persisted_phase"].astype(str).str.len().gt(0), signal["signal_phase"])
        signal["execution_gate"] = signal["persisted_execution_status"].where(signal["persisted_execution_status"].astype(str).str.len().gt(0), signal["execution_gate"])
        signal["promotion_blockers"] = signal["persisted_promotion_blockers"].where(signal["persisted_promotion_blockers"].astype(str).str.len().gt(0), signal["promotion_blockers"])
        signal["readiness_precheck"] = signal["persisted_readiness_precheck"].where(signal["persisted_readiness_precheck"].astype(str).str.len().gt(0), signal["readiness_precheck"])
        # The final frame/bridge must use the persisted lifecycle state, never a transient pre-persistence gate.
        sig_gate = signal.set_index("code")[["signal_phase", "execution_gate", "promotion_blockers", "readiness_precheck"]].to_dict("index")
        for fi, fr in frame.iterrows():
            gd = sig_gate.get(str(fr.get("code", "")))
            if gd:
                for col, val in gd.items(): frame.at[fi, col] = val
    frame["stage_result"] = np.select([frame["profiles"].str.len().gt(0), frame["pullback_setup"].fillna(False).astype(bool)], ["SIGNAL", "PULLBACK_SETUP_CANDIDATE"], default="NO_SIGNAL")
    frame["execution_status"] = frame["execution_gate"]; frame["policy_version"] = V73_VERSION; frame["scan_timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
    rejection["scan_date"] = today; fetch_rej = pd.DataFrame(fetch_fail)
    if not fetch_rej.empty and "scan_date" not in fetch_rej.columns: fetch_rej["scan_date"] = today

    registry = listing[["code", "name", "market", "industry"]].copy(); registry["sector_tags"] = ["|".join(multi_sector_tags(r.code, r.name, r.industry)) for r in registry.itertuples()]
    registry["primary_sector"] = registry["sector_tags"].map(lambda s: next(iter(_sector_families(s)), "미분류")); registry["updated_date"] = today; registry["registry_version"] = V73_VERSION
    old_registry = _read_csv(paths["registry"]); registry_out = pd.concat([old_registry, registry], ignore_index=True, sort=False) if not old_registry.empty else registry
    registry_out["code"] = registry_out["code"].map(_code); registry_out = registry_out.drop_duplicates("code", keep="last")

    life_map = life.set_index("code").to_dict("index") if not life.empty else {}
    bridge_rows: List[dict] = []
    for _, r in frame.iterrows():
        code = str(r["code"]); lr = life_map.get(code, {})
        if not str(r.get("profiles", "")) and not lr: continue
        bridge_rows.append({"scan_date": today, "code": code, "name": r["name"], "state": lr.get("state", "UNTRACKED"), "run_event": lr.get("run_event", ""),
            "signal_phase": r["signal_phase"], "discovery_score": r["discovery_score"], "sector_tags": r["sector_tags"], "macro_sector_regime": r["macro_sector_regime"],
            "tape_sector_regime": r["tape_sector_regime"], "final_sector_regime": r["final_sector_regime"], "liquidity_tier": r["liquidity_tier"],
            "research_entry": r["research_entry"], "research_stop": r["research_stop"], "entry_risk_pct": r["entry_risk_pct"], "entry_risk_status": r["entry_risk_status"],
            "execution_gate": r["execution_gate"], "promotion_blockers": r["promotion_blockers"], "readiness_precheck": r.get("readiness_precheck", ""), "live_injection": "NO_V72_STRICT_FROZEN", "policy_version": V73_VERSION})
    bridge = pd.DataFrame(bridge_rows)
    cross = bridge.copy()
    if not cross.empty:
        prev_sector = {c: str(v.get("sector_tags", "")) for c, v in _state_row_dict(active_state).items()}
        cross["tracking_sector_tags_before"] = cross["code"].map(prev_sector).fillna("")
        cross["sector_changed"] = cross.apply(lambda r: bool(r["tracking_sector_tags_before"] and r["tracking_sector_tags_before"] != r["sector_tags"]), axis=1)
        cross["cross_layer_status"] = np.where(cross["sector_changed"], "SECTOR_MAPPING_CHANGED", "CONSISTENT")

    _append_dedup(paths["stage"], frame, ["scan_date", "code"]); _append_dedup(paths["reject"], pd.concat([rejection, fetch_rej], ignore_index=True, sort=False), ["scan_date", "code", "stage"])
    sector_audit = registry.copy().rename(columns={"updated_date": "scan_date"}); _append_dedup(paths["sector"], sector_audit, ["scan_date", "code"])
    if not life.empty: _append_dedup(paths["life"], life, ["signal_date", "code"])
    _atomic_write_csv(paths["active"], _state_upsert_frame(state_map)); _atomic_write_csv(paths["registry"], registry_out)
    if not tape_table.empty:
        tape_table["scan_date"] = today; _append_dedup(paths["tape"], tape_table, ["scan_date", "sector"])
    if not bridge.empty: _append_dedup(paths["bridge"], bridge, ["scan_date", "code"])
    if not cross.empty: _append_dedup(paths["cross"], cross, ["scan_date", "code"])
    health = pd.DataFrame([{"scan_date": today, "scan_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"), "state_source": state_source, "migrated_count": migrated_count,
        "restored_rows": len(active_state), "active_before": int(pd.to_numeric(active_state.get("is_active", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not active_state.empty else 0,
        "active_after": sum(int(_num(v.get("is_active"))) for v in state_map.values()), "same_day_refresh": same_day_refresh, "same_day_forward": same_day_forward, "expired_count": expired_count,
        "signals": len(signal), "calculated": len(frame), "fetch_fail": len(fetch_fail), "policy_version": V73_VERSION, "state_guard_blocked": 0}])
    _append_dedup(paths["health"], health, ["scan_date"])
    _v73362_write_state_manifest(paths["manifest"], {
        "version": V73_VERSION, "scan_date": today, "scan_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "state_source": state_source, "state_rows": len(state_map),
        "active_after": sum(int(_num(v.get("is_active"))) for v in state_map.values()),
        "same_day_refresh": same_day_refresh, "same_day_forward": same_day_forward,
    })
    signal.sort_values(["discovery_score", "amount_eok"], ascending=False).to_csv(paths["snapshot"], index=False, encoding="utf-8-sig")

    top_n = _env_int("V73_TELEGRAM_TOP_N", 10, 3, 20)
    group_order = [("BASE_IGNITION", "🟣 [저항 전 첫 시동]"), ("EARLY_DISCOVERY", "🟢 [초동 발견]"), ("CONFIRMED_BREAKOUT", "🔵 [확정돌파·첫눌림 등록]"),
                   ("RESTART_CONFIRMED", "✅ [첫눌림 재출발·연구승격 심사]"), ("LOW_CONFIRMATION", "⚪ [저확신 추적]"), ("LATE_SPIKE", "⚠️ [급등연장·첫눌림 대기]")]
    selected_parts: List[pd.DataFrame] = []; remaining = top_n
    for phase, _ in group_order:
        if remaining <= 0: break
        g = signal[signal["signal_phase"].eq(phase)].sort_values(["discovery_score", "amount_eok"], ascending=False)
        if not g.empty:
            take = g.head(remaining); selected_parts.append(take); remaining -= len(take)
    top = pd.concat(selected_parts, ignore_index=True) if selected_parts else pd.DataFrame()
    active_after = sum(int(_num(v.get("is_active"))) for v in state_map.values())
    lines = ["🧭 [V73 Discovery → Tracking → Execution]", f"📌 {V73_VERSION} | {today} | RESEARCH COMPLETE / 기존 V72.29 STRICT 변경 없음",
        f"📌 종목목록: {os.environ.get('V73_LISTING_SOURCE', 'UNKNOWN')} | 상태원장 {state_source} · 복원 {len(active_state)} · 활성 {active_after} · 동일일갱신 {same_day_refresh} · 동일일전진 {same_day_forward}",
        f"📌 유니버스: 상장 {len(listing)} → 기본통과 {int(((listing['price'] >= _env_float('V73_MIN_PRICE',3000)) & ((listing['marcap_eok'] <= 0) | (listing['marcap_eok'] >= _env_float('V73_MIN_MARCAP_EOK',300)))).sum())} → 동적발견 {len(universe)}",
        f"📌 결과: 계산 {len(frame)} | 발견신호 {len(signal)} | 데이터실패 {len(fetch_fail)} | 매크로섹터 {macro_source}",
        "📌 실행계약: RESTART + 점수70 + L1/L2 + 섹터역풍 없음 + 확인진입 손실폭≤5%만 연구승격 · LIVE 주입 금지", ""]
    if top.empty:
        lines += ["- 오늘 V73 최소 발견 조건을 통과한 종목이 없습니다.", "- 억지로 후보를 채우지 않습니다."]
    else:
        rank = 0
        for phase, header in group_order:
            g = top[top["signal_phase"].eq(phase)] if "signal_phase" in top.columns else pd.DataFrame()
            if g.empty: continue
            lines.append(header)
            for _, r in g.iterrows():
                rank += 1; labels = ", ".join(PROFILE_LABELS[p] for p in str(r["profiles"]).split("|") if p); state = str(r.get("state", "NEW_DISCOVERY")); event = str(r.get("run_event", ""))
                support = max(_num(r.get("ma5")), _num(r.get("ma20"))); state_text = state + (f"·{event}" if event == "SAME_DAY_REFRESH" else "")
                lines += [f"{rank}) {r['name']}({r['code']}) | {r['discovery_score']:.1f}점 | {state_text}",
                    f"   🧬 {labels}",
                    f"   {int(round(_num(r['close']))):,}원 ({_num(r['ret1']):+.2f}%) | {_num(r['amount_eok']):,.0f}억·{r['liquidity_tier'].split('_')[0]} | V{_num(r['vol_ratio20']):.1f} | 고가유지 {_num(r['range_pos'])*100:.0f}%",
                    f"   섹터 {str(r['sector_tags']).replace('|','·')} | 매크로/테이프/최종 {r['macro_sector_regime']}/{r['tape_sector_regime']}/{r['final_sector_regime']}",
                    f"   게이트 {r['execution_gate']} | 차단 {str(r['promotion_blockers']) or '-'} | 사전점검 {str(r.get('readiness_precheck','')) or '-'} | 확인진입 {int(round(_num(r.get('research_entry')))):,}→손절 {int(round(_num(r.get('research_stop')))):,} 위험 {_num(r.get('entry_risk_pct')):.1f}% | 추적선 {int(round(support)):,}원",
                    f"   사유: {str(r['reasons'])[:125]}", ""]
    pullback = life[life["state"].astype(str).eq("PULLBACK_SETUP")].copy() if not life.empty else pd.DataFrame()
    if not pullback.empty:
        pullback = pullback.sort_values(["discovery_score", "amount_eok"], ascending=False).head(5)
        lines += ["🔄 [활성 사이클 첫눌림 진행]", "📌 새 매수신호가 아니라 재출발 전 추적 상태입니다."]
        for _, r in pullback.iterrows():
            lines += [f"- {r.get('name')}({r.get('code')}) | 최초 {r.get('first_signal_date')} | {r.get('liquidity_tier','-')} | 섹터 {r.get('final_sector_regime','UNKNOWN')}", f"  {str(r.get('pullback_debug',''))[:170] or '저거래량 눌림·지지·OBV 유지 추적'}"]
        lines.append("")
    ready = bridge[bridge["execution_gate"].eq("PROMOTION_RESEARCH_READY")] if not bridge.empty else pd.DataFrame()
    lines += ["🧪 [연구승격 결과]", f"- PROMOTION_RESEARCH_READY {len(ready)}개 | V72.29 STRICT 자동주입 0개"]
    lines += ["📁 [핵심 원장]", "- v73_active_cycle_state.csv: 버전 독립 활성 사이클 단일 상태", "- v73_shared_sector_registry.csv: Discovery·LIVE 표시 공용 섹터", "- v73_daily_sector_regime.csv: 시황 유리·관찰·불리 섹터", "- v73_execution_bridge.csv: 유동성·섹터·위험계약·승격 차단", "- v73_cross_layer_audit.csv: 계층 간 섹터/상태 불일치", f"⏱️ 실행 {time.monotonic()-started:.1f}초"]
    return "\n".join(lines).strip(), signal, {"universe": len(universe), "calculated": len(frame), "signals": len(signal), "fetch_fail": len(fetch_fail), "state_source": state_source, "active_after": active_after, "same_day_refresh": same_day_refresh, "same_day_forward": same_day_forward, "promotion_ready": len(ready)}

# ✅ END V73.3 COMPLETE LIFECYCLE / SHARED SECTOR / REGIME / EXECUTION CONTRACT
