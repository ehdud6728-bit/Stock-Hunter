from __future__ import annotations

import json
import math
import os
import re
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

VERSION = "V73.3.6.6.20"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "📦 [과거시점 TOP500 × 이벤트 확장 Universe 감사 · RESEARCH_ONLY]"

MEMBERSHIP_FILE = "v73_universe_asof_membership.csv"
SUMMARY_FILE = "v73_universe_asof_summary.csv"
COVERAGE_FILE = "v73_universe_rank_bucket_coverage.csv"
AVAILABILITY_FILE = "v73_universe_data_availability.csv"
REPORT_FILE = "v73_universe_asof_report.txt"

CAUSAL_GEO_MODES = {"FORWARD_CAUSAL", "OFFICIAL_ARCHIVE_CAUSAL"}

# V20 persistent raw-data cache. These files are causal source snapshots only; they do not
# contain strategy outcomes and are safe to reuse across repeated Direct Replay runs.
_CACHE_ROOT = Path(os.getenv("V20_ASOF_CACHE_DIR", "reports/.cache/v20_asof_snapshots"))
_CACHE_STATS = {"listing_hit":0,"listing_miss":0,"market_hit":0,"market_miss":0,"cap_hit":0,"cap_miss":0,"name_hit":0,"name_miss":0}
_TICKER_NAME_MEM: dict[str,str] = {}
_NAME_MAP_LOADED = False

def _set_cache_root(output_dir: str | Path) -> Path:
    global _CACHE_ROOT
    _CACHE_ROOT = Path(os.getenv("V20_ASOF_CACHE_DIR", str(Path(output_dir or "reports") / ".cache/v20_asof_snapshots")))
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT

def _cache_csv(kind: str, ymd: str) -> Path:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / kind / f"{ymd}.csv.gz"

def _read_cache(kind: str, ymd: str) -> pd.DataFrame:
    p = _cache_csv(kind, ymd)
    if not p.exists(): return pd.DataFrame()
    try: return pd.read_csv(p, dtype={"Code":str,"code":str})
    except Exception: return pd.DataFrame()

def _write_cache(kind: str, ymd: str, df: pd.DataFrame) -> None:
    if df is None or df.empty: return
    p = _cache_csv(kind, ymd); p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + f".{os.getpid()}.tmp")
    df.to_csv(tmp, index=False, compression="gzip")
    os.replace(tmp, p)

def _load_name_map() -> None:
    global _NAME_MAP_LOADED, _TICKER_NAME_MEM
    if _NAME_MAP_LOADED: return
    _NAME_MAP_LOADED = True
    p = _CACHE_ROOT / "ticker_name_map.json"
    try:
        if p.exists():
            q=json.loads(p.read_text(encoding="utf-8"))
            if isinstance(q,dict): _TICKER_NAME_MEM.update({str(k):str(v) for k,v in q.items()})
    except Exception: pass

def _save_name_map() -> None:
    try:
        p=_CACHE_ROOT / "ticker_name_map.json"; p.parent.mkdir(parents=True,exist_ok=True)
        tmp=p.with_suffix('.json.tmp'); tmp.write_text(json.dumps(_TICKER_NAME_MEM,ensure_ascii=False),encoding='utf-8'); os.replace(tmp,p)
    except Exception: pass


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(str(os.getenv(name, default)).strip()))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool = False) -> bool:
    v = str(os.getenv(name, "1" if default else "0")).strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _norm_code(v: Any) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s[-6:].zfill(6) if s else ""


def _to_num(s: pd.Series | Any) -> pd.Series:
    if isinstance(s, pd.Series):
        return pd.to_numeric(s, errors="coerce")
    return pd.Series(dtype=float)


def _pick_col(df: pd.DataFrame, names: Iterable[str]) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None


def _sha_df(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "EMPTY"
    cols = sorted(map(str, df.columns))
    z = df[cols].astype(str)
    sort_cols = [c for c in ["signal_date", "code", "universe_rank"] if c in z.columns]
    if sort_cols:
        z = z.sort_values(sort_cols, kind="stable")
    return hashlib.sha256(z.to_csv(index=False).encode("utf-8")).hexdigest()[:20]


def _out_dir(output_dir: str | Path) -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_csv(path, index=False, encoding="utf-8-sig")


def _asof_1503(asof_date: Any) -> pd.Timestamp:
    d = pd.Timestamp(asof_date).normalize()
    # Timestamp is intentionally timezone-naive here because upstream ledgers are usually naive KST.
    return d + pd.Timedelta(hours=15, minutes=3)


def _normalize_market_snapshot(raw: pd.DataFrame, market: str = "ALL") -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["code", "close", "volume", "amount", "ret_pct", "market"])
    q = raw.copy()
    if q.index.name is not None or not isinstance(q.index, pd.RangeIndex):
        q = q.reset_index()
    cc = _pick_col(q, ["티커", "ticker", "Code", "code", "종목코드", "index"])
    if not cc:
        # reset_index usually yields the first column with ticker values.
        cc = q.columns[0] if len(q.columns) else None
    if not cc:
        return pd.DataFrame(columns=["code", "close", "volume", "amount", "ret_pct", "market"])
    out = pd.DataFrame({"code": q[cc].map(_norm_code)})
    close_c = _pick_col(q, ["종가", "Close", "close", "현재가"])
    vol_c = _pick_col(q, ["거래량", "Volume", "volume"])
    amt_c = _pick_col(q, ["거래대금", "Amount", "amount", "거래대금(원)"])
    ret_c = _pick_col(q, ["등락률", "Change", "change", "수익률"])
    out["close"] = pd.to_numeric(q[close_c], errors="coerce") if close_c else np.nan
    out["volume"] = pd.to_numeric(q[vol_c], errors="coerce") if vol_c else np.nan
    out["amount"] = pd.to_numeric(q[amt_c], errors="coerce") if amt_c else np.nan
    if out["amount"].isna().all() and out["close"].notna().any() and out["volume"].notna().any():
        out["amount"] = out["close"] * out["volume"]
    out["ret_pct"] = pd.to_numeric(q[ret_c], errors="coerce") if ret_c else np.nan
    out["market"] = str(market or "ALL").upper()
    return out[out["code"].ne("")].drop_duplicates("code", keep="last").reset_index(drop=True)


def _normalize_cap_snapshot(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["code", "marcap"])
    q = raw.copy()
    if q.index.name is not None or not isinstance(q.index, pd.RangeIndex):
        q = q.reset_index()
    cc = _pick_col(q, ["티커", "ticker", "Code", "code", "종목코드", "index"])
    if not cc:
        cc = q.columns[0] if len(q.columns) else None
    mc = _pick_col(q, ["시가총액", "Marcap", "MarCap", "marcap", "market_cap"])
    if not cc or not mc:
        return pd.DataFrame(columns=["code", "marcap"])
    out = pd.DataFrame({"code": q[cc].map(_norm_code), "marcap": pd.to_numeric(q[mc], errors="coerce")})
    return out[out["code"].ne("")].drop_duplicates("code", keep="last").reset_index(drop=True)


def _ticker_names_pykrx(stock_module: Any, ymd: str) -> tuple[pd.DataFrame, list[str]]:
    cached = _read_cache("listing", ymd)
    if not cached.empty:
        _CACHE_STATS["listing_hit"] += 1
        return cached, []
    _CACHE_STATS["listing_miss"] += 1
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    if stock_module is None:
        return pd.DataFrame(), ["pykrx_stock_module_missing"]
    _load_name_map()
    name_changed = False
    for market in ["KOSPI", "KOSDAQ"]:
        tickers: list[str] = []
        try:
            try:
                tickers = list(stock_module.get_market_ticker_list(date=ymd, market=market) or [])
            except TypeError:
                tickers = list(stock_module.get_market_ticker_list(ymd, market=market) or [])
        except Exception as exc:
            errors.append(f"ticker_list_{market}:{type(exc).__name__}:{exc}")
            continue
        for t in tickers:
            code = _norm_code(t)
            name = _TICKER_NAME_MEM.get(code, "")
            if name:
                _CACHE_STATS["name_hit"] += 1
            else:
                _CACHE_STATS["name_miss"] += 1
                try:
                    name = str(stock_module.get_market_ticker_name(code) or "")
                except Exception:
                    name = ""
                if name:
                    _TICKER_NAME_MEM[code] = name; name_changed = True
            rows.append({"Code": code, "Name": name, "Market": market})
    out = pd.DataFrame(rows).drop_duplicates("Code", keep="last") if rows else pd.DataFrame()
    if not out.empty:
        try: _write_cache("listing", ymd, out)
        except Exception: pass
    if name_changed: _save_name_map()
    return out, errors

def _filter_security_names(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    q = df.copy()
    if "Code" not in q.columns:
        cc = _pick_col(q, ["code", "Symbol", "종목코드"])
        if cc:
            q = q.rename(columns={cc: "Code"})
    if "Name" not in q.columns:
        nc = _pick_col(q, ["name", "종목명"])
        if nc:
            q = q.rename(columns={nc: "Name"})
    if "Code" not in q.columns:
        return pd.DataFrame()
    q["Code"] = q["Code"].map(_norm_code)
    if "Name" not in q.columns:
        q["Name"] = ""
    q["Name"] = q["Name"].fillna("").astype(str)
    if "Market" in q.columns:
        q = q[q["Market"].astype(str).str.upper().isin(["KOSPI", "KOSDAQ", "코스피", "코스닥", "유가"])].copy()
    bad = r"ETF|ETN|스팩|제[0-9]+호|우$|우A$|우B$|우C$|우선주"
    q = q[~q["Name"].str.contains(bad, regex=True, na=False)].copy()
    return q[q["Code"].ne("")].drop_duplicates("Code", keep="last")


def _get_market_snapshot(stock_module: Any, ymd: str) -> tuple[pd.DataFrame, str]:
    cached = _read_cache("market", ymd)
    if not cached.empty:
        _CACHE_STATS["market_hit"] += 1
        return cached, "V20_DISK_CACHE:PYKRX_DAILY_CROSS_SECTION"
    _CACHE_STATS["market_miss"] += 1
    if stock_module is None:
        return pd.DataFrame(), "PYKRX_UNAVAILABLE"
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    for market in ["KOSPI", "KOSDAQ"]:
        try:
            try:
                raw = stock_module.get_market_ohlcv_by_ticker(ymd, market=market)
            except TypeError:
                raw = stock_module.get_market_ohlcv_by_ticker(date=ymd, market=market)
            z = _normalize_market_snapshot(raw, market)
            if not z.empty: frames.append(z)
        except Exception as exc:
            errors.append(f"{market}:{type(exc).__name__}:{exc}")
    if not frames:
        return pd.DataFrame(), "PYKRX_EMPTY" + (":" + "|".join(errors[:2]) if errors else "")
    out = pd.concat(frames, ignore_index=True).drop_duplicates("code", keep="last")
    try: _write_cache("market", ymd, out)
    except Exception: pass
    return out, "PYKRX_DAILY_CROSS_SECTION"

def _get_cap_snapshot(stock_module: Any, ymd: str) -> tuple[pd.DataFrame, str]:
    cached = _read_cache("cap", ymd)
    if not cached.empty:
        _CACHE_STATS["cap_hit"] += 1
        return cached, "V20_DISK_CACHE:PYKRX_MARKET_CAP"
    _CACHE_STATS["cap_miss"] += 1
    if stock_module is None:
        return pd.DataFrame(), "PYKRX_UNAVAILABLE"
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    for market in ["KOSPI", "KOSDAQ"]:
        try:
            try:
                raw = stock_module.get_market_cap_by_ticker(ymd, market=market)
            except TypeError:
                raw = stock_module.get_market_cap_by_ticker(date=ymd, market=market)
            z = _normalize_cap_snapshot(raw)
            if not z.empty: frames.append(z)
        except Exception as exc:
            errors.append(f"{market}:{type(exc).__name__}:{exc}")
    if not frames:
        return pd.DataFrame(), "PYKRX_CAP_EMPTY" + (":" + "|".join(errors[:2]) if errors else "")
    out = pd.concat(frames, ignore_index=True).drop_duplicates("code", keep="last")
    try: _write_cache("cap", ymd, out)
    except Exception: pass
    return out, "PYKRX_MARKET_CAP"

def _calendar_before(asof_date: Any, n: int, fdr_reader: Callable[..., pd.DataFrame] | None = None) -> list[pd.Timestamp]:
    d = pd.Timestamp(asof_date).normalize()
    if callable(fdr_reader):
        start = (d - pd.Timedelta(days=max(90, n * 4))).strftime("%Y-%m-%d")
        end = d.strftime("%Y-%m-%d")
        for symbol in ["KS11", "KOSPI"]:
            try:
                z = fdr_reader(symbol, start, end)
                if z is not None and not z.empty:
                    dates = [pd.Timestamp(x).normalize() for x in pd.to_datetime(z.index) if pd.Timestamp(x).normalize() < d]
                    return sorted(list(dict.fromkeys(dates)))[-n:]
            except Exception:
                continue
    # Fail-safe business-day approximation is diagnostic only. Caller records source quality.
    return list(pd.bdate_range(end=d - pd.Timedelta(days=1), periods=n))


def _official_geo_codes(out: Path, cutoff: pd.Timestamp) -> set[str]:
    p = out / "v73_geo_official_archive_ledger.csv"
    if not p.exists():
        return set()
    try:
        q = pd.read_csv(p, dtype=str)
    except Exception:
        return set()
    if q.empty:
        return set()
    mode_c = _pick_col(q, ["causal_mode", "mode"])
    at_c = _pick_col(q, ["official_at", "published_at", "event_at"])
    if mode_c:
        q = q[q[mode_c].fillna("").isin(CAUSAL_GEO_MODES)].copy()
    if at_c:
        ts = pd.to_datetime(q[at_c], errors="coerce")
        q = q[ts.notna() & ts.le(cutoff)].copy()
    codes: set[str] = set()
    for c in ["code", "Code", "related_code", "ticker"]:
        if c in q.columns:
            codes.update(_norm_code(v) for v in q[c].tolist() if _norm_code(v))
    for c in ["related_codes", "beneficiary_codes", "codes"]:
        if c in q.columns:
            for v in q[c].fillna("").astype(str):
                for part in re.split(r"[,|;\s]+", v):
                    code = _norm_code(part)
                    if code:
                        codes.add(code)
    return codes


def build_asof_universe_from_snapshots(
    asof_date: Any,
    listing: pd.DataFrame,
    history_snapshots: dict[pd.Timestamp, pd.DataFrame],
    cap_snapshot: pd.DataFrame | None = None,
    core_n: int = 500,
    event_max: int = 100,
    min_price: float = 3000.0,
    min_marcap: float = 30_000_000_000.0,
    event_amount_ratio: float = 3.0,
    event_volume_ratio: float = 3.0,
    event_prev_ret_pct: float = 5.0,
    event_min_amount: float = 10_000_000_000.0,
    official_geo_codes: set[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pure deterministic constructor used by runtime and synthetic validation.

    history_snapshots must contain only dates strictly before asof_date. The final date in
    the mapping is D-1. No signal-day OHLCV is consumed by this function.
    """
    asof = pd.Timestamp(asof_date).normalize()
    listing = _filter_security_names(listing)
    if listing.empty or not history_snapshots:
        return pd.DataFrame(), pd.DataFrame()
    clean_hist: list[tuple[pd.Timestamp, pd.DataFrame]] = []
    for dt, raw in history_snapshots.items():
        t = pd.Timestamp(dt).normalize()
        if t >= asof:
            continue
        z = raw.copy() if isinstance(raw, pd.DataFrame) else pd.DataFrame()
        if z.empty:
            continue
        if "code" not in z.columns:
            z = _normalize_market_snapshot(z)
        if not z.empty:
            z["date"] = t
            clean_hist.append((t, z))
    if not clean_hist:
        return pd.DataFrame(), pd.DataFrame()
    clean_hist.sort(key=lambda x: x[0])
    prior = pd.concat([z for _, z in clean_hist], ignore_index=True)
    last_date = clean_hist[-1][0]
    prev = clean_hist[-1][1].drop(columns=["date"], errors="ignore").copy()
    avg = prior.groupby("code", as_index=False).agg(
        avg_amount20=("amount", "mean"),
        avg_volume20=("volume", "mean"),
        obs_days=("date", "nunique"),
    )
    base = listing[[c for c in ["Code", "Name", "Market"] if c in listing.columns]].copy()
    base = base.rename(columns={"Code": "code", "Name": "name", "Market": "market"})
    if "market" not in base.columns:
        base["market"] = "UNKNOWN"
    base = base.merge(avg, on="code", how="left")
    base = base.merge(prev[[c for c in ["code", "close", "volume", "amount", "ret_pct"] if c in prev.columns]].rename(columns={
        "close": "prev_close", "volume": "prev_volume", "amount": "prev_amount", "ret_pct": "prev_ret_pct"
    }), on="code", how="left")
    if cap_snapshot is not None and not cap_snapshot.empty:
        cap = cap_snapshot.copy()
        if "code" not in cap.columns:
            cap = _normalize_cap_snapshot(cap)
        base = base.merge(cap[["code", "marcap"]], on="code", how="left")
    else:
        base["marcap"] = np.nan

    base["signal_date"] = asof
    base["liquidity_asof_date"] = last_date
    base["avg_amount20"] = pd.to_numeric(base["avg_amount20"], errors="coerce")
    base["avg_volume20"] = pd.to_numeric(base["avg_volume20"], errors="coerce")
    base["prev_amount"] = pd.to_numeric(base.get("prev_amount"), errors="coerce")
    base["prev_volume"] = pd.to_numeric(base.get("prev_volume"), errors="coerce")
    base["prev_close"] = pd.to_numeric(base.get("prev_close"), errors="coerce")
    base["prev_ret_pct"] = pd.to_numeric(base.get("prev_ret_pct"), errors="coerce")
    base["amount_ratio_prev_vs20"] = base["prev_amount"] / base["avg_amount20"].replace(0, np.nan)
    base["volume_ratio_prev_vs20"] = base["prev_volume"] / base["avg_volume20"].replace(0, np.nan)

    # Causal eligibility requires D-1 close and sufficient history. Market-cap absence is
    # explicit: do not silently treat missing market cap as pass if the source was expected.
    base["price_pass"] = base["prev_close"].ge(float(min_price))
    if base["marcap"].notna().any():
        base["marcap_pass"] = base["marcap"].ge(float(min_marcap))
    else:
        base["marcap_pass"] = True
    base["history_pass"] = base["obs_days"].fillna(0).ge(min(10, max(3, len(clean_hist) // 2)))
    eligible = base[base["price_pass"] & base["marcap_pass"] & base["history_pass"] & base["avg_amount20"].notna()].copy()
    eligible = eligible.sort_values(["avg_amount20", "marcap", "code"], ascending=[False, False, True], na_position="last", kind="stable")
    eligible["liquidity_rank"] = np.arange(1, len(eligible) + 1)

    core_n = max(20, int(core_n))
    core = eligible.head(core_n).copy()
    core["universe_source"] = "CORE_LIQUID"
    core["event_reason"] = ""

    geo = official_geo_codes or set()
    outside = eligible[~eligible["code"].isin(set(core["code"]))].copy()
    outside["event_amount"] = outside["amount_ratio_prev_vs20"].ge(event_amount_ratio) & outside["prev_amount"].ge(event_min_amount)
    outside["event_volume"] = outside["volume_ratio_prev_vs20"].ge(event_volume_ratio) & outside["prev_amount"].ge(event_min_amount)
    outside["event_return"] = outside["prev_ret_pct"].ge(event_prev_ret_pct) & outside["prev_amount"].ge(event_min_amount)
    outside["event_geo"] = outside["code"].isin(geo)
    outside["event_score"] = outside[["event_amount", "event_volume", "event_return", "event_geo"]].sum(axis=1)
    event = outside[outside["event_score"].ge(1)].copy()
    if not event.empty:
        def _reason(r: pd.Series) -> str:
            names = []
            if bool(r.get("event_amount")): names.append("D1_AMOUNT_EXPANSION")
            if bool(r.get("event_volume")): names.append("D1_VOLUME_EXPANSION")
            if bool(r.get("event_return")): names.append("D1_PRICE_IMPULSE")
            if bool(r.get("event_geo")): names.append("OFFICIAL_GEO_CAUSAL")
            return "|".join(names)
        event["event_reason"] = event.apply(_reason, axis=1)
        event["universe_source"] = "EVENT_EXPANSION_CAUSAL"
        event = event.sort_values(["event_geo", "event_score", "prev_amount", "avg_amount20"], ascending=[False, False, False, False], kind="stable")
        event = event.head(max(0, int(event_max)))

    final = pd.concat([core, event], ignore_index=True) if not event.empty else core.copy()
    final = final.drop_duplicates("code", keep="first").copy()
    final["core_n"] = core_n
    final["is_core"] = final["universe_source"].eq("CORE_LIQUID")
    final["is_event_expansion"] = final["universe_source"].eq("EVENT_EXPANSION_CAUSAL")
    final["universe_rank"] = final["liquidity_rank"]
    final["universe_bucket"] = pd.cut(
        pd.to_numeric(final["liquidity_rank"], errors="coerce"),
        bins=[0, 150, 300, 500, 1000, np.inf],
        labels=["RANK_001_150", "RANK_151_300", "RANK_301_500", "RANK_501_1000", "RANK_1001_PLUS"],
        include_lowest=True,
    ).astype(str).replace("nan", "UNRANKED")
    final["asof_contract"] = "D_MINUS_1_ONLY"
    final["research_only"] = True
    final["live_logic_changed"] = False
    final["real_order_changed"] = False

    audit_cols = [
        "signal_date", "liquidity_asof_date", "code", "name", "market", "universe_source", "event_reason",
        "liquidity_rank", "universe_rank", "universe_bucket", "avg_amount20", "avg_volume20", "obs_days",
        "prev_close", "prev_amount", "prev_volume", "prev_ret_pct", "amount_ratio_prev_vs20", "volume_ratio_prev_vs20",
        "marcap", "price_pass", "marcap_pass", "history_pass", "is_core", "is_event_expansion", "asof_contract",
        "research_only", "live_logic_changed", "real_order_changed",
    ]
    final = final[[c for c in audit_cols if c in final.columns]].reset_index(drop=True)

    stats = pd.DataFrame([{
        "signal_date": asof,
        "liquidity_asof_date": last_date,
        "listing_rows": len(listing),
        "history_days": len(clean_hist),
        "eligible_rows": len(eligible),
        "core_rows": len(core),
        "event_expansion_rows": len(event),
        "final_universe_rows": len(final),
        "rank150_rows": int(final["universe_rank"].le(150).sum()),
        "rank300_rows": int(final["universe_rank"].le(300).sum()),
        "rank500_rows": int(final["universe_rank"].le(500).sum()),
        "official_geo_codes_available": len(geo),
        "contract": "D_MINUS_1_ONLY",
    }])
    return final, stats


@dataclass
class HistoricalUniverseRuntime:
    stock_module: Any = None
    listing_loader: Callable[[], pd.DataFrame] | None = None
    fdr_reader: Callable[..., pd.DataFrame] | None = None

    def build(self, asof_date: Any, output_dir: str | Path = "reports", fallback_df: pd.DataFrame | None = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        out = _out_dir(output_dir)
        _set_cache_root(out)
        cache_before = dict(_CACHE_STATS)
        asof = pd.Timestamp(asof_date).normalize()
        core_n = _env_int("V1081_DIRECT_TOP_N", 500)
        event_max = _env_int("V1081_EVENT_EXPANSION_MAX", 100)
        liq_days = _env_int("V1081_ASOF_LIQUIDITY_DAYS", 20)
        min_price = _env_float("V1081_ASOF_MIN_PRICE", 3000)
        min_marcap = _env_float("V1081_ASOF_MIN_MARCAP", 30_000_000_000)
        amount_ratio = _env_float("V1081_EVENT_AMOUNT_RATIO", 3.0)
        volume_ratio = _env_float("V1081_EVENT_VOLUME_RATIO", 3.0)
        ret_pct = _env_float("V1081_EVENT_PREV_RET_PCT", 5.0)
        event_min_amount = _env_float("V1081_EVENT_MIN_AMOUNT", 10_000_000_000)

        listing, listing_source, listing_errors = pd.DataFrame(), "", []
        ymd = asof.strftime("%Y%m%d")
        py_listing, errs = _ticker_names_pykrx(self.stock_module, ymd)
        if not py_listing.empty:
            listing, listing_source = py_listing, "PYKRX_HISTORICAL_TICKER_LIST"
        else:
            listing_errors.extend(errs)
            if callable(self.listing_loader):
                try:
                    listing = self.listing_loader()
                    listing_source = "CURRENT_LISTING_FALLBACK"
                except Exception as exc:
                    listing_errors.append(f"listing_loader:{type(exc).__name__}:{exc}")
            if (listing is None or listing.empty) and isinstance(fallback_df, pd.DataFrame):
                listing = fallback_df.copy()
                listing_source = "PASSED_UNIVERSE_FALLBACK"
        listing = _filter_security_names(listing)

        dates = _calendar_before(asof, liq_days, self.fdr_reader)
        snapshots: dict[pd.Timestamp, pd.DataFrame] = {}
        snapshot_sources: list[str] = []
        for dt in dates:
            z, src = _get_market_snapshot(self.stock_module, pd.Timestamp(dt).strftime("%Y%m%d"))
            if not z.empty:
                snapshots[pd.Timestamp(dt).normalize()] = z
                snapshot_sources.append(src)
        cap = pd.DataFrame(); cap_source = "MISSING"
        if dates:
            cap, cap_source = _get_cap_snapshot(self.stock_module, pd.Timestamp(dates[-1]).strftime("%Y%m%d"))

        geo_codes = _official_geo_codes(out, _asof_1503(asof))
        final, stats = build_asof_universe_from_snapshots(
            asof, listing, snapshots, cap_snapshot=cap,
            core_n=core_n, event_max=event_max, min_price=min_price, min_marcap=min_marcap,
            event_amount_ratio=amount_ratio, event_volume_ratio=volume_ratio,
            event_prev_ret_pct=ret_pct, event_min_amount=event_min_amount,
            official_geo_codes=geo_codes,
        )

        mode = "HISTORICAL_ASOF_TOP500_EVENT_EXPANSION"
        status = "VALID_CAUSAL_ASOF" if not final.empty and listing_source == "PYKRX_HISTORICAL_TICKER_LIST" and len(snapshots) >= max(10, liq_days // 2) else "FALLBACK_ASOF_APPROX"
        if final.empty and isinstance(fallback_df, pd.DataFrame) and not fallback_df.empty:
            # Fail-open only for keeping legacy research executable. The fallback is explicitly
            # marked and must never be used as historical-universe promotion evidence.
            q = _filter_security_names(fallback_df)
            q = q.rename(columns={"Code": "code", "Name": "name", "Market": "market"})
            if "market" not in q.columns: q["market"] = "UNKNOWN"
            q = q.head(core_n).copy()
            q["signal_date"] = asof
            q["liquidity_asof_date"] = pd.NaT
            q["universe_source"] = "LEGACY_CURRENT_TOPN_FALLBACK"
            q["event_reason"] = ""
            q["liquidity_rank"] = np.arange(1, len(q) + 1)
            q["universe_rank"] = q["liquidity_rank"]
            q["universe_bucket"] = pd.cut(q["universe_rank"], [0,150,300,500,1000,np.inf], labels=["RANK_001_150","RANK_151_300","RANK_301_500","RANK_501_1000","RANK_1001_PLUS"], include_lowest=True).astype(str)
            q["is_core"] = True; q["is_event_expansion"] = False
            q["asof_contract"] = "CURRENT_LISTING_FALLBACK_NOT_POLICY_EVIDENCE"
            q["research_only"] = True; q["live_logic_changed"] = False; q["real_order_changed"] = False
            final = q.reset_index(drop=True)
            stats = pd.DataFrame([{"signal_date": asof, "listing_rows": len(listing), "history_days": len(snapshots), "eligible_rows": len(q), "core_rows": len(q), "event_expansion_rows": 0, "final_universe_rows": len(q), "contract": "FALLBACK"}])
            status = "FALLBACK_CURRENT_UNIVERSE_NOT_POLICY_EVIDENCE"

        availability = pd.DataFrame([{
            "version": VERSION,
            "signal_date": asof,
            "status": status,
            "mode": mode,
            "listing_source": listing_source or "MISSING",
            "listing_rows": len(listing),
            "liquidity_snapshot_days": len(snapshots),
            "liquidity_snapshot_source": "PYKRX_DAILY_CROSS_SECTION" if snapshot_sources else "MISSING",
            "market_cap_source": cap_source,
            "official_geo_codes": len(geo_codes),
            "fallback_used": status != "VALID_CAUSAL_ASOF",
            "errors": "|".join(listing_errors[:5]),
            "v20_listing_cache_hit": int(_CACHE_STATS["listing_hit"] - cache_before.get("listing_hit",0)),
            "v20_market_cache_hit": int(_CACHE_STATS["market_hit"] - cache_before.get("market_hit",0)),
            "v20_market_cache_miss": int(_CACHE_STATS["market_miss"] - cache_before.get("market_miss",0)),
            "v20_cap_cache_hit": int(_CACHE_STATS["cap_hit"] - cache_before.get("cap_hit",0)),
            "v20_cap_cache_miss": int(_CACHE_STATS["cap_miss"] - cache_before.get("cap_miss",0)),
            "v20_name_cache_hit": int(_CACHE_STATS["name_hit"] - cache_before.get("name_hit",0)),
            "v20_name_cache_miss": int(_CACHE_STATS["name_miss"] - cache_before.get("name_miss",0)),
            "research_only": True,
            "live_logic_changed": False,
            "real_order_changed": False,
        }])
        if not final.empty:
            final["version"] = VERSION
            final["universe_mode"] = mode
            final["universe_status"] = status
            final["listing_source"] = listing_source or "MISSING"
        if not stats.empty:
            stats["version"] = VERSION
            stats["status"] = status
            stats["mode"] = mode
            stats["listing_source"] = listing_source or "MISSING"
        return final, stats, availability


def append_runtime_rows(output_dir: str | Path, membership: pd.DataFrame, summary: pd.DataFrame, availability: pd.DataFrame) -> None:
    out = _out_dir(output_dir)
    for file_name, z, keys in [
        (MEMBERSHIP_FILE, membership, ["signal_date", "code"]),
        (SUMMARY_FILE, summary, ["signal_date"]),
        (AVAILABILITY_FILE, availability, ["signal_date"]),
    ]:
        if z is None or z.empty:
            continue
        p = out / file_name
        try:
            old = pd.read_csv(p, dtype={"code": str}) if p.exists() else pd.DataFrame()
        except Exception:
            old = pd.DataFrame()
        q = pd.concat([old, z], ignore_index=True, sort=False)
        present = [k for k in keys if k in q.columns]
        if present:
            q = q.drop_duplicates(present, keep="last")
        _write_csv(p, q)


def reset_runtime_files(output_dir: str | Path) -> None:
    out = _out_dir(output_dir)
    for name in [MEMBERSHIP_FILE, SUMMARY_FILE, COVERAGE_FILE, AVAILABILITY_FILE, REPORT_FILE]:
        try:
            (out / name).unlink(missing_ok=True)
        except Exception:
            pass


def _load_formula_events(out: Path) -> pd.DataFrame:
    p = out / "v72_search_formula_universe_exploded_eval.csv"
    if not p.exists():
        return pd.DataFrame()
    try:
        q = pd.read_csv(p, dtype={"code": str})
    except Exception:
        return pd.DataFrame()
    if q.empty:
        return q
    dc = _pick_col(q, ["signal_date", "date", "신호일"])
    cc = _pick_col(q, ["code", "Code", "종목코드"])
    if not dc or not cc:
        return pd.DataFrame()
    q["signal_date"] = pd.to_datetime(q[dc], errors="coerce").dt.normalize()
    q["code"] = q[cc].map(_norm_code)
    q["ret3"] = pd.to_numeric(q[_pick_col(q, ["next3_close_ret", "ret3", "day3_ret"])], errors="coerce") if _pick_col(q, ["next3_close_ret", "ret3", "day3_ret"]) else np.nan
    q["mfe"] = pd.to_numeric(q[_pick_col(q, ["max10_high_ret", "max_up_10d", "mfe", "MFE"])], errors="coerce") if _pick_col(q, ["max10_high_ret", "max_up_10d", "mfe", "MFE"]) else np.nan
    return q[q["signal_date"].notna() & q["code"].ne("")].copy()


def finalize_audit(output_dir: str | Path = "reports", base_report: str = "") -> tuple[str, dict[str, pd.DataFrame]]:
    out = _out_dir(output_dir)
    try:
        mem = pd.read_csv(out / MEMBERSHIP_FILE, dtype={"code": str}) if (out / MEMBERSHIP_FILE).exists() else pd.DataFrame()
    except Exception:
        mem = pd.DataFrame()
    try:
        summary = pd.read_csv(out / SUMMARY_FILE) if (out / SUMMARY_FILE).exists() else pd.DataFrame()
    except Exception:
        summary = pd.DataFrame()
    try:
        avail = pd.read_csv(out / AVAILABILITY_FILE) if (out / AVAILABILITY_FILE).exists() else pd.DataFrame()
    except Exception:
        avail = pd.DataFrame()
    if not mem.empty:
        mem["signal_date"] = pd.to_datetime(mem["signal_date"], errors="coerce").dt.normalize()
        mem["code"] = mem["code"].map(_norm_code)
        mem["universe_rank"] = pd.to_numeric(mem.get("universe_rank"), errors="coerce")

    formula = _load_formula_events(out)
    coverage_rows: list[dict[str, Any]] = []
    if not mem.empty:
        thresholds = [150, 300, 500]
        # Membership coverage can be computed for every date. Formula-hit/winner coverage is
        # computed only for events that passed the full formula engine.
        joined = pd.DataFrame()
        if not formula.empty:
            f = formula.sort_values(["signal_date", "code"], kind="stable").drop_duplicates(["signal_date", "code"], keep="first")
            joined = mem.merge(f[["signal_date", "code", "ret3", "mfe"]], on=["signal_date", "code"], how="left")
        else:
            joined = mem.copy(); joined["ret3"] = np.nan; joined["mfe"] = np.nan
        for n in thresholds:
            z = joined[pd.to_numeric(joined["universe_rank"], errors="coerce").le(n)].copy()
            coverage_rows.append({
                "scope": f"TOP{n}", "rank_max": n,
                "membership_rows": len(z), "unique_codes": z["code"].nunique(), "signal_days": z["signal_date"].nunique(),
                "formula_evaluated_events": int(z["ret3"].notna().sum()),
                "d3_positive_events": int(z["ret3"].gt(0).sum()),
                "d3_plus3_events": int(z["ret3"].ge(3).sum()),
                "big_mfe10_events": int(z["mfe"].ge(10).sum()),
            })
        ev = joined[joined.get("is_event_expansion", False).astype(str).str.lower().isin(["true", "1"])].copy() if "is_event_expansion" in joined.columns else pd.DataFrame()
        coverage_rows.append({
            "scope": "EVENT_EXPANSION", "rank_max": np.nan, "membership_rows": len(ev), "unique_codes": ev["code"].nunique() if not ev.empty else 0,
            "signal_days": ev["signal_date"].nunique() if not ev.empty else 0, "formula_evaluated_events": int(ev["ret3"].notna().sum()) if not ev.empty else 0,
            "d3_positive_events": int(ev["ret3"].gt(0).sum()) if not ev.empty else 0, "d3_plus3_events": int(ev["ret3"].ge(3).sum()) if not ev.empty else 0,
            "big_mfe10_events": int(ev["mfe"].ge(10).sum()) if not ev.empty else 0,
        })
    coverage = pd.DataFrame(coverage_rows)
    snapshot = _sha_df(mem)
    generated_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    for z in [mem, summary, avail, coverage]:
        if not z.empty:
            z["version"] = VERSION
            z["snapshot_id"] = snapshot
            z["generated_at"] = generated_at
            z["research_only"] = True
            z["live_logic_changed"] = False
            z["real_order_changed"] = False
    _write_csv(out / MEMBERSHIP_FILE, mem)
    _write_csv(out / SUMMARY_FILE, summary)
    _write_csv(out / AVAILABILITY_FILE, avail)
    _write_csv(out / COVERAGE_FILE, coverage)

    valid_days = 0 if avail.empty else int(avail["status"].astype(str).eq("VALID_CAUSAL_ASOF").sum())
    fallback_days = 0 if avail.empty else int(~avail["status"].astype(str).eq("VALID_CAUSAL_ASOF").sum())
    total_days = int(mem["signal_date"].nunique()) if not mem.empty else 0
    event_rows = int(mem["is_event_expansion"].astype(str).str.lower().isin(["true", "1"]).sum()) if (not mem.empty and "is_event_expansion" in mem.columns) else 0
    final_rows_mean = float(pd.to_numeric(summary.get("final_universe_rows"), errors="coerce").mean()) if not summary.empty and "final_universe_rows" in summary.columns else np.nan

    lines = [
        HEADER,
        f"📌 {VERSION} · HISTORICAL_ASOF_TOP500_EVENT_EXPANSION · RESEARCH_ONLY=True",
        "- 목적: 현재 거래대금 TOP150 고정집합 대신, 각 신호일의 D-1까지 확인 가능한 20일 평균 거래대금으로 TOP500을 다시 만들고 인과적 이벤트 확장 종목을 추가합니다.",
        "- 인과계약: Universe 순위에는 신호일 당일 종가/거래대금/미래수익을 사용하지 않습니다. 기본 순위는 D-1까지이며, 공식 지정학 사건도 15:03 이전 시각이 확인된 경우만 확장에 사용합니다.",
        f"🧾 신호일 {total_days}일 | historical-asof 완전일 {valid_days} | fallback일 {fallback_days} | 일평균 최종 Universe {final_rows_mean:.1f}개" if math.isfinite(final_rows_mean) else f"🧾 신호일 {total_days}일 | historical-asof 완전일 {valid_days} | fallback일 {fallback_days}",
        f"⚡ 이벤트 확장 membership {event_rows}행",
    ]
    if not coverage.empty:
        lines.append("🔍 [Universe 크기별 커버리지 · 동일 실행 내]")
        for _, r in coverage.iterrows():
            scope = str(r.get("scope", ""))
            lines.append(
                f"- {scope}: membership {int(r.get('membership_rows',0) or 0)} · 종목 {int(r.get('unique_codes',0) or 0)} · 신호일 {int(r.get('signal_days',0) or 0)} | "
                f"formula평가 {int(r.get('formula_evaluated_events',0) or 0)} · D3+ {int(r.get('d3_positive_events',0) or 0)} · D3≥+3 {int(r.get('d3_plus3_events',0) or 0)} · MFE≥+10 {int(r.get('big_mfe10_events',0) or 0)}"
            )
    lines += [
        "🛡️ [주의] historical pykrx snapshot이 실패한 날짜는 CURRENT_LISTING fallback을 명시하고 정책 승격 근거에서 제외합니다.",
        "🔒 LIVE 점수·순위·후보·진입·청산·주문 변경 0. Universe 확대는 Direct Replay 연구경로에만 적용합니다.",
        f"- Actions CSV: {MEMBERSHIP_FILE} · {SUMMARY_FILE} · {COVERAGE_FILE} · {AVAILABILITY_FILE}",
    ]
    block = "\n".join(lines)
    (out / REPORT_FILE).write_text(block, encoding="utf-8")
    text = str(base_report or "")
    if HEADER in text:
        text = text.split(HEADER)[0].rstrip()
    fixed = (text.rstrip() + "\n\n" + block).strip() if text.strip() else block
    return fixed, {"membership": mem, "summary": summary, "coverage": coverage, "availability": avail}


def force_report(text: str, output_dir: str | Path = "reports") -> str:
    p = _out_dir(output_dir) / REPORT_FILE
    if not p.exists():
        return str(text or "")
    try:
        block = p.read_text(encoding="utf-8")
    except Exception:
        return str(text or "")
    raw = str(text or "")
    if HEADER in raw:
        raw = raw.split(HEADER)[0].rstrip()
    return (raw.rstrip() + "\n\n" + block).strip() if raw.strip() else block
