from __future__ import annotations

import json
import math
import os
import warnings
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", message="Mean of empty slice", category=RuntimeWarning, module=r"numpy\.lib\._nanfunctions_impl")

VERSION = "V73.3.6.6.3"
FACTOR_NAME = "MARKET_REGIME_INDEX_SECTOR_EXCESS_LATE_WAVE_CAUSAL_AUDIT"
RESEARCH_ONLY = True
REPORT_HEADER = "🌦️ [시장국면 × 지수·섹터 초과수익 × LATE_WAVE 인과감사 · RESEARCH_ONLY]"

ROW_AUDIT = "v72_market_excess_signal_audit.csv"
BENCHMARK_DAILY = "v72_market_excess_benchmark_daily.csv"
REGIME_SUMMARY = "v72_market_excess_regime_summary.csv"
PATTERN_REGIME_SUMMARY = "v72_market_excess_pattern_regime_summary.csv"
COMBO_REGIME_SUMMARY = "v72_market_excess_combo_regime_summary.csv"
INTERACTION_SUMMARY = "v72_market_excess_pattern_score_ai_regime_summary.csv"
ABS_EXCESS_MATRIX = "v72_market_excess_absolute_relative_matrix.csv"
SECTOR_SUMMARY = "v72_market_excess_sector_summary.csv"
LATE_WAVE_SUMMARY = "v72_market_excess_late_wave_regime_summary.csv"
LOCKED_POLICY_SUMMARY = "v72_market_excess_locked_policy_regime_summary.csv"
SOURCE_AUDIT = "v72_market_excess_source_audit.csv"
DATA_AUDIT = "v72_market_excess_data_availability_audit.csv"
REPORT_BLOCK_FILE = "v72_market_excess_report_block.txt"

CROSS_ROW_AUDIT = "v72_pattern_ai_cross_signal_audit.csv"
CROSS_POLICY_FILE = "v72_pattern_ai_cross_policy_lock.json"

REGIME_ORDER = ["PANIC", "RECOVERY", "BEAR", "NEUTRAL", "BULL", "UNKNOWN"]
MARKET_SYMBOLS = {"KOSPI": "KS11", "KOSDAQ": "KQ11"}


def _outdir(output_dir: str | Path = "reports") -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _num(v, default=np.nan) -> float:
    try:
        x = float(str(v).replace(",", "").replace("%", "").strip())
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _int(v, default=0) -> int:
    x = _num(v)
    return int(x) if math.isfinite(x) else int(default)


def _fmt(v, digits=2, sign=True) -> str:
    x = _num(v)
    if not math.isfinite(x):
        return "N/A"
    return format(x, ("+" if sign else "") + f".{digits}f")


def _code(v: Any) -> str:
    """Canonical KRX ticker identity; preserves 6-char alphanumeric tickers (e.g. 0126Z0)."""
    raw = str(v or "").strip().upper()
    if raw.endswith(".0") and raw[:-2].isdigit():
        raw = raw[:-2]
    for suffix in (".KS", ".KQ", ".KRX"):
        if raw.endswith(suffix):
            raw = raw[:-len(suffix)]
            break
    s = "".join(ch for ch in raw if ch in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    if len(s) == 7 and s.startswith("A"):
        s = s[1:]
    if s.isdigit() and len(s) <= 6:
        return s.zfill(6)
    if len(s) >= 6:
        return s[-6:]
    return s

def _pick_col(df: pd.DataFrame, names: Sequence[str]) -> Optional[str]:
    return next((c for c in names if c in df.columns), None)


def _series_str(df: pd.DataFrame, names: Sequence[str], default="") -> pd.Series:
    c = _pick_col(df, names)
    if c:
        return df[c].fillna(default).astype(str)
    return pd.Series(default, index=df.index, dtype=str)


def _series_num(df: pd.DataFrame, names: Sequence[str], default=np.nan) -> pd.Series:
    c = _pick_col(df, names)
    if c:
        return pd.to_numeric(df[c], errors="coerce")
    return pd.Series(default, index=df.index, dtype=float)


def _norm_market(v) -> str:
    s = str(v or "").strip().upper().replace(" ", "")
    aliases = {
        "KS": "KOSPI", "KOSPI": "KOSPI", "코스피": "KOSPI", "유가": "KOSPI",
        "KQ": "KOSDAQ", "KOSDAQ": "KOSDAQ", "코스닥": "KOSDAQ",
        "KS11": "KOSPI", "^KS11": "KOSPI", "1001": "KOSPI",
        "KQ11": "KOSDAQ", "^KQ11": "KOSDAQ", "2001": "KOSDAQ",
    }
    return aliases.get(s, "UNKNOWN")


def _trim_mean(s: pd.Series, p=0.1) -> float:
    a = pd.to_numeric(s, errors="coerce").dropna().sort_values()
    if not len(a):
        return np.nan
    k = int(len(a) * p)
    if k > 0 and len(a) > 2 * k:
        a = a.iloc[k:-k]
    return float(a.mean()) if len(a) else np.nan


def _top2_excl(s: pd.Series) -> float:
    a = pd.to_numeric(s, errors="coerce").dropna().sort_values(ascending=False)
    return float(a.iloc[2:].mean()) if len(a) > 2 else np.nan


def _read_csv(fp: Path, **kwargs) -> pd.DataFrame:
    try:
        if fp.exists() and fp.stat().st_size > 0:
            return pd.read_csv(fp, low_memory=False, **kwargs)
    except Exception:
        pass
    return pd.DataFrame()


def _candidate_listing_files(out: Path) -> List[Path]:
    fps: List[Path] = []
    env = str(os.environ.get("V733663_LISTING_CSV", "")).strip()
    if env:
        fps.append(Path(env))
    names = [
        "v73_listing_cache.csv", "v73_latest_discovery_snapshot.csv",
        "v1080_stockhunter_signals.csv", "v72_ham_listing_snapshot.csv",
    ]
    for n in names:
        fps.extend([out / n, Path("reports") / n])
    return fps


def _load_listing_map(out: Path) -> Tuple[Dict[str, str], List[dict]]:
    mapping: Dict[str, str] = {}
    audit: List[dict] = []
    seen = set()
    for fp in _candidate_listing_files(out):
        try:
            k = str(fp.resolve())
        except Exception:
            k = str(fp)
        if k in seen:
            continue
        seen.add(k)
        d = _read_csv(fp, dtype=str)
        if d.empty:
            continue
        cc = _pick_col(d, ["code", "Code", "종목코드", "ticker", "Symbol"])
        mc = _pick_col(d, ["Market", "market", "시장", "exchange", "Exchange"])
        if not cc or not mc:
            audit.append({"source_type": "LISTING", "source": str(fp), "status": "SCHEMA_MISSING", "rows": len(d)})
            continue
        z = pd.DataFrame({"code": d[cc].map(_code), "market": d[mc].map(_norm_market)})
        z = z[z["code"].ne("") & z["market"].isin(["KOSPI", "KOSDAQ"])]
        before = len(mapping)
        for _, r in z.drop_duplicates("code", keep="last").iterrows():
            mapping[str(r["code"])] = str(r["market"])
        audit.append({"source_type": "LISTING", "source": str(fp), "status": "OK", "rows": len(d), "mapped": len(mapping) - before})
    return mapping, audit


def _prepare_base_rows(eval_df: pd.DataFrame, out: Path) -> pd.DataFrame:
    cross = _read_csv(out / CROSS_ROW_AUDIT, dtype={"code": str})
    if cross.empty:
        x = eval_df.copy() if eval_df is not None else pd.DataFrame()
    else:
        x = cross.copy()
    if x.empty:
        return x

    x["signal_date"] = pd.to_datetime(_series_str(x, ["signal_date", "신호일", "date"]), errors="coerce").dt.normalize()
    x["code"] = _series_str(x, ["code", "Code", "종목코드"]).map(_code)
    x = x[x["signal_date"].notna() & x["code"].ne("")].copy()
    x = x.sort_values(["signal_date", "code"]).drop_duplicates(["signal_date", "code"], keep="last")

    # Enrich with raw replay fields not guaranteed to be present in the cross row audit.
    if eval_df is not None and not eval_df.empty:
        e = eval_df.copy()
        e["signal_date"] = pd.to_datetime(_series_str(e, ["signal_date", "신호일", "date"]), errors="coerce").dt.normalize()
        e["code"] = _series_str(e, ["code", "Code", "종목코드"]).map(_code)
        keep = [c for c in [
            "signal_date", "code", "Market", "market", "exchange", "Sector", "sector", "섹터", "업종",
            "v73363_sector_label", "v73363_sector_median_chg", "v73363_sector_rel_vs_pool",
            "theme", "Theme", "테마", "market_regime", "v73363_market_context",
            "distance_from_60d_low_pct", "low60_distance_pct", "upper_resistance_distance_pct", "upper_space_pct",
            "failure_reasons", "reason_tags", "attribution_tags",
        ] if c in e.columns]
        if {"signal_date", "code"}.issubset(keep):
            e = e[keep].drop_duplicates(["signal_date", "code"], keep="last")
            x = x.merge(e, on=["signal_date", "code"], how="left", suffixes=("", "_raw"))

    # Canonical returns and existing cross-audit dimensions.
    x["ret1"] = _series_num(x, ["ret1", "next1_close_ret", "day1_ret"])
    x["ret3"] = _series_num(x, ["ret3", "next3_close_ret", "day3_ret"])
    x["ret5"] = _series_num(x, ["ret5", "next5_close_ret", "day5_ret"])
    x["mfe"] = _series_num(x, ["mfe", "max_up_5d", "MFE_5D"])
    x["mae"] = _series_num(x, ["mae", "max_down_5d", "MAE_5D"])
    x["plus3"] = _series_num(x, ["plus3", "hit_plus3_first", "plus3_first_10d"], 0).fillna(0).clip(0, 1)
    x["stop_first"] = _series_num(x, ["stop_first", "hit_stop_first", "stop_first_10d", "minus3_first"], 0).fillna(0).clip(0, 1)
    x["pattern_combo"] = _series_str(x, ["pattern_combo"], "UNCLASSIFIED").replace({"": "UNCLASSIFIED", "nan": "UNCLASSIFIED"})
    if "pattern_tokens" not in x.columns:
        x["pattern_tokens"] = x["pattern_combo"].map(lambda s: [] if s == "UNCLASSIFIED" else [p.strip() for p in str(s).split("+") if p.strip()])
    else:
        def parse_tokens(v):
            if isinstance(v, list):
                return v
            s = str(v or "").strip()
            if s.startswith("["):
                try:
                    z = json.loads(s.replace("'", '"'))
                    return list(z) if isinstance(z, list) else []
                except Exception:
                    pass
            return [] if not s or s == "UNCLASSIFIED" else [p.strip() for p in s.split("+") if p.strip()]
        x["pattern_tokens"] = x["pattern_tokens"].map(parse_tokens)
    x["score_bucket"] = _series_str(x, ["score_bucket"], "MISSING").replace({"": "MISSING", "nan": "MISSING"})
    x["score_axis"] = _series_num(x, ["score_axis", "n_score", "safe_score"])
    x["ai_pick_label"] = _series_str(x, ["ai_pick_label"], "AI_UNAVAILABLE_HISTORICAL").replace({"": "AI_UNAVAILABLE_HISTORICAL", "nan": "AI_UNAVAILABLE_HISTORICAL"})
    x["pattern_overlap_count"] = _series_num(x, ["pattern_overlap_count"], np.nan).astype(float)
    _missing_overlap = x["pattern_overlap_count"].isna()
    if bool(_missing_overlap.any()):
        x.loc[_missing_overlap, "pattern_overlap_count"] = x.loc[_missing_overlap, "pattern_tokens"].map(len).astype(float)
    x["theme"] = _series_str(x, ["theme", "Theme", "테마"], "UNKNOWN").replace({"": "UNKNOWN", "nan": "UNKNOWN"})
    x["sector_label"] = _series_str(x, ["v73363_sector_label", "Sector", "sector", "섹터", "업종"], "UNKNOWN").replace({"": "UNKNOWN", "nan": "UNKNOWN", "None": "UNKNOWN"})
    x["market_raw"] = _series_str(x, ["Market", "market", "exchange", "시장"], "UNKNOWN")
    x["market"] = x["market_raw"].map(_norm_market)
    x["distance_low60"] = _series_num(x, ["distance_low60", "distance_from_60d_low_pct", "low60_distance_pct", "60일저점이격"])
    x["upper_space"] = _series_num(x, ["upper_space", "upper_resistance_distance_pct", "upper_space_pct", "상단저항거리"])
    reason = _series_str(x, ["failure_reasons", "reason_tags", "attribution_tags", "v73363_reason_tags"], "")
    x["late_wave"] = np.maximum(_series_num(x, ["late_wave"], 0).fillna(0).astype(int), reason.str.contains("LATE_WAVE", case=False, na=False).astype(int))
    return x


def _benchmark_csv_candidates(out: Path) -> List[Path]:
    fps: List[Path] = []
    env = str(os.environ.get("V733663_BENCHMARK_CSV", "")).strip()
    if env:
        fps.append(Path(env))
    for n in ["v72_market_benchmark_daily.csv", "v73_market_index_history.csv", "market_benchmark_daily.csv"]:
        fps.extend([out / n, Path("reports") / n])
    return fps


def _parse_benchmark_csv(fp: Path) -> pd.DataFrame:
    d = _read_csv(fp)
    if d.empty:
        return d
    dc = _pick_col(d, ["date", "Date", "trade_date", "일자", "날짜"])
    if not dc:
        return pd.DataFrame()
    date = pd.to_datetime(d[dc], errors="coerce").dt.normalize()
    mc = _pick_col(d, ["market", "Market", "index", "Index", "symbol", "Symbol", "지수"])
    cc = _pick_col(d, ["close", "Close", "종가", "price", "Price"])
    rows = []
    if mc and cc:
        z = pd.DataFrame({"date": date, "market": d[mc].map(_norm_market), "close": pd.to_numeric(d[cc], errors="coerce")})
        z["source"] = f"LOCAL:{fp.name}"
        rows.append(z)
    else:
        wide = {
            "KOSPI": ["KOSPI", "KS11", "코스피", "KOSPI_Close", "kospi_close"],
            "KOSDAQ": ["KOSDAQ", "KQ11", "코스닥", "KOSDAQ_Close", "kosdaq_close"],
        }
        for market, names in wide.items():
            c = _pick_col(d, names)
            if c:
                z = pd.DataFrame({"date": date, "market": market, "close": pd.to_numeric(d[c], errors="coerce")})
                z["source"] = f"LOCAL:{fp.name}"
                rows.append(z)
    if not rows:
        return pd.DataFrame()
    z = pd.concat(rows, ignore_index=True, sort=False)
    return z[z["date"].notna() & z["market"].isin(MARKET_SYMBOLS) & z["close"].notna()]


def _load_benchmarks(x: pd.DataFrame, out: Path) -> Tuple[pd.DataFrame, List[dict]]:
    audit: List[dict] = []
    rows: List[pd.DataFrame] = []
    seen = set()
    for fp in _benchmark_csv_candidates(out):
        try:
            k = str(fp.resolve())
        except Exception:
            k = str(fp)
        if k in seen:
            continue
        seen.add(k)
        z = _parse_benchmark_csv(fp)
        if not z.empty:
            rows.append(z)
            audit.append({"source_type": "BENCHMARK", "source": str(fp), "status": "OK", "rows": len(z)})
        elif fp.exists():
            audit.append({"source_type": "BENCHMARK", "source": str(fp), "status": "SCHEMA_OR_DATA_EMPTY", "rows": 0})

    have = set(pd.concat(rows, ignore_index=True)["market"].unique()) if rows else set()
    missing = [m for m in MARKET_SYMBOLS if m not in have]
    if missing and not x.empty:
        start = (x["signal_date"].min() - pd.Timedelta(days=120)).strftime("%Y-%m-%d")
        end = (x["signal_date"].max() + pd.Timedelta(days=30)).strftime("%Y-%m-%d")
        try:
            import FinanceDataReader as fdr  # type: ignore
            for market in missing:
                try:
                    d = fdr.DataReader(MARKET_SYMBOLS[market], start, end)
                    if d is None or d.empty or "Close" not in d.columns:
                        audit.append({"source_type": "BENCHMARK", "source": f"FDR:{MARKET_SYMBOLS[market]}", "status": "EMPTY", "rows": 0})
                        continue
                    z = pd.DataFrame({
                        "date": pd.to_datetime(d.index, errors="coerce").normalize(),
                        "market": market,
                        "close": pd.to_numeric(d["Close"], errors="coerce").to_numpy(),
                        "source": f"FDR:{MARKET_SYMBOLS[market]}",
                    })
                    z = z[z["date"].notna() & z["close"].notna()]
                    rows.append(z)
                    audit.append({"source_type": "BENCHMARK", "source": f"FDR:{MARKET_SYMBOLS[market]}", "status": "OK", "rows": len(z)})
                except Exception as e:
                    audit.append({"source_type": "BENCHMARK", "source": f"FDR:{MARKET_SYMBOLS[market]}", "status": f"ERROR:{type(e).__name__}", "rows": 0})
        except Exception as e:
            for market in missing:
                audit.append({"source_type": "BENCHMARK", "source": f"FDR:{MARKET_SYMBOLS[market]}", "status": f"IMPORT_ERROR:{type(e).__name__}", "rows": 0})

    if not rows:
        return pd.DataFrame(columns=["date", "market", "close", "source"]), audit
    z = pd.concat(rows, ignore_index=True, sort=False)
    z = z[z["date"].notna() & z["market"].isin(MARKET_SYMBOLS) & z["close"].notna()].copy()
    # Local rows were appended first and therefore win over network fallback on duplicate dates.
    z = z.drop_duplicates(["market", "date"], keep="first").sort_values(["market", "date"])
    return z, audit


def _classify_regime(r: dict) -> str:
    r1 = _num(r.get("market_past_ret1"))
    r5 = _num(r.get("market_past_ret5"))
    prior5 = _num(r.get("market_prior5_ret"))
    close = _num(r.get("benchmark_close"))
    ma5 = _num(r.get("market_ma5"))
    ma20 = _num(r.get("market_ma20"))
    prev_close = _num(r.get("market_prev_close"))
    prev_ma5 = _num(r.get("market_prev_ma5"))
    slope20 = _num(r.get("market_ma20_slope5"))
    if not math.isfinite(close):
        return "UNKNOWN"
    # All conditions use information available at the signal close only.
    if (math.isfinite(r1) and r1 <= -2.0) or (math.isfinite(r5) and r5 <= -5.0):
        return "PANIC"
    recovery_jump = math.isfinite(r1) and math.isfinite(prior5) and r1 >= 1.0 and prior5 <= -3.0
    recovery_cross = all(math.isfinite(v) for v in [prev_close, prev_ma5, close, ma5, prior5]) and prev_close <= prev_ma5 and close > ma5 and prior5 <= -2.0
    if recovery_jump or recovery_cross:
        return "RECOVERY"
    if all(math.isfinite(v) for v in [close, ma20, slope20, r5]) and close >= ma20 and slope20 > 0 and r5 >= 0:
        return "BULL"
    if all(math.isfinite(v) for v in [close, ma20, slope20, r5]) and close < ma20 and slope20 <= 0 and r5 <= 0:
        return "BEAR"
    return "NEUTRAL"


def _benchmark_feature_table(bench: pd.DataFrame) -> pd.DataFrame:
    rows: List[dict] = []
    if bench.empty:
        return pd.DataFrame()
    for market, g in bench.groupby("market", sort=False):
        q = g.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
        close = pd.to_numeric(q["close"], errors="coerce")
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        for i, r in q.iterrows():
            def fwd(n):
                return (close.iloc[i + n] / close.iloc[i] - 1) * 100 if i + n < len(q) and close.iloc[i] > 0 else np.nan
            def past(n, end=i):
                return (close.iloc[end] / close.iloc[end - n] - 1) * 100 if end - n >= 0 and close.iloc[end - n] > 0 else np.nan
            rec = {
                "signal_date": r["date"], "market": market, "benchmark_close": close.iloc[i], "benchmark_source": r.get("source", ""),
                "market_fwd_ret1": fwd(1), "market_fwd_ret3": fwd(3), "market_fwd_ret5": fwd(5),
                "market_past_ret1": past(1), "market_past_ret5": past(5),
                "market_prior5_ret": past(5, i - 1) if i >= 1 else np.nan,
                "market_ma5": ma5.iloc[i], "market_ma20": ma20.iloc[i],
                "market_prev_close": close.iloc[i - 1] if i >= 1 else np.nan,
                "market_prev_ma5": ma5.iloc[i - 1] if i >= 1 else np.nan,
                "market_ma20_slope5": ((ma20.iloc[i] / ma20.iloc[i - 5] - 1) * 100) if i >= 5 and pd.notna(ma20.iloc[i]) and pd.notna(ma20.iloc[i - 5]) and ma20.iloc[i - 5] != 0 else np.nan,
                "market_drawdown20": ((close.iloc[i] / close.iloc[max(0, i - 19):i + 1].max() - 1) * 100) if close.iloc[max(0, i - 19):i + 1].notna().any() else np.nan,
            }
            rec["market_regime_causal"] = _classify_regime(rec)
            rows.append(rec)
    return pd.DataFrame(rows)


def _sector_csv_candidates(out: Path) -> List[Path]:
    fps: List[Path] = []
    env = str(os.environ.get("V733663_SECTOR_INDEX_CSV", "")).strip()
    if env:
        fps.append(Path(env))
    for n in ["v73_sector_index_daily.csv", "v72_sector_benchmark_daily.csv", "sector_index_daily.csv"]:
        fps.extend([out / n, Path("reports") / n])
    return fps


def _load_sector_index(out: Path) -> Tuple[pd.DataFrame, List[dict]]:
    rows: List[pd.DataFrame] = []
    audit: List[dict] = []
    seen = set()
    for fp in _sector_csv_candidates(out):
        try:
            k = str(fp.resolve())
        except Exception:
            k = str(fp)
        if k in seen:
            continue
        seen.add(k)
        d = _read_csv(fp)
        if d.empty:
            continue
        dc = _pick_col(d, ["date", "Date", "trade_date", "날짜", "일자"])
        sc = _pick_col(d, ["sector", "Sector", "섹터", "업종", "sector_name"])
        cc = _pick_col(d, ["close", "Close", "종가", "price", "Price"])
        if not dc or not sc or not cc:
            audit.append({"source_type": "SECTOR_INDEX", "source": str(fp), "status": "SCHEMA_MISSING", "rows": len(d)})
            continue
        z = pd.DataFrame({
            "date": pd.to_datetime(d[dc], errors="coerce").dt.normalize(),
            "sector_label": d[sc].fillna("").astype(str).str.strip(),
            "close": pd.to_numeric(d[cc], errors="coerce"),
            "source": f"LOCAL:{fp.name}",
        })
        z = z[z["date"].notna() & z["sector_label"].ne("") & z["close"].notna()]
        if not z.empty:
            rows.append(z)
            audit.append({"source_type": "SECTOR_INDEX", "source": str(fp), "status": "OK", "rows": len(z)})
    if not rows:
        return pd.DataFrame(), audit
    return pd.concat(rows, ignore_index=True).drop_duplicates(["sector_label", "date"], keep="first"), audit


def _sector_feature_table(sector_daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if sector_daily.empty:
        return pd.DataFrame()
    for sector, g in sector_daily.groupby("sector_label", sort=False):
        q = g.sort_values("date").reset_index(drop=True)
        c = pd.to_numeric(q["close"], errors="coerce")
        for i, r in q.iterrows():
            rec = {"signal_date": r["date"], "sector_label": sector, "sector_benchmark_source": r.get("source", "")}
            for n in [1, 3, 5]:
                rec[f"sector_fwd_ret{n}"] = (c.iloc[i + n] / c.iloc[i] - 1) * 100 if i + n < len(q) and c.iloc[i] > 0 else np.nan
            rows.append(rec)
    return pd.DataFrame(rows)


def _add_peer_sector_fallback(x: pd.DataFrame) -> pd.DataFrame:
    x = x.copy()
    for n in [1, 3, 5]:
        x[f"sector_peer_ret{n}"] = np.nan
    x["sector_peer_n"] = 0
    valid = x["sector_label"].notna() & ~x["sector_label"].isin(["", "UNKNOWN", "nan"])
    for (_, _), idx in x[valid].groupby(["signal_date", "sector_label"]).groups.items():
        ids = list(idx)
        if len(ids) < 3:
            continue
        x.loc[ids, "sector_peer_n"] = len(ids)
        for n in [1, 3, 5]:
            vals = pd.to_numeric(x.loc[ids, f"ret{n}"], errors="coerce")
            for j in ids:
                others = vals.drop(index=j, errors="ignore").dropna()
                if len(others) >= 2:
                    x.at[j, f"sector_peer_ret{n}"] = float(others.median())
    return x


def _enrich(x: pd.DataFrame, out: Path) -> Tuple[pd.DataFrame, pd.DataFrame, List[dict]]:
    listing, source_audit = _load_listing_map(out)
    missing_market = x["market"].eq("UNKNOWN")
    x.loc[missing_market, "market"] = x.loc[missing_market, "code"].map(listing).fillna("UNKNOWN")
    x["market_mapping_source"] = np.where(x["market_raw"].map(_norm_market).isin(["KOSPI", "KOSDAQ"]), "ROW_EXPLICIT", np.where(x["code"].isin(listing), "LISTING_LEDGER", "MISSING"))

    bench, aud = _load_benchmarks(x, out)
    source_audit.extend(aud)
    bf = _benchmark_feature_table(bench)
    if not bf.empty:
        x = x.merge(bf, on=["signal_date", "market"], how="left")
    else:
        for c in ["benchmark_close", "benchmark_source", "market_fwd_ret1", "market_fwd_ret3", "market_fwd_ret5", "market_past_ret1", "market_past_ret5", "market_prior5_ret", "market_ma5", "market_ma20", "market_prev_close", "market_prev_ma5", "market_ma20_slope5", "market_drawdown20", "market_regime_causal"]:
            x[c] = "" if c in ["benchmark_source", "market_regime_causal"] else np.nan
    x["market_regime_causal"] = x["market_regime_causal"].fillna("UNKNOWN").replace({"": "UNKNOWN"})

    for n in [1, 3, 5]:
        x[f"market_excess{n}"] = pd.to_numeric(x[f"ret{n}"], errors="coerce") - pd.to_numeric(x[f"market_fwd_ret{n}"], errors="coerce")
        x[f"ret{n}_cost20"] = pd.to_numeric(x[f"ret{n}"], errors="coerce") - 0.20
        x[f"ret{n}_cost50"] = pd.to_numeric(x[f"ret{n}"], errors="coerce") - 0.50
        x[f"market_excess{n}_cost20"] = x[f"ret{n}_cost20"] - pd.to_numeric(x[f"market_fwd_ret{n}"], errors="coerce")
        x[f"market_excess{n}_cost50"] = x[f"ret{n}_cost50"] - pd.to_numeric(x[f"market_fwd_ret{n}"], errors="coerce")

    sector_daily, saud = _load_sector_index(out)
    source_audit.extend(saud)
    sf = _sector_feature_table(sector_daily)
    if not sf.empty:
        x = x.merge(sf, on=["signal_date", "sector_label"], how="left")
    else:
        for n in [1, 3, 5]:
            x[f"sector_fwd_ret{n}"] = np.nan
        x["sector_benchmark_source"] = ""
    x = _add_peer_sector_fallback(x)
    for n in [1, 3, 5]:
        true_sector = pd.to_numeric(x[f"sector_fwd_ret{n}"], errors="coerce")
        peer = pd.to_numeric(x[f"sector_peer_ret{n}"], errors="coerce")
        x[f"sector_reference_ret{n}"] = true_sector.where(true_sector.notna(), peer)
        x[f"sector_excess{n}"] = pd.to_numeric(x[f"ret{n}"], errors="coerce") - x[f"sector_reference_ret{n}"]
    x["sector_reference_source"] = np.where(pd.to_numeric(x["sector_fwd_ret3"], errors="coerce").notna(), "TRUE_SECTOR_INDEX", np.where(pd.to_numeric(x["sector_peer_ret3"], errors="coerce").notna(), "SIGNAL_COHORT_PEER_MEDIAN_DIAGNOSTIC", "MISSING"))

    abs3 = pd.to_numeric(x["ret3"], errors="coerce")
    exc3 = pd.to_numeric(x["market_excess3"], errors="coerce")
    x["absolute_relative_matrix"] = "MISSING"
    x.loc[abs3.ge(0) & exc3.ge(0), "absolute_relative_matrix"] = "ABS_POS_EXCESS_POS"
    x.loc[abs3.ge(0) & exc3.lt(0), "absolute_relative_matrix"] = "ABS_POS_EXCESS_NEG"
    x.loc[abs3.lt(0) & exc3.ge(0), "absolute_relative_matrix"] = "ABS_NEG_EXCESS_POS"
    x.loc[abs3.lt(0) & exc3.lt(0), "absolute_relative_matrix"] = "ABS_NEG_EXCESS_NEG"
    x["late_wave_label"] = np.where(x["late_wave"].eq(1), "LATE_WAVE", "NOT_LATE_WAVE")
    x["early_space_state"] = "UNKNOWN"
    x.loc[x["distance_low60"].le(15) & x["upper_space"].ge(20), "early_space_state"] = "EARLY_AND_SPACE_OK"
    x.loc[x["distance_low60"].gt(20) | x["upper_space"].lt(12), "early_space_state"] = "LATE_OR_RESISTANCE_CLOSE"
    x["bear_panic_survivor"] = ((x["market_regime_causal"].isin(["BEAR", "PANIC"])) & abs3.gt(0) & exc3.gt(0)).astype(int)

    bench.to_csv(out / BENCHMARK_DAILY, index=False, encoding="utf-8-sig")
    return x, bench, source_audit


def _perf(g: pd.DataFrame, label: str, dimension: str, regime: str = "ALL") -> dict:
    r3 = pd.to_numeric(g.get("ret3", pd.Series(dtype=float)), errors="coerce")
    m3 = pd.to_numeric(g.get("market_fwd_ret3", pd.Series(dtype=float)), errors="coerce")
    e3 = pd.to_numeric(g.get("market_excess3", pd.Series(dtype=float)), errors="coerce")
    s3 = pd.to_numeric(g.get("sector_excess3", pd.Series(dtype=float)), errors="coerce")
    d = g.sort_values("signal_date").drop_duplicates("code", keep="first") if len(g) else g
    return {
        "dimension": dimension, "label": label, "regime": regime, "n": len(g),
        "unique_codes": g["code"].nunique() if "code" in g else 0,
        "signal_days": g["signal_date"].nunique() if "signal_date" in g else 0,
        "benchmark_n": int(e3.notna().sum()), "benchmark_coverage_pct": e3.notna().mean() * 100 if len(g) else np.nan,
        "ret1_mean": pd.to_numeric(g.get("ret1", pd.Series(dtype=float)), errors="coerce").mean(),
        "ret3_mean": r3.mean(), "ret3_median": r3.median(), "ret3_trim10": _trim_mean(r3), "ret3_top2_excl": _top2_excl(r3),
        "ret5_mean": pd.to_numeric(g.get("ret5", pd.Series(dtype=float)), errors="coerce").mean(),
        "market_ret3_mean": m3.mean(),
        "excess3_mean": e3.mean(), "excess3_median": e3.median(), "excess3_trim10": _trim_mean(e3), "excess3_top2_excl": _top2_excl(e3),
        "excess3_cost20_mean": pd.to_numeric(g.get("market_excess3_cost20", pd.Series(dtype=float)), errors="coerce").mean(),
        "excess3_cost50_mean": pd.to_numeric(g.get("market_excess3_cost50", pd.Series(dtype=float)), errors="coerce").mean(),
        "sector_excess3_mean": s3.mean(), "sector_excess3_median": s3.median(),
        "absolute_positive_rate": r3.gt(0).mean() * 100 if r3.notna().any() else np.nan,
        "excess_positive_rate": e3.gt(0).mean() * 100 if e3.notna().any() else np.nan,
        "both_positive_rate": (r3.gt(0) & e3.gt(0)).mean() * 100 if e3.notna().any() else np.nan,
        "plus3_rate": pd.to_numeric(g.get("plus3", pd.Series(dtype=float)), errors="coerce").mean() * 100 if len(g) else np.nan,
        "stop_rate": pd.to_numeric(g.get("stop_first", pd.Series(dtype=float)), errors="coerce").mean() * 100 if len(g) else np.nan,
        "mfe_mean": pd.to_numeric(g.get("mfe", pd.Series(dtype=float)), errors="coerce").mean(),
        "mae_mean": pd.to_numeric(g.get("mae", pd.Series(dtype=float)), errors="coerce").mean(),
        "code_dedup_ret3": pd.to_numeric(d.get("ret3", pd.Series(dtype=float)), errors="coerce").mean() if len(d) else np.nan,
        "code_dedup_excess3": pd.to_numeric(d.get("market_excess3", pd.Series(dtype=float)), errors="coerce").mean() if len(d) else np.nan,
        "late_wave_rate": pd.to_numeric(g.get("late_wave", pd.Series(dtype=float)), errors="coerce").mean() * 100 if len(g) else np.nan,
        "low60_median": pd.to_numeric(g.get("distance_low60", pd.Series(dtype=float)), errors="coerce").median(),
        "upper_space_median": pd.to_numeric(g.get("upper_space", pd.Series(dtype=float)), errors="coerce").median(),
    }


def _summaries(x: pd.DataFrame, out: Path) -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}

    rows = []
    for regime in REGIME_ORDER:
        g = x[x["market_regime_causal"].eq(regime)]
        if len(g):
            rows.append(_perf(g, regime, "MARKET_REGIME", regime))
    tables[REGIME_SUMMARY] = pd.DataFrame(rows)

    exp = x.explode("pattern_tokens").copy()
    exp["pattern_tokens"] = exp["pattern_tokens"].fillna("UNCLASSIFIED").replace({"": "UNCLASSIFIED"})
    rows = []
    for (p, regime), g in exp.groupby(["pattern_tokens", "market_regime_causal"], dropna=False):
        rows.append(_perf(g, str(p), "PATTERN_X_REGIME", str(regime)))
    tables[PATTERN_REGIME_SUMMARY] = pd.DataFrame(rows)

    rows = []
    for (combo, regime), g in x.groupby(["pattern_combo", "market_regime_causal"], dropna=False):
        rows.append(_perf(g, str(combo), "EXACT_COMBO_X_REGIME", str(regime)))
    tables[COMBO_REGIME_SUMMARY] = pd.DataFrame(rows)

    rows = []
    for keys, g in x.groupby(["pattern_combo", "score_bucket", "ai_pick_label", "market_regime_causal"], dropna=False):
        combo, score, ai, regime = keys
        z = _perf(g, str(combo), "PATTERN_SCORE_AI_REGIME", str(regime))
        z.update({"pattern_combo": combo, "score_bucket": score, "ai_pick_label": ai})
        rows.append(z)
    tables[INTERACTION_SUMMARY] = pd.DataFrame(rows)

    rows = []
    for (regime, matrix), g in x.groupby(["market_regime_causal", "absolute_relative_matrix"], dropna=False):
        z = _perf(g, str(matrix), "ABSOLUTE_RELATIVE_MATRIX", str(regime))
        rows.append(z)
    tables[ABS_EXCESS_MATRIX] = pd.DataFrame(rows)

    rows = []
    for (source, sector), g in x.groupby(["sector_reference_source", "sector_label"], dropna=False):
        z = _perf(g, str(sector), "SECTOR_REFERENCE", "ALL")
        z["sector_reference_source"] = source
        rows.append(z)
    tables[SECTOR_SUMMARY] = pd.DataFrame(rows)

    rows = []
    for (regime, late), g in x.groupby(["market_regime_causal", "late_wave_label"], dropna=False):
        rows.append(_perf(g, str(late), "LATE_WAVE_X_REGIME", str(regime)))
    for state, g in x.groupby("early_space_state", dropna=False):
        rows.append(_perf(g, str(state), "EARLY_SPACE_STATE", "ALL"))
    tables[LATE_WAVE_SUMMARY] = pd.DataFrame(rows)

    for name, d in tables.items():
        d.to_csv(out / name, index=False, encoding="utf-8-sig")
    return tables


def _bucket_floor(bucket: str) -> int:
    return {"ANY": -999, "LT70": -999, "70_79": 70, "80_89": 80, "GE90": 90}.get(str(bucket), -999)


def _apply_locked_policy(x: pd.DataFrame, out: Path) -> Tuple[pd.DataFrame, str]:
    fp = out / CROSS_POLICY_FILE
    if not fp.exists():
        empty = pd.DataFrame(columns=["dimension", "label", "regime", "n"])
        empty.to_csv(out / LOCKED_POLICY_SUMMARY, index=False, encoding="utf-8-sig")
        return empty, "NO_PATTERN_POLICY_LOCK"
    try:
        p = json.loads(fp.read_text(encoding="utf-8"))
        m = x["pattern_overlap_count"].between(int(p["min_overlap"]), int(p["max_overlap"]))
        floor = _bucket_floor(p.get("min_score_bucket", "ANY"))
        if floor > -999:
            m &= x["score_axis"].ge(floor)
        mode = str(p.get("ai_mode", "ANY"))
        if mode == "AI_SELECTED":
            m &= x["ai_pick_label"].isin(["AI_STRONG", "AI_WATCH", "AI_CONSERVATIVE"])
        elif mode == "AI_NONE":
            m &= x["ai_pick_label"].eq("AI_NONE")
        elif mode == "AI_OBSERVED_ANY":
            m &= ~x["ai_pick_label"].eq("AI_UNAVAILABLE_HISTORICAL")
        rule = str(p.get("pattern_rule", "ANY"))
        if rule != "ANY":
            m &= x["pattern_combo"].eq(rule)
        selected = x[m & x["signal_date"].ge(pd.Timestamp(p["oos_start"]))].copy()
        rows = [_perf(selected, str(p.get("policy_id", "LOCKED_POLICY")), "LOCKED_POLICY", "OOS_ALL")]
        for regime, g in selected.groupby("market_regime_causal"):
            rows.append(_perf(g, str(p.get("policy_id", "LOCKED_POLICY")), "LOCKED_POLICY", str(regime)))
        d = pd.DataFrame(rows)
        d["policy_hash"] = p.get("policy_hash", "")
        d["policy_pattern_rule"] = rule
        d["policy_oos_start"] = p.get("oos_start", "")
        d.to_csv(out / LOCKED_POLICY_SUMMARY, index=False, encoding="utf-8-sig")
        return d, "LOCKED_POLICY_EVALUATED_NO_RETUNE"
    except Exception as e:
        empty = pd.DataFrame([{"dimension": "LOCKED_POLICY", "label": "ERROR", "regime": "UNKNOWN", "n": 0, "error": f"{type(e).__name__}:{e}"}])
        empty.to_csv(out / LOCKED_POLICY_SUMMARY, index=False, encoding="utf-8-sig")
        return empty, "LOCKED_POLICY_READ_ERROR"


def _data_audit(x: pd.DataFrame, bench: pd.DataFrame, source_rows: List[dict], out: Path) -> pd.DataFrame:
    total = len(x)
    rows = []
    def add(field, mask, status_note=""):
        n = int(pd.Series(mask, index=x.index).fillna(False).sum()) if total else 0
        rows.append({"field": field, "available": n, "total": total, "coverage_pct": n / total * 100 if total else np.nan, "status": "OK" if n == total and total else ("PARTIAL" if n else "MISSING"), "note": status_note})
    add("market_mapping", x["market"].isin(["KOSPI", "KOSDAQ"]), "No code-prefix market guessing")
    add("benchmark_signal_close", pd.to_numeric(x["benchmark_close"], errors="coerce").notna(), "Exact signal-date index close only")
    add("benchmark_forward_d1", pd.to_numeric(x["market_fwd_ret1"], errors="coerce").notna())
    add("benchmark_forward_d3", pd.to_numeric(x["market_fwd_ret3"], errors="coerce").notna())
    add("benchmark_forward_d5", pd.to_numeric(x["market_fwd_ret5"], errors="coerce").notna())
    add("causal_market_regime", ~x["market_regime_causal"].eq("UNKNOWN"), "Signal-close information only")
    add("true_sector_index_d3", x["sector_reference_source"].eq("TRUE_SECTOR_INDEX"), "Only external/local sector index counts as true sector benchmark")
    add("sector_peer_diagnostic_d3", x["sector_reference_source"].eq("SIGNAL_COHORT_PEER_MEDIAN_DIAGNOSTIC"), "Diagnostic only; prohibited as promotion proof")
    add("late_wave", x["late_wave"].notna())
    add("low60_distance", pd.to_numeric(x["distance_low60"], errors="coerce").notna())
    add("upper_resistance_space", pd.to_numeric(x["upper_space"], errors="coerce").notna())
    d = pd.DataFrame(rows)
    d.to_csv(out / DATA_AUDIT, index=False, encoding="utf-8-sig")
    pd.DataFrame(source_rows or [{"source_type": "NONE", "source": "", "status": "MISSING", "rows": 0}]).to_csv(out / SOURCE_AUDIT, index=False, encoding="utf-8-sig")
    return d


def _insert_report(report: str, block: str) -> str:
    s = str(report or "")
    if REPORT_HEADER in s:
        st = s.find(REPORT_HEADER)
        ends = [s.find(a, st + len(REPORT_HEADER)) for a in ["\n🌙 [전일 야간환경", "\n🤝 [절친", "\n🏆 [V48/V61", "\n🛡️ [손절거리"]]
        ends = [i for i in ends if i >= 0]
        en = min(ends) if ends else len(s)
        s = (s[:st].rstrip() + "\n\n" + s[en:].lstrip()).strip()
    anchors = ["\n🌙 [전일 야간환경", "\n🤝 [절친", "\n🏆 [V48/V61", "\n🛡️ [손절거리"]
    positions = [s.find(a) for a in anchors if s.find(a) >= 0]
    if positions:
        k = min(positions)
        return s[:k].rstrip() + "\n\n" + block + "\n" + s[k:]
    return s.rstrip() + "\n\n" + block


def _top(df: pd.DataFrame, n=6, min_n=2) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    z = df[pd.to_numeric(df.get("n", 0), errors="coerce").ge(min_n)].copy()
    if z.empty:
        return z
    _robust_parts = pd.concat([
        pd.to_numeric(z.get("excess3_median"), errors="coerce"),
        pd.to_numeric(z.get("excess3_trim10"), errors="coerce"),
        pd.to_numeric(z.get("excess3_top2_excl"), errors="coerce"),
    ], axis=1)
    _den = _robust_parts.notna().sum(axis=1).replace(0, np.nan)
    z["_robust"] = _robust_parts.fillna(0).sum(axis=1) / _den
    return z.sort_values(["_robust", "n"], ascending=[False, False]).head(n)


def build_report(x: pd.DataFrame, tables: Dict[str, pd.DataFrame], locked: pd.DataFrame, lock_status: str, data_audit: pd.DataFrame) -> str:
    market_cov = int(pd.to_numeric(x["market_excess3"], errors="coerce").notna().sum())
    true_sector = int(x["sector_reference_source"].eq("TRUE_SECTOR_INDEX").sum())
    peer_sector = int(x["sector_reference_source"].eq("SIGNAL_COHORT_PEER_MEDIAN_DIAGNOSTIC").sum())
    lines = [
        REPORT_HEADER,
        f"📌 {VERSION} · {FACTOR_NAME} · RESEARCH_ONLY=True",
        "- 목적: 검색식 실패가 하락장 때문인지, 시장보다도 약한 종목을 골랐기 때문인지 절대수익과 지수초과수익을 분리합니다.",
        "- 인과원칙: 신호일 종가까지의 지수 정보로 국면을 결정하고, D+1·D+3·D+5 지수수익률은 성과 비교에만 사용합니다. 시장 미확정 코드는 임의 KOSPI 대입 금지입니다.",
        f"📁 분석 {len(x)}행 · 종목 {x['code'].nunique()} · 신호일 {x['signal_date'].nunique()} · D3 지수결합 {market_cov} · TRUE 섹터지수 {true_sector} · 동종후보 진단대체 {peer_sector}",
    ]
    if market_cov == 0:
        lines.extend([
            "⚠️ KOSPI/KOSDAQ 지수 결합 표본이 없습니다. 이번 실행에서는 시장 탓/검색식 탓을 판정하지 않으며 LIVE 변경도 없습니다.",
            f"- 데이터 입력: V733663_BENCHMARK_CSV 또는 FinanceDataReader KS11/KQ11 접근을 확인합니다. Actions CSV: {SOURCE_AUDIT} · {DATA_AUDIT}",
            "- 최종: RESEARCH_ONLY · LIVE 승격 금지 · 기존 점수/검색식/AI/진입/익절/손절 변경 0",
        ])
        return "\n".join(lines)

    lines.append("🌡️ [시장국면별 절대수익 ↔ 지수초과수익]")
    rg = tables.get(REGIME_SUMMARY, pd.DataFrame())
    if rg.empty:
        lines.append("- 표본 없음")
    else:
        order = {v: i for i, v in enumerate(REGIME_ORDER)}
        rg = rg.assign(_ord=rg["regime"].map(order).fillna(99)).sort_values("_ord")
        for _, r in rg.iterrows():
            lines.append(
                f"- {r['regime']}: n{_int(r['n'])}·날짜{_int(r['signal_days'])} | 종목 D3 {_fmt(r['ret3_mean'])}%·중앙 {_fmt(r['ret3_median'])}% | 지수 {_fmt(r['market_ret3_mean'])}% | 초과 평균 {_fmt(r['excess3_mean'])}%·중앙 {_fmt(r['excess3_median'])}%·절사 {_fmt(r['excess3_trim10'])}%·상2제외 {_fmt(r['excess3_top2_excl'])}% | 둘다양수 {_fmt(r['both_positive_rate'],1,False)}% | 50bp후 초과 {_fmt(r['excess3_cost50_mean'])}%"
            )

    lines.append("🧬 [패턴 × 시장국면 · 지수초과 견고성 상위]")
    pr = _top(tables.get(PATTERN_REGIME_SUMMARY, pd.DataFrame()), n=8, min_n=3)
    if pr.empty:
        lines.append("- 국면별 최소표본 부족")
    else:
        for _, r in pr.iterrows():
            lines.append(
                f"- {r['label']} · {r['regime']}: n{_int(r['n'])} | D3 중앙 {_fmt(r['ret3_median'])}% ↔ 초과 중앙 {_fmt(r['excess3_median'])}%·절사 {_fmt(r['excess3_trim10'])}%·상2제외 {_fmt(r['excess3_top2_excl'])}% | 둘다양수 {_fmt(r['both_positive_rate'],1,False)}% | LATE {_fmt(r['late_wave_rate'],1,False)}%"
            )

    lines.append("🧩 [정확조합 × 시장국면 · n≥3]")
    cr = _top(tables.get(COMBO_REGIME_SUMMARY, pd.DataFrame()), n=6, min_n=3)
    if cr.empty:
        lines.append("- 최소표본 부족")
    else:
        for _, r in cr.iterrows():
            lines.append(
                f"- {r['label']} · {r['regime']}: n{_int(r['n'])} | D3 평균 {_fmt(r['ret3_mean'])}%·중앙 {_fmt(r['ret3_median'])}% | 초과 평균 {_fmt(r['excess3_mean'])}%·중앙 {_fmt(r['excess3_median'])}%·상2제외 {_fmt(r['excess3_top2_excl'])}% | 중복제거 초과 {_fmt(r['code_dedup_excess3'])}%"
            )

    lines.append("⚖️ [절대수익과 상대수익의 의미]")
    mx = tables.get(ABS_EXCESS_MATRIX, pd.DataFrame())
    if mx.empty:
        lines.append("- 표본 없음")
    else:
        counts = mx.groupby("label")["n"].sum().sort_values(ascending=False)
        labels = {
            "ABS_POS_EXCESS_POS": "절대수익+·지수초과+ (진짜 우위 후보)",
            "ABS_POS_EXCESS_NEG": "절대수익+·지수초과- (시장 베타 의존)",
            "ABS_NEG_EXCESS_POS": "절대수익-·지수초과+ (상대강도 관찰용)",
            "ABS_NEG_EXCESS_NEG": "절대수익-·지수초과- (검색식·환경 동시 열위)",
            "MISSING": "지수결합 없음",
        }
        for k, v in counts.items():
            lines.append(f"- {labels.get(k,k)}: n{_int(v)}")

    lines.append("🧯 [LATE_WAVE·저점이격·상단공간 분리]")
    lw = tables.get(LATE_WAVE_SUMMARY, pd.DataFrame())
    if lw.empty:
        lines.append("- 위치지표 표본 없음")
    else:
        q = lw[(lw["dimension"].eq("LATE_WAVE_X_REGIME")) & (pd.to_numeric(lw["n"], errors="coerce") >= 3)].copy()
        for _, r in q.sort_values(["regime", "label"]).head(10).iterrows():
            lines.append(f"- {r['regime']} · {r['label']}: n{_int(r['n'])} | D3 중앙 {_fmt(r['ret3_median'])}% | 초과 중앙 {_fmt(r['excess3_median'])}%·상2제외 {_fmt(r['excess3_top2_excl'])}% | 저점이격 {_fmt(r['low60_median'],1,False)}%·상단공간 {_fmt(r['upper_space_median'],1,False)}%")

    lines.append("🔒 [기존 TRAIN 정책 LOCK의 국면별 OOS 재평가]")
    if locked.empty:
        lines.append(f"- {lock_status} · OOS 정책을 새로 고르거나 재튜닝하지 않습니다.")
    else:
        for _, r in locked.iterrows():
            lines.append(f"- {r['regime']}: n{_int(r['n'])} | D3 {_fmt(r['ret3_mean'])}%·중앙 {_fmt(r['ret3_median'])}% | 지수초과 {_fmt(r['excess3_mean'])}%·중앙 {_fmt(r['excess3_median'])}% | 50bp후 초과 {_fmt(r['excess3_cost50_mean'])}%")

    lines.extend([
        "- TRUE 섹터지수가 없을 때 동종 후보 중앙값은 원인탐색용 진단값일 뿐, 섹터초과 성과나 LIVE 승격 근거로 인정하지 않습니다.",
        "- BULL에서 종목수익은 양수지만 지수초과가 음수면 검색식 우위가 아니라 시장 베타로 분류합니다. BEAR/PANIC에서 절대·초과가 모두 양수일 때만 하락장 생존 후보입니다.",
        "- 20bp·50bp 비용, 동일종목 중복 제거, 평균·중앙·절사·상위2개 제외를 모두 저장합니다.",
        "- 최종: RESEARCH_ONLY · 자동 LIVE 승격 금지 · 기존 LIVE 후보/점수/PRIME/LCZ/M5R/ENVUP/RESTART/V72 PRC/P1/HAM 정책 변경 0",
        f"- Actions CSV: {ROW_AUDIT} · {BENCHMARK_DAILY} · {REGIME_SUMMARY} · {PATTERN_REGIME_SUMMARY} · {COMBO_REGIME_SUMMARY} · {INTERACTION_SUMMARY} · {ABS_EXCESS_MATRIX} · {SECTOR_SUMMARY} · {LATE_WAVE_SUMMARY} · {LOCKED_POLICY_SUMMARY} · {SOURCE_AUDIT} · {DATA_AUDIT}",
    ])
    return "\n".join(lines)


def _empty_csvs(out: Path):
    schemas = {
        ROW_AUDIT: ["signal_date", "code", "market", "market_regime_causal", "ret3", "market_fwd_ret3", "market_excess3"],
        BENCHMARK_DAILY: ["date", "market", "close", "source"],
        REGIME_SUMMARY: ["dimension", "label", "regime", "n"],
        PATTERN_REGIME_SUMMARY: ["dimension", "label", "regime", "n"],
        COMBO_REGIME_SUMMARY: ["dimension", "label", "regime", "n"],
        INTERACTION_SUMMARY: ["pattern_combo", "score_bucket", "ai_pick_label", "regime", "n"],
        ABS_EXCESS_MATRIX: ["dimension", "label", "regime", "n"],
        SECTOR_SUMMARY: ["sector_reference_source", "label", "n"],
        LATE_WAVE_SUMMARY: ["dimension", "label", "regime", "n"],
        LOCKED_POLICY_SUMMARY: ["dimension", "label", "regime", "n"],
        SOURCE_AUDIT: ["source_type", "source", "status", "rows"],
        DATA_AUDIT: ["field", "available", "total", "coverage_pct", "status", "note"],
    }
    for name, cols in schemas.items():
        fp = out / name
        if not fp.exists():
            pd.DataFrame(columns=cols).to_csv(fp, index=False, encoding="utf-8-sig")


def run_backtest(eval_df: pd.DataFrame, output_dir: str | Path = "reports", base_report: str = "") -> Tuple[str, pd.DataFrame]:
    out = _outdir(output_dir)
    x = _prepare_base_rows(eval_df, out)
    if x.empty:
        _empty_csvs(out)
        block = REPORT_HEADER + f"\n📌 {VERSION} · RESEARCH_ONLY=True\n- 평가 가능한 종목·신호일 표본이 없어 시장국면/초과수익을 계산하지 않았습니다. 기존 LIVE 영향 0."
        (out / REPORT_BLOCK_FILE).write_text(block, encoding="utf-8")
        return _insert_report(base_report, block), x
    x, bench, source_rows = _enrich(x, out)
    tables = _summaries(x, out)
    locked, lock_status = _apply_locked_policy(x, out)
    da = _data_audit(x, bench, source_rows, out)
    x.to_csv(out / ROW_AUDIT, index=False, encoding="utf-8-sig")
    block = build_report(x, tables, locked, lock_status, da)
    (out / REPORT_BLOCK_FILE).write_text(block, encoding="utf-8")
    return _insert_report(base_report, block), x


def force_report(report: str, output_dir: str | Path = "reports") -> str:
    out = _outdir(output_dir)
    try:
        block = (out / REPORT_BLOCK_FILE).read_text(encoding="utf-8").strip()
    except Exception:
        block = ""
    return _insert_report(report, block) if block else str(report or "")
