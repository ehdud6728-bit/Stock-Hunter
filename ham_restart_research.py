from __future__ import annotations

"""V73.3.6.5 HAM → SURVIVE → RESTART → CLOSE research layer.

RESEARCH-ONLY CONTRACT
----------------------
This module MUST NOT participate in live candidate selection, ranking, PRIME/LCZ/M5R/
ENVUP routing, or the existing RESTART execution contract.  It only:
  1) captures/loads actual intraday minute bars where available,
  2) derives descriptive HAM/SURVIVE/RESTART/15:03 features,
  3) performs TRAIN -> policy-lock -> OOS research,
  4) runs matched-control and execution-PnL diagnostics,
  5) writes reports/CSVs and a research card.

Historical minute values are never fabricated from daily bars.  A Yahoo OHLCV fallback can
be used to preserve minute PRICE/VOLUME structure, but its minute traded value is explicitly
marked PROXY_CLOSE_X_VOLUME and is not presented as exchange-reported turnover.
"""

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
import hashlib
import itertools
import json
import math
import os
import re
import statistics
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

try:
    import FinanceDataReader as fdr
except Exception:  # pragma: no cover - optional in synthetic tests
    fdr = None

VERSION = "V73.3.6.5"
RESEARCH_ONLY = True
FACTOR_NAME = "HAM_SURVIVE_RESTART_CLOSE"
REPORT_HEADER = "🍔 [HAM→SURVIVE→RESTART→15:03 CLOSE 연구 · RESEARCH_ONLY]"
KST = ZoneInfo("Asia/Seoul")

# Persistent research paths.  All live policy consumers are intentionally absent.
MINUTE_DIR_NAME = "ham_minute_history"
FEATURE_LEDGER = "v72_ham_intraday_feature_ledger.csv"
CAPTURE_AUDIT = "v72_ham_capture_audit.csv"
BASELINE_AUDIT = "v72_ham_minute_baseline_audit.csv"
EVAL_LEDGER = "v72_ham_research_eval.csv"
STAGE_SUMMARY = "v72_ham_stage_performance_summary.csv"
SWEEP_SUMMARY = "v72_ham_threshold_sweep_train.csv"
POLICY_SPEC = "v72_ham_policy_lock.json"
POLICY_AUDIT = "v72_ham_policy_lock_audit.csv"
OOS_SUMMARY = "v72_ham_locked_policy_oos_summary.csv"
MATCHED_PAIRS = "v72_ham_matched_control_pairs.csv"
MATCHED_SUMMARY = "v72_ham_matched_control_summary.csv"
TIME_BUCKET_SUMMARY = "v72_ham_time_bucket_summary.csv"
BASELINE_METHOD_SUMMARY = "v72_ham_baseline_method_summary.csv"
DIST_SUMMARY = "v72_ham_distribution_warning_summary.csv"
INCREMENTAL_SUMMARY = "v72_ham_incremental_edge_summary.csv"

RATIO_GRID = (2.0, 3.0, 4.0, 5.0, 7.0, 10.0)
VALUE_EOK_GRID = (0.5, 1.0, 3.0, 5.0, 10.0, 20.0, 50.0)
DRAWDOWN_GRID = (-2.0, -3.0, -4.0, -5.0)
RESTART_GRID = (1.5, 2.0, 3.0, 5.0)
CLOSE_HIGH_DIST_GRID = (-1.0, -2.0, -3.0, -5.0)
SURVIVE_MODES = ("LOW", "MID", "CLOSE", "ANY")

STAGE_ORDER = [
    "HAM_ONLY",
    "HAM_SURVIVE",
    "HAM_SURVIVE_RESTART",
    "HAM_RESTART_CLOSE",
]


def _now() -> datetime:
    return datetime.now(KST)


def _on(name: str, default: str = "0") -> bool:
    return str(os.environ.get(name, default)).strip().lower() in {"1", "true", "yes", "on", "y"}


def _num(v, default=np.nan) -> float:
    try:
        if v is None:
            return float(default)
        if isinstance(v, str):
            s = v.replace(",", "").replace("%", "").replace("원", "").replace("억", "").strip()
            if not s or s.lower() in {"nan", "none", "nat"}:
                return float(default)
            v = s
        x = float(v)
        return x if math.isfinite(x) else float(default)
    except Exception:
        return float(default)


def _finite(v) -> bool:
    try:
        return math.isfinite(float(v))
    except Exception:
        return False


def _code(v) -> str:
    s = re.sub(r"\D", "", str(v or "").replace(".0", ""))
    return s.zfill(6)[-6:] if s else ""


def _date(v) -> Optional[pd.Timestamp]:
    try:
        x = pd.to_datetime(v, errors="coerce")
        return None if pd.isna(x) else x.normalize()
    except Exception:
        return None


def _pct(a, b) -> float:
    try:
        return (float(a) / float(b) - 1.0) * 100.0 if float(b) != 0 else np.nan
    except Exception:
        return np.nan


def _safe_div(a, b) -> float:
    try:
        return float(a) / float(b) if float(b) != 0 else np.nan
    except Exception:
        return np.nan


def _trim_mean(s: pd.Series, frac: float = 0.10) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values()
    if x.empty:
        return np.nan
    k = int(math.floor(len(x) * frac))
    if k > 0 and len(x) > 2 * k:
        x = x.iloc[k:-k]
    return float(x.mean()) if len(x) else np.nan


def _ex_top2_mean(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna().sort_values(ascending=False)
    if len(x) <= 2:
        return np.nan
    return float(x.iloc[2:].mean())


def _rate(s: pd.Series) -> float:
    x = pd.Series(s).dropna()
    if x.empty:
        return np.nan
    try:
        return float(x.astype(bool).mean() * 100.0)
    except Exception:
        return np.nan


def _fmt(v, suffix="%") -> str:
    return "N/A" if not _finite(v) else f"{float(v):+.2f}{suffix}"


def _r(v) -> str:
    return "N/A" if not _finite(v) else f"{float(v):.1f}%"


def _ensure_dir(output_dir: str | Path) -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    (p / MINUTE_DIR_NAME).mkdir(parents=True, exist_ok=True)
    return p


def _normalize_minute_df(df: pd.DataFrame, *, code: str = "", trade_date: str = "", source: str = "UNKNOWN", value_quality: str = "UNKNOWN") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    x = df.copy()
    ren = {}
    for c in x.columns:
        z = str(c).strip().lower().replace(" ", "_")
        if z in {"datetime", "timestamp", "date_time", "체결시각", "일시"}: ren[c] = "datetime"
        elif z in {"date", "trade_date", "날짜"}: ren[c] = "trade_date"
        elif z in {"time", "hhmm", "시간"}: ren[c] = "time"
        elif z in {"code", "ticker", "symbol", "종목코드"}: ren[c] = "code"
        elif z in {"open", "시가"}: ren[c] = "open"
        elif z in {"high", "고가"}: ren[c] = "high"
        elif z in {"low", "저가"}: ren[c] = "low"
        elif z in {"close", "종가", "현재가"}: ren[c] = "close"
        elif z in {"volume", "거래량"}: ren[c] = "volume"
        elif z in {"value", "amount", "turnover", "거래대금", "trading_value"}: ren[c] = "value"
        elif z in {"source", "data_source"}: ren[c] = "source"
        elif z in {"value_quality", "amount_quality"}: ren[c] = "value_quality"
    x = x.rename(columns=ren)
    if "datetime" not in x.columns:
        if "trade_date" in x.columns and "time" in x.columns:
            x["datetime"] = pd.to_datetime(x["trade_date"].astype(str).str[:10] + " " + x["time"].astype(str).str[:5], errors="coerce")
        elif trade_date and "time" in x.columns:
            x["datetime"] = pd.to_datetime(str(trade_date)[:10] + " " + x["time"].astype(str).str[:5], errors="coerce")
        else:
            return pd.DataFrame()
    else:
        # Epoch seconds/milliseconds are supported as well as text timestamps.
        if pd.api.types.is_numeric_dtype(x["datetime"]):
            med = pd.to_numeric(x["datetime"], errors="coerce").median()
            unit = "ms" if _finite(med) and med > 10_000_000_000 else "s"
            x["datetime"] = pd.to_datetime(x["datetime"], unit=unit, utc=True, errors="coerce").dt.tz_convert(KST).dt.tz_localize(None)
        else:
            x["datetime"] = pd.to_datetime(x["datetime"], errors="coerce")
            try:
                if getattr(x["datetime"].dt, "tz", None) is not None:
                    x["datetime"] = x["datetime"].dt.tz_convert(KST).dt.tz_localize(None)
            except Exception:
                pass
    x = x.dropna(subset=["datetime"]).copy()
    if x.empty:
        return x
    if "code" not in x.columns: x["code"] = _code(code)
    x["code"] = x["code"].map(_code)
    if trade_date:
        target = pd.to_datetime(trade_date).date()
        x = x[x["datetime"].dt.date == target].copy()
    x["trade_date"] = x["datetime"].dt.strftime("%Y-%m-%d")
    x["time"] = x["datetime"].dt.strftime("%H:%M")
    for c in ["open", "high", "low", "close", "volume", "value"]:
        if c not in x.columns: x[c] = np.nan
        x[c] = pd.to_numeric(x[c], errors="coerce")
    x = x[(x["time"] >= "09:00") & (x["time"] <= "15:03")].copy()
    x = x[(x["close"] > 0) & (x["volume"] >= 0)].copy()
    if "value" not in x.columns or x["value"].isna().all():
        x["value"] = x["close"] * x["volume"]
        value_quality = "PROXY_CLOSE_X_VOLUME"
    else:
        missing = x["value"].isna()
        if missing.any():
            x.loc[missing, "value"] = x.loc[missing, "close"] * x.loc[missing, "volume"]
            if value_quality == "ACTUAL_REPORTED": value_quality = "MIXED_ACTUAL_PROXY"
    if "source" not in x.columns: x["source"] = source
    x["source"] = x["source"].fillna(source).astype(str)
    if "value_quality" not in x.columns: x["value_quality"] = value_quality
    x["value_quality"] = x["value_quality"].fillna(value_quality).astype(str)
    return x[["trade_date", "time", "datetime", "code", "open", "high", "low", "close", "volume", "value", "source", "value_quality"]].sort_values(["code", "datetime"]).drop_duplicates(["code", "datetime"], keep="last")


def _read_csv_any(path: Path) -> pd.DataFrame:
    try:
        if path.suffix.lower() in {".parquet", ".pq"}:
            return pd.read_parquet(path)
        return pd.read_csv(path, dtype={"code": str, "Code": str, "종목코드": str})
    except Exception:
        return pd.DataFrame()


def _load_external_minutes(code: str, trade_date: str, output_dir: str | Path) -> pd.DataFrame:
    """Provider A: broker/licensed/current local minute files. Actual value is preferred."""
    candidates: List[Path] = []
    env_today = str(os.environ.get("HAM_TODAY_MINUTE_CSV", "")).strip()
    if env_today:
        candidates.append(Path(env_today))
    root = str(os.environ.get("HAM_MINUTE_HISTORY_ROOT", "")).strip()
    if root:
        rp = Path(root)
        if rp.is_file(): candidates.append(rp)
        elif rp.exists():
            candidates += [rp / f"{trade_date}.csv.gz", rp / f"{trade_date}.csv", rp / f"{trade_date}_{code}.csv", rp / f"{code}_{trade_date}.csv"]
    p = _ensure_dir(output_dir) / MINUTE_DIR_NAME
    candidates += [p / f"{trade_date}.csv.gz", p / f"{trade_date}.csv"]
    seen = set()
    for f in candidates:
        try:
            if not f.exists() or str(f) in seen: continue
            seen.add(str(f))
            d = _read_csv_any(f)
            if d.empty: continue
            x = _normalize_minute_df(d, code=code, trade_date=trade_date, source=f"LOCAL:{f.name}", value_quality="ACTUAL_REPORTED")
            x = x[x["code"] == _code(code)].copy()
            if not x.empty:
                # Respect explicit quality if supplied; otherwise local value column is treated as reported actual.
                return x
        except Exception:
            continue
    return pd.DataFrame()


def _yahoo_symbols(code: str, market: str = "") -> List[str]:
    c = _code(code)
    m = str(market or "").upper()
    if "KOSDAQ" in m or m in {"KQ", "KOSDAQ"}: return [f"{c}.KQ", f"{c}.KS"]
    if "KOSPI" in m or m in {"KS", "KOSPI"}: return [f"{c}.KS", f"{c}.KQ"]
    return [f"{c}.KS", f"{c}.KQ"]


def _fetch_yahoo_minutes(code: str, trade_date: str, market: str = "") -> pd.DataFrame:
    """Fallback minute PRICE/VOLUME. Value is explicitly a close*volume proxy."""
    if not _on("HAM_ALLOW_YAHOO_MINUTE_PROXY", "1"):
        return pd.DataFrame()
    dt = pd.to_datetime(trade_date)
    # Yahoo's chart endpoint availability for 1m history is provider-dependent; try exact day first, current 5d second.
    p1 = int((dt.tz_localize(KST) - pd.Timedelta(hours=1)).timestamp())
    p2 = int((dt.tz_localize(KST) + pd.Timedelta(days=1, hours=2)).timestamp())
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
    for sym in _yahoo_symbols(code, market):
        for params in [
            {"period1": p1, "period2": p2, "interval": "1m", "includePrePost": "false", "events": "div,splits"},
            {"range": "5d", "interval": "1m", "includePrePost": "false", "events": "div,splits"},
        ]:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
                r = requests.get(url, params=params, headers=headers, timeout=float(os.environ.get("HAM_MINUTE_HTTP_TIMEOUT", "8")))
                r.raise_for_status()
                js = r.json()
                res = (((js or {}).get("chart") or {}).get("result") or [None])[0]
                if not res: continue
                ts = res.get("timestamp") or []
                q = (((res.get("indicators") or {}).get("quote") or [{}])[0])
                if not ts: continue
                d = pd.DataFrame({
                    "datetime": ts,
                    "open": q.get("open", []), "high": q.get("high", []), "low": q.get("low", []), "close": q.get("close", []), "volume": q.get("volume", []),
                })
                x = _normalize_minute_df(d, code=code, trade_date=trade_date, source=f"YAHOO_CHART:{sym}", value_quality="PROXY_CLOSE_X_VOLUME")
                if not x.empty:
                    return x
            except Exception:
                continue
    return pd.DataFrame()


def fetch_minutes(code: str, trade_date: str, market: str = "", output_dir: str | Path = "reports") -> pd.DataFrame:
    x = _load_external_minutes(code, trade_date, output_dir)
    if not x.empty:
        return x
    return _fetch_yahoo_minutes(code, trade_date, market)


def _minute_file(output_dir: str | Path, trade_date: str) -> Path:
    return _ensure_dir(output_dir) / MINUTE_DIR_NAME / f"{trade_date}.csv.gz"


def persist_minutes(df: pd.DataFrame, output_dir: str | Path, trade_date: str) -> Path:
    p = _minute_file(output_dir, trade_date)
    if df is None or df.empty:
        return p
    x = _normalize_minute_df(df)
    if p.exists():
        try:
            old = _read_csv_any(p)
            old = _normalize_minute_df(old)
            x = pd.concat([old, x], ignore_index=True, sort=False)
        except Exception:
            pass
    x = x.drop_duplicates(["trade_date", "code", "time"], keep="last").sort_values(["code", "datetime"])
    x.drop(columns=["datetime"], errors="ignore").to_csv(p, index=False, encoding="utf-8-sig", compression="gzip")
    return p


def load_history(output_dir: str | Path, end_date: str, lookback_days: int = 45, include_end: bool = False) -> pd.DataFrame:
    p = _ensure_dir(output_dir) / MINUTE_DIR_NAME
    end = pd.to_datetime(end_date).normalize()
    frames = []
    # Local persistent files.
    for f in sorted(p.glob("*.csv*")):
        try:
            dstr = f.name[:10]
            dt = pd.to_datetime(dstr, errors="coerce")
            if pd.isna(dt): continue
            if dt > end or (dt == end and not include_end) or dt < end - pd.Timedelta(days=lookback_days): continue
            d = _read_csv_any(f)
            n = _normalize_minute_df(d, trade_date=dstr, source=f"CACHE:{f.name}", value_quality="UNKNOWN")
            if not n.empty: frames.append(n)
        except Exception:
            continue
    # Optional external historical consolidated file/dir is handled here as well.
    root = str(os.environ.get("HAM_MINUTE_HISTORY_ROOT", "")).strip()
    if root:
        rp = Path(root)
        candidates = [rp] if rp.is_file() else list(rp.glob("*.csv*")) + list(rp.glob("*.parquet")) if rp.exists() else []
        for f in candidates:
            try:
                d = _read_csv_any(f)
                if d.empty: continue
                n = _normalize_minute_df(d, source=f"EXTERNAL:{f.name}", value_quality="ACTUAL_REPORTED")
                if n.empty: continue
                dates = pd.to_datetime(n["trade_date"], errors="coerce")
                n = n[(dates < end if not include_end else dates <= end) & (dates >= end - pd.Timedelta(days=lookback_days))]
                if not n.empty: frames.append(n)
            except Exception:
                continue
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    return out.drop_duplicates(["trade_date", "code", "time"], keep="last")


def _time_bucket(t: str) -> str:
    if "09:00" <= t < "09:30": return "OPEN_HAM"
    if "09:30" <= t < "11:30": return "AM_HAM"
    if "11:30" <= t < "13:30": return "MID_HAM"
    if "13:30" <= t < "14:30": return "PM_HAM"
    return "LATE_HAM"


def add_baselines(today: pd.DataFrame, history: pd.DataFrame, n_days: int = 20) -> Tuple[pd.DataFrame, dict]:
    """A=same-minute 20D median/mean, B=same time-bucket historical median, C=prior-30m same-day median."""
    if today is None or today.empty:
        return pd.DataFrame(), {"history_days": 0, "method_a_eligible": 0}
    x = today.copy().sort_values(["code", "datetime"])
    hist = history.copy() if history is not None else pd.DataFrame()
    if not hist.empty:
        hist["trade_date"] = hist["trade_date"].astype(str).str[:10]
        # Last N distinct dates per code, preserving only prior dates supplied by caller.
        keep_rows = []
        for code, g in hist.groupby("code"):
            dates = sorted(g["trade_date"].dropna().unique())[-int(n_days):]
            keep_rows.append(g[g["trade_date"].isin(dates)])
        hist = pd.concat(keep_rows, ignore_index=True) if keep_rows else pd.DataFrame()
    if hist.empty:
        stats = pd.DataFrame(columns=["code", "time", "a_median", "a_mean", "a_n"])
        bucket = pd.DataFrame(columns=["code", "bucket", "b_median", "b_mean", "b_n"])
    else:
        stats = hist.groupby(["code", "time"])["value"].agg(a_median="median", a_mean="mean", a_n="count").reset_index()
        h = hist.copy(); h["bucket"] = h["time"].map(_time_bucket)
        bucket = h.groupby(["code", "bucket"])["value"].agg(b_median="median", b_mean="mean", b_n="count").reset_index()
    x["bucket"] = x["time"].map(_time_bucket)
    x = x.merge(stats, on=["code", "time"], how="left").merge(bucket, on=["code", "bucket"], how="left")
    x["ham_ratio_a"] = x["value"] / x["a_median"].replace(0, np.nan)
    x["ham_ratio_a_mean"] = x["value"] / x["a_mean"].replace(0, np.nan)
    x["ham_ratio_b"] = x["value"] / x["b_median"].replace(0, np.nan)
    x["c_median"] = np.nan
    for code, idx in x.groupby("code").groups.items():
        s = x.loc[idx, "value"]
        x.loc[idx, "c_median"] = s.shift(1).rolling(int(os.environ.get("HAM_ROLLING_BASELINE_MINUTES", "30")), min_periods=5).median().values
    x["ham_ratio_c"] = x["value"] / x["c_median"].replace(0, np.nan)
    min_a = int(os.environ.get("HAM_BASELINE_A_MIN_DAYS", str(n_days)))
    x["baseline_a_eligible"] = x["a_n"].fillna(0) >= min_a
    # A always preferred; then B; then C.  Source remains explicit.
    x["ham_ratio"] = np.where(x["baseline_a_eligible"] & x["ham_ratio_a"].notna(), x["ham_ratio_a"], np.where(x["ham_ratio_b"].notna(), x["ham_ratio_b"], x["ham_ratio_c"]))
    x["ham_ratio_source"] = np.where(x["baseline_a_eligible"] & x["ham_ratio_a"].notna(), "A_SAME_MINUTE_MEDIAN", np.where(x["ham_ratio_b"].notna(), "B_TIME_BUCKET_NORMALIZED", "C_ROLLING_30M_MEDIAN"))
    hist_days = int(hist["trade_date"].nunique()) if not hist.empty else 0
    return x, {"history_days": hist_days, "method_a_eligible": int(x["baseline_a_eligible"].sum()), "rows": len(x)}


def _first_true_time(df: pd.DataFrame, mask: pd.Series) -> str:
    try:
        q = df[mask.fillna(False)]
        return str(q.iloc[0]["time"]) if not q.empty else ""
    except Exception:
        return ""


def _json_top3(df: pd.DataFrame, col: str) -> str:
    try:
        q = df.nlargest(min(3, len(df)), col)[["time", col]]
        return json.dumps([{"time": str(r["time"]), "value": round(float(r[col]), 6)} for _, r in q.iterrows()], ensure_ascii=False)
    except Exception:
        return "[]"


def derive_features(one: pd.DataFrame, meta: Optional[dict] = None, baseline_diag: Optional[dict] = None) -> dict:
    if one is None or one.empty:
        return {}
    g = one.copy().sort_values("datetime").reset_index(drop=True)
    g = g[g["time"] <= "15:03"].copy()
    if g.empty: return {}
    # Pick the strongest relative event; absolute value is retained independently.
    valid_ratio = pd.to_numeric(g["ham_ratio"], errors="coerce")
    if valid_ratio.notna().any():
        ham_i = valid_ratio.idxmax()
    else:
        ham_i = pd.to_numeric(g["value"], errors="coerce").idxmax()
    h = g.loc[ham_i]
    hi = int(g.index.get_loc(ham_i))
    pre = g.iloc[:hi+1]
    post = g.iloc[hi+1:].copy()
    ham_mid = (float(h["open"]) + float(h["close"])) / 2.0
    day_low = float(g["low"].min()); day_high = float(g["high"].max())
    rng = max(1e-12, float(h["high"]) - float(h["low"]))
    ham_close_pos = (float(h["close"]) - float(h["low"])) / rng
    ham_price_pos = (float(h["close"]) - day_low) / max(1e-12, day_high - day_low)
    upper_wick = (float(h["high"]) - max(float(h["open"]), float(h["close"]))) / max(1e-12, float(h["close"])) * 100.0
    lower_wick = (min(float(h["open"]), float(h["close"])) - float(h["low"])) / max(1e-12, float(h["close"])) * 100.0
    body = abs(float(h["close"]) - float(h["open"])) / max(1e-12, float(h["open"])) * 100.0

    if post.empty:
        post_low = post_high = float(h["close"])
        low_break = mid_break = close_break = False
        recovery = np.nan; high_retest = False; dd_close = dd_high = 0.0
        low_idx_pos = hi
    else:
        post_low = float(post["low"].min()); post_high = float(post["high"].max())
        low_break = bool((post["low"] < float(h["low"])).any())
        mid_break = bool((post["low"] < ham_mid).any())
        close_break = bool((post["low"] < float(h["close"])).any())
        dd_close = _pct(post_low, float(h["close"])); dd_high = _pct(post_low, float(h["high"]))
        low_label = post["low"].idxmin(); low_idx_pos = int(low_label)
        after_low = g.loc[low_label:]
        recovery = _pct(float(after_low["high"].max()), post_low) if not after_low.empty else np.nan
        high_retest = bool((post["high"] >= float(h["high"]) * 0.995).any())

    # Restart candidate is identified descriptively after the post-HAM low; policy thresholds are selected later on TRAIN only.
    after_low = g.loc[low_idx_pos:].copy() if low_idx_pos < len(g) else pd.DataFrame()
    restart = None
    pullback_end_pos = low_idx_pos
    if not after_low.empty:
        after_low["ret1"] = after_low["close"].pct_change().fillna(0) * 100.0
        # Preliminary pullback median up to each point; select price-positive minute with strongest value-vs-pullback ratio.
        pb = g.iloc[hi+1:max(hi+2, low_idx_pos+1)].copy()
        pb_med_seed = float(pb["value"].median()) if not pb.empty else float(h["value"])
        after_low["restart_value_vs_pb_seed"] = after_low["value"] / max(1e-12, pb_med_seed)
        cand = after_low[(after_low["ret1"] > 0) & (after_low["close"] >= float(h["close"]) * 0.98)].copy()
        if not cand.empty:
            ri = cand["restart_value_vs_pb_seed"].idxmax()
            restart = g.loc[ri]
            pullback_end_pos = int(ri)
    pb = g.iloc[hi+1:pullback_end_pos].copy() if pullback_end_pos > hi+1 else g.iloc[hi+1:max(hi+2, low_idx_pos+1)].copy()
    if pb.empty and not post.empty:
        pb = post.head(min(5, len(post))).copy()
    pb_val_mean = float(pb["value"].mean()) if not pb.empty else np.nan
    pb_val_med = float(pb["value"].median()) if not pb.empty else np.nan
    pb_vol_mean = float(pb["volume"].mean()) if not pb.empty else np.nan
    pb_low = float(pb["low"].min()) if not pb.empty else post_low
    pb_depth = _pct(pb_low, float(h["close"]))
    pb_duration = max(0, len(pb))

    if restart is not None:
        restart_time = str(restart["time"]); restart_value = float(restart["value"]); restart_ratio = _num(restart.get("ham_ratio"))
        restart_price_chg = _pct(float(restart["close"]), float(h["close"]))
        restart_break_close = bool(float(restart["close"]) >= float(h["close"]))
        restart_break_high = bool(float(restart["high"]) >= float(h["high"]))
        restart_vs_pb = _safe_div(restart_value, pb_val_mean)
    else:
        restart_time = ""; restart_value = restart_ratio = restart_price_chg = restart_vs_pb = np.nan
        restart_break_close = restart_break_high = False
    # Seed restart count: descriptive only, not policy.
    if _finite(pb_val_mean) and pb_val_mean > 0:
        rr = post.copy(); rr["vpb"] = rr["value"] / pb_val_mean; rr["r1"] = rr["close"].pct_change().fillna(0) * 100
        restart_count = int(((rr["vpb"] >= 1.5) & (rr["r1"] > 0)).sum())
    else:
        restart_count = 0

    c1503 = g.iloc[-1]
    cprice = float(c1503["close"])
    c_from_hh = _pct(cprice, float(h["high"])); c_from_hc = _pct(cprice, float(h["close"])); c_from_dh = _pct(cprice, day_high)
    c_above_low = bool(cprice >= float(h["low"])); c_above_mid = bool(cprice >= ham_mid); c_above_close = bool(cprice >= float(h["close"]))
    restart_alive = bool(restart is not None and cprice >= float(restart["close"]) * 0.99)
    last10 = g.tail(min(10, len(g)))
    c_val_strength = _safe_div(float(last10["value"].median()), pb_val_med) if _finite(pb_val_med) else np.nan

    # Distribution warning components.
    post_neg = post.copy()
    if not post_neg.empty:
        post_neg["r1"] = post_neg["close"].pct_change().fillna(0) * 100
        sell = post_neg[post_neg["r1"] < 0]
        post_sell_value = float(sell["value"].sum()) if not sell.empty else 0.0
        break_rows = post_neg[post_neg["low"] < float(h["low"])]
        break_speed = int(break_rows.index[0] - ham_i) if not break_rows.empty else np.nan
    else:
        post_sell_value = 0.0; break_speed = np.nan
    high_pos = (float(h["high"]) - day_low) / max(1e-12, day_high-day_low)
    dist_warn = bool(high_pos >= 0.85 and upper_wick >= 1.5 and (low_break or c_from_hc <= -2.0) and post_sell_value > float(h["value"]))

    ratio = _num(h.get("ham_ratio")); value = float(h["value"])
    seed_ham = bool(_finite(ratio) and ratio >= 2.0)
    seed_survive = bool(c_above_low or c_above_mid or c_above_close)
    seed_dry = bool(_finite(pb_val_mean) and pb_val_mean < value)
    seed_restart = bool(restart is not None and _finite(restart_vs_pb) and restart_vs_pb >= 1.5 and _finite(restart_price_chg) and restart_price_chg > 0)
    seed_close = bool(c_above_mid and c_from_dh >= -5.0 and restart_alive)

    meta = meta or {}
    out = {
        "policy_version": VERSION, "research_only": 1, "factor_name": FACTOR_NAME,
        "trade_date": str(g.iloc[0]["trade_date"]), "code": _code(g.iloc[0]["code"]),
        "name": str(meta.get("name", meta.get("Name", "")) or ""), "market": str(meta.get("market", meta.get("Market", "")) or ""),
        "sector": str(meta.get("sector", meta.get("Sector", "")) or ""), "market_cap_eok": _num(meta.get("market_cap_eok", meta.get("MarcapEok", np.nan))),
        "minute_source": str(h.get("source", "")), "value_quality": str(h.get("value_quality", "UNKNOWN")),
        "baseline_history_days": int((baseline_diag or {}).get("history_days", 0)), "baseline_a_n_at_ham": int(_num(h.get("a_n"), 0)),
        "HAM_MAX_VALUE": float(g["value"].max()), "HAM_MAX_RATIO": float(pd.to_numeric(g["ham_ratio"], errors="coerce").max()), "HAM_MAX_TIME": str(g.loc[pd.to_numeric(g["ham_ratio"], errors="coerce").idxmax(), "time"]) if pd.to_numeric(g["ham_ratio"], errors="coerce").notna().any() else str(h["time"]),
        "HAM_FIRST_TIME": _first_true_time(g, pd.to_numeric(g["ham_ratio"], errors="coerce") >= 2.0),
        "HAM_COUNT_2X": int((pd.to_numeric(g["ham_ratio"], errors="coerce") >= 2).sum()), "HAM_COUNT_3X": int((pd.to_numeric(g["ham_ratio"], errors="coerce") >= 3).sum()),
        "HAM_COUNT_5X": int((pd.to_numeric(g["ham_ratio"], errors="coerce") >= 5).sum()), "HAM_COUNT_10X": int((pd.to_numeric(g["ham_ratio"], errors="coerce") >= 10).sum()),
        "HAM_TOP3_VALUE": _json_top3(g, "value"), "HAM_TOP3_RATIO": _json_top3(g[pd.to_numeric(g["ham_ratio"], errors="coerce").notna()], "ham_ratio") if pd.to_numeric(g["ham_ratio"], errors="coerce").notna().any() else "[]",
        "HAM_TIME": str(h["time"]), "HAM_TIME_BUCKET": _time_bucket(str(h["time"])), "HAM_VALUE": value, "HAM_VALUE_EOK": value / 100_000_000.0,
        "HAM_RATIO": ratio, "HAM_RATIO_SOURCE": str(h.get("ham_ratio_source", "")), "HAM_RATIO_A": _num(h.get("ham_ratio_a")), "HAM_RATIO_A_MEAN": _num(h.get("ham_ratio_a_mean")), "HAM_RATIO_B": _num(h.get("ham_ratio_b")), "HAM_RATIO_C": _num(h.get("ham_ratio_c")),
        "HAM_OPEN": float(h["open"]), "HAM_HIGH": float(h["high"]), "HAM_LOW": float(h["low"]), "HAM_CLOSE": float(h["close"]), "HAM_MID": ham_mid,
        "HAM_BODY_PCT": body, "HAM_UPPER_WICK_PCT": upper_wick, "HAM_LOWER_WICK_PCT": lower_wick, "HAM_CLOSE_POSITION": ham_close_pos, "HAM_PRICE_POSITION": ham_price_pos,
        "POST_HAM_LOW": post_low, "POST_HAM_HIGH": post_high, "POST_HAM_MAX_DRAWDOWN": dd_close, "POST_HAM_MAX_DRAWDOWN_FROM_HIGH": dd_high,
        "POST_HAM_LOW_BREAK": int(low_break), "POST_HAM_MID_BREAK": int(mid_break), "POST_HAM_CLOSE_BREAK": int(close_break), "POST_HAM_RECOVERY_PCT": recovery, "POST_HAM_HIGH_RETEST": int(high_retest),
        "SURVIVE_HAM_LOW": int(not low_break), "SURVIVE_HAM_MID": int(not mid_break), "SURVIVE_HAM_CLOSE": int(not close_break),
        "PULLBACK_VALUE_MEAN": pb_val_mean, "PULLBACK_VALUE_MEDIAN": pb_val_med, "PULLBACK_VALUE_RATIO_TO_HAM": _safe_div(pb_val_mean, value), "PULLBACK_VOLUME_MEAN": pb_vol_mean,
        "PULLBACK_DEPTH_PCT": pb_depth, "PULLBACK_DURATION_MIN": pb_duration,
        "RESTART_TIME": restart_time, "RESTART_VALUE": restart_value, "RESTART_RATIO": restart_ratio, "RESTART_PRICE_CHANGE": restart_price_chg,
        "RESTART_BREAK_HAM_CLOSE": int(restart_break_close), "RESTART_BREAK_HAM_HIGH": int(restart_break_high), "RESTART_VALUE_VS_PULLBACK": restart_vs_pb, "RESTART_COUNT": restart_count,
        "CLOSE1503_PRICE": cprice, "CLOSE1503_FROM_HAM_HIGH": c_from_hh, "CLOSE1503_FROM_HAM_CLOSE": c_from_hc, "CLOSE1503_FROM_DAY_HIGH": c_from_dh,
        "CLOSE1503_ABOVE_HAM_LOW": int(c_above_low), "CLOSE1503_ABOVE_HAM_MID": int(c_above_mid), "CLOSE1503_ABOVE_HAM_CLOSE": int(c_above_close),
        "CLOSE1503_RESTART_ALIVE": int(restart_alive), "CLOSE1503_VALUE_STRENGTH": c_val_strength,
        "HAM_HIGH_POSITION": high_pos, "HAM_UPPER_WICK": upper_wick, "POST_HAM_SELL_VALUE": post_sell_value, "POST_HAM_BREAK_SPEED": break_speed, "HAM_DIST_WARNING": int(dist_warn),
        "DAY_OPEN_0900": float(g.iloc[0]["open"]), "DAY_HIGH_TO_1503": day_high, "DAY_LOW_TO_1503": day_low, "DAY_VALUE_TO_1503": float(g["value"].sum()),
        "DAY_RET_TO_1503": _pct(cprice, float(g.iloc[0]["open"])),
        "HAM_EVENT_CANDIDATE": int(seed_ham), "HAM_SURVIVE_SEED": int(seed_ham and seed_survive), "HAM_PULLBACK_DRY_SEED": int(seed_ham and seed_survive and seed_dry),
        "HAM_SURVIVE_RESTART_SEED": int(seed_ham and seed_survive and seed_dry and seed_restart), "HAM_RESTART_CLOSE_CANDIDATE": int(seed_ham and seed_survive and seed_dry and seed_restart and seed_close),
        "HAM_RESTART_CLOSE": 0,  # Reserved for TRAIN-locked policy only; never a live signal here.
        "captured_at_kst": _now().strftime("%Y-%m-%d %H:%M:%S%z"),
    }
    return out


def _append_csv(path: Path, row_or_df) -> None:
    df = row_or_df if isinstance(row_or_df, pd.DataFrame) else pd.DataFrame([row_or_df])
    if df is None or df.empty: return
    if path.exists():
        try: old = pd.read_csv(path, dtype={"code": str})
        except Exception: old = pd.DataFrame()
        out = pd.concat([old, df], ignore_index=True, sort=False)
    else: out = df.copy()
    keys = [k for k in ["trade_date", "code"] if k in out.columns]
    if keys: out = out.drop_duplicates(keys, keep="last")
    out.to_csv(path, index=False, encoding="utf-8-sig")


def _capture_universe(snapshot: pd.DataFrame, signal_path: str, max_codes: int = 60, top_amount_n: int = 35) -> pd.DataFrame:
    s = snapshot.copy() if snapshot is not None else pd.DataFrame()
    if s.empty:
        return pd.DataFrame(columns=["code", "name", "market", "sector", "amount"])
    ren = {}
    for c in s.columns:
        z = str(c)
        if z in {"Code", "종목코드", "ticker"}: ren[c] = "code"
        elif z in {"Name", "종목명", "name"}: ren[c] = "name"
        elif z in {"Market", "시장", "market"}: ren[c] = "market"
        elif z in {"Sector0930", "Sector", "업종", "sector"}: ren[c] = "sector"
        elif z in {"Amount", "거래대금", "amount"}: ren[c] = "amount"
        elif z in {"Marcap", "시가총액", "market_cap"}: ren[c] = "market_cap"
    s = s.rename(columns=ren)
    for c in ["code", "name", "market", "sector"]:
        if c not in s.columns: s[c] = ""
    if "amount" not in s.columns: s["amount"] = 0
    s["code"] = s["code"].map(_code); s["amount"] = pd.to_numeric(s["amount"], errors="coerce").fillna(0)
    if "market_cap" in s.columns: s["market_cap_eok"] = pd.to_numeric(s["market_cap"], errors="coerce") / 100_000_000.0
    else: s["market_cap_eok"] = np.nan
    sig_codes = []
    try:
        p = Path(signal_path)
        if p.exists():
            d = pd.read_csv(p, dtype={"code": str})
            if not d.empty and "signal_date" in d.columns:
                today = _now().strftime("%Y-%m-%d")
                d = d[pd.to_datetime(d["signal_date"], errors="coerce").dt.strftime("%Y-%m-%d") == today]
            sig_codes = [_code(x) for x in d.get("code", pd.Series(dtype=str)).tolist() if _code(x)]
    except Exception: pass
    extra = [_code(x) for x in str(os.environ.get("HAM_CAPTURE_CODES", "")).split(",") if _code(x)]
    pri = list(dict.fromkeys(sig_codes + extra))
    top = s.sort_values("amount", ascending=False).head(max(0, int(top_amount_n)))["code"].tolist()
    codes = list(dict.fromkeys(pri + top))[:max(1, int(max_codes))]
    out = s[s["code"].isin(codes)].copy()
    # Preserve requested priority order.
    order = {c: i for i, c in enumerate(codes)}; out["_order"] = out["code"].map(order)
    return out.sort_values("_order").drop(columns="_order")


def run_capture(snapshot: pd.DataFrame, signal_path: str = "reports/v1080_stockhunter_signals.csv", output_dir: str | Path = "reports", trade_date: str = "") -> Tuple[str, pd.DataFrame, pd.DataFrame]:
    """15:03 forward capture.  Does not emit or modify live candidates."""
    outdir = _ensure_dir(output_dir)
    td = str(trade_date or _now().strftime("%Y-%m-%d"))[:10]
    max_codes = int(os.environ.get("HAM_CAPTURE_MAX_CODES", "60")); topn = int(os.environ.get("HAM_CAPTURE_TOP_AMOUNT_N", "35"))
    uni = _capture_universe(snapshot, signal_path, max_codes=max_codes, top_amount_n=topn)
    if uni.empty:
        report = REPORT_HEADER + "\n- 15:03 연구 유니버스가 비어 있습니다. LIVE에는 영향 없음."
        return report, pd.DataFrame(), pd.DataFrame()
    history = load_history(outdir, td, lookback_days=int(os.environ.get("HAM_HISTORY_CALENDAR_DAYS", "45")), include_end=False)
    all_minutes = []; feats = []; audit = []
    from concurrent.futures import ThreadPoolExecutor, as_completed
    workers = max(1, min(10, int(os.environ.get("HAM_MINUTE_WORKERS", "6"))))
    rows = [r.to_dict() for _, r in uni.iterrows()]
    def one(meta):
        c = _code(meta.get("code")); m = str(meta.get("market", ""))
        raw = fetch_minutes(c, td, market=m, output_dir=outdir)
        if raw.empty:
            return c, raw, {}, {"trade_date": td, "code": c, "status": "NO_MINUTE_DATA", "source": "", "value_quality": ""}
        h = history[history["code"] == c].copy() if not history.empty else pd.DataFrame()
        b, diag = add_baselines(raw, h, n_days=int(os.environ.get("HAM_BASELINE_N_DAYS", "20")))
        f = derive_features(b, meta=meta, baseline_diag=diag)
        q = str(f.get("value_quality", ""))
        return c, b, f, {"trade_date": td, "code": c, "status": "OK", "minute_rows": len(b), "history_days": diag.get("history_days", 0), "source": f.get("minute_source", ""), "value_quality": q, "true_value_eligible": int(q in {"ACTUAL_REPORTED", "MIXED_ACTUAL_PROXY"} and "PROXY" not in q)}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(one, r) for r in rows]
        for fu in as_completed(futs):
            try:
                c, b, f, a = fu.result(); audit.append(a)
                if b is not None and not b.empty: all_minutes.append(b)
                if f: feats.append(f)
            except Exception as e:
                audit.append({"trade_date": td, "code": "", "status": f"ERROR:{type(e).__name__}:{e}"})
    mins = pd.concat(all_minutes, ignore_index=True, sort=False) if all_minutes else pd.DataFrame()
    if not mins.empty: persist_minutes(mins, outdir, td)
    fdf = pd.DataFrame(feats); adf = pd.DataFrame(audit)
    if not fdf.empty: _append_csv(outdir / FEATURE_LEDGER, fdf)
    if not adf.empty: _append_csv(outdir / CAPTURE_AUDIT, adf)
    # Baseline audit is one row per capture day.
    bqa = {
        "trade_date": td, "policy_version": VERSION, "research_only": 1, "capture_universe": len(uni), "minute_ok": int((adf.get("status", pd.Series(dtype=str)) == "OK").sum()) if not adf.empty else 0,
        "actual_value_codes": int(adf.get("true_value_eligible", pd.Series(dtype=float)).fillna(0).sum()) if not adf.empty else 0,
        "history_distinct_days": int(history["trade_date"].nunique()) if not history.empty else 0, "baseline_target_days": int(os.environ.get("HAM_BASELINE_N_DAYS", "20")),
        "note": "A=same-minute historical median preferred; B=time-bucket normalization; C=prior-30m median. Proxy value is labeled, never disguised as actual turnover.",
    }
    _append_csv(outdir / BASELINE_AUDIT, bqa)
    report = build_capture_report(fdf, adf, bqa)
    return report, fdf, adf


def build_capture_report(fdf: pd.DataFrame, adf: pd.DataFrame, bqa: dict) -> str:
    lines = [REPORT_HEADER, f"📌 {VERSION} · {FACTOR_NAME} · RESEARCH_ONLY=True", "- LIVE/PRIME/LCZ/M5R/ENVUP/기존 RESTART 후보·점수·카드에는 연결하지 않습니다."]
    lines.append(f"📁 15:03 capture: universe {bqa.get('capture_universe',0)} · minute OK {bqa.get('minute_ok',0)} · 실제 거래대금 eligible {bqa.get('actual_value_codes',0)} · 과거 minute {bqa.get('history_distinct_days',0)}/20일")
    if fdf is None or fdf.empty:
        lines.append("- 분봉 피처 없음. 과거/당일 실제 분봉 소스를 연결하거나 provider 상태를 확인하세요.")
        return "\n".join(lines)
    lines.append(f"- HAM candidate(2x seed) {int(fdf['HAM_EVENT_CANDIDATE'].sum())} · SURVIVE {int(fdf['HAM_SURVIVE_SEED'].sum())} · RESTART {int(fdf['HAM_SURVIVE_RESTART_SEED'].sum())} · CLOSE candidate {int(fdf['HAM_RESTART_CLOSE_CANDIDATE'].sum())}")
    top = fdf.sort_values(["HAM_RESTART_CLOSE_CANDIDATE", "HAM_MAX_RATIO", "HAM_MAX_VALUE"], ascending=[False, False, False]).head(8)
    for _, r in top.iterrows():
        lines.append(f"- {r.get('name') or r['code']}({r['code']}) | {r['HAM_TIME_BUCKET']} {r['HAM_TIME']} | HAM {_fmt(r['HAM_RATIO'],'x')} · {r['HAM_VALUE_EOK']:.1f}억 | 눌림값/HAM {_num(r['PULLBACK_VALUE_RATIO_TO_HAM']):.2f} | restart×pb {_num(r['RESTART_VALUE_VS_PULLBACK']):.2f} | 15:03 day-high {_fmt(r['CLOSE1503_FROM_DAY_HIGH'])} | DIST {int(r['HAM_DIST_WARNING'])}")
    lines.append("- 주의: Yahoo fallback의 거래대금은 close×volume proxy로 명시 저장됩니다. 실제 거래대금 정책검증은 ACTUAL_REPORTED 표본을 우선합니다.")
    return "\n".join(lines)


_DAILY_CACHE: Dict[Tuple[str, str, str], pd.DataFrame] = {}


def _daily(code: str, start: str, end: str) -> pd.DataFrame:
    if fdr is None: return pd.DataFrame()
    key = (_code(code), start, end)
    if key not in _DAILY_CACHE:
        try: _DAILY_CACHE[key] = fdr.DataReader(_code(code), start, end)
        except Exception: _DAILY_CACHE[key] = pd.DataFrame()
    d = _DAILY_CACHE[key].copy()
    if not d.empty: d.index = pd.to_datetime(d.index)
    return d.sort_index()


def evaluate_features(features: pd.DataFrame, hold_days: int = 10) -> pd.DataFrame:
    if features is None or features.empty: return pd.DataFrame()
    rows = []
    for _, r in features.iterrows():
        b = r.to_dict(); code = _code(r.get("code")); td = _date(r.get("trade_date")); entry = _num(r.get("CLOSE1503_PRICE"))
        if not code or td is None or not _finite(entry) or entry <= 0:
            b["eval_status"] = "BAD_KEY"; rows.append(b); continue
        d = _daily(code, (td - pd.Timedelta(days=3)).strftime("%Y-%m-%d"), (td + pd.Timedelta(days=max(30, hold_days*4))).strftime("%Y-%m-%d"))
        fut = d[d.index > td].head(max(hold_days, 5)) if not d.empty else pd.DataFrame()
        if fut.empty:
            b["eval_status"] = "NO_FUTURE_DAILY"; rows.append(b); continue
        def ret(v, ep=entry): return _pct(float(v), ep)
        close = pd.to_numeric(fut.get("Close"), errors="coerce"); high = pd.to_numeric(fut.get("High", close), errors="coerce"); low = pd.to_numeric(fut.get("Low", close), errors="coerce"); op = pd.to_numeric(fut.get("Open", close), errors="coerce")
        b.update({
            "eval_status": "OK", "entry_price_1503": entry,
            "NEXT_DAY_OPEN_RET": ret(op.iloc[0]), "NEXT_DAY_HIGH_RET": ret(high.iloc[0]), "NEXT_DAY_RET": ret(close.iloc[0]),
            "D3_RET": ret(close.iloc[min(2, len(close)-1)]), "D5_RET": ret(close.iloc[min(4, len(close)-1)]),
            "MFE_5D": ret(high.head(5).max()), "MAE_5D": ret(low.head(5).min()), "MFE_10D": ret(high.head(10).max()), "MAE_10D": ret(low.head(10).min()),
        })
        # PATH-FIRST with ambiguity retained rather than guessed.
        p3 = m3 = p5 = m5 = False; amb3 = amb5 = False; first3 = "NONE"; first5 = "NONE"
        for _, z in fut.head(10).iterrows():
            hr = ret(z.get("High", z.get("Close"))); lr = ret(z.get("Low", z.get("Close")))
            if first3 == "NONE":
                if hr >= 3 and lr <= -3: first3 = "AMBIGUOUS"; amb3 = True
                elif hr >= 3: first3 = "+3"
                elif lr <= -3: first3 = "-3"
            if first5 == "NONE":
                if hr >= 5 and lr <= -5: first5 = "AMBIGUOUS"; amb5 = True
                elif hr >= 5: first5 = "+5"
                elif lr <= -5: first5 = "-5"
        p3 = first3 == "+3"; m3 = first3 == "-3"; p5 = first5 == "+5"; m5 = first5 == "-5"
        b.update({"PATH_FIRST_3": first3, "PLUS3_FIRST": p3, "MINUS3_FIRST": m3, "PATH3_AMBIGUOUS": amb3, "PATH_FIRST_5": first5, "PLUS5_FIRST": p5, "MINUS5_FIRST": m5, "PATH5_AMBIGUOUS": amb5})
        for bp in (20, 50):
            ep = entry * (1 + bp / 10000.0)
            b[f"EXEC_D1_{bp}BP"] = _pct(close.iloc[0], ep); b[f"EXEC_D3_{bp}BP"] = _pct(close.iloc[min(2,len(close)-1)], ep); b[f"EXEC_D5_{bp}BP"] = _pct(close.iloc[min(4,len(close)-1)], ep)
        rows.append(b)
    return pd.DataFrame(rows)


def perf(g: pd.DataFrame) -> dict:
    if g is None or g.empty: return {"n": 0}
    q = g[g.get("eval_status", "") == "OK"].copy() if "eval_status" in g.columns else g.copy()
    if q.empty: return {"n": 0}
    r3 = pd.to_numeric(q.get("D3_RET"), errors="coerce")
    return {
        "n": len(q), "days": int(pd.to_datetime(q.get("trade_date"), errors="coerce").dt.normalize().nunique()) if "trade_date" in q.columns else 0,
        "next_day_mean": float(pd.to_numeric(q.get("NEXT_DAY_RET"), errors="coerce").mean()), "d3_mean": float(r3.mean()), "d3_median": float(r3.median()), "d3_trim10": _trim_mean(r3), "d3_ex_top2": _ex_top2_mean(r3), "d5_mean": float(pd.to_numeric(q.get("D5_RET"), errors="coerce").mean()),
        "plus3_first": _rate(q.get("PLUS3_FIRST", pd.Series(dtype=bool))), "minus3_first": _rate(q.get("MINUS3_FIRST", pd.Series(dtype=bool))), "plus5_first": _rate(q.get("PLUS5_FIRST", pd.Series(dtype=bool))), "minus5_first": _rate(q.get("MINUS5_FIRST", pd.Series(dtype=bool))),
        "mfe5": float(pd.to_numeric(q.get("MFE_5D"), errors="coerce").mean()), "mae5": float(pd.to_numeric(q.get("MAE_5D"), errors="coerce").mean()),
        "exec_d1_20": float(pd.to_numeric(q.get("EXEC_D1_20BP"), errors="coerce").mean()), "exec_d3_20": float(pd.to_numeric(q.get("EXEC_D3_20BP"), errors="coerce").mean()), "exec_d3_50": float(pd.to_numeric(q.get("EXEC_D3_50BP"), errors="coerce").mean()),
    }


def _survive_mask(df: pd.DataFrame, mode: str) -> pd.Series:
    mode = str(mode).upper()
    if mode == "LOW": return pd.to_numeric(df.get("SURVIVE_HAM_LOW"), errors="coerce").fillna(0).astype(bool)
    if mode == "MID": return pd.to_numeric(df.get("SURVIVE_HAM_MID"), errors="coerce").fillna(0).astype(bool)
    if mode == "CLOSE": return pd.to_numeric(df.get("SURVIVE_HAM_CLOSE"), errors="coerce").fillna(0).astype(bool)
    a = pd.to_numeric(df.get("SURVIVE_HAM_LOW"), errors="coerce").fillna(0).astype(bool)
    b = pd.to_numeric(df.get("SURVIVE_HAM_MID"), errors="coerce").fillna(0).astype(bool)
    c = pd.to_numeric(df.get("SURVIVE_HAM_CLOSE"), errors="coerce").fillna(0).astype(bool)
    return a | b | c


def policy_masks(df: pd.DataFrame, pol: dict) -> Dict[str, pd.Series]:
    idx = df.index
    ratio = pd.to_numeric(df.get("HAM_RATIO"), errors="coerce"); val = pd.to_numeric(df.get("HAM_VALUE_EOK"), errors="coerce"); dd = pd.to_numeric(df.get("POST_HAM_MAX_DRAWDOWN"), errors="coerce")
    pb = pd.to_numeric(df.get("PULLBACK_VALUE_RATIO_TO_HAM"), errors="coerce"); rr = pd.to_numeric(df.get("RESTART_VALUE_VS_PULLBACK"), errors="coerce"); rp = pd.to_numeric(df.get("RESTART_PRICE_CHANGE"), errors="coerce")
    close_dist = pd.to_numeric(df.get("CLOSE1503_FROM_DAY_HIGH"), errors="coerce")
    ham = (ratio >= float(pol["ham_ratio"])) & (val >= float(pol["ham_value_eok"]))
    survive = ham & _survive_mask(df, pol["survive_mode"]) & (dd >= float(pol["max_drawdown_pct"]))
    dry = survive & (pb < 1.0)
    restart = dry & (rr >= float(pol["restart_value_ratio"])) & (rp > 0)
    close = restart & (close_dist >= float(pol["close_high_dist_pct"])) & pd.to_numeric(df.get("CLOSE1503_RESTART_ALIVE"), errors="coerce").fillna(0).astype(bool)
    return {"HAM_ONLY": ham, "HAM_SURVIVE": survive, "HAM_SURVIVE_RESTART": restart, "HAM_RESTART_CLOSE": close}


def _objective(p: dict) -> float:
    n = int(p.get("n", 0)); days = int(p.get("days", 0))
    if n < int(os.environ.get("HAM_TRAIN_MIN_N", "5")) or days < int(os.environ.get("HAM_TRAIN_MIN_DAYS", "3")):
        return -1e9
    vals = [p.get("d3_median"), p.get("d3_trim10"), p.get("d3_ex_top2")]
    if not any(_finite(v) for v in vals): return -1e9
    robust = sum(float(v) for v in vals if _finite(v)) / max(1, sum(_finite(v) for v in vals))
    path = (_num(p.get("plus3_first"), 0) - _num(p.get("minus3_first"), 0)) / 20.0
    exec_edge = _num(p.get("exec_d3_20"), 0) * 0.25
    # Small complexity/sample bonus only; no live interpretation.
    return robust + path + exec_edge + min(1.0, math.log1p(n) / 5.0)


def _train_split(eval_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    q = eval_df[eval_df.get("eval_status", "") == "OK"].copy()
    if q.empty: return q, q, {"status": "NO_EVAL"}
    q["_d"] = pd.to_datetime(q["trade_date"], errors="coerce").dt.normalize(); q = q.dropna(subset=["_d"])
    lock_end = str(os.environ.get("HAM_TRAIN_END", "")).strip()
    if lock_end:
        end = pd.to_datetime(lock_end, errors="coerce")
        if pd.isna(end): return q.iloc[0:0], q, {"status": "BAD_TRAIN_END"}
    else:
        dates = sorted(q["_d"].unique())
        need = int(os.environ.get("HAM_TRAIN_DAYS", "20"))
        if len(dates) < need:
            return q.copy(), q.iloc[0:0], {"status": "TRAIN_WARMUP", "have_days": len(dates), "need_days": need}
        end = pd.Timestamp(dates[need-1])
    train = q[q["_d"] <= end].copy(); oos = q[q["_d"] > end].copy()
    return train, oos, {"status": "SPLIT_OK", "train_end": end.strftime("%Y-%m-%d"), "train_start": train["_d"].min().strftime("%Y-%m-%d") if not train.empty else "", "train_days": int(train["_d"].nunique()), "oos_days": int(oos["_d"].nunique())}


def _policy_hash(pol: dict) -> str:
    return hashlib.sha256(json.dumps(pol, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def sweep_and_lock(eval_df: pd.DataFrame, output_dir: str | Path) -> Tuple[Optional[dict], pd.DataFrame, dict]:
    outdir = _ensure_dir(output_dir); spec_path = outdir / POLICY_SPEC
    if spec_path.exists() and not _on("HAM_FORCE_RELOCK", "0"):
        try:
            pol = json.loads(spec_path.read_text(encoding="utf-8")); return pol, pd.DataFrame(), {"status": "LOCKED_EXISTING", "policy_hash": pol.get("policy_hash", "")}
        except Exception: pass
    train, oos, split = _train_split(eval_df)
    if split.get("status") != "SPLIT_OK":
        return None, pd.DataFrame(), split
    rows = []
    # Only ACTUAL_REPORTED can select an absolute-value policy by default. Proxy rows remain visible in descriptive reports.
    fit = train.copy()
    if not _on("HAM_ALLOW_PROXY_VALUE_POLICY_LOCK", "0"):
        fit = fit[fit.get("value_quality", "").astype(str).eq("ACTUAL_REPORTED")].copy()
    if fit.empty:
        return None, pd.DataFrame(), {**split, "status": "NO_TRUE_VALUE_TRAIN"}
    # Staged deterministic selection reduces combinatorial overfit while still emitting full local grids.
    # Stage 1: HAM ratio/value.
    best = None
    for ratio, val in itertools.product(RATIO_GRID, VALUE_EOK_GRID):
        pol = {"ham_ratio": ratio, "ham_value_eok": val, "survive_mode": "ANY", "max_drawdown_pct": -5.0, "restart_value_ratio": 1.5, "close_high_dist_pct": -5.0}
        m = policy_masks(fit, pol)["HAM_ONLY"]; pf = perf(fit[m]); score = _objective(pf)
        rows.append({"selection_stage": "HAM", **pol, **pf, "objective": score})
        if best is None or score > best[0]: best = (score, pol.copy())
    if best is None or best[0] <= -1e8:
        pd.DataFrame(rows).to_csv(outdir / SWEEP_SUMMARY, index=False, encoding="utf-8-sig")
        return None, pd.DataFrame(rows), {**split, "status": "INSUFFICIENT_TRAIN_FOR_LOCK"}
    pol = best[1]
    # Stage 2: survive mode + max drawdown on the selected HAM definition.
    best2 = None
    for mode, dd in itertools.product(SURVIVE_MODES, DRAWDOWN_GRID):
        p2 = {**pol, "survive_mode": mode, "max_drawdown_pct": dd}; m = policy_masks(fit, p2)["HAM_SURVIVE"]; pf = perf(fit[m]); sc = _objective(pf)
        rows.append({"selection_stage": "SURVIVE", **p2, **pf, "objective": sc})
        if best2 is None or sc > best2[0]: best2 = (sc, p2.copy())
    if best2 and best2[0] > -1e8: pol = best2[1]
    # Stage 3: restart value ratio.
    best3 = None
    for rr in RESTART_GRID:
        p3 = {**pol, "restart_value_ratio": rr}; m = policy_masks(fit, p3)["HAM_SURVIVE_RESTART"]; pf = perf(fit[m]); sc = _objective(pf)
        rows.append({"selection_stage": "RESTART", **p3, **pf, "objective": sc})
        if best3 is None or sc > best3[0]: best3 = (sc, p3.copy())
    if best3 and best3[0] > -1e8: pol = best3[1]
    # Stage 4: close/high distance.
    best4 = None
    for cd in CLOSE_HIGH_DIST_GRID:
        p4 = {**pol, "close_high_dist_pct": cd}; m = policy_masks(fit, p4)["HAM_RESTART_CLOSE"]; pf = perf(fit[m]); sc = _objective(pf)
        rows.append({"selection_stage": "CLOSE1503", **p4, **pf, "objective": sc})
        if best4 is None or sc > best4[0]: best4 = (sc, p4.copy())
    if best4 and best4[0] > -1e8: pol = best4[1]
    pol.update({
        "policy_id": "HAM_P1_TRAIN_LOCK", "factor_name": FACTOR_NAME, "version": VERSION, "research_only": True,
        "train_start": split["train_start"], "train_end": split["train_end"], "train_days": split["train_days"],
        "selection": "staged robust objective; never re-fit on OOS unless HAM_FORCE_RELOCK=1",
        "value_policy_source": "ACTUAL_REPORTED_ONLY" if not _on("HAM_ALLOW_PROXY_VALUE_POLICY_LOCK", "0") else "ACTUAL_OR_PROXY_EXPLICIT",
    })
    pol["policy_hash"] = _policy_hash(pol)
    spec_path.write_text(json.dumps(pol, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    sw = pd.DataFrame(rows); sw.to_csv(outdir / SWEEP_SUMMARY, index=False, encoding="utf-8-sig")
    pd.DataFrame([{**pol, "lock_created_at_kst": _now().isoformat()}]).to_csv(outdir / POLICY_AUDIT, index=False, encoding="utf-8-sig")
    return pol, sw, {**split, "status": "LOCKED_NEW", "policy_hash": pol["policy_hash"]}


def stage_table(eval_df: pd.DataFrame, policy: Optional[dict] = None, bucket: str = "ALL") -> pd.DataFrame:
    if eval_df is None or eval_df.empty: return pd.DataFrame()
    q = eval_df.copy()
    if policy:
        masks = policy_masks(q, policy)
    else:
        masks = {
            "HAM_ONLY": pd.to_numeric(q.get("HAM_EVENT_CANDIDATE"), errors="coerce").fillna(0).astype(bool),
            "HAM_SURVIVE": pd.to_numeric(q.get("HAM_SURVIVE_SEED"), errors="coerce").fillna(0).astype(bool),
            "HAM_SURVIVE_RESTART": pd.to_numeric(q.get("HAM_SURVIVE_RESTART_SEED"), errors="coerce").fillna(0).astype(bool),
            "HAM_RESTART_CLOSE": pd.to_numeric(q.get("HAM_RESTART_CLOSE_CANDIDATE"), errors="coerce").fillna(0).astype(bool),
        }
    rows = []
    for st in STAGE_ORDER:
        p = perf(q[masks[st]])
        rows.append({"bucket": bucket, "stage": st, **p})
    return pd.DataFrame(rows)


def _mask_existing_restart(eval_df: pd.DataFrame) -> pd.Series:
    cols = [c for c in ["phase", "strategy", "pattern", "search_pattern_matches", "final_decision", "structure_pattern", "source"] if c in eval_df.columns]
    if not cols: return pd.Series(False, index=eval_df.index)
    txt = eval_df[cols].fillna("").astype(str).agg(" ".join, axis=1).str.upper()
    return txt.str.contains("RESTART", regex=False)


def _baseline_perf(eval_df: pd.DataFrame) -> pd.DataFrame:
    if eval_df is None or eval_df.empty: return pd.DataFrame()
    # Existing backtest rows use next1/3/5_close_ret; map a compact comparable view.
    q = eval_df.copy(); ok = q[q.get("eval_status", "") == "OK"].copy() if "eval_status" in q.columns else q
    def bp(g, label):
        if g.empty: return {"group": label, "n": 0}
        return {"group": label, "n": len(g), "next_day_mean": float(pd.to_numeric(g.get("next1_close_ret"), errors="coerce").mean()), "d3_mean": float(pd.to_numeric(g.get("next3_close_ret"), errors="coerce").mean()), "d3_median": float(pd.to_numeric(g.get("next3_close_ret"), errors="coerce").median()), "d5_mean": float(pd.to_numeric(g.get("next5_close_ret"), errors="coerce").mean()), "plus3_first": _rate(g.get("plus3_first_10d", g.get("plus3_first", pd.Series(dtype=bool)))), "minus3_first": _rate(g.get("stop_first_10d", g.get("stop_first", pd.Series(dtype=bool))))}
    rst = _mask_existing_restart(ok)
    return pd.DataFrame([bp(ok, "EXISTING_CLOSEBET_ALL"), bp(ok[rst], "EXISTING_RESTART")])


def _time_summary(eval_df: pd.DataFrame, policy: Optional[dict]) -> pd.DataFrame:
    if eval_df is None or eval_df.empty: return pd.DataFrame()
    m = policy_masks(eval_df, policy)["HAM_ONLY"] if policy else pd.to_numeric(eval_df.get("HAM_EVENT_CANDIDATE"), errors="coerce").fillna(0).astype(bool)
    rows=[]
    for b,g in eval_df[m].groupby("HAM_TIME_BUCKET", dropna=False): rows.append({"HAM_TIME_BUCKET": str(b), **perf(g)})
    return pd.DataFrame(rows)


def _method_summary(eval_df: pd.DataFrame) -> pd.DataFrame:
    if eval_df is None or eval_df.empty: return pd.DataFrame()
    rows=[]
    for c,label in [("HAM_RATIO_A","A_SAME_MINUTE_MEDIAN"),("HAM_RATIO_B","B_TIME_BUCKET_NORMALIZED"),("HAM_RATIO_C","C_ROLLING_30M_MEDIAN")]:
        if c not in eval_df.columns: continue
        for th in RATIO_GRID:
            m = pd.to_numeric(eval_df[c], errors="coerce") >= th
            rows.append({"baseline_method": label, "ratio_threshold": th, **perf(eval_df[m])})
    return pd.DataFrame(rows)


def _dist_summary(eval_df: pd.DataFrame, policy: Optional[dict]) -> pd.DataFrame:
    if eval_df is None or eval_df.empty: return pd.DataFrame()
    m = policy_masks(eval_df, policy)["HAM_ONLY"] if policy else pd.to_numeric(eval_df.get("HAM_EVENT_CANDIDATE"), errors="coerce").fillna(0).astype(bool)
    q = eval_df[m].copy(); rows=[]
    if q.empty: return pd.DataFrame()
    for v,g in q.groupby(pd.to_numeric(q.get("HAM_DIST_WARNING"), errors="coerce").fillna(0).astype(int)):
        rows.append({"HAM_DIST_WARNING": int(v), **perf(g)})
    return pd.DataFrame(rows)


def matched_control(eval_df: pd.DataFrame, policy: dict, output_dir: str | Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    outdir = _ensure_dir(output_dir)
    if eval_df is None or eval_df.empty or not policy:
        return pd.DataFrame(), pd.DataFrame()
    q = eval_df[eval_df.get("eval_status", "") == "OK"].copy()
    if q.empty: return pd.DataFrame(), pd.DataFrame()
    masks = policy_masks(q, policy); treated = q[masks["HAM_RESTART_CLOSE"]].copy(); controls = q[~masks["HAM_ONLY"]].copy()
    if treated.empty or controls.empty: return pd.DataFrame(), pd.DataFrame()
    vars_ = ["DAY_RET_TO_1503", "DAY_VALUE_TO_1503", "CLOSE1503_FROM_DAY_HIGH", "market_cap_eok"]
    for c in vars_: q[c] = pd.to_numeric(q.get(c), errors="coerce")
    pairs=[]; used=set()
    for ti,tr in treated.iterrows():
        td=str(tr.get("trade_date","")); cand=controls[controls["trade_date"].astype(str).eq(td)].copy()
        if cand.empty: continue
        # Prefer same sector then market; relax only if necessary and record it.
        sec=str(tr.get("sector","") or ""); market=str(tr.get("market","") or "")
        level="DATE_ONLY"
        if sec and not cand[cand.get("sector","").astype(str).eq(sec)].empty:
            cand=cand[cand.get("sector","").astype(str).eq(sec)].copy(); level="DATE_SECTOR"
        elif market and not cand[cand.get("market","").astype(str).eq(market)].empty:
            cand=cand[cand.get("market","").astype(str).eq(market)].copy(); level="DATE_MARKET"
        cand=cand[~cand.index.isin(used)].copy()
        if cand.empty: continue
        dist=pd.Series(0.0,index=cand.index)
        for c in vars_:
            tv=_num(tr.get(c)); cv=pd.to_numeric(cand.get(c),errors="coerce")
            if not _finite(tv) or cv.notna().sum()<2: continue
            sd=float(cv.std())
            if not _finite(sd) or sd<=1e-9: sd=max(1.0,abs(float(cv.median()))*0.1)
            if c=="DAY_VALUE_TO_1503" or c=="market_cap_eok":
                tv=math.log1p(max(0.0,tv)); cv=np.log1p(cv.clip(lower=0)); sd=float(cv.std()) or 1.0
            dist += ((cv-tv)/sd).abs().fillna(2.0)
        ci=dist.idxmin(); used.add(ci); cr=cand.loc[ci]
        pairs.append({
            "trade_date":td,"treated_code":tr.get("code"),"control_code":cr.get("code"),"match_level":level,"distance":float(dist.loc[ci]),
            "treated_D1":tr.get("NEXT_DAY_RET"),"control_D1":cr.get("NEXT_DAY_RET"),"diff_D1":_num(tr.get("NEXT_DAY_RET"),0)-_num(cr.get("NEXT_DAY_RET"),0),
            "treated_D3":tr.get("D3_RET"),"control_D3":cr.get("D3_RET"),"diff_D3":_num(tr.get("D3_RET"),0)-_num(cr.get("D3_RET"),0),
            "treated_EXEC_D3_20BP":tr.get("EXEC_D3_20BP"),"control_EXEC_D3_20BP":cr.get("EXEC_D3_20BP"),"diff_EXEC_D3_20BP":_num(tr.get("EXEC_D3_20BP"),0)-_num(cr.get("EXEC_D3_20BP"),0),
            "treated_plus3":bool(tr.get("PLUS3_FIRST",False)),"control_plus3":bool(cr.get("PLUS3_FIRST",False)),
            "sector":sec,"market":market,
        })
    pdf=pd.DataFrame(pairs)
    if pdf.empty: return pdf,pd.DataFrame()
    summ=pd.DataFrame([{"n_pairs":len(pdf),"D1_paired_edge":float(pd.to_numeric(pdf["diff_D1"],errors="coerce").mean()),"D3_paired_edge":float(pd.to_numeric(pdf["diff_D3"],errors="coerce").mean()),"EXEC_D3_20BP_paired_edge":float(pd.to_numeric(pdf["diff_EXEC_D3_20BP"],errors="coerce").mean()),"plus3_treated_rate":float(pdf["treated_plus3"].mean()*100),"plus3_control_rate":float(pdf["control_plus3"].mean()*100),"policy_hash":policy.get("policy_hash","")}])
    pdf.to_csv(outdir/MATCHED_PAIRS,index=False,encoding="utf-8-sig");summ.to_csv(outdir/MATCHED_SUMMARY,index=False,encoding="utf-8-sig")
    return pdf,summ


def run_backtest(existing_eval_df: Optional[pd.DataFrame], output_dir: str | Path = "reports") -> Tuple[str, pd.DataFrame]:
    outdir = _ensure_dir(output_dir)
    fp = outdir / FEATURE_LEDGER
    if not fp.exists():
        _force_empty_outputs(outdir)
        return _empty_report("아직 15:03 HAM 분봉 피처 원장이 없습니다. forward capture 또는 historical minute CSV가 필요합니다."), pd.DataFrame()
    try: features = pd.read_csv(fp, dtype={"code": str})
    except Exception: features = pd.DataFrame()
    if features.empty:
        _force_empty_outputs(outdir); return _empty_report("HAM feature ledger가 비어 있습니다."), pd.DataFrame()
    ev = evaluate_features(features, hold_days=int(os.environ.get("HAM_EVAL_HOLD_DAYS", "10")))
    ev.to_csv(outdir/EVAL_LEDGER,index=False,encoding="utf-8-sig")
    policy, sweep, lock_diag = sweep_and_lock(ev, outdir)
    # Before a policy is locked, descriptive seed stages are still reported but never called OOS alpha.
    all_stage = stage_table(ev, policy=None, bucket="DESCRIPTIVE_SEED")
    tabs=[all_stage]
    train,oos,split=_train_split(ev)
    if policy:
        tabs.append(stage_table(train,policy,"TRAIN_LOCKED"));tabs.append(stage_table(oos,policy,"OOS_LOCKED"))
        masks=policy_masks(ev,policy);ev["HAM_RESTART_CLOSE"] = masks["HAM_RESTART_CLOSE"].astype(int)
    st=pd.concat(tabs,ignore_index=True,sort=False) if tabs else pd.DataFrame();st.to_csv(outdir/STAGE_SUMMARY,index=False,encoding="utf-8-sig")
    if policy:
        oos_tab=stage_table(oos,policy,"OOS_LOCKED");oos_tab.to_csv(outdir/OOS_SUMMARY,index=False,encoding="utf-8-sig")
        pairs,ms=matched_control(oos if not oos.empty else ev.iloc[0:0],policy,outdir)
    else: pairs=ms=pd.DataFrame()
    ttab=_time_summary(ev,policy);ttab.to_csv(outdir/TIME_BUCKET_SUMMARY,index=False,encoding="utf-8-sig")
    mtab=_method_summary(ev);mtab.to_csv(outdir/BASELINE_METHOD_SUMMARY,index=False,encoding="utf-8-sig")
    dtab=_dist_summary(ev,policy);dtab.to_csv(outdir/DIST_SUMMARY,index=False,encoding="utf-8-sig")
    base=_baseline_perf(existing_eval_df if existing_eval_df is not None else pd.DataFrame())
    inc=[]
    if not base.empty:
        for _,r in base.iterrows(): inc.append(r.to_dict())
    if policy:
        for _,r in stage_table(oos,policy,"OOS_LOCKED").iterrows(): inc.append({"group":r["stage"],**r.to_dict()})
    pd.DataFrame(inc).to_csv(outdir/INCREMENTAL_SUMMARY,index=False,encoding="utf-8-sig")
    report = build_backtest_report(ev, existing_eval_df, policy, lock_diag, st, ttab, mtab, dtab, ms)
    return report, ev


def _empty_report(reason: str) -> str:
    return "\n".join([REPORT_HEADER, f"📌 {VERSION} · {FACTOR_NAME} · RESEARCH_ONLY=True", "- LIVE / PRIME / LCZ / M5R / ENVUP / 기존 RESTART 정책 영향 0", f"- {reason}", "- 과거 분봉이 없으면 일봉으로 HAM을 소급 생성하지 않습니다."])


def _force_empty_outputs(outdir: Path) -> None:
    cols={
        EVAL_LEDGER:["trade_date","code","eval_status"], STAGE_SUMMARY:["bucket","stage","n"], SWEEP_SUMMARY:["selection_stage","objective"], POLICY_AUDIT:["policy_id","policy_hash"], OOS_SUMMARY:["bucket","stage","n"], MATCHED_PAIRS:["trade_date","treated_code","control_code"], MATCHED_SUMMARY:["n_pairs"], TIME_BUCKET_SUMMARY:["HAM_TIME_BUCKET","n"], BASELINE_METHOD_SUMMARY:["baseline_method","ratio_threshold","n"], DIST_SUMMARY:["HAM_DIST_WARNING","n"], INCREMENTAL_SUMMARY:["group","n"]}
    for name,c in cols.items():
        p=outdir/name
        if not p.exists(): pd.DataFrame(columns=c).to_csv(p,index=False,encoding="utf-8-sig")


def build_backtest_report(ev: pd.DataFrame, existing_eval_df: Optional[pd.DataFrame], policy: Optional[dict], lock_diag: dict, st: pd.DataFrame, ttab: pd.DataFrame, mtab: pd.DataFrame, dtab: pd.DataFrame, ms: pd.DataFrame) -> str:
    lines=[REPORT_HEADER,f"📌 {VERSION} · {FACTOR_NAME} · RESEARCH_ONLY=True","- 철학: 햄버거 단타 매수법을 도입하지 않고, 장중 주도자금 흔적이 종가까지 생존하는지 보조 팩터로만 연구합니다.","- 격리: LIVE 후보/점수/PRIME/LCZ/M5R/ENVUP/기존 RESTART/실전 Telegram 카드에는 주입하지 않습니다."]
    ok=ev[ev.get("eval_status","")=="OK"] if not ev.empty else pd.DataFrame();actual=int(ev.get("value_quality",pd.Series(dtype=str)).astype(str).eq("ACTUAL_REPORTED").sum()) if not ev.empty else 0
    lines.append(f"📁 표본: feature {len(ev)} · 평가OK {len(ok)} · ACTUAL_REPORTED 거래대금 {actual} | policy {lock_diag.get('status','NO_POLICY')}")
    if policy:
        lines.append(f"🔒 POLICY LOCK: TRAIN {policy.get('train_start')}~{policy.get('train_end')} | HAM ratio≥{policy['ham_ratio']}x · value≥{policy['ham_value_eok']}억 · survive={policy['survive_mode']} · DD≥{policy['max_drawdown_pct']}% · restart≥{policy['restart_value_ratio']}x · 15:03 high거리≥{policy['close_high_dist_pct']}% | hash {policy.get('policy_hash','')[:12]}")
    else:
        lines.append("🔒 POLICY LOCK: 아직 없음. TRAIN 표본/실제 거래대금이 충분해질 때 1회 선택 후 OOS에서는 재튜닝하지 않습니다.")
    # Existing baselines.
    base=_baseline_perf(existing_eval_df if existing_eval_df is not None else pd.DataFrame())
    lines.append("📊 [단계별 incremental edge]")
    if not base.empty:
        for _,r in base.iterrows(): lines.append(f"- {r['group']}: n{int(r['n'])} | D1 {_fmt(r.get('next_day_mean'))} / D3 {_fmt(r.get('d3_mean'))}·중앙 {_fmt(r.get('d3_median'))} | D5 {_fmt(r.get('d5_mean'))} | +3first {_r(r.get('plus3_first'))}")
    if st is None or st.empty: lines.append("- HAM 단계 평가 표본 없음")
    else:
        show=st[st["bucket"].isin(["OOS_LOCKED","DESCRIPTIVE_SEED"])].copy()
        for _,r in show.iterrows(): lines.append(f"- {r['bucket']} · {r['stage']}: n{int(r.get('n',0))} | D1 {_fmt(r.get('next_day_mean'))} | D3 {_fmt(r.get('d3_mean'))}·중앙 {_fmt(r.get('d3_median'))}·절사 {_fmt(r.get('d3_trim10'))}·상2제외 {_fmt(r.get('d3_ex_top2'))} | +3/-3 {_r(r.get('plus3_first'))}/{_r(r.get('minus3_first'))} | MFE/MAE {_fmt(r.get('mfe5'))}/{_fmt(r.get('mae5'))} | exec20 D3 {_fmt(r.get('exec_d3_20'))}")
    lines.append("⏱️ [HAM 시간대]")
    if ttab is None or ttab.empty: lines.append("- 표본 없음")
    else:
        for _,r in ttab.sort_values("n",ascending=False).iterrows(): lines.append(f"- {r['HAM_TIME_BUCKET']}: n{int(r['n'])} | D3 {_fmt(r.get('d3_mean'))}·중앙 {_fmt(r.get('d3_median'))} | +3/-3 {_r(r.get('plus3_first'))}/{_r(r.get('minus3_first'))}")
    lines.append("🧪 [A/B/C 거래대금 baseline 비교]")
    if mtab is None or mtab.empty: lines.append("- 표본 없음")
    else:
        # Empty/partial baseline schemas must render N/A instead of terminating the research block.
        mtab = mtab.copy()
        if "n" not in mtab.columns: mtab["n"] = 0
        if "d3_median" not in mtab.columns: mtab["d3_median"] = np.nan
        if "d3_mean" not in mtab.columns: mtab["d3_mean"] = np.nan
        if "d3_ex_top2" not in mtab.columns: mtab["d3_ex_top2"] = np.nan
        q=mtab[pd.to_numeric(mtab["n"],errors="coerce").fillna(0)>=3].sort_values(["d3_median","n"],ascending=[False,False],na_position="last").head(8)
        if q.empty: lines.append("- n≥3 baseline 셀이 아직 없습니다.")
        for _,r in q.iterrows(): lines.append(f"- {r['baseline_method']} ≥{r['ratio_threshold']}x: n{int(r['n'])} | D3 {_fmt(r.get('d3_mean'))}·중앙 {_fmt(r.get('d3_median'))}·상2제외 {_fmt(r.get('d3_ex_top2'))}")
    lines.append("⚠️ [고점 분배형 HAM]")
    if dtab is None or dtab.empty: lines.append("- 표본 없음")
    else:
        for _,r in dtab.iterrows(): lines.append(f"- DIST_WARNING={int(r['HAM_DIST_WARNING'])}: n{int(r['n'])} | D3 {_fmt(r.get('d3_mean'))}·중앙 {_fmt(r.get('d3_median'))} | +3/-3 {_r(r.get('plus3_first'))}/{_r(r.get('minus3_first'))}")
    lines.append("🎯 [matched-control]")
    if ms is None or ms.empty: lines.append("- 아직 OOS locked HAM_RESTART_CLOSE와 동일일 비-HAM control 쌍이 없습니다.")
    else:
        r=ms.iloc[0];lines.append(f"- {int(r['n_pairs'])}쌍 | D1 paired edge {_fmt(r['D1_paired_edge'])} | D3 {_fmt(r['D3_paired_edge'])} | exec20 D3 {_fmt(r['EXEC_D3_20BP_paired_edge'])} | +3 {r['plus3_treated_rate']:.1f}% vs control {r['plus3_control_rate']:.1f}%")
    lines.extend(["- 최종 질문: HAM 자체 → 생존 → 거래대금 수축 → 재가속 → 15:03 유지가 단계적으로 edge를 추가하는지, 그리고 matched-control에서도 남는지만 봅니다.","- 누수금지: OOS 결과를 보고 policy threshold를 다시 고치지 않습니다. 변경 연구가 필요하면 새 policy ID/새 TRAIN으로 분리합니다.",f"- Actions CSV: {FEATURE_LEDGER} · {EVAL_LEDGER} · {STAGE_SUMMARY} · {SWEEP_SUMMARY} · {POLICY_SPEC} · {OOS_SUMMARY} · {MATCHED_PAIRS} · {MATCHED_SUMMARY} · {TIME_BUCKET_SUMMARY} · {BASELINE_METHOD_SUMMARY}"])
    return "\n".join(lines)


def report_from_existing(existing_eval_df: Optional[pd.DataFrame], output_dir: str | Path = "reports") -> str:
    return run_backtest(existing_eval_df, output_dir)[0]
