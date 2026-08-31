#!/usr/bin/env python3
"""TRIANGLE1PB independent chronology-first research lane.

Research only. No LIVE, ranking, scoring, orders, or CORE224 policy reuse.
The detector is intentionally deterministic and causal:
  TRI_SQUEEZE -> TRI_BREAKOUT_WAVE1 -> TRI_FIRST_PULLBACK
  -> TRI_HEALTHY_PULLBACK -> TRI_RESTART

Forward returns are written only to a separate outcome sidecar after signals are frozen.
Actual Amount is required for amount gates; close*volume is never used as a fallback.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import hashlib
import json
import math
import os
import pickle
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

SCHEMA = "TRIANGLE1PB_RESEARCH_SCHEMA_V1"
STRATEGY_ID = "TRIANGLE1PB_R1_CHRONOLOGY_FIRST"
LOADER_REVISION = "TRIANGLE1PB_R1_15_3_DISCOVERY_WINDOW_LOCK_TELEGRAM_SPLIT"

R2_CANDIDATE_ID = "TRIANGLE1PB_R2C1_QUALIFIED_HEALTHY_RESTART_WAVE_HIGH_RECLAIM"
R2_CANDIDATE_FREEZE_DATE = "2026-08-30"
R2_CANDIDATE_PROSPECTIVE_START_DATE = "2026-08-31"
R2_DISCOVERY_START_DATE = "2024-08-27"
RESEARCH_AUTHORITY = "RESEARCH_ONLY_NO_LIVE_NO_POLICY_NO_ORDERS"
STAGES = [
    "TRI_SQUEEZE",
    "TRI_BREAKOUT_WAVE1",
    "TRI_FIRST_PULLBACK",
    "TRI_HEALTHY_PULLBACK",
    "TRI_RESTART",
]


@dataclass(frozen=True)
class FrozenConfig:
    # These are hypothesis-definition constants, not optimized parameters.
    squeeze_lookback: int = 20
    squeeze_min_consecutive_windows: int = 4
    squeeze_max_contraction_ratio: float = 0.72
    squeeze_min_upper_r2: float = 0.10
    squeeze_min_lower_r2: float = 0.10
    squeeze_min_end_width_pct: float = 0.008
    squeeze_max_end_width_pct: float = 0.14
    breakout_buffer_pct: float = 0.005
    breakout_min_amount20_ratio: float = 1.50
    breakout_wave_max_bars: int = 8
    pullback_min_drawdown_pct: float = 0.02
    pullback_max_drawdown_pct: float = 0.12
    breakout_floor_tolerance_pct: float = 0.015
    healthy_max_breakout_amount_ratio: float = 0.75
    healthy_max_amount20_ratio: float = 1.10
    healthy_wait_max_bars: int = 3
    restart_wait_max_bars: int = 5
    restart_min_pullback_amount_ratio: float = 1.20
    amount20_min_observations: int = 15
    amount20_window: int = 20
    universe_max_calendar_age_days: int = 10
    post_signal_cooldown_bars: int = 10
    forward_horizons: Tuple[int, ...] = (1, 3, 5, 10)


CONFIG = FrozenConfig()

DATE_ALIASES = ["date", "Date", "날짜", "일자", "trading_date", "trade_date", "signal_date", "asof_date"]
CODE_ALIASES = ["code", "Code", "ticker", "symbol", "종목코드", "단축코드", "isu_cd"]
OPEN_ALIASES = ["open", "Open", "시가"]
HIGH_ALIASES = ["high", "High", "고가"]
LOW_ALIASES = ["low", "Low", "저가"]
CLOSE_ALIASES = ["close", "Close", "종가"]
VOLUME_ALIASES = ["volume", "Volume", "거래량"]
# Only columns that explicitly represent traded value/Amount are accepted.
AMOUNT_ALIASES = [
    "amount", "Amount", "거래대금", "거래금액", "trading_value", "trading_amount",
    "trade_value", "value", "acc_trade_value", "accTradePrice",
]


def _first_col(columns: Iterable[str], aliases: Sequence[str]) -> Optional[str]:
    cols = list(columns)
    exact = {str(c): c for c in cols}
    for a in aliases:
        if a in exact:
            return exact[a]
    lower = {str(c).strip().lower(): c for c in cols}
    for a in aliases:
        if a.strip().lower() in lower:
            return lower[a.strip().lower()]
    return None


def _parse_date_value(v: Any) -> Optional[pd.Timestamp]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        t = pd.to_datetime(v, errors="coerce")
        if pd.isna(t):
            s = re.sub(r"\D", "", str(v))
            if len(s) >= 8:
                t = pd.to_datetime(s[:8], format="%Y%m%d", errors="coerce")
        if pd.isna(t):
            return None
        return pd.Timestamp(t).normalize()
    except Exception:
        return None


def _date_from_filename(path: Path) -> Optional[pd.Timestamp]:
    s = path.name
    for pat in (r"(20\d{2}[01]\d[0-3]\d)", r"(20\d{2})[-_.]([01]\d)[-_.]([0-3]\d)"):
        m = re.search(pat, s)
        if m:
            raw = "".join(m.groups()) if len(m.groups()) > 1 else m.group(1)
            t = pd.to_datetime(raw, format="%Y%m%d", errors="coerce")
            if not pd.isna(t):
                return pd.Timestamp(t).normalize()
    return None


def _code_from_filename(path: Path) -> Optional[str]:
    matches = re.findall(r"(?<!\d)(\d{6})(?!\d)", path.name)
    return matches[-1] if matches else None


def _load_any(path: Path) -> Any:
    name = path.name.lower()
    if name.endswith((".pkl", ".pickle", ".pkl.gz", ".pickle.gz")):
        try:
            return pd.read_pickle(path)
        except Exception:
            opener = gzip.open if name.endswith(".gz") else open
            with opener(path, "rb") as f:
                return pickle.load(f)
    if name.endswith((".parquet", ".pq")):
        return pd.read_parquet(path)
    if name.endswith((".csv", ".csv.gz")):
        return pd.read_csv(path)
    if name.endswith((".json", ".json.gz")):
        opener = gzip.open if name.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    # Last-resort probes for cache files without useful extensions.
    for loader in (pd.read_pickle, pd.read_csv):
        try:
            return loader(path)
        except Exception:
            pass
    raise ValueError(f"unsupported cache file: {path}")



def _iter_frame_candidates(obj: Any, max_depth: int = 4) -> Iterable[Tuple[pd.DataFrame, Dict[str, Any]]]:
    """Yield DataFrame candidates from cache containers without inventing data.

    Existing Stock-Hunter caches have changed shape over time (raw DataFrame,
    dict-wrapped DataFrame, records list, etc.).  This adapter only unwraps
    explicit stored structures; it never derives OHLC/Amount values.
    """
    seen: set[int] = set()

    def walk(x: Any, meta: Dict[str, Any], depth: int):
        if depth > max_depth or x is None:
            return
        oid = id(x)
        if oid in seen:
            return
        seen.add(oid)
        if isinstance(x, pd.DataFrame):
            yield x, dict(meta)
            return
        if isinstance(x, dict):
            child_meta = dict(meta)
            for k in ("code", "Code", "ticker", "symbol", "종목코드", "단축코드", "isu_cd"):
                if k in x and not isinstance(x[k], (dict, list, tuple, pd.DataFrame)):
                    child_meta.setdefault("code", x[k])
                    break
            for k in ("date", "Date", "날짜", "일자", "signal_date", "asof_date", "snapshot_date"):
                if k in x and not isinstance(x[k], (dict, list, tuple, pd.DataFrame)):
                    child_meta.setdefault("date", x[k])
                    break
            preferred = (
                "df", "frame", "dataframe", "price", "prices", "history", "price_history",
                "ohlcv", "bars", "data", "rows", "items", "payload",
            )
            yielded = set()
            for k in preferred:
                if k in x:
                    yielded.add(k)
                    yield from walk(x[k], child_meta, depth + 1)
            for k, v in x.items():
                if k in yielded:
                    continue
                if isinstance(v, (pd.DataFrame, dict, list, tuple)):
                    yield from walk(v, child_meta, depth + 1)
            return
        if isinstance(x, (list, tuple)):
            if x and all(isinstance(v, dict) for v in x):
                try:
                    df = pd.DataFrame(x)
                    if not df.empty:
                        yield df, dict(meta)
                except Exception:
                    pass
            for v in x:
                if isinstance(v, (pd.DataFrame, dict, list, tuple)):
                    yield from walk(v, meta, depth + 1)

    yield from walk(obj, {}, 0)


def _date_series_from_frame(df: pd.DataFrame) -> Tuple[Optional[pd.Series], str]:
    dcol = _first_col(df.columns, DATE_ALIASES)
    if dcol:
        return pd.Series(pd.to_datetime(df[dcol], errors="coerce"), index=df.index), f"COLUMN:{dcol}"
    idx = df.index
    if isinstance(idx, pd.DatetimeIndex):
        return pd.Series(pd.to_datetime(idx, errors="coerce"), index=df.index), "DATETIME_INDEX"
    # Some historical caches persisted YYYYMMDD / YYYY-MM-DD as the index.
    if len(idx):
        sample = pd.Series(idx[: min(20, len(idx))]).astype(str)
        parseable = pd.to_datetime(sample, errors="coerce").notna().mean()
        if parseable >= 0.8:
            vals = pd.to_datetime(pd.Series(idx.astype(str), index=df.index), errors="coerce")
            return vals, "PARSEABLE_INDEX"
    return None, "MISSING"


def _frame_code(df: pd.DataFrame, path: Path, meta: Optional[Dict[str, Any]] = None) -> str:
    code = _code_from_filename(path) or ""
    if not code and meta:
        code = _normalize_code(meta.get("code"))
    ccol = _first_col(df.columns, CODE_ALIASES)
    if not code and ccol and df[ccol].notna().any():
        code = _normalize_code(df.loc[df[ccol].notna(), ccol].iloc[0])
    return code


def _normalize_code(v: Any) -> str:
    if v is None:
        return ""
    s = re.sub(r"\D", "", str(v))
    return s[-6:].zfill(6) if s else ""


def _extract_codes(obj: Any, path: Path) -> List[str]:
    out: List[str] = []
    if isinstance(obj, pd.DataFrame):
        c = _first_col(obj.columns, CODE_ALIASES)
        if c:
            out = [_normalize_code(v) for v in obj[c].tolist()]
    elif isinstance(obj, dict):
        for key in ("codes", "universe", "tickers", "symbols", "top500", "rows", "data", "items"):
            if key in obj:
                out = _extract_codes(obj[key], path)
                if out:
                    break
        if not out:
            # Some snapshots are dicts keyed directly by code.
            maybe = [_normalize_code(k) for k in obj.keys()]
            maybe = [x for x in maybe if len(x) == 6 and x != "000000"]
            if len(maybe) >= 10:
                out = maybe
    elif isinstance(obj, (list, tuple, set)):
        for x in obj:
            if isinstance(x, dict):
                for k in CODE_ALIASES:
                    if k in x:
                        out.append(_normalize_code(x[k]))
                        break
            else:
                c = _normalize_code(x)
                if c:
                    out.append(c)
    return sorted(set(x for x in out if len(x) == 6 and x != "000000"))


def _extract_snapshot_date(obj: Any, path: Path) -> Optional[pd.Timestamp]:
    if isinstance(obj, pd.DataFrame):
        c = _first_col(obj.columns, DATE_ALIASES)
        if c and len(obj):
            vals = [_parse_date_value(x) for x in obj[c].dropna().head(20)]
            vals = [x for x in vals if x is not None]
            if vals and len(set(vals)) == 1:
                return vals[0]
    if isinstance(obj, dict):
        for k in DATE_ALIASES + ["snapshot_date", "end_date", "as_of"]:
            if k in obj:
                t = _parse_date_value(obj[k])
                if t is not None:
                    return t
    return _date_from_filename(path)


class UniverseAuthority:
    def __init__(self, root: Path, max_age_days: int):
        self.root = root
        self.max_age_days = max_age_days
        self.snapshots: Dict[pd.Timestamp, set[str]] = {}
        self.files_loaded = 0
        self.files_failed = 0
        self._load()
        self.dates = sorted(self.snapshots)

    def _load(self) -> None:
        if not self.root.exists():
            return
        for p in sorted(x for x in self.root.rglob("*") if x.is_file()):
            try:
                obj = _load_any(p)
                d = _extract_snapshot_date(obj, p)
                codes = _extract_codes(obj, p)
                if d is None or not codes:
                    self.files_failed += 1
                    continue
                self.snapshots.setdefault(d, set()).update(codes)
                self.files_loaded += 1
            except Exception:
                self.files_failed += 1

    def lookup(self, event_date: pd.Timestamp, code: str) -> Tuple[bool, str, int, str]:
        if not self.dates:
            return False, "", 999999, "ASOF_UNIVERSE_MISSING"
        i = bisect.bisect_right(self.dates, event_date) - 1
        if i < 0:
            return False, "", 999999, "ASOF_UNIVERSE_NO_CAUSAL_SNAPSHOT"
        d = self.dates[i]
        age = int((event_date - d).days)
        if age < 0:
            return False, d.date().isoformat(), age, "ASOF_UNIVERSE_FUTURE_SNAPSHOT_FORBIDDEN"
        if age > self.max_age_days:
            return False, d.date().isoformat(), age, "ASOF_UNIVERSE_STALE"
        ok = code in self.snapshots[d]
        return ok, d.date().isoformat(), age, "PASS" if ok else "CODE_NOT_IN_ASOF_UNIVERSE"


class AmountAuthority:
    """Explicit historical traded-value authority.

    Priority:
      1) dedicated v25_actual_amount_history cache when present;
      2) explicit Amount/traded-value fields contained in causal historical
         as-of snapshots.

    Close*Volume is deliberately never used.
    """
    def __init__(self, root: Path, asof_root: Optional[Path] = None):
        self.root = root
        self.asof_root = asof_root
        self.index: Dict[str, List[Path]] = {}
        self._cache: Dict[str, pd.DataFrame] = {}
        self.asof_index: Dict[str, List[Tuple[pd.Timestamp, float]]] = {}
        self.external_files = 0
        self.asof_files_scanned = 0
        self.asof_amount_rows = 0
        if root.exists():
            for p in sorted(x for x in root.rglob("*") if x.is_file()):
                code = _code_from_filename(p)
                if code:
                    self.index.setdefault(code, []).append(p)
                    self.external_files += 1
        if asof_root is not None and asof_root.exists():
            self._load_asof_amounts(asof_root)

    def _load_asof_amounts(self, root: Path) -> None:
        for p in sorted(x for x in root.rglob("*") if x.is_file()):
            try:
                obj = _load_any(p)
            except Exception:
                continue
            self.asof_files_scanned += 1
            snap_date = _extract_snapshot_date(obj, p)
            for df, meta in _iter_frame_candidates(obj):
                if df.empty:
                    continue
                ccol = _first_col(df.columns, CODE_ALIASES)
                acol = _first_col(df.columns, AMOUNT_ALIASES)
                if not ccol or not acol:
                    continue
                dates, _ = _date_series_from_frame(df)
                if dates is None:
                    if snap_date is None:
                        continue
                    dates = pd.Series([snap_date] * len(df), index=df.index)
                codes = df[ccol].map(_normalize_code)
                amounts = pd.to_numeric(df[acol], errors="coerce")
                for d, c, a in zip(pd.to_datetime(dates, errors="coerce"), codes, amounts):
                    if pd.isna(d) or not c or pd.isna(a) or float(a) <= 0:
                        continue
                    self.asof_index.setdefault(c, []).append((pd.Timestamp(d).normalize(), float(a)))
                    self.asof_amount_rows += 1
        for code, vals in list(self.asof_index.items()):
            # Same-day later cache files are allowed to replace earlier copies,
            # but no future value is ever backfilled to an earlier date.
            by_date: Dict[pd.Timestamp, float] = {}
            for d, a in vals:
                by_date[d] = a
            self.asof_index[code] = sorted(by_date.items())

    def for_code(self, code: str) -> pd.DataFrame:
        if code in self._cache:
            return self._cache[code]
        frames = []
        for p in self.index.get(code, []):
            try:
                obj = _load_any(p)
                for df, meta in _iter_frame_candidates(obj):
                    dser, _ = _date_series_from_frame(df)
                    acol = _first_col(df.columns, AMOUNT_ALIASES)
                    if dser is None or not acol:
                        continue
                    x = pd.DataFrame({
                        "date": pd.to_datetime(dser, errors="coerce").dt.normalize(),
                        "amount_external": pd.to_numeric(df[acol], errors="coerce"),
                    })
                    x = x.dropna(subset=["date"]).drop_duplicates("date", keep="last")
                    if not x.empty:
                        frames.append(x)
            except Exception:
                pass
        if frames:
            out = pd.concat(frames, ignore_index=True).sort_values("date").drop_duplicates("date", keep="last")
        else:
            vals = self.asof_index.get(code, [])
            out = pd.DataFrame(vals, columns=["date", "amount_external"]) if vals else pd.DataFrame(columns=["date", "amount_external"])
        self._cache[code] = out
        return out



def normalize_price_frame(obj: Any, path: Path, amount_auth: AmountAuthority) -> Optional[Tuple[str, pd.DataFrame, str, str]]:
    best: Optional[Tuple[str, pd.DataFrame, str, str]] = None
    best_rows = -1
    for raw, meta in _iter_frame_candidates(obj):
        if raw.empty:
            continue
        code = _frame_code(raw, path, meta)
        if not code:
            continue
        dser, date_source = _date_series_from_frame(raw)
        ocol = _first_col(raw.columns, OPEN_ALIASES)
        hcol = _first_col(raw.columns, HIGH_ALIASES)
        lcol = _first_col(raw.columns, LOW_ALIASES)
        cclose = _first_col(raw.columns, CLOSE_ALIASES)
        vcol = _first_col(raw.columns, VOLUME_ALIASES)
        acol = _first_col(raw.columns, AMOUNT_ALIASES)
        if dser is None or not all((ocol, hcol, lcol, cclose)):
            continue

        x = pd.DataFrame({
            "date": pd.to_datetime(dser, errors="coerce").dt.normalize(),
            "open": pd.to_numeric(raw[ocol], errors="coerce"),
            "high": pd.to_numeric(raw[hcol], errors="coerce"),
            "low": pd.to_numeric(raw[lcol], errors="coerce"),
            "close": pd.to_numeric(raw[cclose], errors="coerce"),
            "volume": pd.to_numeric(raw[vcol], errors="coerce") if vcol else np.nan,
            "amount_price": pd.to_numeric(raw[acol], errors="coerce") if acol else np.nan,
        })
        x = x.dropna(subset=["date", "open", "high", "low", "close"])
        x = x[(x["open"] > 0) & (x["high"] > 0) & (x["low"] > 0) & (x["close"] > 0)]
        x = x.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
        if x.empty:
            continue

        ext = amount_auth.for_code(code)
        source = "MISSING"
        if not ext.empty:
            x = x.merge(ext, on="date", how="left")
            x["amount"] = x["amount_external"]
            source = "V25_OR_ASOF_EXPLICIT_ACTUAL_AMOUNT"
            if acol:
                m = x["amount"].isna() & x["amount_price"].notna()
                x.loc[m, "amount"] = x.loc[m, "amount_price"]
                if m.any():
                    source = "MIXED_EXTERNAL_ASOF_THEN_PRICE_ACTUAL_AMOUNT"
        else:
            x["amount"] = x["amount_price"] if acol else np.nan
            source = "PRICE_CACHE_ACTUAL_AMOUNT" if acol else "MISSING"

        candidate = (code, x, source, date_source)
        if len(x) > best_rows:
            best = candidate
            best_rows = len(x)
    return best



def r2_linear(y: np.ndarray) -> float:
    if len(y) < 2:
        return 0.0
    x = np.arange(len(y), dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    pred = slope * x + intercept
    ss_res = float(np.sum((y - pred) ** 2))
    ss_tot = float(np.sum((y - float(np.mean(y))) ** 2))
    return 1.0 if ss_tot <= 1e-12 and ss_res <= 1e-12 else (0.0 if ss_tot <= 1e-12 else max(-1.0, 1.0 - ss_res / ss_tot))


def squeeze_geometry(window: pd.DataFrame, cfg: FrozenConfig) -> Dict[str, Any]:
    if len(window) != cfg.squeeze_lookback:
        return {"qualifies": False}
    hi = window["high"].to_numpy(dtype=float)
    lo = window["low"].to_numpy(dtype=float)
    cl = window["close"].to_numpy(dtype=float)
    if not np.isfinite(hi).all() or not np.isfinite(lo).all() or not np.isfinite(cl).all():
        return {"qualifies": False}
    x = np.arange(len(window), dtype=float)
    hs, hi0 = np.polyfit(x, hi, 1)
    ls, lo0 = np.polyfit(x, lo, 1)
    mid = float(np.median(cl))
    if mid <= 0:
        return {"qualifies": False}
    upper_start = float(hi0)
    lower_start = float(lo0)
    upper_end = float(hs * (len(window) - 1) + hi0)
    lower_end = float(ls * (len(window) - 1) + lo0)
    width_start = (upper_start - lower_start) / mid
    width_end = (upper_end - lower_end) / mid
    contraction = width_end / width_start if width_start > 1e-9 else np.inf
    upper_r2 = r2_linear(hi)
    lower_r2 = r2_linear(lo)
    gate_upper_falling = bool(hs < 0)
    gate_lower_rising = bool(ls > 0)
    gate_width_order = bool(upper_start > lower_start and upper_end > lower_end)
    gate_contraction = bool(0 < contraction <= cfg.squeeze_max_contraction_ratio)
    gate_r2 = bool(upper_r2 >= cfg.squeeze_min_upper_r2 and lower_r2 >= cfg.squeeze_min_lower_r2)
    gate_end_width = bool(cfg.squeeze_min_end_width_pct <= width_end <= cfg.squeeze_max_end_width_pct)
    qualifies = bool(
        gate_upper_falling and gate_lower_rising and gate_width_order and
        gate_contraction and gate_r2 and gate_end_width
    )
    projected_upper_next = float(hs * len(window) + hi0)
    projected_lower_next = float(ls * len(window) + lo0)
    return {
        "qualifies": qualifies,
        "gate_upper_falling": gate_upper_falling,
        "gate_lower_rising": gate_lower_rising,
        "gate_width_order": gate_width_order,
        "gate_contraction": gate_contraction,
        "gate_r2": gate_r2,
        "gate_end_width": gate_end_width,
        "upper_slope_pct_per_bar": hs / mid,
        "lower_slope_pct_per_bar": ls / mid,
        "upper_slope_price_per_bar": hs,
        "lower_slope_price_per_bar": ls,
        "upper_r2": upper_r2,
        "lower_r2": lower_r2,
        "width_start_pct": width_start,
        "width_end_pct": width_end,
        "contraction_ratio": contraction,
        "projected_upper_next": projected_upper_next,
        "projected_lower_next": projected_lower_next,
        "feature_end_date": window["date"].iloc[-1].date().isoformat(),
    }


def amount20_stats(df: pd.DataFrame, i: int, cfg: FrozenConfig) -> Tuple[float, int]:
    a = pd.to_numeric(df.iloc[max(0, i - cfg.amount20_window):i]["amount"], errors="coerce")
    a = a[(a > 0) & np.isfinite(a)]
    return (float(a.mean()), int(len(a))) if len(a) else (float("nan"), 0)


def _episode_id(code: str, breakout_date: pd.Timestamp) -> str:
    return hashlib.sha1(f"{STRATEGY_ID}|{code}|{breakout_date.date().isoformat()}".encode()).hexdigest()[:16]


def _event_id(episode_id: str, stage: str, event_date: pd.Timestamp) -> str:
    return hashlib.sha1(f"{episode_id}|{stage}|{event_date.date().isoformat()}".encode()).hexdigest()[:18]


def _finite(v: Any) -> Any:
    if isinstance(v, (float, np.floating)) and not math.isfinite(float(v)):
        return ""
    return v



def _audit_num(v: Any) -> float:
    try:
        x = float(v)
    except Exception:
        return float("nan")
    return x if math.isfinite(x) else float("nan")


def _audit_ratio(a: Any, b: Any) -> float:
    aa = _audit_num(a)
    bb = _audit_num(b)
    if not (math.isfinite(aa) and math.isfinite(bb)) or bb == 0:
        return float("nan")
    return float(aa / bb)


def _audit_close_location_pct(row: pd.Series) -> float:
    hi = _audit_num(row.get("high"))
    lo = _audit_num(row.get("low"))
    cl = _audit_num(row.get("close"))
    if not (math.isfinite(hi) and math.isfinite(lo) and math.isfinite(cl)) or hi <= lo:
        return float("nan")
    return float((cl - lo) / (hi - lo) * 100.0)


def _audit_upper_wick_pct(row: pd.Series) -> float:
    hi = _audit_num(row.get("high"))
    op = _audit_num(row.get("open"))
    cl = _audit_num(row.get("close"))
    if not (math.isfinite(hi) and math.isfinite(op) and math.isfinite(cl)) or cl <= 0:
        return float("nan")
    return float(max(0.0, hi - max(op, cl)) / cl * 100.0)


def _audit_obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    c = pd.to_numeric(close, errors="coerce")
    v = pd.to_numeric(volume, errors="coerce").fillna(0.0)
    direction = np.sign(c.diff().fillna(0.0))
    return (direction * v).cumsum()


def _audit_bb40_width_pct(close: pd.Series) -> pd.Series:
    c = pd.to_numeric(close, errors="coerce")
    ma = c.rolling(40, min_periods=40).mean()
    sd = c.rolling(40, min_periods=40).std(ddof=0)
    return (4.0 * sd / ma.replace(0, np.nan) * 100.0)


def build_structure_fidelity_row(
    code: str,
    df: pd.DataFrame,
    squeeze_end_idx: int,
    geom: Dict[str, Any],
) -> Dict[str, Any]:
    """Descriptive audit only; never used by the TRIANGLE1PB state machine."""
    row = df.iloc[squeeze_end_idx]
    squeeze_date = pd.Timestamp(row["date"])
    pre60 = df.iloc[max(0, squeeze_end_idx - 59):squeeze_end_idx + 1].copy()
    post15 = df.iloc[squeeze_end_idx + 1:min(len(df), squeeze_end_idx + 16)].copy()

    vol = pd.to_numeric(pre60["volume"], errors="coerce") if "volume" in pre60 else pd.Series(dtype=float)
    amt = pd.to_numeric(pre60["amount"], errors="coerce") if "amount" in pre60 else pd.Series(dtype=float)
    close = pd.to_numeric(pre60["close"], errors="coerce")

    vma20 = vol.rolling(20, min_periods=10).mean() if len(vol) else pd.Series(dtype=float)
    ama20 = amt.rolling(20, min_periods=10).mean() if len(amt) else pd.Series(dtype=float)
    vr = vol / vma20.replace(0, np.nan) if len(vol) else pd.Series(dtype=float)
    ar = amt / ama20.replace(0, np.nan) if len(amt) else pd.Series(dtype=float)

    vol_spike_15 = int((vr >= 1.5).fillna(False).sum()) if len(vr) else 0
    vol_spike_20 = int((vr >= 2.0).fillna(False).sum()) if len(vr) else 0
    amt_spike_15 = int((ar >= 1.5).fillna(False).sum()) if len(ar) else 0
    amt_spike_20 = int((ar >= 2.0).fillna(False).sum()) if len(ar) else 0

    # Legacy "accumulation bar" observation tag only:
    # Volume >= 2x its prior/rolling context + upper wick >= 3%.
    legacy_accum = 0
    for j in range(len(pre60)):
        rr = pre60.iloc[j]
        vratio = _audit_num(vr.iloc[j]) if j < len(vr) else float("nan")
        wick = _audit_upper_wick_pct(rr)
        if math.isfinite(vratio) and vratio >= 2.0 and math.isfinite(wick) and wick >= 3.0:
            legacy_accum += 1

    obv_change10 = float("nan")
    if len(pre60) >= 12 and "volume" in pre60:
        obv = _audit_obv(pre60["close"], pre60["volume"])
        a0 = _audit_num(obv.iloc[-11])
        a1 = _audit_num(obv.iloc[-1])
        if math.isfinite(a0) and math.isfinite(a1):
            obv_change10 = float((a1 - a0) / max(abs(a0), 1.0))

    bbw = _audit_bb40_width_pct(close)
    bb_now = _audit_num(bbw.iloc[-1]) if len(bbw) else float("nan")
    bb_10ago = _audit_num(bbw.iloc[-11]) if len(bbw) >= 11 else float("nan")
    bb_ratio10 = _audit_ratio(bb_now, bb_10ago)

    sq_close = _audit_num(row.get("close"))
    upper_next = _audit_num(geom.get("projected_upper_next"))

    # Post-event look-forward audit only. It is explicitly excluded from all gates.
    wave1_found = 0
    wave1_abs_idx = -1
    wave1_date = ""
    wave1_close_ret_pct = float("nan")
    wave1_high_ret_pct = float("nan")
    wave1_volume20_ratio = float("nan")
    wave1_amount20_ratio = float("nan")
    wave1_close_loc_pct = float("nan")
    max_high_ret15 = float("nan")

    if len(post15) and math.isfinite(sq_close) and sq_close > 0:
        ph = pd.to_numeric(post15["high"], errors="coerce")
        if ph.notna().any():
            max_high_ret15 = float((ph.max() / sq_close - 1.0) * 100.0)

        for off in range(1, len(post15) + 1):
            rr = df.iloc[squeeze_end_idx + off]
            cl = _audit_num(rr.get("close"))
            hi = _audit_num(rr.get("high"))
            price_cross = math.isfinite(upper_next) and math.isfinite(cl) and cl > upper_next
            impulse5 = math.isfinite(cl) and cl >= sq_close * 1.05
            if not (price_cross or impulse5):
                continue
            wave1_found = 1
            wave1_abs_idx = squeeze_end_idx + off
            wave1_date = str(pd.Timestamp(rr["date"]).date())
            wave1_close_ret_pct = float((cl / sq_close - 1.0) * 100.0) if cl > 0 else float("nan")
            wave1_high_ret_pct = float((hi / sq_close - 1.0) * 100.0) if hi > 0 else float("nan")
            wave1_close_loc_pct = _audit_close_location_pct(rr)
            prior20 = df.iloc[max(0, wave1_abs_idx - 20):wave1_abs_idx]
            vavg = pd.to_numeric(prior20["volume"], errors="coerce").mean() if "volume" in prior20 else float("nan")
            aavg = pd.to_numeric(prior20["amount"], errors="coerce").mean() if "amount" in prior20 else float("nan")
            wave1_volume20_ratio = _audit_ratio(rr.get("volume"), vavg)
            wave1_amount20_ratio = _audit_ratio(rr.get("amount"), aavg)
            break

    pullback_found = 0
    pullback_date = ""
    pullback_dd_pct = float("nan")
    pullback_vol_vs_wave1 = float("nan")
    pullback_amt_vs_wave1 = float("nan")
    pullback_support_sq_close = 0
    restart_after_pullback = 0

    if wave1_found and wave1_abs_idx >= 0:
        wave1_row = df.iloc[wave1_abs_idx]
        wave1_vol = _audit_num(wave1_row.get("volume"))
        wave1_amt = _audit_num(wave1_row.get("amount"))
        wave_high = _audit_num(wave1_row.get("high"))
        pb_idx = -1

        for k in range(wave1_abs_idx + 1, min(len(df), wave1_abs_idx + 9)):
            rr = df.iloc[k]
            wave_high = max(wave_high, _audit_num(rr.get("high")))
            cl = _audit_num(rr.get("close"))
            prev_cl = _audit_num(df.iloc[k - 1].get("close"))
            dd = float((cl / wave_high - 1.0) * 100.0) if wave_high > 0 and cl > 0 else float("nan")
            if math.isfinite(dd) and dd <= -2.0 and cl < prev_cl:
                pullback_found = 1
                pb_idx = k
                pullback_date = str(pd.Timestamp(rr["date"]).date())
                pullback_dd_pct = dd
                pullback_vol_vs_wave1 = _audit_ratio(rr.get("volume"), wave1_vol)
                pullback_amt_vs_wave1 = _audit_ratio(rr.get("amount"), wave1_amt)
                pullback_support_sq_close = int(math.isfinite(cl) and cl >= sq_close)
                break

        if pb_idx >= 0:
            for k in range(pb_idx + 1, min(len(df), pb_idx + 6)):
                rr = df.iloc[k]
                prev = df.iloc[k - 1]
                if (
                    _audit_num(rr.get("close")) > _audit_num(prev.get("high"))
                    and _audit_num(rr.get("close")) > _audit_num(rr.get("open"))
                ):
                    restart_after_pullback = 1
                    break

    r2_min = min(_audit_num(geom.get("upper_r2")), _audit_num(geom.get("lower_r2")))
    contraction = _audit_num(geom.get("contraction_ratio"))
    width_end = _audit_num(geom.get("width_end_pct"))
    shape_score = 0.0
    if math.isfinite(r2_min):
        shape_score += max(0.0, min(1.0, r2_min)) * 40.0
    if math.isfinite(contraction):
        shape_score += max(0.0, min(1.0, 1.0 - contraction)) * 35.0
    if math.isfinite(width_end):
        shape_score += max(0.0, 1.0 - abs(width_end - 0.06) / 0.06) * 25.0

    return {
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "audit_version": "TRIANGLE1PB_R1_3_STRUCTURE_FIDELITY_AUDIT_1",
        "audit_role": "DESCRIPTIVE_ONLY_NOT_A_GATE",
        "post_event_fields_use_future_data": 1,
        "used_as_strategy_gate": 0,
        "legacy_accumulation_bar_used_as_gate": 0,
        "code": code,
        "squeeze_date": str(squeeze_date.date()),
        "squeeze_close": _audit_num(row.get("close")),
        "projected_upper_next": _audit_num(geom.get("projected_upper_next")),
        "projected_lower_next": _audit_num(geom.get("projected_lower_next")),
        "upper_slope_price_per_bar": _audit_num(geom.get("upper_slope_price_per_bar")),
        "lower_slope_price_per_bar": _audit_num(geom.get("lower_slope_price_per_bar")),
        "shape_score": round(shape_score, 6),
        "upper_r2": _audit_num(geom.get("upper_r2")),
        "lower_r2": _audit_num(geom.get("lower_r2")),
        "contraction_ratio": contraction,
        "width_end_pct": width_end,
        "upper_slope_pct_per_bar": _audit_num(geom.get("upper_slope_pct_per_bar")),
        "lower_slope_pct_per_bar": _audit_num(geom.get("lower_slope_pct_per_bar")),
        "pre60_volume_spike_1p5_count": vol_spike_15,
        "pre60_volume_spike_2p0_count": vol_spike_20,
        "pre60_amount_spike_1p5_count": amt_spike_15,
        "pre60_amount_spike_2p0_count": amt_spike_20,
        "pre60_legacy_accumulation_bar_count": int(legacy_accum),
        "pre10_obv_relative_change": obv_change10,
        "bb40_width_pct_at_squeeze": bb_now,
        "bb40_width_10bars_ago_pct": bb_10ago,
        "bb40_contraction_ratio_10bar": bb_ratio10,
        "post15_wave1_found": int(wave1_found),
        "wave1_date": wave1_date,
        "wave1_close_ret_pct": wave1_close_ret_pct,
        "wave1_high_ret_pct": wave1_high_ret_pct,
        "wave1_volume20_ratio": wave1_volume20_ratio,
        "wave1_amount20_ratio": wave1_amount20_ratio,
        "wave1_close_location_pct": wave1_close_loc_pct,
        "post15_max_high_ret_pct": max_high_ret15,
        "first_pullback_found": int(pullback_found),
        "first_pullback_date": pullback_date,
        "pullback_from_wave_high_pct": pullback_dd_pct,
        "pullback_volume_vs_wave1": pullback_vol_vs_wave1,
        "pullback_amount_vs_wave1": pullback_amt_vs_wave1,
        "pullback_support_above_squeeze_close": int(pullback_support_sq_close),
        "restart_after_pullback_found": int(restart_after_pullback),
    }


def build_structure_manual_review_sample(audit: pd.DataFrame, n_each: int = 8) -> pd.DataFrame:
    if audit is None or audit.empty:
        return pd.DataFrame()
    parts: List[pd.DataFrame] = []

    def add(bucket: str, frame: pd.DataFrame) -> None:
        if frame is None or frame.empty:
            return
        x = frame.head(n_each).copy()
        x.insert(0, "review_bucket", bucket)
        parts.append(x)

    add("SHAPE_ONLY_TOP", audit.sort_values(["shape_score","code","squeeze_date"], ascending=[False,True,True]))

    x = audit.copy()
    x["_pre_energy"] = (
        pd.to_numeric(x["pre60_volume_spike_2p0_count"], errors="coerce").fillna(0) * 2.0
        + pd.to_numeric(x["pre60_amount_spike_2p0_count"], errors="coerce").fillna(0) * 2.0
        + pd.to_numeric(x["pre60_legacy_accumulation_bar_count"], errors="coerce").fillna(0)
        + pd.to_numeric(x["pre10_obv_relative_change"], errors="coerce").fillna(0).clip(lower=0) * 5.0
    )
    add("PRE_ENERGY_TOP", x.sort_values(["_pre_energy","shape_score"], ascending=[False,False]))

    post = audit[pd.to_numeric(audit["post15_wave1_found"], errors="coerce").fillna(0).astype(int).eq(1)].copy()
    add("POST_WAVE1_AUDIT", post.sort_values(["wave1_high_ret_pct","shape_score"], ascending=[False,False]))

    broad = audit.sort_values(["code","squeeze_date"]).copy()
    if not broad.empty:
        step = max(1, len(broad) // n_each)
        add("DETERMINISTIC_BROAD", broad.iloc[::step])

    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True, sort=False)
    if "_pre_energy" in out.columns:
        out = out.drop(columns=["_pre_energy"])
    out = out.drop_duplicates(["review_bucket","code","squeeze_date"]).reset_index(drop=True)
    out["manual_triangle_shape"] = "UNREVIEWED"
    out["manual_pre_energy_accumulation"] = "UNREVIEWED"
    out["manual_wave1"] = "UNREVIEWED"
    out["manual_first_pullback"] = "UNREVIEWED"
    out["manual_notes"] = ""
    return out


def build_structure_review_bars(
    sample: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
    pre_bars: int = 60,
    post_bars: int = 20,
) -> pd.DataFrame:
    if sample is None or sample.empty:
        return pd.DataFrame()
    chunks = []
    for _, sr in sample.iterrows():
        code = str(sr.get("code") or "")
        d = pd.to_datetime(sr.get("squeeze_date"), errors="coerce")
        if code not in frames or pd.isna(d):
            continue
        df = frames[code]
        idxs = df.index[pd.to_datetime(df["date"]).dt.normalize().eq(pd.Timestamp(d).normalize())]
        if len(idxs) == 0:
            continue
        i = int(idxs[-1])
        x = df.iloc[max(0, i-pre_bars):min(len(df), i+post_bars+1)].copy()
        x.insert(0, "review_bucket", sr.get("review_bucket",""))
        x.insert(1, "review_code", code)
        x.insert(2, "review_squeeze_date", str(pd.Timestamp(d).date()))
        x["relative_bar"] = np.arange(max(0, i-pre_bars), min(len(df), i+post_bars+1)) - i
        keep = ["review_bucket","review_code","review_squeeze_date","relative_bar","date","open","high","low","close","volume","amount"]
        chunks.append(x[[c for c in keep if c in x.columns]])
    return pd.concat(chunks, ignore_index=True, sort=False) if chunks else pd.DataFrame()


def detect_code(
    code: str,
    df: pd.DataFrame,
    amount_source: str,
    universe: UniverseAuthority,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cfg: FrozenConfig,
    structure_audit_sink: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    signals: List[Dict[str, Any]] = []
    rejects: List[Dict[str, Any]] = []
    diag: Dict[str, Any] = {
        "code": code,
        "windows_tested": 0,
        "upper_falling_windows": 0,
        "lower_rising_windows": 0,
        "both_slope_signs_windows": 0,
        "width_order_windows": 0,
        "contraction_pass_windows": 0,
        "r2_pass_windows": 0,
        "end_width_pass_windows": 0,
        "squeeze_qualifying_windows": 0,
        "max_squeeze_streak": 0,
        "squeeze_streak_reached": 0,
        "squeeze_universe_pass": 0,
        "squeeze_universe_fail": 0,
        "squeeze_context_ready": 0,
        "breakout_price_cross": 0,
        "breakout_amount_ready": 0,
        "breakout_amount_expansion_pass": 0,
        "breakout_candle_confirm_pass": 0,
        "breakout_universe_pass": 0,
        "breakout_accepted": 0,
        "first_pullback_accepted": 0,
        "healthy_pullback_accepted": 0,
        "restart_accepted": 0,
        "structure_audit_errors": 0,
        "short_frame": 0,
    }
    if len(df) < cfg.squeeze_lookback + 5:
        diag["short_frame"] = 1
        return events, signals, rejects, diag

    squeeze_streak = 0
    squeeze_run_seq = 0
    squeeze_context: Optional[Dict[str, Any]] = None
    state = "IDLE"
    ctx: Dict[str, Any] = {}
    cooldown_until = -1

    def reject(i: int, reason: str, stage: str, extra: Optional[Dict[str, Any]] = None) -> None:
        row = {
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "code": code,
            "event_date": df.iloc[i]["date"].date().isoformat(),
            "stage": stage,
            "reason": reason,
            "episode_id": ctx.get("episode_id", ""),
        }
        if extra:
            row.update({k: _finite(v) for k, v in extra.items()})
        rejects.append(row)

    for i in range(cfg.squeeze_lookback, len(df)):
        d = pd.Timestamp(df.iloc[i]["date"])
        if d > end:
            break
        prev_window = df.iloc[i - cfg.squeeze_lookback:i]
        geom = squeeze_geometry(prev_window, cfg)
        diag["windows_tested"] += 1
        if geom.get("gate_upper_falling"):
            diag["upper_falling_windows"] += 1
        if geom.get("gate_lower_rising"):
            diag["lower_rising_windows"] += 1
        if geom.get("gate_upper_falling") and geom.get("gate_lower_rising"):
            diag["both_slope_signs_windows"] += 1
        if geom.get("gate_width_order"):
            diag["width_order_windows"] += 1
        if geom.get("gate_contraction"):
            diag["contraction_pass_windows"] += 1
        if geom.get("gate_r2"):
            diag["r2_pass_windows"] += 1
        if geom.get("gate_end_width"):
            diag["end_width_pass_windows"] += 1
        if geom.get("qualifies"):
            diag["squeeze_qualifying_windows"] += 1
            if squeeze_streak == 0:
                squeeze_run_seq += 1
            next_streak = int(squeeze_streak) + 1
            if structure_audit_sink is not None:
                try:
                    audit_row = build_structure_fidelity_row(code, df, i - 1, geom)
                    audit_row.update({
                        "squeeze_end_index": int(i - 1),
                        "squeeze_run_seq": int(squeeze_run_seq),
                        "squeeze_streak_ending": int(next_streak),
                        "counterfactual_candidate_streak_1": int(next_streak == 1),
                        "counterfactual_candidate_streak_2": int(next_streak == 2),
                        "counterfactual_candidate_streak_3": int(next_streak == 3),
                        "counterfactual_candidate_streak_4": int(next_streak == 4),
                    })
                    structure_audit_sink.append(audit_row)
                except Exception as audit_exc:
                    diag["structure_audit_errors"] += 1
            squeeze_streak = next_streak
            diag["max_squeeze_streak"] = max(int(diag["max_squeeze_streak"]), int(squeeze_streak))
            if squeeze_streak >= cfg.squeeze_min_consecutive_windows:
                diag["squeeze_streak_reached"] += 1
                sq_date = pd.Timestamp(prev_window["date"].iloc[-1])
                uok, udate, uage, ure = universe.lookup(sq_date, code)
                if uok:
                    diag["squeeze_universe_pass"] += 1
                    squeeze_context = dict(geom)
                    squeeze_context.update({"event_date": sq_date, "universe_snapshot_date": udate, "universe_age_days": uage})
                    diag["squeeze_context_ready"] += 1
                else:
                    diag["squeeze_universe_fail"] += 1
                    squeeze_context = None
        else:
            squeeze_streak = 0
            if state == "IDLE":
                squeeze_context = None

        if i < cooldown_until:
            continue

        row = df.iloc[i]
        prev = df.iloc[i - 1]
        amount = float(row["amount"]) if pd.notna(row["amount"]) else float("nan")
        amt20, amt_obs = amount20_stats(df, i, cfg)
        amt20_ratio = amount / amt20 if math.isfinite(amount) and amount > 0 and math.isfinite(amt20) and amt20 > 0 else float("nan")

        if state == "IDLE":
            if d < start:
                continue
            if squeeze_context is None or squeeze_streak < cfg.squeeze_min_consecutive_windows:
                continue
            upper = float(geom.get("projected_upper_next", float("nan")))
            if not (math.isfinite(upper) and row["close"] > upper * (1.0 + cfg.breakout_buffer_pct)):
                continue
            diag["breakout_price_cross"] += 1
            if not (math.isfinite(amount) and amount > 0 and amt_obs >= cfg.amount20_min_observations and math.isfinite(amt20_ratio)):
                reject(i, "BREAKOUT_ACTUAL_AMOUNT_NOT_READY", "TRI_BREAKOUT_WAVE1", {"amount_source": amount_source, "amount20_obs": amt_obs})
                squeeze_streak = 0; squeeze_context = None
                continue
            diag["breakout_amount_ready"] += 1
            if amt20_ratio < cfg.breakout_min_amount20_ratio:
                reject(i, "BREAKOUT_AMOUNT_EXPANSION_TOO_LOW", "TRI_BREAKOUT_WAVE1", {"amount20_ratio": amt20_ratio})
                squeeze_streak = 0; squeeze_context = None
                continue
            diag["breakout_amount_expansion_pass"] += 1
            if not (row["close"] > row["open"] and row["close"] >= (row["high"] + row["low"]) / 2.0):
                reject(i, "BREAKOUT_CANDLE_NOT_CONFIRMING", "TRI_BREAKOUT_WAVE1")
                squeeze_streak = 0; squeeze_context = None
                continue
            diag["breakout_candle_confirm_pass"] += 1
            # Universe must be causal at breakout; squeeze context already passed separately.
            uok, udate, uage, ure = universe.lookup(d, code)
            if not uok:
                reject(i, ure, "TRI_BREAKOUT_WAVE1", {"universe_snapshot_date": udate, "universe_age_days": uage})
                squeeze_streak = 0; squeeze_context = None
                continue
            diag["breakout_universe_pass"] += 1
            diag["breakout_accepted"] += 1
            ep = _episode_id(code, d)
            sqd = pd.Timestamp(squeeze_context["event_date"])
            sq_event = {
                "schema": SCHEMA, "strategy_id": STRATEGY_ID, "episode_id": ep,
                "event_id": _event_id(ep, "TRI_SQUEEZE", sqd), "code": code,
                "stage": "TRI_SQUEEZE", "event_date": sqd.date().isoformat(),
                "feature_max_date": squeeze_context["feature_end_date"],
                "universe_snapshot_date": squeeze_context["universe_snapshot_date"],
                "universe_age_days": squeeze_context["universe_age_days"],
                "amount_source": amount_source,
                "upper_slope_pct_per_bar": squeeze_context["upper_slope_pct_per_bar"],
                "lower_slope_pct_per_bar": squeeze_context["lower_slope_pct_per_bar"],
                "upper_r2": squeeze_context["upper_r2"], "lower_r2": squeeze_context["lower_r2"],
                "width_start_pct": squeeze_context["width_start_pct"], "width_end_pct": squeeze_context["width_end_pct"],
                "contraction_ratio": squeeze_context["contraction_ratio"],
            }
            br_event = {
                "schema": SCHEMA, "strategy_id": STRATEGY_ID, "episode_id": ep,
                "event_id": _event_id(ep, "TRI_BREAKOUT_WAVE1", d), "code": code,
                "stage": "TRI_BREAKOUT_WAVE1", "event_date": d.date().isoformat(),
                "feature_max_date": d.date().isoformat(), "universe_snapshot_date": udate,
                "universe_age_days": uage, "amount_source": amount_source,
                "open": row["open"], "high": row["high"], "low": row["low"], "close": row["close"],
                "actual_amount": amount, "amount20_mean_prior": amt20, "amount20_ratio": amt20_ratio,
                "breakout_reference": upper, "squeeze_contraction_ratio": geom.get("contraction_ratio", ""),
            }
            events.extend([sq_event, br_event])
            state = "BREAKOUT_WAVE1"
            ctx = {
                "episode_id": ep, "breakout_idx": i, "breakout_date": d, "breakout_ref": upper,
                "breakout_amount": amount, "wave_high": float(row["high"]), "pullback_start_idx": None,
                "healthy_idx": None, "pullback_amounts": [],
            }
            squeeze_streak = 0
            squeeze_context = None
            continue

        # Active chronology after breakout.
        age = i - int(ctx["breakout_idx"])
        ctx["wave_high"] = max(float(ctx["wave_high"]), float(row["high"]))
        drawdown = max(0.0, (float(ctx["wave_high"]) - float(row["close"])) / float(ctx["wave_high"])) if ctx["wave_high"] > 0 else 0.0
        floor_price = float(ctx["breakout_ref"]) * (1.0 - cfg.breakout_floor_tolerance_pct)

        if row["close"] < floor_price or drawdown > cfg.pullback_max_drawdown_pct:
            reject(i, "STRUCTURE_BROKEN", state, {"drawdown_pct": drawdown, "breakout_floor": floor_price})
            state = "IDLE"; ctx = {}; squeeze_streak = 0; squeeze_context = None
            continue

        if state == "BREAKOUT_WAVE1":
            if age > cfg.breakout_wave_max_bars:
                reject(i, "NO_FIRST_PULLBACK_WITHIN_WAVE_WINDOW", state, {"wave_age_bars": age})
                state = "IDLE"; ctx = {}; squeeze_streak = 0; squeeze_context = None
                continue
            if drawdown < cfg.pullback_min_drawdown_pct or not (row["close"] < prev["close"]):
                continue
            uok, udate, uage, ure = universe.lookup(d, code)
            if not uok:
                reject(i, ure, "TRI_FIRST_PULLBACK", {"universe_snapshot_date": udate, "universe_age_days": uage})
                state = "IDLE"; ctx = {}; continue
            ev = {
                "schema": SCHEMA, "strategy_id": STRATEGY_ID, "episode_id": ctx["episode_id"],
                "event_id": _event_id(ctx["episode_id"], "TRI_FIRST_PULLBACK", d), "code": code,
                "stage": "TRI_FIRST_PULLBACK", "event_date": d.date().isoformat(), "feature_max_date": d.date().isoformat(),
                "universe_snapshot_date": udate, "universe_age_days": uage, "amount_source": amount_source,
                "close": row["close"], "wave_high": ctx["wave_high"], "drawdown_pct": drawdown,
                "breakout_reference": ctx["breakout_ref"], "actual_amount": amount,
                "amount20_mean_prior": amt20, "amount20_ratio": amt20_ratio,
            }
            events.append(ev)
            diag["first_pullback_accepted"] += 1
            ctx["pullback_start_idx"] = i
            ctx["pullback_amounts"] = [amount] if math.isfinite(amount) and amount > 0 else []
            state = "FIRST_PULLBACK"
            # fall through to healthy test on the first pullback bar

        if state == "FIRST_PULLBACK":
            pb_age = i - int(ctx["pullback_start_idx"])
            if pb_age > cfg.healthy_wait_max_bars:
                reject(i, "PULLBACK_NOT_HEALTHY_IN_TIME", "TRI_HEALTHY_PULLBACK", {"pullback_age_bars": pb_age})
                state = "IDLE"; ctx = {}; continue
            if i != int(ctx["pullback_start_idx"]) and math.isfinite(amount) and amount > 0:
                ctx["pullback_amounts"].append(amount)
            if not (math.isfinite(amount) and amount > 0 and amt_obs >= cfg.amount20_min_observations and math.isfinite(amt20_ratio)):
                continue
            breakout_amt_ratio = amount / float(ctx["breakout_amount"]) if float(ctx["breakout_amount"]) > 0 else float("nan")
            healthy = (
                drawdown >= cfg.pullback_min_drawdown_pct and drawdown <= cfg.pullback_max_drawdown_pct and
                row["close"] >= floor_price and
                breakout_amt_ratio <= cfg.healthy_max_breakout_amount_ratio and
                amt20_ratio <= cfg.healthy_max_amount20_ratio
            )
            if not healthy:
                continue
            uok, udate, uage, ure = universe.lookup(d, code)
            if not uok:
                reject(i, ure, "TRI_HEALTHY_PULLBACK", {"universe_snapshot_date": udate, "universe_age_days": uage})
                state = "IDLE"; ctx = {}; continue
            events.append({
                "schema": SCHEMA, "strategy_id": STRATEGY_ID, "episode_id": ctx["episode_id"],
                "event_id": _event_id(ctx["episode_id"], "TRI_HEALTHY_PULLBACK", d), "code": code,
                "stage": "TRI_HEALTHY_PULLBACK", "event_date": d.date().isoformat(), "feature_max_date": d.date().isoformat(),
                "universe_snapshot_date": udate, "universe_age_days": uage, "amount_source": amount_source,
                "close": row["close"], "wave_high": ctx["wave_high"], "drawdown_pct": drawdown,
                "breakout_reference": ctx["breakout_ref"], "actual_amount": amount,
                "amount20_mean_prior": amt20, "amount20_ratio": amt20_ratio,
                "amount_vs_breakout_ratio": breakout_amt_ratio,
            })
            diag["healthy_pullback_accepted"] += 1
            ctx["healthy_idx"] = i
            state = "HEALTHY_PULLBACK"
            continue

        if state == "HEALTHY_PULLBACK":
            rest_age = i - int(ctx["healthy_idx"])
            if rest_age > cfg.restart_wait_max_bars:
                reject(i, "NO_RESTART_IN_WINDOW", "TRI_RESTART", {"restart_wait_bars": rest_age})
                state = "IDLE"; ctx = {}; continue
            prior_pb_amounts = [x for x in ctx.get("pullback_amounts", []) if math.isfinite(x) and x > 0]
            pb_median = float(np.median(prior_pb_amounts)) if prior_pb_amounts else float("nan")
            restart_ratio = amount / pb_median if math.isfinite(amount) and amount > 0 and math.isfinite(pb_median) and pb_median > 0 else float("nan")
            restart = (
                rest_age >= 1 and row["close"] > prev["high"] and row["close"] > row["open"] and
                row["close"] >= float(ctx["breakout_ref"]) and
                math.isfinite(restart_ratio) and restart_ratio >= cfg.restart_min_pullback_amount_ratio
            )
            if not restart:
                if math.isfinite(amount) and amount > 0:
                    ctx["pullback_amounts"].append(amount)
                continue
            uok, udate, uage, ure = universe.lookup(d, code)
            if not uok:
                reject(i, ure, "TRI_RESTART", {"universe_snapshot_date": udate, "universe_age_days": uage})
                state = "IDLE"; ctx = {}; continue
            ev = {
                "schema": SCHEMA, "strategy_id": STRATEGY_ID, "episode_id": ctx["episode_id"],
                "event_id": _event_id(ctx["episode_id"], "TRI_RESTART", d), "code": code,
                "stage": "TRI_RESTART", "event_date": d.date().isoformat(), "feature_max_date": d.date().isoformat(),
                "universe_snapshot_date": udate, "universe_age_days": uage, "amount_source": amount_source,
                "open": row["open"], "high": row["high"], "low": row["low"], "close": row["close"],
                "actual_amount": amount, "amount20_mean_prior": amt20, "amount20_ratio": amt20_ratio,
                "pullback_amount_median": pb_median, "amount_vs_pullback_median": restart_ratio,
                "wave_high": ctx["wave_high"], "breakout_reference": ctx["breakout_ref"], "drawdown_pct": drawdown,
                "signal_index": i,
            }
            events.append(ev)
            signals.append(dict(ev))
            diag["restart_accepted"] += 1
            cooldown_until = i + cfg.post_signal_cooldown_bars + 1
            state = "IDLE"; ctx = {}; squeeze_streak = 0; squeeze_context = None

    return events, signals, rejects, diag



def _cf_bool(v: Any) -> int:
    return int(bool(v))


def _cf_breakout_eval(
    df: pd.DataFrame,
    idx: int,
    breakout_ref: float,
    universe: UniverseAuthority,
    code: str,
    cfg: FrozenConfig,
) -> Dict[str, Any]:
    row = df.iloc[idx]
    d = pd.Timestamp(row["date"])
    amount = float(row["amount"]) if pd.notna(row["amount"]) else float("nan")
    amt20, amt_obs = amount20_stats(df, idx, cfg)
    amt20_ratio = (
        amount / amt20
        if math.isfinite(amount) and amount > 0 and math.isfinite(amt20) and amt20 > 0
        else float("nan")
    )
    price_cross = bool(
        math.isfinite(breakout_ref)
        and float(row["close"]) > breakout_ref * (1.0 + cfg.breakout_buffer_pct)
    )
    amount_ready = bool(
        math.isfinite(amount)
        and amount > 0
        and amt_obs >= cfg.amount20_min_observations
        and math.isfinite(amt20_ratio)
    )
    amount_pass = bool(amount_ready and amt20_ratio >= cfg.breakout_min_amount20_ratio)
    candle_pass = bool(
        float(row["close"]) > float(row["open"])
        and float(row["close"]) >= (float(row["high"]) + float(row["low"])) / 2.0
    )
    uok, udate, uage, ure = universe.lookup(d, code)
    exact_no_universe = bool(price_cross and amount_pass and candle_pass)
    exact_with_universe = bool(exact_no_universe and uok)
    return {
        "idx": int(idx),
        "date": d.date().isoformat(),
        "breakout_reference": float(breakout_ref) if math.isfinite(breakout_ref) else float("nan"),
        "price_cross": _cf_bool(price_cross),
        "amount_ready": _cf_bool(amount_ready),
        "amount_pass": _cf_bool(amount_pass),
        "candle_pass": _cf_bool(candle_pass),
        "exact_no_universe": _cf_bool(exact_no_universe),
        "exact_with_universe": _cf_bool(exact_with_universe),
        "actual_amount": amount,
        "amount20_mean_prior": amt20,
        "amount20_ratio": amt20_ratio,
        "universe_pass": _cf_bool(uok),
        "universe_snapshot_date": udate,
        "universe_age_days": uage,
        "universe_reason": ure,
    }


def _cf_downstream_existing_rules(
    code: str,
    df: pd.DataFrame,
    breakout: Dict[str, Any],
    universe: UniverseAuthority,
    cfg: FrozenConfig,
    squeeze_close: float,
) -> Dict[str, Any]:
    """Replay existing R1 downstream rules from an accepted audit breakout."""
    out = {
        "first_pullback_existing": 0,
        "healthy_pullback_existing": 0,
        "restart_existing": 0,
        "structure_broken": 0,
        "first_pullback_date": "",
        "healthy_pullback_date": "",
        "restart_date": "",
        "first_pullback_drawdown_pct": float("nan"),
        "healthy_drawdown_pct": float("nan"),
        "healthy_amount_vs_breakout_ratio": float("nan"),
        "healthy_amount20_ratio": float("nan"),
        "restart_amount_vs_pullback_median": float("nan"),
        "post_breakout_max_high_ret_5bar_pct": float("nan"),
        "post_breakout_max_high_ret_8bar_pct": float("nan"),
        "post_breakout_reaches_5pct_5bar": 0,
        "post_breakout_reaches_10pct_8bar": 0,
    }
    if not int(breakout.get("exact_with_universe", 0)):
        return out

    bidx = int(breakout["idx"])
    bref = float(breakout["breakout_reference"])
    bamt = float(breakout["actual_amount"])
    wave_high = float(df.iloc[bidx]["high"])
    floor_price = bref * (1.0 - cfg.breakout_floor_tolerance_pct)

    # Descriptive impulse strength after the exact breakout.
    if math.isfinite(squeeze_close) and squeeze_close > 0:
        seg5 = df.iloc[bidx:min(len(df), bidx + 5)]
        seg8 = df.iloc[bidx:min(len(df), bidx + 8)]
        if not seg5.empty:
            h5 = pd.to_numeric(seg5["high"], errors="coerce").max()
            if pd.notna(h5):
                out["post_breakout_max_high_ret_5bar_pct"] = float((h5 / squeeze_close - 1.0) * 100.0)
                out["post_breakout_reaches_5pct_5bar"] = int(h5 >= squeeze_close * 1.05)
        if not seg8.empty:
            h8 = pd.to_numeric(seg8["high"], errors="coerce").max()
            if pd.notna(h8):
                out["post_breakout_max_high_ret_8bar_pct"] = float((h8 / squeeze_close - 1.0) * 100.0)
                out["post_breakout_reaches_10pct_8bar"] = int(h8 >= squeeze_close * 1.10)

    state = "BREAKOUT_WAVE1"
    pullback_start_idx = None
    healthy_idx = None
    pullback_amounts: List[float] = []

    for i in range(bidx + 1, min(len(df), bidx + cfg.breakout_wave_max_bars + cfg.healthy_wait_max_bars + cfg.restart_wait_max_bars + 4)):
        row = df.iloc[i]
        prev = df.iloc[i - 1]
        d = pd.Timestamp(row["date"])
        amount = float(row["amount"]) if pd.notna(row["amount"]) else float("nan")
        amt20, amt_obs = amount20_stats(df, i, cfg)
        amt20_ratio = (
            amount / amt20
            if math.isfinite(amount) and amount > 0 and math.isfinite(amt20) and amt20 > 0
            else float("nan")
        )

        wave_high = max(wave_high, float(row["high"]))
        drawdown = max(0.0, (wave_high - float(row["close"])) / wave_high) if wave_high > 0 else 0.0

        if float(row["close"]) < floor_price or drawdown > cfg.pullback_max_drawdown_pct:
            out["structure_broken"] = 1
            return out

        if state == "BREAKOUT_WAVE1":
            age = i - bidx
            if age > cfg.breakout_wave_max_bars:
                return out
            if drawdown < cfg.pullback_min_drawdown_pct or not (float(row["close"]) < float(prev["close"])):
                continue
            uok, _, _, _ = universe.lookup(d, code)
            if not uok:
                return out
            out["first_pullback_existing"] = 1
            out["first_pullback_date"] = d.date().isoformat()
            out["first_pullback_drawdown_pct"] = float(drawdown)
            pullback_start_idx = i
            pullback_amounts = [amount] if math.isfinite(amount) and amount > 0 else []
            state = "FIRST_PULLBACK"
            # Same bar can satisfy healthy condition, matching detect_code().

        if state == "FIRST_PULLBACK":
            pb_age = i - int(pullback_start_idx)
            if pb_age > cfg.healthy_wait_max_bars:
                return out
            if i != int(pullback_start_idx) and math.isfinite(amount) and amount > 0:
                pullback_amounts.append(amount)
            if not (
                math.isfinite(amount)
                and amount > 0
                and amt_obs >= cfg.amount20_min_observations
                and math.isfinite(amt20_ratio)
            ):
                continue
            amount_vs_breakout = amount / bamt if bamt > 0 else float("nan")
            healthy = bool(
                drawdown >= cfg.pullback_min_drawdown_pct
                and drawdown <= cfg.pullback_max_drawdown_pct
                and float(row["close"]) >= floor_price
                and math.isfinite(amount_vs_breakout)
                and amount_vs_breakout <= cfg.healthy_max_breakout_amount_ratio
                and amt20_ratio <= cfg.healthy_max_amount20_ratio
            )
            if not healthy:
                continue
            uok, _, _, _ = universe.lookup(d, code)
            if not uok:
                return out
            out["healthy_pullback_existing"] = 1
            out["healthy_pullback_date"] = d.date().isoformat()
            out["healthy_drawdown_pct"] = float(drawdown)
            out["healthy_amount_vs_breakout_ratio"] = float(amount_vs_breakout)
            out["healthy_amount20_ratio"] = float(amt20_ratio)
            healthy_idx = i
            state = "HEALTHY_PULLBACK"
            continue

        if state == "HEALTHY_PULLBACK":
            rest_age = i - int(healthy_idx)
            if rest_age > cfg.restart_wait_max_bars:
                return out
            prior_pb_amounts = [x for x in pullback_amounts if math.isfinite(x) and x > 0]
            pb_median = float(np.median(prior_pb_amounts)) if prior_pb_amounts else float("nan")
            restart_ratio = (
                amount / pb_median
                if math.isfinite(amount) and amount > 0 and math.isfinite(pb_median) and pb_median > 0
                else float("nan")
            )
            restart = bool(
                rest_age >= 1
                and float(row["close"]) > float(prev["high"])
                and float(row["close"]) > float(row["open"])
                and float(row["close"]) >= bref
                and math.isfinite(restart_ratio)
                and restart_ratio >= cfg.restart_min_pullback_amount_ratio
            )
            if restart:
                out["restart_existing"] = 1
                out["restart_date"] = d.date().isoformat()
                out["restart_amount_vs_pullback_median"] = float(restart_ratio)
                return out
            if math.isfinite(amount) and amount > 0:
                pullback_amounts.append(amount)

    return out


def build_counterfactual_streak_audit(
    structure_audit: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
    universe: UniverseAuthority,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cfg: FrozenConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Research-only comparison of first-reach streak thresholds 1/2/3/4.

    This lane does not modify detect_code(), stage acceptance, or FrozenConfig.
    For threshold N, only the first window that reaches streak N within a
    consecutive squeeze run is evaluated, preventing duplicate episode counts.
    """
    base_cols = [
        "schema","strategy_id","loader_revision","audit_role","streak_threshold","code",
        "squeeze_date","squeeze_run_seq","squeeze_streak_ending","research_period",
        "squeeze_universe_pass","squeeze_universe_reason",
        "first_price_cross_found","first_price_cross_offset","first_price_cross_date",
        "first_cross_amount_ready","first_cross_amount_pass","first_cross_candle_pass",
        "first_cross_exact_no_universe","first_cross_exact_with_universe",
        "any_exact_breakout_no_universe","any_exact_breakout_with_universe",
        "any_exact_breakout_date","any_exact_breakout_offset",
        "probe_then_qualified_breakout","probe_to_qualified_delay_bars",
        "qualified_breakout_date","qualified_breakout_offset",
        "qualified_first_pullback_existing","qualified_healthy_pullback_existing",
        "qualified_restart_existing","qualified_structure_broken",
        "first_pullback_existing","healthy_pullback_existing","restart_existing","structure_broken",
        "post_breakout_max_high_ret_5bar_pct","post_breakout_max_high_ret_8bar_pct",
        "post_breakout_reaches_5pct_5bar","post_breakout_reaches_10pct_8bar",
    ]
    if structure_audit is None or structure_audit.empty:
        return pd.DataFrame(columns=base_cols), pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for threshold in (1, 2, 3, 4):
        cand = structure_audit[
            pd.to_numeric(structure_audit.get("squeeze_streak_ending"), errors="coerce")
            .fillna(0).astype(int).eq(threshold)
        ].copy()
        if cand.empty:
            continue

        for _, sr in cand.iterrows():
            code = str(sr.get("code") or "")
            sq_date = pd.to_datetime(sr.get("squeeze_date"), errors="coerce")
            if code not in frames or pd.isna(sq_date):
                continue
            research_period = int(pd.Timestamp(start) <= pd.Timestamp(sq_date) <= pd.Timestamp(end))
            if not research_period:
                continue

            df = frames[code]
            idxs = df.index[
                pd.to_datetime(df["date"]).dt.normalize().eq(pd.Timestamp(sq_date).normalize())
            ]
            if len(idxs) == 0:
                continue
            sq_idx = int(idxs[-1])
            projected_upper_next = _audit_num(sr.get("projected_upper_next"))
            upper_slope_price = _audit_num(sr.get("upper_slope_price_per_bar"))
            squeeze_close = _audit_num(sr.get("squeeze_close"))

            sq_uok, sq_udate, sq_uage, sq_ure = universe.lookup(pd.Timestamp(sq_date), code)

            first_cross: Optional[Dict[str, Any]] = None
            any_exact_no_u: Optional[Dict[str, Any]] = None
            any_exact_with_u: Optional[Dict[str, Any]] = None
            max_scan = min(len(df) - 1, sq_idx + cfg.breakout_wave_max_bars)

            for idx in range(sq_idx + 1, max_scan + 1):
                offset = idx - sq_idx
                bref = (
                    projected_upper_next + upper_slope_price * (offset - 1)
                    if math.isfinite(projected_upper_next) and math.isfinite(upper_slope_price)
                    else float("nan")
                )
                ev = _cf_breakout_eval(df, idx, bref, universe, code, cfg)
                ev["offset"] = int(offset)
                if first_cross is None and int(ev["price_cross"]):
                    first_cross = ev
                if any_exact_no_u is None and int(ev["exact_no_universe"]):
                    any_exact_no_u = ev
                if any_exact_with_u is None and int(ev["exact_with_universe"]):
                    any_exact_with_u = ev

            if first_cross is None:
                first_cross = {
                    "idx": -1, "date": "", "offset": -1, "price_cross": 0,
                    "amount_ready": 0, "amount_pass": 0, "candle_pass": 0,
                    "exact_no_universe": 0, "exact_with_universe": 0,
                    "breakout_reference": float("nan"), "actual_amount": float("nan"),
                    "amount20_mean_prior": float("nan"), "amount20_ratio": float("nan"),
                    "universe_pass": 0, "universe_snapshot_date": "",
                    "universe_age_days": 999999, "universe_reason": "NO_PRICE_CROSS",
                }

            # Existing chronology semantics use the first price cross. If that
            # first cross fails Amount/candle/universe, downstream is not replayed.
            downstream = _cf_downstream_existing_rules(
                code, df, first_cross, universe, cfg, squeeze_close
            )

            # Alternative chronology AUDIT ONLY:
            # ignore an earlier price-only probe and anchor Wave1 at the first
            # later bar that satisfies the SAME price+Amount+candle+universe gates.
            qualified_breakout = any_exact_with_u if (any_exact_with_u is not None and sq_uok) else None
            if qualified_breakout is not None:
                qualified_downstream = _cf_downstream_existing_rules(
                    code, df, qualified_breakout, universe, cfg, squeeze_close
                )
            else:
                qualified_downstream = _cf_downstream_existing_rules(
                    code, df, {"exact_with_universe": 0}, universe, cfg, squeeze_close
                )
            probe_then_qualified = int(
                qualified_breakout is not None
                and int(first_cross.get("price_cross", 0)) == 1
                and int(first_cross.get("exact_with_universe", 0)) == 0
            )
            probe_delay = (
                int(qualified_breakout.get("offset", -1)) - int(first_cross.get("offset", -1))
                if probe_then_qualified else 0
            )

            rows.append({
                "schema": SCHEMA,
                "strategy_id": STRATEGY_ID,
                "loader_revision": LOADER_REVISION,
                "audit_role": "COUNTERFACTUAL_ONLY_NOT_A_GATE",
                "streak_threshold": int(threshold),
                "code": code,
                "squeeze_date": pd.Timestamp(sq_date).date().isoformat(),
                "squeeze_run_seq": int(pd.to_numeric(sr.get("squeeze_run_seq"), errors="coerce") or 0),
                "squeeze_streak_ending": int(threshold),
                "research_period": 1,
                "shape_score": _audit_num(sr.get("shape_score")),
                "squeeze_close": squeeze_close,
                "squeeze_universe_pass": int(sq_uok),
                "squeeze_universe_snapshot_date": sq_udate,
                "squeeze_universe_age_days": sq_uage,
                "squeeze_universe_reason": sq_ure,
                "first_price_cross_found": int(first_cross.get("price_cross", 0)),
                "first_price_cross_offset": int(first_cross.get("offset", -1)),
                "first_price_cross_date": str(first_cross.get("date", "")),
                "first_cross_amount_ready": int(first_cross.get("amount_ready", 0)),
                "first_cross_amount_pass": int(first_cross.get("amount_pass", 0)),
                "first_cross_candle_pass": int(first_cross.get("candle_pass", 0)),
                "first_cross_exact_no_universe": int(first_cross.get("exact_no_universe", 0)),
                "first_cross_exact_with_universe": int(first_cross.get("exact_with_universe", 0) and sq_uok),
                "first_cross_amount20_ratio": _audit_num(first_cross.get("amount20_ratio")),
                "first_cross_breakout_reference": _audit_num(first_cross.get("breakout_reference")),
                "first_cross_actual_amount": _audit_num(first_cross.get("actual_amount")),
                "any_exact_breakout_no_universe": int(any_exact_no_u is not None),
                "any_exact_breakout_with_universe": int(any_exact_with_u is not None and sq_uok),
                "any_exact_breakout_date": str((any_exact_with_u or any_exact_no_u or {}).get("date", "")),
                "any_exact_breakout_offset": int((any_exact_with_u or any_exact_no_u or {}).get("offset", -1)),
                "probe_then_qualified_breakout": probe_then_qualified,
                "probe_to_qualified_delay_bars": int(probe_delay),
                "probe_first_cross_amount_fail": int(probe_then_qualified and not int(first_cross.get("amount_pass", 0))),
                "probe_first_cross_candle_fail": int(probe_then_qualified and not int(first_cross.get("candle_pass", 0))),
                "qualified_breakout_date": str((qualified_breakout or {}).get("date", "")),
                "qualified_breakout_offset": int((qualified_breakout or {}).get("offset", -1)),
                "qualified_first_pullback_existing": int(qualified_downstream.get("first_pullback_existing", 0)),
                "qualified_healthy_pullback_existing": int(qualified_downstream.get("healthy_pullback_existing", 0)),
                "qualified_restart_existing": int(qualified_downstream.get("restart_existing", 0)),
                "qualified_structure_broken": int(qualified_downstream.get("structure_broken", 0)),
                "qualified_first_pullback_date": str(qualified_downstream.get("first_pullback_date", "")),
                "qualified_healthy_pullback_date": str(qualified_downstream.get("healthy_pullback_date", "")),
                "qualified_restart_date": str(qualified_downstream.get("restart_date", "")),
                "qualified_first_pullback_drawdown_pct": _audit_num(qualified_downstream.get("first_pullback_drawdown_pct")),
                "qualified_healthy_drawdown_pct": _audit_num(qualified_downstream.get("healthy_drawdown_pct")),
                "qualified_healthy_amount_vs_breakout_ratio": _audit_num(qualified_downstream.get("healthy_amount_vs_breakout_ratio")),
                "qualified_healthy_amount20_ratio": _audit_num(qualified_downstream.get("healthy_amount20_ratio")),
                "qualified_restart_amount_vs_pullback_median": _audit_num(qualified_downstream.get("restart_amount_vs_pullback_median")),
                "qualified_post_breakout_max_high_ret_5bar_pct": _audit_num(qualified_downstream.get("post_breakout_max_high_ret_5bar_pct")),
                "qualified_post_breakout_max_high_ret_8bar_pct": _audit_num(qualified_downstream.get("post_breakout_max_high_ret_8bar_pct")),
                "qualified_post_breakout_reaches_5pct_5bar": int(qualified_downstream.get("post_breakout_reaches_5pct_5bar", 0)),
                "qualified_post_breakout_reaches_10pct_8bar": int(qualified_downstream.get("post_breakout_reaches_10pct_8bar", 0)),
                **downstream,
            })

    detail = pd.DataFrame(rows)
    summary_rows = []
    for threshold in (1, 2, 3, 4):
        x = detail[detail["streak_threshold"].eq(threshold)] if not detail.empty else pd.DataFrame()
        def isum(col: str) -> int:
            return int(pd.to_numeric(x.get(col, pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not x.empty else 0
        strict_events = (
            x[pd.to_numeric(x.get("first_cross_exact_with_universe"), errors="coerce").fillna(0).astype(int).eq(1)]
            [["code","first_price_cross_date"]].drop_duplicates()
            if not x.empty else pd.DataFrame()
        )
        qualified_events = (
            x[pd.to_numeric(x.get("any_exact_breakout_with_universe"), errors="coerce").fillna(0).astype(int).eq(1)]
            [["code","qualified_breakout_date"]].drop_duplicates()
            if not x.empty else pd.DataFrame()
        )
        qualified_restart_events = (
            x[pd.to_numeric(x.get("qualified_restart_existing"), errors="coerce").fillna(0).astype(int).eq(1)]
            [["code","qualified_breakout_date"]].drop_duplicates()
            if not x.empty else pd.DataFrame()
        )
        summary_rows.append({
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "streak_threshold": threshold,
            "research_candidate_runs": int(len(x)),
            "squeeze_universe_pass": isum("squeeze_universe_pass"),
            "first_price_cross": isum("first_price_cross_found"),
            "first_cross_amount_ready": isum("first_cross_amount_ready"),
            "first_cross_amount_pass": isum("first_cross_amount_pass"),
            "first_cross_candle_pass": isum("first_cross_candle_pass"),
            "first_cross_exact_no_universe": isum("first_cross_exact_no_universe"),
            "first_cross_exact_with_universe": isum("first_cross_exact_with_universe"),
            "any_exact_breakout_no_universe": isum("any_exact_breakout_no_universe"),
            "any_exact_breakout_with_universe": isum("any_exact_breakout_with_universe"),
            "probe_then_qualified_breakout": isum("probe_then_qualified_breakout"),
            "probe_first_cross_amount_fail": isum("probe_first_cross_amount_fail"),
            "probe_first_cross_candle_fail": isum("probe_first_cross_candle_fail"),
            "qualified_first_pullback_existing": isum("qualified_first_pullback_existing"),
            "qualified_healthy_pullback_existing": isum("qualified_healthy_pullback_existing"),
            "qualified_restart_existing": isum("qualified_restart_existing"),
            "strict_unique_breakout_events": int(len(strict_events)),
            "qualified_unique_breakout_events": int(len(qualified_events)),
            "qualified_unique_restart_events": int(len(qualified_restart_events)),
            "first_pullback_existing": isum("first_pullback_existing"),
            "healthy_pullback_existing": isum("healthy_pullback_existing"),
            "restart_existing": isum("restart_existing"),
            "post_breakout_reaches_5pct_5bar": isum("post_breakout_reaches_5pct_5bar"),
            "post_breakout_reaches_10pct_8bar": isum("post_breakout_reaches_10pct_8bar"),
            "used_as_strategy_gate": 0,
            "frozen_config_changed": 0,
        })
    return detail, pd.DataFrame(summary_rows)



def build_qualified_event_fidelity_audit(
    counterfactual_detail: pd.DataFrame,
    structure_audit: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
    cfg: FrozenConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Research-only event-level audit for streak=1 qualified breakouts.

    Dedup key is (code, qualified_breakout_date). If multiple squeeze runs map to
    the same breakout event, the latest squeeze date is selected as canonical
    review source and source_run_count is retained. No strategy gate is changed.
    """
    if counterfactual_detail is None or counterfactual_detail.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    x = counterfactual_detail[
        counterfactual_detail["streak_threshold"].eq(1)
        & counterfactual_detail["any_exact_breakout_with_universe"].eq(1)
        & counterfactual_detail["qualified_breakout_date"].fillna("").astype(str).ne("")
    ].copy()

    if x.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    structure_key = {}
    if structure_audit is not None and not structure_audit.empty:
        for _, sr in structure_audit.iterrows():
            structure_key[(str(sr.get("code") or ""), str(sr.get("squeeze_date") or ""))] = sr.to_dict()

    rows: List[Dict[str, Any]] = []

    for (code, breakout_date), grp in x.groupby(["code","qualified_breakout_date"], sort=True):
        grp = grp.copy()
        grp["__sq"] = pd.to_datetime(grp["squeeze_date"], errors="coerce")
        grp = grp.sort_values("__sq", ascending=False)
        cr = grp.iloc[0].to_dict()  # latest squeeze = closest causal precursor

        source_run_count = int(len(grp))
        any_probe_source = int(pd.to_numeric(grp["probe_then_qualified_breakout"], errors="coerce").fillna(0).max())
        any_direct_source = int(pd.to_numeric(grp["first_cross_exact_with_universe"], errors="coerce").fillna(0).max())

        sq_date = str(cr.get("squeeze_date") or "")
        sr = structure_key.get((str(code), sq_date), {})
        df = frames.get(str(code))

        wait_lower_break_count = 0
        wait_structure_intact_lower = 1
        wait_min_close_vs_squeeze_pct = float("nan")
        wait_max_drawdown_from_interim_high_pct = float("nan")
        max_abs_close_to_close_pct = float("nan")
        max_abs_open_gap_pct = float("nan")
        suspicious_price_discontinuity = 0
        squeeze_idx = -1
        breakout_idx = -1

        if df is not None and not df.empty:
            dates = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
            sq_ts = pd.to_datetime(sq_date, errors="coerce")
            bo_ts = pd.to_datetime(breakout_date, errors="coerce")
            sq_match = df.index[dates.eq(pd.Timestamp(sq_ts).normalize())] if pd.notna(sq_ts) else []
            bo_match = df.index[dates.eq(pd.Timestamp(bo_ts).normalize())] if pd.notna(bo_ts) else []
            if len(sq_match) and len(bo_match):
                squeeze_idx = int(sq_match[-1])
                breakout_idx = int(bo_match[-1])
                squeeze_close = _audit_num(df.iloc[squeeze_idx].get("close"))
                projected_lower_next = _audit_num(sr.get("projected_lower_next"))
                lower_slope_price = _audit_num(sr.get("lower_slope_price_per_bar"))

                running_high = _audit_num(df.iloc[squeeze_idx].get("high"))
                min_close = float("inf")
                max_dd = 0.0
                max_cc = 0.0
                max_gap = 0.0

                # Audit from squeeze+1 through breakout inclusive.
                for idx in range(squeeze_idx + 1, breakout_idx + 1):
                    rr = df.iloc[idx]
                    close = _audit_num(rr.get("close"))
                    high = _audit_num(rr.get("high"))
                    op = _audit_num(rr.get("open"))
                    prev_close = _audit_num(df.iloc[idx - 1].get("close"))

                    if math.isfinite(close):
                        min_close = min(min_close, close)
                    if math.isfinite(high):
                        running_high = max(running_high, high)
                    if math.isfinite(close) and math.isfinite(running_high) and running_high > 0:
                        max_dd = max(max_dd, (running_high - close) / running_high * 100.0)

                    offset = idx - squeeze_idx
                    if math.isfinite(projected_lower_next) and math.isfinite(lower_slope_price):
                        lower_ref = projected_lower_next + lower_slope_price * (offset - 1)
                        if math.isfinite(close) and close < lower_ref:
                            wait_lower_break_count += 1

                    if math.isfinite(close) and math.isfinite(prev_close) and prev_close > 0:
                        cc = abs((close / prev_close - 1.0) * 100.0)
                        max_cc = max(max_cc, cc)
                    if math.isfinite(op) and math.isfinite(prev_close) and prev_close > 0:
                        gap = abs((op / prev_close - 1.0) * 100.0)
                        max_gap = max(max_gap, gap)

                if min_close != float("inf") and math.isfinite(squeeze_close) and squeeze_close > 0:
                    wait_min_close_vs_squeeze_pct = (min_close / squeeze_close - 1.0) * 100.0
                wait_max_drawdown_from_interim_high_pct = float(max_dd)
                wait_structure_intact_lower = int(wait_lower_break_count == 0)

                # Extend discontinuity audit through 8 bars after breakout.
                for idx in range(max(1, squeeze_idx + 1), min(len(df), breakout_idx + 9)):
                    rr = df.iloc[idx]
                    op = _audit_num(rr.get("open"))
                    close = _audit_num(rr.get("close"))
                    prev_close = _audit_num(df.iloc[idx - 1].get("close"))
                    if math.isfinite(close) and math.isfinite(prev_close) and prev_close > 0:
                        max_cc = max(max_cc, abs((close / prev_close - 1.0) * 100.0))
                    if math.isfinite(op) and math.isfinite(prev_close) and prev_close > 0:
                        max_gap = max(max_gap, abs((op / prev_close - 1.0) * 100.0))

                max_abs_close_to_close_pct = float(max_cc)
                max_abs_open_gap_pct = float(max_gap)
                # Audit tag only. Does not exclude the event.
                suspicious_price_discontinuity = int(max(max_cc, max_gap) >= 35.0)

        rows.append({
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "audit_role": "QUALIFIED_EVENT_FIDELITY_ONLY_NOT_A_GATE",
            "code": str(code),
            "canonical_squeeze_date": sq_date,
            "qualified_breakout_date": str(breakout_date),
            "source_run_count": source_run_count,
            "any_probe_source": any_probe_source,
            "any_direct_source": any_direct_source,
            "canonical_probe_then_qualified": int(cr.get("probe_then_qualified_breakout", 0)),
            "canonical_probe_to_qualified_delay_bars": int(pd.to_numeric(cr.get("probe_to_qualified_delay_bars"), errors="coerce") or 0),
            "canonical_shape_score": _audit_num(cr.get("shape_score")),
            "qualified_breakout_offset": int(pd.to_numeric(cr.get("qualified_breakout_offset"), errors="coerce") or -1),
            "first_cross_amount20_ratio": _audit_num(cr.get("first_cross_amount20_ratio")),
            "qualified_first_pullback_existing": int(cr.get("qualified_first_pullback_existing", 0)),
            "qualified_healthy_pullback_existing": int(cr.get("qualified_healthy_pullback_existing", 0)),
            "qualified_restart_existing": int(cr.get("qualified_restart_existing", 0)),
            "qualified_first_pullback_date": str(cr.get("qualified_first_pullback_date") or ""),
            "qualified_healthy_pullback_date": str(cr.get("qualified_healthy_pullback_date") or ""),
            "qualified_restart_date": str(cr.get("qualified_restart_date") or ""),
            "qualified_healthy_drawdown_pct": _audit_num(cr.get("qualified_healthy_drawdown_pct")),
            "qualified_healthy_amount_vs_breakout_ratio": _audit_num(cr.get("qualified_healthy_amount_vs_breakout_ratio")),
            "qualified_healthy_amount20_ratio": _audit_num(cr.get("qualified_healthy_amount20_ratio")),
            "qualified_restart_amount_vs_pullback_median": _audit_num(cr.get("qualified_restart_amount_vs_pullback_median")),
            "qualified_post_breakout_max_high_ret_5bar_pct": _audit_num(cr.get("qualified_post_breakout_max_high_ret_5bar_pct")),
            "qualified_post_breakout_max_high_ret_8bar_pct": _audit_num(cr.get("qualified_post_breakout_max_high_ret_8bar_pct")),
            "qualified_post_breakout_reaches_5pct_5bar": int(cr.get("qualified_post_breakout_reaches_5pct_5bar", 0)),
            "qualified_post_breakout_reaches_10pct_8bar": int(cr.get("qualified_post_breakout_reaches_10pct_8bar", 0)),
            "wait_lower_trendline_break_count": int(wait_lower_break_count),
            "wait_structure_intact_lower": int(wait_structure_intact_lower),
            "wait_min_close_vs_squeeze_pct": wait_min_close_vs_squeeze_pct,
            "wait_max_drawdown_from_interim_high_pct": wait_max_drawdown_from_interim_high_pct,
            "max_abs_close_to_close_pct_squeeze_to_post8": max_abs_close_to_close_pct,
            "max_abs_open_gap_pct_squeeze_to_post8": max_abs_open_gap_pct,
            "suspicious_price_discontinuity_35pct": int(suspicious_price_discontinuity),
            "used_as_strategy_gate": 0,
        })

    event = pd.DataFrame(rows)
    if event.empty:
        return event, pd.DataFrame(), pd.DataFrame()

    def isum(col: str, frame: Optional[pd.DataFrame] = None) -> int:
        f = event if frame is None else frame
        return int(pd.to_numeric(f[col], errors="coerce").fillna(0).sum()) if col in f else 0

    direct = event[event["canonical_probe_then_qualified"].eq(0)]
    probe = event[event["canonical_probe_then_qualified"].eq(1)]
    clean = event[event["suspicious_price_discontinuity_35pct"].eq(0)]
    intact = event[event["wait_structure_intact_lower"].eq(1)]
    clean_intact = event[
        event["suspicious_price_discontinuity_35pct"].eq(0)
        & event["wait_structure_intact_lower"].eq(1)
    ]

    summary = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "qualified_unique_events": int(len(event)),
        "duplicate_source_runs": int((event["source_run_count"] - 1).clip(lower=0).sum()),
        "direct_canonical_events": int(len(direct)),
        "probe_to_qualified_canonical_events": int(len(probe)),
        "healthy_events": isum("qualified_healthy_pullback_existing"),
        "restart_events": isum("qualified_restart_existing"),
        "impulse_5pct_events": isum("qualified_post_breakout_reaches_5pct_5bar"),
        "impulse_10pct_events": isum("qualified_post_breakout_reaches_10pct_8bar"),
        "wait_structure_intact_lower_events": int(len(intact)),
        "wait_lower_break_events": int(len(event) - len(intact)),
        "suspicious_price_discontinuity_events": int(len(event) - len(clean)),
        "clean_events": int(len(clean)),
        "clean_intact_events": int(len(clean_intact)),
        "clean_intact_healthy_events": isum("qualified_healthy_pullback_existing", clean_intact),
        "clean_intact_restart_events": isum("qualified_restart_existing", clean_intact),
        "clean_intact_impulse_5pct_events": isum("qualified_post_breakout_reaches_5pct_5bar", clean_intact),
        "clean_intact_impulse_10pct_events": isum("qualified_post_breakout_reaches_10pct_8bar", clean_intact),
        "used_as_strategy_gate": 0,
    }])

    # Store full bars around every unique qualified event for visual review.
    bar_chunks: List[pd.DataFrame] = []
    for _, er in event.iterrows():
        code = str(er["code"])
        df = frames.get(code)
        if df is None or df.empty:
            continue
        dates = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        sq_ts = pd.to_datetime(er["canonical_squeeze_date"], errors="coerce")
        bo_ts = pd.to_datetime(er["qualified_breakout_date"], errors="coerce")
        sq_m = df.index[dates.eq(pd.Timestamp(sq_ts).normalize())] if pd.notna(sq_ts) else []
        bo_m = df.index[dates.eq(pd.Timestamp(bo_ts).normalize())] if pd.notna(bo_ts) else []
        if not len(sq_m) or not len(bo_m):
            continue
        si = int(sq_m[-1]); bi = int(bo_m[-1])
        lo = max(0, si - 40)
        hi = min(len(df), bi + 16)
        b = df.iloc[lo:hi].copy()
        b.insert(0, "review_code", code)
        b.insert(1, "canonical_squeeze_date", str(er["canonical_squeeze_date"]))
        b.insert(2, "qualified_breakout_date", str(er["qualified_breakout_date"]))
        b.insert(3, "relative_to_squeeze", np.arange(lo, hi) - si)
        b.insert(4, "relative_to_breakout", np.arange(lo, hi) - bi)
        b.insert(5, "event_probe_then_qualified", int(er["canonical_probe_then_qualified"]))
        b.insert(6, "event_healthy", int(er["qualified_healthy_pullback_existing"]))
        b.insert(7, "event_restart", int(er["qualified_restart_existing"]))
        b.insert(8, "event_suspicious_discontinuity", int(er["suspicious_price_discontinuity_35pct"]))
        keep = [
            "review_code","canonical_squeeze_date","qualified_breakout_date",
            "relative_to_squeeze","relative_to_breakout",
            "event_probe_then_qualified","event_healthy","event_restart",
            "event_suspicious_discontinuity",
            "date","open","high","low","close","volume","amount"
        ]
        bar_chunks.append(b[[c for c in keep if c in b.columns]])

    bars = pd.concat(bar_chunks, ignore_index=True, sort=False) if bar_chunks else pd.DataFrame()
    return event, summary, bars



def build_phase_sequence_audit(
    qualified_event_detail: pd.DataFrame,
    structure_audit: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Audit the intended sequence:
    prior flow/accumulation -> squeeze cooling/compression -> qualified breakout
    re-acceleration -> healthy pullback drying -> restart.

    Descriptive only. No field here is used by detect_code() or any strategy gate.
    """
    if qualified_event_detail is None or qualified_event_detail.empty:
        return pd.DataFrame(), pd.DataFrame()

    structure_key = {}
    if structure_audit is not None and not structure_audit.empty:
        for _, sr in structure_audit.iterrows():
            structure_key[(str(sr.get("code") or "").zfill(6), str(sr.get("squeeze_date") or ""))] = sr.to_dict()

    rows: List[Dict[str, Any]] = []

    def med(series: pd.Series) -> float:
        x = pd.to_numeric(series, errors="coerce")
        return float(x.median()) if x.notna().any() else float("nan")

    def mean(series: pd.Series) -> float:
        x = pd.to_numeric(series, errors="coerce")
        return float(x.mean()) if x.notna().any() else float("nan")

    def ratio(a: float, b: float) -> float:
        return float(a / b) if math.isfinite(a) and math.isfinite(b) and b > 0 else float("nan")

    def daily_range_median(frame: pd.DataFrame) -> float:
        if frame.empty:
            return float("nan")
        c = pd.to_numeric(frame["close"], errors="coerce").replace(0, np.nan)
        r = (
            pd.to_numeric(frame["high"], errors="coerce")
            - pd.to_numeric(frame["low"], errors="coerce")
        ) / c * 100.0
        return float(r.median()) if r.notna().any() else float("nan")

    for _, er in qualified_event_detail.iterrows():
        code = str(er.get("code") or "").zfill(6)
        sq_date = str(er.get("canonical_squeeze_date") or "")
        bo_date = str(er.get("qualified_breakout_date") or "")
        df = frames.get(str(int(code)) if code.isdigit() else code)
        if df is None:
            df = frames.get(code)
        if df is None or df.empty:
            continue

        dates = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        sq_ts = pd.to_datetime(sq_date, errors="coerce")
        bo_ts = pd.to_datetime(bo_date, errors="coerce")
        sq_match = df.index[dates.eq(pd.Timestamp(sq_ts).normalize())] if pd.notna(sq_ts) else []
        bo_match = df.index[dates.eq(pd.Timestamp(bo_ts).normalize())] if pd.notna(bo_ts) else []
        if not len(sq_match) or not len(bo_match):
            continue
        si = int(sq_match[-1])
        bi = int(bo_match[-1])

        pre = df.iloc[max(0, si - 40):max(0, si - 20)].copy()      # -40..-21
        squeeze = df.iloc[max(0, si - 20):si + 1].copy()          # -20..0
        prior20_bo = df.iloc[max(0, bi - 20):bi].copy()
        bo = df.iloc[bi]

        pre_amt_med = med(pre["amount"]) if "amount" in pre else float("nan")
        sq_amt_med = med(squeeze["amount"]) if "amount" in squeeze else float("nan")
        pre_vol_med = med(pre["volume"]) if "volume" in pre else float("nan")
        sq_vol_med = med(squeeze["volume"]) if "volume" in squeeze else float("nan")
        pre_range_med = daily_range_median(pre)
        sq_range_med = daily_range_median(squeeze)

        bo_amt = _audit_num(bo.get("amount"))
        bo_vol = _audit_num(bo.get("volume"))
        bo_amt20_mean = mean(prior20_bo["amount"]) if "amount" in prior20_bo else float("nan")
        bo_vol20_mean = mean(prior20_bo["volume"]) if "volume" in prior20_bo else float("nan")
        bo_amt20_med = med(prior20_bo["amount"]) if "amount" in prior20_bo else float("nan")
        bo_vol20_med = med(prior20_bo["volume"]) if "volume" in prior20_bo else float("nan")

        sr = structure_key.get((code, sq_date), {})

        # Healthy pullback row, if one exists.
        hp_date = pd.to_datetime(er.get("qualified_healthy_pullback_date"), errors="coerce")
        hp_amt = float("nan")
        hp_vol = float("nan")
        if pd.notna(hp_date):
            hp_match = df.index[dates.eq(pd.Timestamp(hp_date).normalize())]
            if len(hp_match):
                hp = df.iloc[int(hp_match[-1])]
                hp_amt = _audit_num(hp.get("amount"))
                hp_vol = _audit_num(hp.get("volume"))

        # Broader extreme-move context audit: pre-60 through post-breakout+8.
        lo = max(0, si - 60)
        hi = min(len(df), bi + 9)
        ctx = df.iloc[lo:hi].copy()
        ctx_close = pd.to_numeric(ctx["close"], errors="coerce")
        ctx_open = pd.to_numeric(ctx["open"], errors="coerce")
        prev_close = ctx_close.shift(1)
        abs_cc = ((ctx_close / prev_close) - 1.0).abs() * 100.0
        abs_gap = ((ctx_open / prev_close) - 1.0).abs() * 100.0
        extreme_29_count = int((abs_cc >= 29.0).fillna(False).sum())
        extreme_gap29_count = int((abs_gap >= 29.0).fillna(False).sum())

        cool_amt_ratio = ratio(sq_amt_med, pre_amt_med)
        cool_vol_ratio = ratio(sq_vol_med, pre_vol_med)
        range_ratio = ratio(sq_range_med, pre_range_med)
        bo_amt_mean_ratio = ratio(bo_amt, bo_amt20_mean)
        bo_vol_mean_ratio = ratio(bo_vol, bo_vol20_mean)
        bo_amt_med_ratio = ratio(bo_amt, bo_amt20_med)
        bo_vol_med_ratio = ratio(bo_vol, bo_vol20_med)
        hp_amt_vs_bo = ratio(hp_amt, bo_amt)
        hp_vol_vs_bo = ratio(hp_vol, bo_vol)

        cooling_amount = int(math.isfinite(cool_amt_ratio) and cool_amt_ratio < 1.0)
        cooling_volume = int(math.isfinite(cool_vol_ratio) and cool_vol_ratio < 1.0)
        range_compressed = int(math.isfinite(range_ratio) and range_ratio < 1.0)
        three_phase_compression = int(cooling_amount and cooling_volume and range_compressed)

        rows.append({
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "audit_role": "PHASE_SEQUENCE_ONLY_NOT_A_GATE",
            "code": code,
            "canonical_squeeze_date": sq_date,
            "qualified_breakout_date": bo_date,
            "probe_then_qualified": int(er.get("canonical_probe_then_qualified", 0)),
            "healthy": int(er.get("qualified_healthy_pullback_existing", 0)),
            "restart": int(er.get("qualified_restart_existing", 0)),
            "impulse_5pct_5bar": int(er.get("qualified_post_breakout_reaches_5pct_5bar", 0)),
            "impulse_10pct_8bar": int(er.get("qualified_post_breakout_reaches_10pct_8bar", 0)),
            "pre60_volume_spike_2x_count": int(pd.to_numeric(sr.get("pre60_volume_spike_2p0_count"), errors="coerce") or 0),
            "pre60_amount_spike_2x_count": int(pd.to_numeric(sr.get("pre60_amount_spike_2p0_count"), errors="coerce") or 0),
            "pre60_legacy_accumulation_bar_count": int(pd.to_numeric(sr.get("pre60_legacy_accumulation_bar_count"), errors="coerce") or 0),
            "pre10_obv_relative_change": _audit_num(sr.get("pre10_obv_relative_change")),
            "bb40_contraction_ratio_10bar": _audit_num(sr.get("bb40_contraction_ratio_10bar")),
            "pre20_amount_median": pre_amt_med,
            "squeeze20_amount_median": sq_amt_med,
            "squeeze_vs_pre_amount_median_ratio": cool_amt_ratio,
            "pre20_volume_median": pre_vol_med,
            "squeeze20_volume_median": sq_vol_med,
            "squeeze_vs_pre_volume_median_ratio": cool_vol_ratio,
            "pre20_daily_range_pct_median": pre_range_med,
            "squeeze20_daily_range_pct_median": sq_range_med,
            "squeeze_vs_pre_range_ratio": range_ratio,
            "squeeze_amount_cooling": cooling_amount,
            "squeeze_volume_cooling": cooling_volume,
            "squeeze_range_compression": range_compressed,
            "three_phase_compression": three_phase_compression,
            "qualified_breakout_amount": bo_amt,
            "qualified_breakout_amount20_mean_ratio": bo_amt_mean_ratio,
            "qualified_breakout_amount20_median_ratio": bo_amt_med_ratio,
            "qualified_breakout_volume20_mean_ratio": bo_vol_mean_ratio,
            "qualified_breakout_volume20_median_ratio": bo_vol_med_ratio,
            "healthy_pullback_amount_vs_breakout_ratio": hp_amt_vs_bo,
            "healthy_pullback_volume_vs_breakout_ratio": hp_vol_vs_bo,
            "lower_structure_intact_tag": int(er.get("wait_structure_intact_lower", 0)),
            "lower_break_count": int(er.get("wait_lower_trendline_break_count", 0)),
            "extreme_close_move_29pct_count_pre60_to_post8": extreme_29_count,
            "extreme_open_gap_29pct_count_pre60_to_post8": extreme_gap29_count,
            "extreme_context_tag": int(extreme_29_count > 0 or extreme_gap29_count > 0),
            "used_as_strategy_gate": 0,
        })

    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail, pd.DataFrame()

    def isum(col: str) -> int:
        return int(pd.to_numeric(detail[col], errors="coerce").fillna(0).sum())

    def median_col(col: str) -> float:
        x = pd.to_numeric(detail[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
        return float(x.median()) if x.notna().any() else float("nan")

    sequence_mask = (
        detail["three_phase_compression"].eq(1)
        & detail["impulse_5pct_5bar"].eq(1)
        & detail["healthy"].eq(1)
    )
    sequence_restart = int((sequence_mask & detail["restart"].eq(1)).sum())

    summary = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "qualified_unique_events": int(len(detail)),
        "pre60_volume_spike_2x_present": int((detail["pre60_volume_spike_2x_count"] >= 1).sum()),
        "pre60_amount_spike_2x_present": int((detail["pre60_amount_spike_2x_count"] >= 1).sum()),
        "legacy_accumulation_proxy_present": int((detail["pre60_legacy_accumulation_bar_count"] >= 1).sum()),
        "squeeze_amount_cooling_events": isum("squeeze_amount_cooling"),
        "squeeze_volume_cooling_events": isum("squeeze_volume_cooling"),
        "squeeze_range_compression_events": isum("squeeze_range_compression"),
        "three_phase_compression_events": isum("three_phase_compression"),
        "median_squeeze_vs_pre_amount_ratio": median_col("squeeze_vs_pre_amount_median_ratio"),
        "median_squeeze_vs_pre_volume_ratio": median_col("squeeze_vs_pre_volume_median_ratio"),
        "median_squeeze_vs_pre_range_ratio": median_col("squeeze_vs_pre_range_ratio"),
        "median_breakout_amount20_mean_ratio": median_col("qualified_breakout_amount20_mean_ratio"),
        "median_breakout_amount20_median_ratio": median_col("qualified_breakout_amount20_median_ratio"),
        "healthy_events": isum("healthy"),
        "restart_events": isum("restart"),
        "impulse_5pct_events": isum("impulse_5pct_5bar"),
        "impulse_10pct_events": isum("impulse_10pct_8bar"),
        "median_healthy_pullback_amount_vs_breakout": median_col("healthy_pullback_amount_vs_breakout_ratio"),
        "median_healthy_pullback_volume_vs_breakout": median_col("healthy_pullback_volume_vs_breakout_ratio"),
        "sequence_compression_plus_5pct_plus_healthy": int(sequence_mask.sum()),
        "sequence_compression_plus_5pct_plus_healthy_restart": sequence_restart,
        "extreme_context_29pct_events": isum("extreme_context_tag"),
        "lower_structure_intact_tag_events": isum("lower_structure_intact_tag"),
        "used_as_strategy_gate": 0,
    }])
    return detail, summary



def build_terminal_energy_profile_audit(
    qualified_event_detail: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Research-only temporal energy-profile audit.

    Splits the causal precursor into:
      early flow (-60..-21),
      squeeze early (-20..-11),
      squeeze late (-5..0),
      probe/wait (if any),
      qualified breakout,
      healthy pullback (descriptive downstream).

    No result is used as a strategy gate.
    """
    if qualified_event_detail is None or qualified_event_detail.empty:
        return pd.DataFrame(), pd.DataFrame()

    rows: List[Dict[str, Any]] = []

    def _med(frame: pd.DataFrame, col: str) -> float:
        if frame is None or frame.empty or col not in frame:
            return float("nan")
        x = pd.to_numeric(frame[col], errors="coerce")
        return float(x.median()) if x.notna().any() else float("nan")

    def _mean(frame: pd.DataFrame, col: str) -> float:
        if frame is None or frame.empty or col not in frame:
            return float("nan")
        x = pd.to_numeric(frame[col], errors="coerce")
        return float(x.mean()) if x.notna().any() else float("nan")

    def _range_med(frame: pd.DataFrame) -> float:
        if frame is None or frame.empty:
            return float("nan")
        c = pd.to_numeric(frame["close"], errors="coerce").replace(0, np.nan)
        r = (
            pd.to_numeric(frame["high"], errors="coerce")
            - pd.to_numeric(frame["low"], errors="coerce")
        ) / c * 100.0
        return float(r.median()) if r.notna().any() else float("nan")

    def _ratio(a: float, b: float) -> float:
        return float(a / b) if math.isfinite(a) and math.isfinite(b) and b > 0 else float("nan")

    def _spike_count(df: pd.DataFrame, start_idx: int, end_idx: int, col: str) -> Tuple[int, int, int]:
        count = 0
        first_offset = 999999
        last_offset = -999999
        for idx in range(max(0, start_idx), min(len(df), end_idx + 1)):
            val = _audit_num(df.iloc[idx].get(col))
            prior = df.iloc[max(0, idx - 20):idx]
            baseline = _mean(prior, col)
            if math.isfinite(val) and val > 0 and math.isfinite(baseline) and baseline > 0 and val >= 2.0 * baseline:
                count += 1
                off = idx - end_idx
                first_offset = min(first_offset, off)
                last_offset = max(last_offset, off)
        if count == 0:
            return 0, 999999, 999999
        return count, first_offset, last_offset

    for _, er in qualified_event_detail.iterrows():
        code = str(er.get("code") or "").zfill(6)
        sq_date = pd.to_datetime(er.get("canonical_squeeze_date"), errors="coerce")
        bo_date = pd.to_datetime(er.get("qualified_breakout_date"), errors="coerce")

        df = frames.get(code)
        if df is None and code.isdigit():
            df = frames.get(str(int(code)))
        if df is None or df.empty or pd.isna(sq_date) or pd.isna(bo_date):
            continue

        dates = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        sq_match = df.index[dates.eq(pd.Timestamp(sq_date).normalize())]
        bo_match = df.index[dates.eq(pd.Timestamp(bo_date).normalize())]
        if not len(sq_match) or not len(bo_match):
            continue
        si = int(sq_match[-1])
        bi = int(bo_match[-1])

        early_flow = df.iloc[max(0, si - 60):max(0, si - 20)].copy()
        squeeze_early = df.iloc[max(0, si - 20):max(0, si - 10)].copy()
        squeeze_late = df.iloc[max(0, si - 4):si + 1].copy()
        wait = df.iloc[si + 1:bi].copy() if bi > si + 1 else pd.DataFrame()
        bo = df.iloc[bi]

        early_amt_spikes, _, early_amt_last = _spike_count(df, si - 60, si - 21, "amount")
        early_vol_spikes, _, early_vol_last = _spike_count(df, si - 60, si - 21, "volume")

        early_amt = _med(squeeze_early, "amount")
        late_amt = _med(squeeze_late, "amount")
        early_vol = _med(squeeze_early, "volume")
        late_vol = _med(squeeze_late, "volume")
        early_range = _range_med(squeeze_early)
        late_range = _range_med(squeeze_late)

        late_amt_ratio = _ratio(late_amt, early_amt)
        late_vol_ratio = _ratio(late_vol, early_vol)
        late_range_ratio = _ratio(late_range, early_range)

        late_amt_dry = int(math.isfinite(late_amt_ratio) and late_amt_ratio < 1.0)
        late_vol_dry = int(math.isfinite(late_vol_ratio) and late_vol_ratio < 1.0)
        late_range_dry = int(math.isfinite(late_range_ratio) and late_range_ratio < 1.0)
        terminal_all3 = int(late_amt_dry and late_vol_dry and late_range_dry)

        bo_amt = _audit_num(bo.get("amount"))
        bo_vol = _audit_num(bo.get("volume"))
        bo_range = (
            (_audit_num(bo.get("high")) - _audit_num(bo.get("low"))) / _audit_num(bo.get("close")) * 100.0
            if _audit_num(bo.get("close")) > 0 else float("nan")
        )
        bo_vs_late_amt = _ratio(bo_amt, late_amt)
        bo_vs_late_vol = _ratio(bo_vol, late_vol)
        bo_vs_late_range = _ratio(bo_range, late_range)

        wait_amt_med = _med(wait, "amount")
        wait_vol_med = _med(wait, "volume")
        wait_range_med = _range_med(wait)

        hp_amt = float("nan")
        hp_vol = float("nan")
        hp_date = pd.to_datetime(er.get("qualified_healthy_pullback_date"), errors="coerce")
        if pd.notna(hp_date):
            hp_match = df.index[dates.eq(pd.Timestamp(hp_date).normalize())]
            if len(hp_match):
                hp = df.iloc[int(hp_match[-1])]
                hp_amt = _audit_num(hp.get("amount"))
                hp_vol = _audit_num(hp.get("volume"))

        prior_flow_present = int(early_amt_spikes > 0 or early_vol_spikes > 0)
        flow_then_terminal_dry = int(prior_flow_present and terminal_all3)

        rows.append({
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "audit_role": "TERMINAL_ENERGY_PROFILE_ONLY_NOT_A_GATE",
            "code": code,
            "canonical_squeeze_date": pd.Timestamp(sq_date).date().isoformat(),
            "qualified_breakout_date": pd.Timestamp(bo_date).date().isoformat(),
            "probe_then_qualified": int(er.get("canonical_probe_then_qualified", 0)),
            "healthy": int(er.get("qualified_healthy_pullback_existing", 0)),
            "restart": int(er.get("qualified_restart_existing", 0)),
            "impulse_5pct_5bar": int(er.get("qualified_post_breakout_reaches_5pct_5bar", 0)),
            "impulse_10pct_8bar": int(er.get("qualified_post_breakout_reaches_10pct_8bar", 0)),
            "early_flow_amount_2x_spike_count": early_amt_spikes,
            "early_flow_volume_2x_spike_count": early_vol_spikes,
            "early_flow_present": prior_flow_present,
            "early_flow_last_amount_spike_offset_to_squeeze": early_amt_last,
            "early_flow_last_volume_spike_offset_to_squeeze": early_vol_last,
            "squeeze_early_amount_median": early_amt,
            "squeeze_late5_amount_median": late_amt,
            "terminal_amount_ratio_late5_vs_early10": late_amt_ratio,
            "squeeze_early_volume_median": early_vol,
            "squeeze_late5_volume_median": late_vol,
            "terminal_volume_ratio_late5_vs_early10": late_vol_ratio,
            "squeeze_early_range_pct_median": early_range,
            "squeeze_late5_range_pct_median": late_range,
            "terminal_range_ratio_late5_vs_early10": late_range_ratio,
            "terminal_amount_dry": late_amt_dry,
            "terminal_volume_dry": late_vol_dry,
            "terminal_range_dry": late_range_dry,
            "terminal_all3_dry": terminal_all3,
            "flow_then_terminal_all3_dry": flow_then_terminal_dry,
            "probe_wait_bars": max(0, bi - si - 1),
            "probe_wait_amount_median": wait_amt_med,
            "probe_wait_volume_median": wait_vol_med,
            "probe_wait_range_pct_median": wait_range_med,
            "qualified_breakout_amount": bo_amt,
            "qualified_breakout_volume": bo_vol,
            "qualified_breakout_range_pct": bo_range,
            "breakout_vs_terminal_amount_ratio": bo_vs_late_amt,
            "breakout_vs_terminal_volume_ratio": bo_vs_late_vol,
            "breakout_vs_terminal_range_ratio": bo_vs_late_range,
            "healthy_pullback_amount_vs_breakout_ratio": _ratio(hp_amt, bo_amt),
            "healthy_pullback_volume_vs_breakout_ratio": _ratio(hp_vol, bo_vol),
            "used_as_strategy_gate": 0,
        })

    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail, pd.DataFrame()

    def isum(col: str, frame: Optional[pd.DataFrame] = None) -> int:
        f = detail if frame is None else frame
        return int(pd.to_numeric(f[col], errors="coerce").fillna(0).sum())

    def med_col(col: str, frame: Optional[pd.DataFrame] = None) -> float:
        f = detail if frame is None else frame
        x = pd.to_numeric(f[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
        return float(x.median()) if x.notna().any() else float("nan")

    terminal = detail[detail["terminal_all3_dry"].eq(1)]
    nonterminal = detail[detail["terminal_all3_dry"].eq(0)]
    flowterminal = detail[detail["flow_then_terminal_all3_dry"].eq(1)]

    summary = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "qualified_unique_events": int(len(detail)),
        "early_flow_present_events": isum("early_flow_present"),
        "terminal_amount_dry_events": isum("terminal_amount_dry"),
        "terminal_volume_dry_events": isum("terminal_volume_dry"),
        "terminal_range_dry_events": isum("terminal_range_dry"),
        "terminal_all3_dry_events": int(len(terminal)),
        "flow_then_terminal_all3_dry_events": int(len(flowterminal)),
        "median_terminal_amount_ratio": med_col("terminal_amount_ratio_late5_vs_early10"),
        "median_terminal_volume_ratio": med_col("terminal_volume_ratio_late5_vs_early10"),
        "median_terminal_range_ratio": med_col("terminal_range_ratio_late5_vs_early10"),
        "median_breakout_vs_terminal_amount_ratio": med_col("breakout_vs_terminal_amount_ratio"),
        "median_breakout_vs_terminal_volume_ratio": med_col("breakout_vs_terminal_volume_ratio"),
        "terminal_all3_impulse_5pct": isum("impulse_5pct_5bar", terminal),
        "terminal_all3_impulse_10pct": isum("impulse_10pct_8bar", terminal),
        "terminal_all3_healthy": isum("healthy", terminal),
        "terminal_all3_restart": isum("restart", terminal),
        "nonterminal_impulse_5pct": isum("impulse_5pct_5bar", nonterminal),
        "nonterminal_impulse_10pct": isum("impulse_10pct_8bar", nonterminal),
        "nonterminal_healthy": isum("healthy", nonterminal),
        "nonterminal_restart": isum("restart", nonterminal),
        "flow_terminal_impulse_5pct": isum("impulse_5pct_5bar", flowterminal),
        "flow_terminal_impulse_10pct": isum("impulse_10pct_8bar", flowterminal),
        "flow_terminal_healthy": isum("healthy", flowterminal),
        "flow_terminal_restart": isum("restart", flowterminal),
        "median_healthy_pullback_amount_vs_breakout": med_col(
            "healthy_pullback_amount_vs_breakout_ratio",
            detail[detail["healthy"].eq(1)]
        ),
        "median_healthy_pullback_volume_vs_breakout": med_col(
            "healthy_pullback_volume_vs_breakout_ratio",
            detail[detail["healthy"].eq(1)]
        ),
        "used_as_strategy_gate": 0,
    }])

    return detail, summary



def build_causal_precursor_case_control_audit(
    counterfactual_detail: pd.DataFrame,
    structure_audit: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Causal precursor case-control audit for ALL research-period streak=1 runs.

    Features use bars only through the squeeze date. Future qualified-breakout /
    healthy / restart fields are labels only, never candidate features or gates.

    This avoids the selection bias of computing precursor prevalence only inside
    already-qualified breakout events.
    """
    if counterfactual_detail is None or counterfactual_detail.empty:
        return pd.DataFrame(), pd.DataFrame()

    cand = counterfactual_detail[
        counterfactual_detail["streak_threshold"].eq(1)
        & counterfactual_detail["research_period"].eq(1)
    ].copy()
    if cand.empty:
        return pd.DataFrame(), pd.DataFrame()

    structure_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if structure_audit is not None and not structure_audit.empty:
        for _, sr in structure_audit.iterrows():
            code = str(sr.get("code") or "").zfill(6)
            sq_date = str(sr.get("squeeze_date") or "")
            structure_key[(code, sq_date)] = sr.to_dict()

    def _med(frame: pd.DataFrame, col: str) -> float:
        if frame is None or frame.empty or col not in frame:
            return float("nan")
        x = pd.to_numeric(frame[col], errors="coerce")
        return float(x.median()) if x.notna().any() else float("nan")

    def _mean(frame: pd.DataFrame, col: str) -> float:
        if frame is None or frame.empty or col not in frame:
            return float("nan")
        x = pd.to_numeric(frame[col], errors="coerce")
        return float(x.mean()) if x.notna().any() else float("nan")

    def _range_med(frame: pd.DataFrame) -> float:
        if frame is None or frame.empty:
            return float("nan")
        close = pd.to_numeric(frame["close"], errors="coerce").replace(0, np.nan)
        rng = (
            pd.to_numeric(frame["high"], errors="coerce")
            - pd.to_numeric(frame["low"], errors="coerce")
        ) / close * 100.0
        return float(rng.median()) if rng.notna().any() else float("nan")

    def _ratio(a: float, b: float) -> float:
        return float(a / b) if math.isfinite(a) and math.isfinite(b) and b > 0 else float("nan")

    def _spike_count(df: pd.DataFrame, start_idx: int, end_idx: int, col: str) -> int:
        count = 0
        for idx in range(max(0, start_idx), min(len(df), end_idx + 1)):
            val = _audit_num(df.iloc[idx].get(col))
            prior = df.iloc[max(0, idx - 20):idx]
            baseline = _mean(prior, col)
            if (
                math.isfinite(val) and val > 0
                and math.isfinite(baseline) and baseline > 0
                and val >= 2.0 * baseline
            ):
                count += 1
        return int(count)

    rows: List[Dict[str, Any]] = []
    for _, cr in cand.iterrows():
        code = str(cr.get("code") or "").zfill(6)
        sq_date = str(cr.get("squeeze_date") or "")
        sq_ts = pd.to_datetime(sq_date, errors="coerce")
        if pd.isna(sq_ts):
            continue

        df = frames.get(code)
        if df is None and code.isdigit():
            df = frames.get(str(int(code)))
        if df is None or df.empty:
            continue

        dates = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        sq_match = df.index[dates.eq(pd.Timestamp(sq_ts).normalize())]
        if not len(sq_match):
            continue
        si = int(sq_match[-1])

        early_flow = df.iloc[max(0, si - 60):max(0, si - 20)].copy()   # -60..-21
        squeeze_early = df.iloc[max(0, si - 20):max(0, si - 10)].copy() # -20..-11
        terminal5 = df.iloc[max(0, si - 4):si + 1].copy()              # -4..0

        early_amt_spikes = _spike_count(df, si - 60, si - 21, "amount")
        early_vol_spikes = _spike_count(df, si - 60, si - 21, "volume")
        early_flow_present = int(early_amt_spikes > 0 or early_vol_spikes > 0)

        se_amt = _med(squeeze_early, "amount")
        t_amt = _med(terminal5, "amount")
        se_vol = _med(squeeze_early, "volume")
        t_vol = _med(terminal5, "volume")
        se_range = _range_med(squeeze_early)
        t_range = _range_med(terminal5)

        amount_ratio = _ratio(t_amt, se_amt)
        volume_ratio = _ratio(t_vol, se_vol)
        range_ratio = _ratio(t_range, se_range)

        amount_dry = int(math.isfinite(amount_ratio) and amount_ratio < 1.0)
        volume_dry = int(math.isfinite(volume_ratio) and volume_ratio < 1.0)
        range_dry = int(math.isfinite(range_ratio) and range_ratio < 1.0)
        terminal_all3 = int(amount_dry and volume_dry and range_dry)

        sr = structure_key.get((code, sq_date), {})
        obv_change = _audit_num(sr.get("pre10_obv_relative_change"))
        bb40_ratio = _audit_num(sr.get("bb40_contraction_ratio_10bar"))
        shape_score = _audit_num(sr.get("shape_score"))

        qualified = int(cr.get("any_exact_breakout_with_universe", 0))
        strict = int(cr.get("first_cross_exact_with_universe", 0))
        healthy = int(cr.get("qualified_healthy_pullback_existing", 0))
        restart = int(cr.get("qualified_restart_existing", 0))
        impulse5 = int(cr.get("qualified_post_breakout_reaches_5pct_5bar", 0))
        impulse10 = int(cr.get("qualified_post_breakout_reaches_10pct_8bar", 0))

        rows.append({
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "audit_role": "CAUSAL_PRECURSOR_CASE_CONTROL_ONLY_NOT_A_GATE",
            "code": code,
            "squeeze_date": sq_date,
            "feature_max_date": sq_date,
            "streak_threshold": 1,
            # causal precursor features
            "early_flow_amount_2x_spike_count": early_amt_spikes,
            "early_flow_volume_2x_spike_count": early_vol_spikes,
            "early_flow_present": early_flow_present,
            "terminal_amount_ratio_late5_vs_early10": amount_ratio,
            "terminal_volume_ratio_late5_vs_early10": volume_ratio,
            "terminal_range_ratio_late5_vs_early10": range_ratio,
            "terminal_amount_dry": amount_dry,
            "terminal_volume_dry": volume_dry,
            "terminal_range_dry": range_dry,
            "terminal_all3_dry": terminal_all3,
            "pre10_obv_relative_change": obv_change,
            "pre10_obv_positive": int(math.isfinite(obv_change) and obv_change > 0),
            "bb40_contraction_ratio_10bar": bb40_ratio,
            "bb40_contracting": int(math.isfinite(bb40_ratio) and bb40_ratio < 1.0),
            "shape_score": shape_score,
            # outcome labels only
            "label_first_cross_exact": strict,
            "label_qualified_breakout": qualified,
            "label_healthy_pullback": healthy,
            "label_restart": restart,
            "label_impulse_5pct_5bar": impulse5,
            "label_impulse_10pct_8bar": impulse10,
            "label_probe_then_qualified": int(cr.get("probe_then_qualified_breakout", 0)),
            "label_qualified_breakout_date": str(cr.get("qualified_breakout_date") or ""),
            "used_as_strategy_gate": 0,
            "future_labels_not_features": 1,
        })

    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail, pd.DataFrame()

    def _sum(frame: pd.DataFrame, col: str) -> int:
        if frame.empty or col not in frame:
            return 0
        return int(pd.to_numeric(frame[col], errors="coerce").fillna(0).sum())

    def _rate(frame: pd.DataFrame, col: str) -> float:
        if frame.empty or col not in frame:
            return float("nan")
        x = pd.to_numeric(frame[col], errors="coerce").fillna(0)
        return float(x.mean())

    def _median(frame: pd.DataFrame, col: str) -> float:
        if frame.empty or col not in frame:
            return float("nan")
        x = pd.to_numeric(frame[col], errors="coerce").replace([np.inf,-np.inf], np.nan)
        return float(x.median()) if x.notna().any() else float("nan")

    qualified = detail[detail["label_qualified_breakout"].eq(1)]
    no_qualified = detail[detail["label_qualified_breakout"].eq(0)]
    terminal = detail[detail["terminal_all3_dry"].eq(1)]
    nonterminal = detail[detail["terminal_all3_dry"].eq(0)]
    earlyflow = detail[detail["early_flow_present"].eq(1)]
    no_earlyflow = detail[detail["early_flow_present"].eq(0)]
    obvpos = detail[detail["pre10_obv_positive"].eq(1)]
    obvnon = detail[detail["pre10_obv_positive"].eq(0)]
    bbcontract = detail[detail["bb40_contracting"].eq(1)]
    bbnon = detail[detail["bb40_contracting"].eq(0)]

    def _lift(a: float, b: float) -> float:
        return float(a / b) if math.isfinite(a) and math.isfinite(b) and b > 0 else float("nan")

    terminal_q_rate = _rate(terminal, "label_qualified_breakout")
    nonterminal_q_rate = _rate(nonterminal, "label_qualified_breakout")
    early_q_rate = _rate(earlyflow, "label_qualified_breakout")
    noearly_q_rate = _rate(no_earlyflow, "label_qualified_breakout")
    obv_q_rate = _rate(obvpos, "label_qualified_breakout")
    obvnon_q_rate = _rate(obvnon, "label_qualified_breakout")
    bb_q_rate = _rate(bbcontract, "label_qualified_breakout")
    bbnon_q_rate = _rate(bbnon, "label_qualified_breakout")

    summary = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "streak1_candidate_runs": int(len(detail)),
        "qualified_breakouts": _sum(detail, "label_qualified_breakout"),
        "strict_first_cross_exact": _sum(detail, "label_first_cross_exact"),

        "terminal_all3_runs": int(len(terminal)),
        "terminal_all3_qualified": _sum(terminal, "label_qualified_breakout"),
        "terminal_all3_qualified_rate": terminal_q_rate,
        "nonterminal_runs": int(len(nonterminal)),
        "nonterminal_qualified": _sum(nonterminal, "label_qualified_breakout"),
        "nonterminal_qualified_rate": nonterminal_q_rate,
        "terminal_all3_qualified_rate_lift": _lift(terminal_q_rate, nonterminal_q_rate),

        "early_flow_runs": int(len(earlyflow)),
        "early_flow_qualified": _sum(earlyflow, "label_qualified_breakout"),
        "early_flow_qualified_rate": early_q_rate,
        "no_early_flow_runs": int(len(no_earlyflow)),
        "no_early_flow_qualified": _sum(no_earlyflow, "label_qualified_breakout"),
        "no_early_flow_qualified_rate": noearly_q_rate,
        "early_flow_qualified_rate_lift": _lift(early_q_rate, noearly_q_rate),

        "obv_positive_runs": int(len(obvpos)),
        "obv_positive_qualified": _sum(obvpos, "label_qualified_breakout"),
        "obv_positive_qualified_rate": obv_q_rate,
        "obv_nonpositive_runs": int(len(obvnon)),
        "obv_nonpositive_qualified": _sum(obvnon, "label_qualified_breakout"),
        "obv_nonpositive_qualified_rate": obvnon_q_rate,
        "obv_positive_qualified_rate_lift": _lift(obv_q_rate, obvnon_q_rate),

        "bb40_contracting_runs": int(len(bbcontract)),
        "bb40_contracting_qualified": _sum(bbcontract, "label_qualified_breakout"),
        "bb40_contracting_qualified_rate": bb_q_rate,
        "bb40_noncontracting_runs": int(len(bbnon)),
        "bb40_noncontracting_qualified": _sum(bbnon, "label_qualified_breakout"),
        "bb40_noncontracting_qualified_rate": bbnon_q_rate,
        "bb40_contracting_qualified_rate_lift": _lift(bb_q_rate, bbnon_q_rate),

        "qualified_median_terminal_amount_ratio": _median(qualified, "terminal_amount_ratio_late5_vs_early10"),
        "nonqualified_median_terminal_amount_ratio": _median(no_qualified, "terminal_amount_ratio_late5_vs_early10"),
        "qualified_median_terminal_volume_ratio": _median(qualified, "terminal_volume_ratio_late5_vs_early10"),
        "nonqualified_median_terminal_volume_ratio": _median(no_qualified, "terminal_volume_ratio_late5_vs_early10"),
        "qualified_median_terminal_range_ratio": _median(qualified, "terminal_range_ratio_late5_vs_early10"),
        "nonqualified_median_terminal_range_ratio": _median(no_qualified, "terminal_range_ratio_late5_vs_early10"),
        "qualified_median_pre10_obv_change": _median(qualified, "pre10_obv_relative_change"),
        "nonqualified_median_pre10_obv_change": _median(no_qualified, "pre10_obv_relative_change"),
        "qualified_median_bb40_ratio": _median(qualified, "bb40_contraction_ratio_10bar"),
        "nonqualified_median_bb40_ratio": _median(no_qualified, "bb40_contraction_ratio_10bar"),
        "qualified_median_shape_score": _median(qualified, "shape_score"),
        "nonqualified_median_shape_score": _median(no_qualified, "shape_score"),

        "terminal_all3_healthy_labels": _sum(terminal, "label_healthy_pullback"),
        "terminal_all3_restart_labels": _sum(terminal, "label_restart"),
        "nonterminal_healthy_labels": _sum(nonterminal, "label_healthy_pullback"),
        "nonterminal_restart_labels": _sum(nonterminal, "label_restart"),

        "feature_cutoff_is_squeeze_date": 1,
        "future_labels_not_features": 1,
        "used_as_strategy_gate": 0,
    }])

    return detail, summary



def build_joint_precursor_stability_audit(
    precursor_detail: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Exploratory stability audit for the post-hoc candidate:
    pre10 OBV positive AND BB40 contracting.

    This is NOT a strategy gate. Because the hypothesis was discovered on this
    same two-year research set, the outputs are descriptive and intended to
    determine whether the direction is temporally stable enough to justify a
    future prospectively locked observation lane.
    """
    if precursor_detail is None or precursor_detail.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    d = precursor_detail.copy()
    d["squeeze_date_ts"] = pd.to_datetime(d["squeeze_date"], errors="coerce")
    d["joint_obv_positive_bb40_contracting"] = (
        d["pre10_obv_positive"].eq(1) & d["bb40_contracting"].eq(1)
    ).astype(int)

    def group_name(row: pd.Series) -> str:
        obv = int(row.get("pre10_obv_positive", 0))
        bb = int(row.get("bb40_contracting", 0))
        return f"OBV{'+' if obv else '-'}_BB40{'DOWN' if bb else 'NO_DOWN'}"

    d["joint_group"] = d.apply(group_name, axis=1)

    # Fixed midpoint of declared research calendar span, not outcome-derived.
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    midpoint = start_ts + (end_ts - start_ts) / 2
    d["temporal_half"] = np.where(
        d["squeeze_date_ts"] <= midpoint,
        "H1_EARLY",
        "H2_LATE",
    )

    def summarize(frame: pd.DataFrame, label: str, split: str) -> Dict[str, Any]:
        n = int(len(frame))
        q = int(pd.to_numeric(frame.get("label_qualified_breakout"), errors="coerce").fillna(0).sum()) if n else 0
        strict = int(pd.to_numeric(frame.get("label_first_cross_exact"), errors="coerce").fillna(0).sum()) if n else 0
        h = int(pd.to_numeric(frame.get("label_healthy_pullback"), errors="coerce").fillna(0).sum()) if n else 0
        r = int(pd.to_numeric(frame.get("label_restart"), errors="coerce").fillna(0).sum()) if n else 0
        p5 = int(pd.to_numeric(frame.get("label_impulse_5pct_5bar"), errors="coerce").fillna(0).sum()) if n else 0
        p10 = int(pd.to_numeric(frame.get("label_impulse_10pct_8bar"), errors="coerce").fillna(0).sum()) if n else 0
        return {
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "audit_role": "JOINT_PRECURSOR_STABILITY_ONLY_NOT_A_GATE",
            "split": split,
            "group": label,
            "runs": n,
            "strict_first_cross_exact": strict,
            "qualified_breakout": q,
            "qualified_rate": float(q / n) if n else float("nan"),
            "impulse_5pct": p5,
            "impulse_10pct": p10,
            "healthy": h,
            "restart": r,
            "used_as_strategy_gate": 0,
            "hypothesis_posthoc_on_same_research_period": 1,
        }

    rows: List[Dict[str, Any]] = []
    # Full 2x2.
    for group in [
        "OBV-_BB40NO_DOWN",
        "OBV-_BB40DOWN",
        "OBV+_BB40NO_DOWN",
        "OBV+_BB40DOWN",
    ]:
        rows.append(summarize(d[d["joint_group"].eq(group)], group, "FULL"))

    # Joint-vs-rest, full and temporal halves.
    for split, frame in [
        ("FULL", d),
        ("H1_EARLY", d[d["temporal_half"].eq("H1_EARLY")]),
        ("H2_LATE", d[d["temporal_half"].eq("H2_LATE")]),
    ]:
        joint = frame[frame["joint_obv_positive_bb40_contracting"].eq(1)]
        rest = frame[frame["joint_obv_positive_bb40_contracting"].eq(0)]
        rows.append(summarize(joint, "OBV+_AND_BB40DOWN", split))
        rows.append(summarize(rest, "REST", split))

    summary = pd.DataFrame(rows)

    # Add joint-vs-rest rate lift to the dedicated rows.
    for split in ["FULL","H1_EARLY","H2_LATE"]:
        jidx = summary.index[
            summary["split"].eq(split) & summary["group"].eq("OBV+_AND_BB40DOWN")
        ]
        ridx = summary.index[
            summary["split"].eq(split) & summary["group"].eq("REST")
        ]
        if len(jidx) and len(ridx):
            jr = _audit_num(summary.loc[jidx[0], "qualified_rate"])
            rr = _audit_num(summary.loc[ridx[0], "qualified_rate"])
            lift = jr / rr if math.isfinite(jr) and math.isfinite(rr) and rr > 0 else float("nan")
            summary.loc[jidx[0], "qualified_rate_lift_vs_rest"] = lift
            summary.loc[ridx[0], "qualified_rate_lift_vs_rest"] = 1.0

    # A compact candidate-run detail file for easier manual sorting.
    detail_cols = [
        "schema","strategy_id","loader_revision","audit_role","code","squeeze_date",
        "feature_max_date","pre10_obv_relative_change","pre10_obv_positive",
        "bb40_contraction_ratio_10bar","bb40_contracting",
        "terminal_amount_ratio_late5_vs_early10",
        "terminal_volume_ratio_late5_vs_early10",
        "terminal_range_ratio_late5_vs_early10",
        "terminal_all3_dry","early_flow_present","shape_score",
        "label_first_cross_exact","label_qualified_breakout",
        "label_qualified_breakout_date","label_probe_then_qualified",
        "label_impulse_5pct_5bar","label_impulse_10pct_8bar",
        "label_healthy_pullback","label_restart",
        "used_as_strategy_gate","future_labels_not_features",
    ]
    detail = d[[c for c in detail_cols if c in d.columns]].copy()
    detail["joint_group"] = d["joint_group"].values
    detail["joint_obv_positive_bb40_contracting"] = d["joint_obv_positive_bb40_contracting"].values
    detail["temporal_half"] = d["temporal_half"].values
    detail["hypothesis_posthoc_on_same_research_period"] = 1

    # Export bars for ALL 192 streak1 candidates, including non-qualified controls.
    bar_chunks: List[pd.DataFrame] = []
    for _, row in d.iterrows():
        code = str(row.get("code") or "").zfill(6)
        sq = pd.to_datetime(row.get("squeeze_date"), errors="coerce")
        if pd.isna(sq):
            continue
        df = frames.get(code)
        if df is None and code.isdigit():
            df = frames.get(str(int(code)))
        if df is None or df.empty:
            continue
        dates = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        matches = df.index[dates.eq(pd.Timestamp(sq).normalize())]
        if not len(matches):
            continue
        si = int(matches[-1])
        lo = max(0, si - 40)
        hi = min(len(df), si + 13)  # through +12 bars after squeeze
        b = df.iloc[lo:hi].copy()
        b.insert(0, "review_code", code)
        b.insert(1, "review_squeeze_date", pd.Timestamp(sq).date().isoformat())
        b.insert(2, "relative_to_squeeze", np.arange(lo, hi) - si)
        b.insert(3, "joint_group", str(row.get("joint_group") or ""))
        b.insert(4, "joint_obv_positive_bb40_contracting", int(row.get("joint_obv_positive_bb40_contracting",0)))
        b.insert(5, "label_qualified_breakout", int(row.get("label_qualified_breakout",0)))
        b.insert(6, "label_qualified_breakout_date", str(row.get("label_qualified_breakout_date") or ""))
        b.insert(7, "label_healthy_pullback", int(row.get("label_healthy_pullback",0)))
        b.insert(8, "label_restart", int(row.get("label_restart",0)))
        keep = [
            "review_code","review_squeeze_date","relative_to_squeeze","joint_group",
            "joint_obv_positive_bb40_contracting","label_qualified_breakout",
            "label_qualified_breakout_date","label_healthy_pullback","label_restart",
            "date","open","high","low","close","volume","amount"
        ]
        bar_chunks.append(b[[c for c in keep if c in b.columns]])

    bars = pd.concat(bar_chunks, ignore_index=True, sort=False) if bar_chunks else pd.DataFrame()
    return detail, summary, bars



def _r111_get_frame(frames: Dict[str, pd.DataFrame], code: str) -> Optional[pd.DataFrame]:
    c = str(code or "").zfill(6)
    df = frames.get(c)
    if df is None and c.isdigit():
        df = frames.get(str(int(c)))
    return df


def _r111_ma_at(df: pd.DataFrame, idx: int, window: int) -> float:
    if df is None or idx < 0 or idx + 1 < window:
        return float("nan")
    x = pd.to_numeric(df.iloc[idx - window + 1:idx + 1]["close"], errors="coerce")
    if int(x.notna().sum()) < window:
        return float("nan")
    return float(x.mean())


def _r111_ma_context(df: Optional[pd.DataFrame], idx: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if df is None or idx < 0 or idx >= len(df):
        out["close"] = float("nan")
        for w in (120, 200, 224):
            out[f"ma{w}"] = float("nan")
            out[f"close_vs_ma{w}_pct"] = float("nan")
            out[f"below_ma{w}"] = 0
            out[f"ma{w}_ready"] = 0
        out["ma224_slope_20bar_pct"] = float("nan")
        out["ma224_distance_bucket"] = "MA224_NOT_READY"
        return out

    close = _audit_num(df.iloc[idx].get("close"))
    out["close"] = close
    for w in (120, 200, 224):
        ma = _r111_ma_at(df, idx, w)
        ready = int(math.isfinite(ma) and ma > 0 and math.isfinite(close) and close > 0)
        dist = ((close / ma) - 1.0) * 100.0 if ready else float("nan")
        out[f"ma{w}"] = ma
        out[f"close_vs_ma{w}_pct"] = dist
        out[f"below_ma{w}"] = int(ready and close < ma)
        out[f"ma{w}_ready"] = ready

    ma224 = out["ma224"]
    ma224_prev20 = _r111_ma_at(df, idx - 20, 224) if idx >= 20 else float("nan")
    out["ma224_slope_20bar_pct"] = (
        ((ma224 / ma224_prev20) - 1.0) * 100.0
        if math.isfinite(ma224) and ma224 > 0 and math.isfinite(ma224_prev20) and ma224_prev20 > 0
        else float("nan")
    )
    dist224 = out["close_vs_ma224_pct"]
    if not math.isfinite(dist224):
        bucket = "MA224_NOT_READY"
    elif dist224 < -15.0:
        bucket = "LT_-15"
    elif dist224 < -5.0:
        bucket = "-15_TO_-5"
    elif dist224 < 0.0:
        bucket = "-5_TO_0"
    elif dist224 < 5.0:
        bucket = "0_TO_+5"
    elif dist224 < 15.0:
        bucket = "+5_TO_+15"
    else:
        bucket = "GE_+15"
    out["ma224_distance_bucket"] = bucket
    return out


def build_qualified_d1_d15_path_study(
    qualified_event_detail: pd.DataFrame,
    precursor_detail: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Future-outcome study from the qualified breakout close through D+15."""
    if qualified_event_detail is None or qualified_event_detail.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    precursor_key = {}
    if precursor_detail is not None and not precursor_detail.empty:
        for _, pr in precursor_detail.iterrows():
            precursor_key[(str(pr.get("code") or "").zfill(6), str(pr.get("squeeze_date") or ""))] = pr.to_dict()

    path_rows, event_rows = [], []
    for _, er in qualified_event_detail.iterrows():
        code = str(er.get("code") or "").zfill(6)
        sq_date = str(er.get("canonical_squeeze_date") or "")
        bo_date = pd.to_datetime(er.get("qualified_breakout_date"), errors="coerce")
        df = _r111_get_frame(frames, code)
        if df is None or df.empty or pd.isna(bo_date):
            continue
        dates = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        bm = df.index[dates.eq(pd.Timestamp(bo_date).normalize())]
        if not len(bm):
            continue
        bi = int(bm[-1])
        baseline = _audit_num(df.iloc[bi].get("close"))
        if not math.isfinite(baseline) or baseline <= 0:
            continue

        pr = precursor_key.get((code, sq_date), {})
        joint = int(int(pr.get("pre10_obv_positive",0)) == 1 and int(pr.get("bb40_contracting",0)) == 1)

        cum_high, cum_low = baseline, baseline
        first5 = first10 = -1
        peak_ret, trough_ret = float("-inf"), float("inf")
        peak_day = trough_day = -1
        endpoint = {}
        available = 0

        for d in range(1, 16):
            idx = bi + d
            if idx >= len(df):
                break
            row = df.iloc[idx]
            available = d
            close = _audit_num(row.get("close"))
            high = _audit_num(row.get("high"))
            low = _audit_num(row.get("low"))
            if math.isfinite(high):
                cum_high = max(cum_high, high)
            if math.isfinite(low):
                cum_low = min(cum_low, low)
            close_ret = ((close / baseline) - 1.0) * 100.0 if math.isfinite(close) else float("nan")
            hi_ret = ((high / baseline) - 1.0) * 100.0 if math.isfinite(high) else float("nan")
            lo_ret = ((low / baseline) - 1.0) * 100.0 if math.isfinite(low) else float("nan")
            mfe = ((cum_high / baseline) - 1.0) * 100.0
            mae = ((cum_low / baseline) - 1.0) * 100.0
            if first5 < 0 and mfe >= 5.0: first5 = d
            if first10 < 0 and mfe >= 10.0: first10 = d
            if math.isfinite(hi_ret) and hi_ret > peak_ret:
                peak_ret, peak_day = hi_ret, d
            if math.isfinite(lo_ret) and lo_ret < trough_ret:
                trough_ret, trough_day = lo_ret, d

            rec = {
                "schema": SCHEMA, "strategy_id": STRATEGY_ID, "loader_revision": LOADER_REVISION,
                "audit_role": "QUALIFIED_D1_D15_FUTURE_OUTCOME_ONLY_NOT_A_GATE",
                "code": code, "canonical_squeeze_date": sq_date,
                "qualified_breakout_date": pd.Timestamp(bo_date).date().isoformat(),
                "day_after_breakout": d, "path_date": pd.Timestamp(row["date"]).date().isoformat(),
                "breakout_close": baseline, "close_ret_from_breakout_pct": close_ret,
                "daily_high_ret_from_breakout_pct": hi_ret, "daily_low_ret_from_breakout_pct": lo_ret,
                "cumulative_mfe_pct": mfe, "cumulative_mae_pct": mae,
                "probe_then_qualified": int(er.get("canonical_probe_then_qualified",0)),
                "joint_obv_positive_bb40_contracting": joint,
                "healthy": int(er.get("qualified_healthy_pullback_existing",0)),
                "restart": int(er.get("qualified_restart_existing",0)),
                "future_outcome_only": 1, "used_as_strategy_gate": 0,
            }
            path_rows.append(rec)
            endpoint[d] = rec

        def ep(day, field):
            r = endpoint.get(day)
            return _audit_num(r.get(field)) if r else float("nan")

        hdate = pd.to_datetime(er.get("qualified_healthy_pullback_date"), errors="coerce")
        rdate = pd.to_datetime(er.get("qualified_restart_date"), errors="coerce")
        hbar = rbar = -1
        if pd.notna(hdate):
            hm = df.index[dates.eq(pd.Timestamp(hdate).normalize())]
            if len(hm): hbar = int(hm[-1]) - bi
        if pd.notna(rdate):
            rm = df.index[dates.eq(pd.Timestamp(rdate).normalize())]
            if len(rm): rbar = int(rm[-1]) - bi

        event_rows.append({
            "schema": SCHEMA, "strategy_id": STRATEGY_ID, "loader_revision": LOADER_REVISION,
            "audit_role": "QUALIFIED_D1_D15_EVENT_SUMMARY_FUTURE_ONLY",
            "code": code, "canonical_squeeze_date": sq_date,
            "qualified_breakout_date": pd.Timestamp(bo_date).date().isoformat(),
            "probe_then_qualified": int(er.get("canonical_probe_then_qualified",0)),
            "joint_obv_positive_bb40_contracting": joint,
            "healthy": int(er.get("qualified_healthy_pullback_existing",0)),
            "restart": int(er.get("qualified_restart_existing",0)),
            "available_post_breakout_bars": available, "d15_complete": int(available >= 15),
            "first_plus5_day": first5, "first_plus10_day": first10,
            "peak_day_within_15": peak_day,
            "peak_high_ret_within_15_pct": peak_ret if peak_day > 0 else float("nan"),
            "trough_day_within_15": trough_day,
            "trough_low_ret_within_15_pct": trough_ret if trough_day > 0 else float("nan"),
            "healthy_day_after_breakout": hbar, "restart_day_after_breakout": rbar,
            "d5_close_ret_pct": ep(5,"close_ret_from_breakout_pct"),
            "d5_mfe_pct": ep(5,"cumulative_mfe_pct"), "d5_mae_pct": ep(5,"cumulative_mae_pct"),
            "d10_close_ret_pct": ep(10,"close_ret_from_breakout_pct"),
            "d10_mfe_pct": ep(10,"cumulative_mfe_pct"), "d10_mae_pct": ep(10,"cumulative_mae_pct"),
            "d15_close_ret_pct": ep(15,"close_ret_from_breakout_pct"),
            "d15_mfe_pct": ep(15,"cumulative_mfe_pct"), "d15_mae_pct": ep(15,"cumulative_mae_pct"),
            "future_outcome_only": 1, "used_as_strategy_gate": 0,
        })

    detail, events = pd.DataFrame(path_rows), pd.DataFrame(event_rows)
    if events.empty:
        return detail, events, pd.DataFrame()

    def med(g, col):
        x = pd.to_numeric(g.get(col, pd.Series(dtype=float)), errors="coerce")
        return float(x.median()) if x.notna().any() else float("nan")

    groups = [
        ("ALL", events),
        ("DIRECT", events[events["probe_then_qualified"].eq(0)]),
        ("PROBE_TO_QUALIFIED", events[events["probe_then_qualified"].eq(1)]),
        ("OBV+_AND_BB40DOWN", events[events["joint_obv_positive_bb40_contracting"].eq(1)]),
        ("REST", events[events["joint_obv_positive_bb40_contracting"].eq(0)]),
        ("RESTART", events[events["restart"].eq(1)]),
        ("NO_RESTART", events[events["restart"].eq(0)]),
    ]
    rows = []
    for name, g in groups:
        rows.append({
            "schema": SCHEMA, "strategy_id": STRATEGY_ID, "loader_revision": LOADER_REVISION,
            "group": name, "events": int(len(g)),
            "d15_complete_events": int(pd.to_numeric(g.get("d15_complete",pd.Series(dtype=float)),errors="coerce").fillna(0).sum()),
            "median_peak_day_within_15": med(g,"peak_day_within_15"),
            "median_peak_high_ret_within_15_pct": med(g,"peak_high_ret_within_15_pct"),
            "plus5_reached_within_15": int((pd.to_numeric(g.get("first_plus5_day",pd.Series(dtype=float)),errors="coerce") > 0).sum()),
            "plus10_reached_within_15": int((pd.to_numeric(g.get("first_plus10_day",pd.Series(dtype=float)),errors="coerce") > 0).sum()),
            "median_first_plus5_day": med(g[g["first_plus5_day"]>0],"first_plus5_day") if not g.empty else float("nan"),
            "median_first_plus10_day": med(g[g["first_plus10_day"]>0],"first_plus10_day") if not g.empty else float("nan"),
            "median_healthy_day_after_breakout": med(g[g["healthy_day_after_breakout"]>0],"healthy_day_after_breakout") if not g.empty else float("nan"),
            "median_restart_day_after_breakout": med(g[g["restart_day_after_breakout"]>0],"restart_day_after_breakout") if not g.empty else float("nan"),
            "d5_close_ret_median_pct": med(g,"d5_close_ret_pct"), "d5_mfe_median_pct": med(g,"d5_mfe_pct"), "d5_mae_median_pct": med(g,"d5_mae_pct"),
            "d10_close_ret_median_pct": med(g,"d10_close_ret_pct"), "d10_mfe_median_pct": med(g,"d10_mfe_pct"), "d10_mae_median_pct": med(g,"d10_mae_pct"),
            "d15_close_ret_median_pct": med(g,"d15_close_ret_pct"), "d15_mfe_median_pct": med(g,"d15_mfe_pct"), "d15_mae_median_pct": med(g,"d15_mae_pct"),
            "future_outcome_only": 1, "used_as_strategy_gate": 0,
        })
    return detail, events, pd.DataFrame(rows)


def build_long_ma_context_audit(
    precursor_detail: pd.DataFrame,
    qualified_event_detail: pd.DataFrame,
    path_event_summary: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """MA120/200/224 research context; no MA gate is promoted."""
    if precursor_detail is None or precursor_detail.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    qmap = {}
    if qualified_event_detail is not None and not qualified_event_detail.empty:
        for _, q in qualified_event_detail.iterrows():
            qmap[(str(q.get("code") or "").zfill(6), str(q.get("canonical_squeeze_date") or ""))] = q.to_dict()

    pmap = {}
    if path_event_summary is not None and not path_event_summary.empty:
        for _, p in path_event_summary.iterrows():
            pmap[(str(p.get("code") or "").zfill(6), str(p.get("qualified_breakout_date") or ""))] = p.to_dict()

    crows, erows = [], []
    for _, pr in precursor_detail.iterrows():
        code = str(pr.get("code") or "").zfill(6)
        sq_date = str(pr.get("squeeze_date") or "")
        df = _r111_get_frame(frames, code)
        if df is None or df.empty:
            continue
        dates = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        st = pd.to_datetime(sq_date, errors="coerce")
        sm = df.index[dates.eq(pd.Timestamp(st).normalize())] if pd.notna(st) else []
        if not len(sm):
            continue
        si = int(sm[-1])
        ctx = _r111_ma_context(df, si)
        joint = int(int(pr.get("pre10_obv_positive",0)) == 1 and int(pr.get("bb40_contracting",0)) == 1)

        crows.append({
            "schema": SCHEMA, "strategy_id": STRATEGY_ID, "loader_revision": LOADER_REVISION,
            "audit_role": "LONG_MA_SQUEEZE_CONTEXT_CAUSAL_ONLY_NOT_A_GATE",
            "code": code, "squeeze_date": sq_date, "feature_max_date": sq_date,
            "joint_obv_positive_bb40_contracting": joint,
            "label_qualified_breakout": int(pr.get("label_qualified_breakout",0)),
            "label_healthy_pullback": int(pr.get("label_healthy_pullback",0)),
            "label_restart": int(pr.get("label_restart",0)),
            **ctx, "future_labels_not_features": 1, "used_as_strategy_gate": 0,
        })

        q = qmap.get((code, sq_date))
        if not q:
            continue
        bo_date = str(q.get("qualified_breakout_date") or "")
        bt = pd.to_datetime(bo_date, errors="coerce")
        bm = df.index[dates.eq(pd.Timestamp(bt).normalize())] if pd.notna(bt) else []
        if not len(bm):
            continue
        boctx = _r111_ma_context(df, int(bm[-1]))

        restart_date = str(q.get("qualified_restart_date") or "")
        rctx = _r111_ma_context(None, -1)
        if restart_date:
            rt = pd.to_datetime(restart_date, errors="coerce")
            rm = df.index[dates.eq(pd.Timestamp(rt).normalize())] if pd.notna(rt) else []
            if len(rm):
                rctx = _r111_ma_context(df, int(rm[-1]))

        path = pmap.get((code, bo_date), {})
        row = {
            "schema": SCHEMA, "strategy_id": STRATEGY_ID, "loader_revision": LOADER_REVISION,
            "audit_role": "LONG_MA_QUALIFIED_EVENT_CONTEXT_ONLY_NOT_A_GATE",
            "code": code, "canonical_squeeze_date": sq_date,
            "qualified_breakout_date": bo_date, "restart_date": restart_date,
            "probe_then_qualified": int(q.get("canonical_probe_then_qualified",0)),
            "joint_obv_positive_bb40_contracting": joint,
            "healthy": int(q.get("qualified_healthy_pullback_existing",0)),
            "restart": int(q.get("qualified_restart_existing",0)),
        }
        for k,v in ctx.items(): row[f"squeeze_{k}"] = v
        for k,v in boctx.items(): row[f"breakout_{k}"] = v
        for k,v in rctx.items(): row[f"restart_{k}"] = v
        for col in ("d5_close_ret_pct","d5_mfe_pct","d5_mae_pct","d10_close_ret_pct","d10_mfe_pct","d10_mae_pct","d15_close_ret_pct","d15_mfe_pct","d15_mae_pct","first_plus5_day","first_plus10_day","peak_day_within_15"):
            row[col] = _audit_num(path.get(col))
        row["future_path_fields_are_outcomes"] = 1
        row["used_as_strategy_gate"] = 0
        erows.append(row)

    candidates, events = pd.DataFrame(crows), pd.DataFrame(erows)
    if candidates.empty:
        return candidates, events, pd.DataFrame()

    def med(g,col):
        x = pd.to_numeric(g.get(col,pd.Series(dtype=float)),errors="coerce")
        return float(x.median()) if x.notna().any() else float("nan")
    def qrate(g):
        return float(pd.to_numeric(g["label_qualified_breakout"],errors="coerce").fillna(0).mean()) if len(g) else float("nan")

    rows = []
    ready = candidates[candidates["ma224_ready"].eq(1)]
    for label,g in [("SQUEEZE_BELOW_MA224",ready[ready["below_ma224"].eq(1)]),("SQUEEZE_AT_OR_ABOVE_MA224",ready[ready["below_ma224"].eq(0)])]:
        rows.append({"schema":SCHEMA,"strategy_id":STRATEGY_ID,"loader_revision":LOADER_REVISION,"summary_type":"SQUEEZE_CAUSAL_QUALIFIED_RATE","group":label,"runs":len(g),"qualified":int(g["label_qualified_breakout"].sum()),"qualified_rate":qrate(g),"healthy":int(g["label_healthy_pullback"].sum()),"restart":int(g["label_restart"].sum()),"used_as_strategy_gate":0})

    for bucket in ["LT_-15","-15_TO_-5","-5_TO_0","0_TO_+5","+5_TO_+15","GE_+15"]:
        g = ready[ready["ma224_distance_bucket"].eq(bucket)]
        rows.append({"schema":SCHEMA,"strategy_id":STRATEGY_ID,"loader_revision":LOADER_REVISION,"summary_type":"SQUEEZE_MA224_DISTANCE_BUCKET","group":bucket,"runs":len(g),"qualified":int(g["label_qualified_breakout"].sum()),"qualified_rate":qrate(g),"healthy":int(g["label_healthy_pullback"].sum()),"restart":int(g["label_restart"].sum()),"used_as_strategy_gate":0})

    for jv,jn in [(1,"JOINT"),(0,"REST")]:
        for bv,bn in [(1,"BELOW224"),(0,"AT_OR_ABOVE224")]:
            g=ready[ready["joint_obv_positive_bb40_contracting"].eq(jv)&ready["below_ma224"].eq(bv)]
            rows.append({"schema":SCHEMA,"strategy_id":STRATEGY_ID,"loader_revision":LOADER_REVISION,"summary_type":"SQUEEZE_JOINT_X_MA224","group":f"{jn}_{bn}","runs":len(g),"qualified":int(g["label_qualified_breakout"].sum()),"qualified_rate":qrate(g),"healthy":int(g["label_healthy_pullback"].sum()),"restart":int(g["label_restart"].sum()),"used_as_strategy_gate":0})

    if not events.empty:
        eready=events[events["breakout_ma224_ready"].eq(1)]
        for label,g in [("BREAKOUT_BELOW_MA224",eready[eready["breakout_below_ma224"].eq(1)]),("BREAKOUT_AT_OR_ABOVE_MA224",eready[eready["breakout_below_ma224"].eq(0)])]:
            rows.append({"schema":SCHEMA,"strategy_id":STRATEGY_ID,"loader_revision":LOADER_REVISION,"summary_type":"QUALIFIED_PATH_BY_BREAKOUT_MA224","group":label,"runs":len(g),"qualified":len(g),"qualified_rate":float("nan"),"healthy":int(g["healthy"].sum()),"restart":int(g["restart"].sum()),"d5_close_ret_median_pct":med(g,"d5_close_ret_pct"),"d5_mfe_median_pct":med(g,"d5_mfe_pct"),"d5_mae_median_pct":med(g,"d5_mae_pct"),"d10_close_ret_median_pct":med(g,"d10_close_ret_pct"),"d10_mfe_median_pct":med(g,"d10_mfe_pct"),"d10_mae_median_pct":med(g,"d10_mae_pct"),"d15_close_ret_median_pct":med(g,"d15_close_ret_pct"),"d15_mfe_median_pct":med(g,"d15_mfe_pct"),"d15_mae_median_pct":med(g,"d15_mae_pct"),"used_as_strategy_gate":0})

    rows.append({"schema":SCHEMA,"strategy_id":STRATEGY_ID,"loader_revision":LOADER_REVISION,"summary_type":"MA_COVERAGE","group":"ALL","runs":len(candidates),"ma120_ready":int(candidates["ma120_ready"].sum()),"ma200_ready":int(candidates["ma200_ready"].sum()),"ma224_ready":int(candidates["ma224_ready"].sum()),"qualified_event_rows":len(events),"qualified_breakout_ma224_ready":int(events["breakout_ma224_ready"].sum()) if not events.empty else 0,"used_as_strategy_gate":0})
    return candidates, events, pd.DataFrame(rows)



def _r112_anchor_path(
    code: str,
    anchor_name: str,
    anchor_date: str,
    df: pd.DataFrame,
    event_meta: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Track D+1..D+15 from a stage anchor close. Future outcome only."""
    if df is None or df.empty or not anchor_date:
        return [], {}
    at = pd.to_datetime(anchor_date, errors="coerce")
    if pd.isna(at):
        return [], {}
    dates = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    am = df.index[dates.eq(pd.Timestamp(at).normalize())]
    if not len(am):
        return [], {}
    ai = int(am[-1])
    base = _audit_num(df.iloc[ai].get("close"))
    if not math.isfinite(base) or base <= 0:
        return [], {}

    rows: List[Dict[str, Any]] = []
    endpoints: Dict[int, Dict[str, Any]] = {}
    cum_high, cum_low = base, base
    first5 = first10 = -1
    peak_day = trough_day = -1
    peak_ret, trough_ret = float("-inf"), float("inf")
    available = 0

    for d in range(1, 16):
        idx = ai + d
        if idx >= len(df):
            break
        rr = df.iloc[idx]
        available = d
        close = _audit_num(rr.get("close"))
        high = _audit_num(rr.get("high"))
        low = _audit_num(rr.get("low"))
        if math.isfinite(high):
            cum_high = max(cum_high, high)
        if math.isfinite(low):
            cum_low = min(cum_low, low)
        close_ret = ((close / base) - 1.0) * 100.0 if math.isfinite(close) else float("nan")
        high_ret = ((high / base) - 1.0) * 100.0 if math.isfinite(high) else float("nan")
        low_ret = ((low / base) - 1.0) * 100.0 if math.isfinite(low) else float("nan")
        mfe = ((cum_high / base) - 1.0) * 100.0
        mae = ((cum_low / base) - 1.0) * 100.0
        if first5 < 0 and mfe >= 5.0:
            first5 = d
        if first10 < 0 and mfe >= 10.0:
            first10 = d
        if math.isfinite(high_ret) and high_ret > peak_ret:
            peak_ret, peak_day = high_ret, d
        if math.isfinite(low_ret) and low_ret < trough_ret:
            trough_ret, trough_day = low_ret, d

        rec = {
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "audit_role": "STAGE_ANCHOR_D1_D15_FUTURE_OUTCOME_ONLY_NOT_A_GATE",
            "code": code,
            "anchor": anchor_name,
            "anchor_date": pd.Timestamp(at).date().isoformat(),
            "day_after_anchor": d,
            "path_date": pd.Timestamp(rr["date"]).date().isoformat(),
            "anchor_close": base,
            "close_ret_from_anchor_pct": close_ret,
            "daily_high_ret_from_anchor_pct": high_ret,
            "daily_low_ret_from_anchor_pct": low_ret,
            "cumulative_mfe_pct": mfe,
            "cumulative_mae_pct": mae,
            **event_meta,
            "future_outcome_only": 1,
            "used_as_strategy_gate": 0,
        }
        rows.append(rec)
        endpoints[d] = rec

    def ep(day: int, field: str) -> float:
        r = endpoints.get(day)
        return _audit_num(r.get(field)) if r else float("nan")

    summary = {
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "audit_role": "STAGE_ANCHOR_EVENT_SUMMARY_FUTURE_ONLY",
        "code": code,
        "anchor": anchor_name,
        "anchor_date": pd.Timestamp(at).date().isoformat(),
        "available_post_anchor_bars": available,
        "d15_complete": int(available >= 15),
        "first_plus5_day": first5,
        "first_plus10_day": first10,
        "peak_day_within_15": peak_day,
        "peak_high_ret_within_15_pct": peak_ret if peak_day > 0 else float("nan"),
        "trough_day_within_15": trough_day,
        "trough_low_ret_within_15_pct": trough_ret if trough_day > 0 else float("nan"),
        "d5_close_ret_pct": ep(5, "close_ret_from_anchor_pct"),
        "d5_mfe_pct": ep(5, "cumulative_mfe_pct"),
        "d5_mae_pct": ep(5, "cumulative_mae_pct"),
        "d10_close_ret_pct": ep(10, "close_ret_from_anchor_pct"),
        "d10_mfe_pct": ep(10, "cumulative_mfe_pct"),
        "d10_mae_pct": ep(10, "cumulative_mae_pct"),
        "d15_close_ret_pct": ep(15, "close_ret_from_anchor_pct"),
        "d15_mfe_pct": ep(15, "cumulative_mfe_pct"),
        "d15_mae_pct": ep(15, "cumulative_mae_pct"),
        **event_meta,
        "future_outcome_only": 1,
        "used_as_strategy_gate": 0,
    }
    return rows, summary


def build_stage_anchor_path_audit(
    qualified_event_detail: pd.DataFrame,
    precursor_detail: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compare future paths from QUALIFIED, HEALTHY and RESTART stage closes.

    HEALTHY and RESTART are future stage anchors relative to the squeeze and
    therefore this entire lane is outcome research, never a candidate feature.
    """
    if qualified_event_detail is None or qualified_event_detail.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    pkey: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if precursor_detail is not None and not precursor_detail.empty:
        for _, p in precursor_detail.iterrows():
            pkey[(str(p.get("code") or "").zfill(6), str(p.get("squeeze_date") or ""))] = p.to_dict()

    detail_rows: List[Dict[str, Any]] = []
    event_rows: List[Dict[str, Any]] = []

    for _, er in qualified_event_detail.iterrows():
        code = str(er.get("code") or "").zfill(6)
        sq_date = str(er.get("canonical_squeeze_date") or "")
        df = _r111_get_frame(frames, code)
        if df is None or df.empty:
            continue
        pr = pkey.get((code, sq_date), {})
        joint = int(int(pr.get("pre10_obv_positive",0)) == 1 and int(pr.get("bb40_contracting",0)) == 1)

        meta = {
            "canonical_squeeze_date": sq_date,
            "qualified_breakout_date": str(er.get("qualified_breakout_date") or ""),
            "probe_then_qualified": int(er.get("canonical_probe_then_qualified",0)),
            "joint_obv_positive_bb40_contracting": joint,
            "healthy_event": int(er.get("qualified_healthy_pullback_existing",0)),
            "restart_event": int(er.get("qualified_restart_existing",0)),
        }

        anchors = [
            ("QUALIFIED", str(er.get("qualified_breakout_date") or "")),
        ]
        if int(er.get("qualified_healthy_pullback_existing",0)):
            anchors.append(("HEALTHY", str(er.get("qualified_healthy_pullback_date") or "")))
        if int(er.get("qualified_restart_existing",0)):
            anchors.append(("RESTART", str(er.get("qualified_restart_date") or "")))

        for anchor_name, anchor_date in anchors:
            rows, ev = _r112_anchor_path(code, anchor_name, anchor_date, df, meta)
            detail_rows.extend(rows)
            if ev:
                # causal MA context at the anchor date itself, still descriptive
                dates = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
                at = pd.to_datetime(anchor_date, errors="coerce")
                am = df.index[dates.eq(pd.Timestamp(at).normalize())] if pd.notna(at) else []
                ctx = _r111_ma_context(df, int(am[-1])) if len(am) else _r111_ma_context(None, -1)
                for k,v in ctx.items():
                    ev[f"anchor_{k}"] = v
                event_rows.append(ev)

    detail = pd.DataFrame(detail_rows)
    events = pd.DataFrame(event_rows)
    if events.empty:
        return detail, events, pd.DataFrame()

    def med(g: pd.DataFrame, col: str) -> float:
        x = pd.to_numeric(g.get(col, pd.Series(dtype=float)), errors="coerce")
        return float(x.median()) if x.notna().any() else float("nan")

    rows: List[Dict[str, Any]] = []
    for anchor_name in ("QUALIFIED","HEALTHY","RESTART"):
        g = events[events["anchor"].eq(anchor_name)]
        if g.empty:
            continue
        rows.append({
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "anchor": anchor_name,
            "events": int(len(g)),
            "d15_complete_events": int(pd.to_numeric(g["d15_complete"],errors="coerce").fillna(0).sum()),
            "plus5_within15": int((pd.to_numeric(g["first_plus5_day"],errors="coerce") > 0).sum()),
            "plus10_within15": int((pd.to_numeric(g["first_plus10_day"],errors="coerce") > 0).sum()),
            "median_first_plus5_day": med(g[g["first_plus5_day"]>0],"first_plus5_day"),
            "median_first_plus10_day": med(g[g["first_plus10_day"]>0],"first_plus10_day"),
            "median_peak_day_within15": med(g,"peak_day_within_15"),
            "d5_close_ret_median_pct": med(g,"d5_close_ret_pct"),
            "d5_mfe_median_pct": med(g,"d5_mfe_pct"),
            "d5_mae_median_pct": med(g,"d5_mae_pct"),
            "d10_close_ret_median_pct": med(g,"d10_close_ret_pct"),
            "d10_mfe_median_pct": med(g,"d10_mfe_pct"),
            "d10_mae_median_pct": med(g,"d10_mae_pct"),
            "d15_close_ret_median_pct": med(g,"d15_close_ret_pct"),
            "d15_mfe_median_pct": med(g,"d15_mfe_pct"),
            "d15_mae_median_pct": med(g,"d15_mae_pct"),
            "ma224_ready_events": int(pd.to_numeric(g["anchor_ma224_ready"],errors="coerce").fillna(0).sum()),
            "below_ma224_events": int(
                pd.to_numeric(g.loc[g["anchor_ma224_ready"].eq(1),"anchor_below_ma224"],errors="coerce").fillna(0).sum()
            ),
            "future_outcome_only": 1,
            "used_as_strategy_gate": 0,
        })

    # Restart-specific MA224 split, because restart is the intended conservative entry stage.
    rg = events[events["anchor"].eq("RESTART") & events["anchor_ma224_ready"].eq(1)]
    for label, g in [
        ("RESTART_BELOW_MA224", rg[rg["anchor_below_ma224"].eq(1)]),
        ("RESTART_AT_OR_ABOVE_MA224", rg[rg["anchor_below_ma224"].eq(0)]),
    ]:
        if g.empty:
            continue
        rows.append({
            "schema": SCHEMA, "strategy_id": STRATEGY_ID, "loader_revision": LOADER_REVISION,
            "anchor": label, "events": int(len(g)),
            "d15_complete_events": int(g["d15_complete"].sum()),
            "plus5_within15": int((g["first_plus5_day"]>0).sum()),
            "plus10_within15": int((g["first_plus10_day"]>0).sum()),
            "median_first_plus5_day": med(g[g["first_plus5_day"]>0],"first_plus5_day"),
            "median_first_plus10_day": med(g[g["first_plus10_day"]>0],"first_plus10_day"),
            "median_peak_day_within15": med(g,"peak_day_within_15"),
            "d5_close_ret_median_pct": med(g,"d5_close_ret_pct"),
            "d5_mfe_median_pct": med(g,"d5_mfe_pct"),
            "d5_mae_median_pct": med(g,"d5_mae_pct"),
            "d10_close_ret_median_pct": med(g,"d10_close_ret_pct"),
            "d10_mfe_median_pct": med(g,"d10_mfe_pct"),
            "d10_mae_median_pct": med(g,"d10_mae_pct"),
            "d15_close_ret_median_pct": med(g,"d15_close_ret_pct"),
            "d15_mfe_median_pct": med(g,"d15_mfe_pct"),
            "d15_mae_median_pct": med(g,"d15_mae_pct"),
            "ma224_ready_events": int(len(g)),
            "below_ma224_events": int(g["anchor_below_ma224"].sum()),
            "future_outcome_only": 1, "used_as_strategy_gate": 0,
        })

    return detail, events, pd.DataFrame(rows)


def build_conversion_vs_quality_audit(
    precursor_detail: pd.DataFrame,
    qualified_path_events: pd.DataFrame,
) -> pd.DataFrame:
    """Separate precursor conversion probability from conditional path quality."""
    if precursor_detail is None or precursor_detail.empty:
        return pd.DataFrame()

    p = precursor_detail.copy()
    p["joint"] = (
        p["pre10_obv_positive"].eq(1) & p["bb40_contracting"].eq(1)
    ).astype(int)

    q = qualified_path_events.copy() if qualified_path_events is not None else pd.DataFrame()
    rows: List[Dict[str, Any]] = []

    def med(g: pd.DataFrame, col: str) -> float:
        x = pd.to_numeric(g.get(col, pd.Series(dtype=float)), errors="coerce")
        return float(x.median()) if x.notna().any() else float("nan")

    for jv, label in [(1,"OBV+_AND_BB40DOWN"),(0,"REST")]:
        pg = p[p["joint"].eq(jv)]
        qg = q[q["joint_obv_positive_bb40_contracting"].eq(jv)] if not q.empty else pd.DataFrame()
        rows.append({
            "schema": SCHEMA, "strategy_id": STRATEGY_ID, "loader_revision": LOADER_REVISION,
            "group": label,
            "candidate_runs": int(len(pg)),
            "qualified_runs": int(pd.to_numeric(pg["label_qualified_breakout"],errors="coerce").fillna(0).sum()),
            "conversion_rate": float(pd.to_numeric(pg["label_qualified_breakout"],errors="coerce").fillna(0).mean()) if len(pg) else float("nan"),
            "unique_qualified_events": int(len(qg)),
            "conditional_plus5_within15": int((pd.to_numeric(qg.get("first_plus5_day",pd.Series(dtype=float)),errors="coerce")>0).sum()),
            "conditional_plus10_within15": int((pd.to_numeric(qg.get("first_plus10_day",pd.Series(dtype=float)),errors="coerce")>0).sum()),
            "conditional_healthy": int(pd.to_numeric(qg.get("healthy",pd.Series(dtype=float)),errors="coerce").fillna(0).sum()),
            "conditional_restart": int(pd.to_numeric(qg.get("restart",pd.Series(dtype=float)),errors="coerce").fillna(0).sum()),
            "conditional_d15_close_median_pct": med(qg,"d15_close_ret_pct"),
            "conditional_d15_mfe_median_pct": med(qg,"d15_mfe_pct"),
            "conditional_d15_mae_median_pct": med(qg,"d15_mae_pct"),
            "interpretation_role": "CONVERSION_AND_CONDITIONAL_QUALITY_MUST_REMAIN_SEPARATE",
            "used_as_strategy_gate": 0,
        })
    return pd.DataFrame(rows)



def build_stage_quality_anatomy_audit(
    qualified_event_detail: pd.DataFrame,
    stage_anchor_event_summary: pd.DataFrame,
    stage_anchor_path_detail: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
    cfg: FrozenConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Audit why HEALTHY and RESTART anchors do or do not behave like entries.

    No field in this lane is promoted into detect_code(). HEALTHY lower-low
    fields use future bars and are outcome taxonomy only. RESTART descriptors
    use information available at the restart close; the wave-high-reclaim
    hypothesis is explicitly post-hoc on this same research set.
    """
    if qualified_event_detail is None or qualified_event_detail.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    qmap: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for _, q in qualified_event_detail.iterrows():
        key = (
            str(q.get("code") or "").zfill(6),
            str(q.get("canonical_squeeze_date") or ""),
            str(q.get("qualified_breakout_date") or ""),
        )
        qmap[key] = q.to_dict()

    healthy_rows: List[Dict[str, Any]] = []
    restart_rows: List[Dict[str, Any]] = []

    for _, ev in stage_anchor_event_summary.iterrows():
        anchor_name = str(ev.get("anchor") or "")
        if anchor_name not in ("HEALTHY", "RESTART"):
            continue
        code = str(ev.get("code") or "").zfill(6)
        sq = str(ev.get("canonical_squeeze_date") or "")
        qdate = str(ev.get("qualified_breakout_date") or "")
        qmeta = qmap.get((code, sq, qdate))
        if not qmeta:
            continue
        df = _r111_get_frame(frames, code)
        if df is None or df.empty:
            continue

        dates = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        qt = pd.to_datetime(qdate, errors="coerce")
        at = pd.to_datetime(ev.get("anchor_date"), errors="coerce")
        qm = df.index[dates.eq(pd.Timestamp(qt).normalize())] if pd.notna(qt) else []
        am = df.index[dates.eq(pd.Timestamp(at).normalize())] if pd.notna(at) else []
        if not len(qm) or not len(am):
            continue
        qi = int(qm[-1]); ai = int(am[-1])
        qrow = df.iloc[qi]; arow = df.iloc[ai]
        qclose = _audit_num(qrow.get("close"))
        aclose = _audit_num(arow.get("close"))

        base = {
            "schema": SCHEMA, "strategy_id": STRATEGY_ID, "loader_revision": LOADER_REVISION,
            "code": code, "canonical_squeeze_date": sq,
            "qualified_breakout_date": qdate,
            "anchor_date": pd.Timestamp(at).date().isoformat(),
            "probe_then_qualified": int(ev.get("probe_then_qualified",0)),
            "joint_obv_positive_bb40_contracting": int(ev.get("joint_obv_positive_bb40_contracting",0)),
            "anchor_close_vs_ma224_pct": _audit_num(ev.get("anchor_close_vs_ma224_pct")),
            "anchor_below_ma224": int(ev.get("anchor_below_ma224",0)),
            "anchor_ma224_ready": int(ev.get("anchor_ma224_ready",0)),
            "d5_close_ret_pct": _audit_num(ev.get("d5_close_ret_pct")),
            "d5_mfe_pct": _audit_num(ev.get("d5_mfe_pct")),
            "d5_mae_pct": _audit_num(ev.get("d5_mae_pct")),
            "d10_close_ret_pct": _audit_num(ev.get("d10_close_ret_pct")),
            "d10_mfe_pct": _audit_num(ev.get("d10_mfe_pct")),
            "d10_mae_pct": _audit_num(ev.get("d10_mae_pct")),
            "d15_close_ret_pct": _audit_num(ev.get("d15_close_ret_pct")),
            "d15_mfe_pct": _audit_num(ev.get("d15_mfe_pct")),
            "d15_mae_pct": _audit_num(ev.get("d15_mae_pct")),
            "used_as_strategy_gate": 0,
        }

        if anchor_name == "HEALTHY":
            alow = _audit_num(arow.get("low"))
            seg = df.iloc[ai+1:min(len(df), ai+6)].copy()
            lower_low = 0
            min_low_ret = float("nan")
            min_low_day = -1
            if not seg.empty and math.isfinite(aclose) and aclose > 0:
                lows = pd.to_numeric(seg["low"], errors="coerce")
                if lows.notna().any():
                    min_pos = int(np.nanargmin(lows.to_numpy(dtype=float)))
                    min_low = float(lows.iloc[min_pos])
                    min_low_ret = ((min_low / aclose) - 1.0) * 100.0
                    min_low_day = min_pos + 1
                    lower_low = int(math.isfinite(alow) and min_low < alow)

            healthy_rows.append({
                **base,
                "audit_role": "HEALTHY_BOTTOMING_ANATOMY_FUTURE_TAXONOMY_ONLY",
                "q_to_healthy_bars": int(ai - qi),
                "healthy_close_vs_qualified_close_pct": (
                    ((aclose / qclose) - 1.0) * 100.0
                    if math.isfinite(aclose) and aclose > 0 and math.isfinite(qclose) and qclose > 0
                    else float("nan")
                ),
                "lower_low_within5_after_healthy": lower_low,
                "min_low_within5_ret_from_healthy_close_pct": min_low_ret,
                "day_of_min_low_within5": min_low_day,
                "future_bottoming_label_only": 1,
            })
            continue

        # RESTART anatomy: descriptors use data known by restart close.
        hdate = pd.to_datetime(qmeta.get("qualified_healthy_pullback_date"), errors="coerce")
        hm = df.index[dates.eq(pd.Timestamp(hdate).normalize())] if pd.notna(hdate) else []
        hi = int(hm[-1]) if len(hm) else -1
        hclose = _audit_num(df.iloc[hi].get("close")) if hi >= 0 else float("nan")
        prev = df.iloc[ai-1] if ai > 0 else None

        # Previous wave high excludes the restart bar itself.
        pre_wave_high = float("nan")
        if ai > qi:
            hh = pd.to_numeric(df.iloc[qi:ai]["high"], errors="coerce")
            if hh.notna().any():
                pre_wave_high = float(hh.max())

        amount = _audit_num(arow.get("amount"))
        amt20, amt_obs = amount20_stats(df, ai, cfg)
        amt20_ratio = (
            amount / amt20
            if math.isfinite(amount) and amount > 0 and math.isfinite(amt20) and amt20 > 0
            else float("nan")
        )
        high = _audit_num(arow.get("high")); low = _audit_num(arow.get("low"))
        close_location = (
            (aclose - low) / (high - low)
            if all(math.isfinite(x) for x in (aclose,high,low)) and high > low
            else float("nan")
        )
        prev_high = _audit_num(prev.get("high")) if prev is not None else float("nan")
        prev_close = _audit_num(prev.get("close")) if prev is not None else float("nan")
        aopen = _audit_num(arow.get("open"))

        restart_rows.append({
            **base,
            "audit_role": "RESTART_REACCELERATION_ANATOMY_CAUSAL_AT_RESTART_OUTCOME_AFTER",
            "q_to_restart_bars": int(ai - qi),
            "healthy_to_restart_bars": int(ai - hi) if hi >= 0 else -1,
            "restart_close_vs_qualified_close_pct": (
                ((aclose/qclose)-1.0)*100.0
                if math.isfinite(aclose) and aclose>0 and math.isfinite(qclose) and qclose>0
                else float("nan")
            ),
            "restart_close_vs_healthy_close_pct": (
                ((aclose/hclose)-1.0)*100.0
                if math.isfinite(aclose) and aclose>0 and math.isfinite(hclose) and hclose>0
                else float("nan")
            ),
            "pre_restart_wave_high": pre_wave_high,
            "restart_close_vs_pre_wave_high_pct": (
                ((aclose/pre_wave_high)-1.0)*100.0
                if math.isfinite(aclose) and aclose>0 and math.isfinite(pre_wave_high) and pre_wave_high>0
                else float("nan")
            ),
            "restart_reclaim_pre_wave_high": int(
                math.isfinite(pre_wave_high) and math.isfinite(aclose) and aclose >= pre_wave_high
            ),
            "restart_amount20_mean_prior": amt20,
            "restart_amount20_observations": int(amt_obs),
            "restart_amount20_ratio": amt20_ratio,
            "restart_amount20_ge_1x": int(math.isfinite(amt20_ratio) and amt20_ratio >= 1.0),
            "restart_amount20_ge_breakout_1_5x": int(
                math.isfinite(amt20_ratio) and amt20_ratio >= cfg.breakout_min_amount20_ratio
            ),
            "restart_amount_vs_pullback_median_existing": _audit_num(
                qmeta.get("qualified_restart_amount_vs_pullback_median")
            ),
            "restart_close_location_in_bar": close_location,
            "restart_close_over_prev_high_pct": (
                ((aclose/prev_high)-1.0)*100.0
                if math.isfinite(aclose) and aclose>0 and math.isfinite(prev_high) and prev_high>0
                else float("nan")
            ),
            "restart_open_gap_vs_prev_close_pct": (
                ((aopen/prev_close)-1.0)*100.0
                if math.isfinite(aopen) and aopen>0 and math.isfinite(prev_close) and prev_close>0
                else float("nan")
            ),
            "wave_high_reclaim_hypothesis_posthoc": 1,
            "future_outcome_fields_not_features": 1,
        })

    hdf = pd.DataFrame(healthy_rows)
    rdf = pd.DataFrame(restart_rows)

    def med(g: pd.DataFrame, col: str) -> float:
        x = pd.to_numeric(g.get(col,pd.Series(dtype=float)), errors="coerce")
        return float(x.median()) if x.notna().any() else float("nan")

    def summarize(category: str, group: str, g: pd.DataFrame, posthoc: int = 0) -> Dict[str, Any]:
        return {
            "schema": SCHEMA, "strategy_id": STRATEGY_ID, "loader_revision": LOADER_REVISION,
            "category": category, "group": group, "events": int(len(g)),
            "d15_positive": int((pd.to_numeric(g.get("d15_close_ret_pct",pd.Series(dtype=float)),errors="coerce")>0).sum()),
            "d5_close_ret_median_pct": med(g,"d5_close_ret_pct"),
            "d10_close_ret_median_pct": med(g,"d10_close_ret_pct"),
            "d15_close_ret_median_pct": med(g,"d15_close_ret_pct"),
            "d15_mfe_median_pct": med(g,"d15_mfe_pct"),
            "d15_mae_median_pct": med(g,"d15_mae_pct"),
            "posthoc_hypothesis": int(posthoc),
            "used_as_strategy_gate": 0,
        }

    rows: List[Dict[str, Any]] = []
    if not hdf.empty:
        rows.append(summarize("HEALTHY_BOTTOMING","ALL_HEALTHY",hdf))
        g1 = hdf[hdf["lower_low_within5_after_healthy"].eq(1)]
        g0 = hdf[hdf["lower_low_within5_after_healthy"].eq(0)]
        r1 = summarize("HEALTHY_BOTTOMING","LOWER_LOW_WITHIN5",g1)
        r0 = summarize("HEALTHY_BOTTOMING","NO_LOWER_LOW_WITHIN5",g0)
        r1["future_taxonomy_only"] = 1; r0["future_taxonomy_only"] = 1
        rows.extend([r1,r0])

    if not rdf.empty:
        rows.append(summarize("RESTART_REACCELERATION","ALL_RESTART",rdf))
        rows.append(summarize(
            "RESTART_REACCELERATION","RECLAIM_PRE_WAVE_HIGH",
            rdf[rdf["restart_reclaim_pre_wave_high"].eq(1)],1
        ))
        rows.append(summarize(
            "RESTART_REACCELERATION","NO_RECLAIM_PRE_WAVE_HIGH",
            rdf[rdf["restart_reclaim_pre_wave_high"].eq(0)],1
        ))
        rows.append(summarize(
            "RESTART_REACCELERATION","AMOUNT20_GE_BREAKOUT_1_5X",
            rdf[rdf["restart_amount20_ge_breakout_1_5x"].eq(1)]
        ))
        rows.append(summarize(
            "RESTART_REACCELERATION","AMOUNT20_LT_BREAKOUT_1_5X",
            rdf[rdf["restart_amount20_ge_breakout_1_5x"].eq(0)]
        ))
        rows.append(summarize(
            "RESTART_REACCELERATION","DIRECT",
            rdf[rdf["probe_then_qualified"].eq(0)]
        ))
        rows.append(summarize(
            "RESTART_REACCELERATION","PROBE_TO_QUALIFIED",
            rdf[rdf["probe_then_qualified"].eq(1)]
        ))

    return hdf, rdf, pd.DataFrame(rows)



def build_restart_reclaim_robustness_audit(
    healthy_bottoming_detail: pd.DataFrame,
    restart_reacceleration_detail: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """Robustness audit for one frozen post-hoc structural hypothesis.

    Hypothesis under audit:
      restart close >= prior wave high (prior wave excludes restart bar)

    The rule is natural/structural and has no tuned numeric threshold, but it
    was discovered on the same research set. Therefore every row remains
    audit-only and cannot change detect_code().

    Also report the already-observed DIRECT/PROBE difference in HEALTHY
    bottoming without promoting it.
    """
    rows: List[Dict[str, Any]] = []

    def med(g: pd.DataFrame, col: str) -> float:
        x = pd.to_numeric(g.get(col, pd.Series(dtype=float)), errors="coerce")
        return float(x.median()) if x.notna().any() else float("nan")

    def base_summary(category: str, group: str, g: pd.DataFrame) -> Dict[str, Any]:
        return {
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "category": category,
            "group": group,
            "events": int(len(g)),
            "d15_positive": int((pd.to_numeric(g.get("d15_close_ret_pct", pd.Series(dtype=float)), errors="coerce") > 0).sum()),
            "d5_close_ret_median_pct": med(g, "d5_close_ret_pct"),
            "d10_close_ret_median_pct": med(g, "d10_close_ret_pct"),
            "d15_close_ret_median_pct": med(g, "d15_close_ret_pct"),
            "d15_mfe_median_pct": med(g, "d15_mfe_pct"),
            "d15_mae_median_pct": med(g, "d15_mae_pct"),
            "hypothesis_posthoc_on_same_research_period": 1,
            "used_as_strategy_gate": 0,
        }

    r = restart_reacceleration_detail.copy() if restart_reacceleration_detail is not None else pd.DataFrame()
    if not r.empty:
        r["anchor_date_ts"] = pd.to_datetime(r["anchor_date"], errors="coerce")
        start_ts = pd.Timestamp(start).normalize()
        end_ts = pd.Timestamp(end).normalize()
        midpoint = start_ts + (end_ts - start_ts) / 2
        r["temporal_half"] = np.where(r["anchor_date_ts"] <= midpoint, "H1_EARLY", "H2_LATE")

        # Full and fixed temporal halves.
        for split, sg in [
            ("FULL", r),
            ("H1_EARLY", r[r["temporal_half"].eq("H1_EARLY")]),
            ("H2_LATE", r[r["temporal_half"].eq("H2_LATE")]),
        ]:
            for val, name in [(1, "RECLAIM"), (0, "NO_RECLAIM")]:
                g = sg[sg["restart_reclaim_pre_wave_high"].eq(val)]
                rec = base_summary("RESTART_RECLAIM_TEMPORAL", f"{split}_{name}", g)
                rec["split"] = split
                rows.append(rec)

        # Stratify by direct vs probe-qualified so reclaim is not confused with
        # breakout chronology phenotype.
        for probe, pname in [(0, "DIRECT"), (1, "PROBE_TO_QUALIFIED")]:
            sg = r[r["probe_then_qualified"].eq(probe)]
            for val, name in [(1, "RECLAIM"), (0, "NO_RECLAIM")]:
                g = sg[sg["restart_reclaim_pre_wave_high"].eq(val)]
                rows.append(base_summary("RESTART_RECLAIM_BY_BREAKOUT_TYPE", f"{pname}_{name}", g))

        # Amount interaction. The breakout 1.5x level is an existing frozen
        # reference, not a newly tuned threshold.
        for reclaim, rname in [(1,"RECLAIM"),(0,"NO_RECLAIM")]:
            for amt, aname in [(1,"AMOUNT20_GE_1_5X"),(0,"AMOUNT20_LT_1_5X")]:
                g = r[
                    r["restart_reclaim_pre_wave_high"].eq(reclaim)
                    & r["restart_amount20_ge_breakout_1_5x"].eq(amt)
                ]
                if len(g):
                    rows.append(base_summary("RESTART_RECLAIM_X_AMOUNT", f"{rname}_{aname}", g))

        # Leave-one-event-out median sensitivity. This avoids a single event
        # (including a large limit-move sequence) determining the conclusion.
        for val, name in [(1,"RECLAIM"),(0,"NO_RECLAIM")]:
            g = r[r["restart_reclaim_pre_wave_high"].eq(val)].copy()
            vals = pd.to_numeric(g["d15_close_ret_pct"], errors="coerce").dropna().to_numpy(dtype=float)
            if len(vals) >= 2:
                loo = [float(np.median(np.delete(vals, i))) for i in range(len(vals))]
                rec = base_summary("RESTART_RECLAIM_LOO", name, g)
                rec["loo_d15_median_min_pct"] = float(min(loo))
                rec["loo_d15_median_max_pct"] = float(max(loo))
                rec["loo_d15_all_positive"] = int(all(x > 0 for x in loo))
                rec["loo_d15_all_negative"] = int(all(x < 0 for x in loo))
                rows.append(rec)

    # HEALTHY: causal history tag DIRECT/PROBE vs future lower-low taxonomy.
    h = healthy_bottoming_detail.copy() if healthy_bottoming_detail is not None else pd.DataFrame()
    if not h.empty:
        for probe, pname in [(0,"DIRECT"),(1,"PROBE_TO_QUALIFIED")]:
            g = h[h["probe_then_qualified"].eq(probe)]
            n = int(len(g))
            ll = int(pd.to_numeric(g.get("lower_low_within5_after_healthy", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
            rec = base_summary("HEALTHY_BOTTOMING_BY_BREAKOUT_TYPE", pname, g)
            rec["lower_low_within5_events"] = ll
            rec["lower_low_within5_rate"] = float(ll / n) if n else float("nan")
            rec["future_lower_low_is_taxonomy_only"] = 1
            rows.append(rec)

    return pd.DataFrame(rows)



def build_r2_candidate_prospective_shadow_audit(
    restart_reacceleration_detail: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Freeze one R2 candidate hypothesis and separate discovery from OOS.

    Candidate definition is intentionally simple and uses only information
    available by the existing RESTART close:

      - research streak1 squeeze episode (counterfactual research lane);
      - first fully QUALIFIED breakout within the existing 8-bar horizon,
        allowing an earlier price-only probe;
      - existing HEALTHY pullback state;
      - existing RESTART state;
      - RESTART close >= prior wave high, excluding the restart bar.

    No OBV/BB40, MA, terminal-dry, Amount rank, or new numeric threshold is
    added. The existing restart amount-vs-pullback condition remains inherited.

    Events through 2026-08-30 are discovery/pre-freeze and are NEVER counted as
    prospective validation. Only event dates on/after 2026-08-31 enter the OOS
    cohort. Future outcome columns are evaluated only after they mature.
    """
    definition = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "candidate_id": R2_CANDIDATE_ID,
        "freeze_date": R2_CANDIDATE_FREEZE_DATE,
        "prospective_start_date": R2_CANDIDATE_PROSPECTIVE_START_DATE,
        "research_only": 1,
        "actual_strategy_changed": 0,
        "squeeze_research_streak": 1,
        "qualified_breakout_mode": "FIRST_FULLY_QUALIFIED_WITHIN_EXISTING_8BAR_HORIZON_ALLOW_EARLIER_PRICE_PROBE",
        "qualified_actual_amount20_min_ratio": CONFIG.breakout_min_amount20_ratio,
        "healthy_definition": "EXISTING_R1_HEALTHY_UNCHANGED",
        "restart_definition": "EXISTING_R1_RESTART_UNCHANGED_PLUS_RESTART_CLOSE_GE_PRIOR_WAVE_HIGH",
        "prior_wave_high_excludes_restart_bar": 1,
        "obv_bb40_gate": 0,
        "ma_gate": 0,
        "terminal_dry_gate": 0,
        "amount_rank_prefilter": 0,
        "used_as_actual_strategy_gate": 0,
    }])

    if restart_reacceleration_detail is None or restart_reacceleration_detail.empty:
        summary = pd.DataFrame([{
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "candidate_id": R2_CANDIDATE_ID,
            "freeze_date": R2_CANDIDATE_FREEZE_DATE,
            "prospective_start_date": R2_CANDIDATE_PROSPECTIVE_START_DATE,
            "discovery_candidate_events": 0,
            "prospective_candidate_events": 0,
            "prospective_d5_mature": 0,
            "prospective_d10_mature": 0,
            "prospective_d15_mature": 0,
            "prospective_d15_positive": 0,
            "promotion_status": "WAIT_PROSPECTIVE_OOS",
            "used_as_actual_strategy_gate": 0,
        }])
        return definition, pd.DataFrame(), summary

    d = restart_reacceleration_detail.copy()
    d = d[d["restart_reclaim_pre_wave_high"].eq(1)].copy()
    if d.empty:
        summary = pd.DataFrame([{
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "candidate_id": R2_CANDIDATE_ID,
            "freeze_date": R2_CANDIDATE_FREEZE_DATE,
            "prospective_start_date": R2_CANDIDATE_PROSPECTIVE_START_DATE,
            "discovery_candidate_events": 0,
            "prospective_candidate_events": 0,
            "prospective_d5_mature": 0,
            "prospective_d10_mature": 0,
            "prospective_d15_mature": 0,
            "prospective_d15_positive": 0,
            "prospective_d5_close_median_pct": float("nan"),
            "prospective_d10_close_median_pct": float("nan"),
            "prospective_d15_close_median_pct": float("nan"),
            "prospective_d15_mfe_median_pct": float("nan"),
            "prospective_d15_mae_median_pct": float("nan"),
            "promotion_status": "WAIT_PROSPECTIVE_OOS",
            "discovery_performance_excluded_from_promotion_decision": 1,
            "used_as_actual_strategy_gate": 0,
        }])
        return definition, d, summary

    d["candidate_id"] = R2_CANDIDATE_ID
    d["freeze_date"] = R2_CANDIDATE_FREEZE_DATE
    d["prospective_start_date"] = R2_CANDIDATE_PROSPECTIVE_START_DATE
    d["candidate_event_date"] = pd.to_datetime(d["anchor_date"], errors="coerce").dt.date.astype(str)

    pstart = pd.Timestamp(R2_CANDIDATE_PROSPECTIVE_START_DATE).normalize()
    event_ts = pd.to_datetime(d["anchor_date"], errors="coerce").dt.normalize()
    d["validation_cohort"] = np.where(
        event_ts >= pstart,
        "PROSPECTIVE_OOS",
        "DISCOVERY_OR_PRE_FREEZE_NOT_VALIDATION",
    )

    for horizon in (5, 10, 15):
        col = f"d{horizon}_close_ret_pct"
        d[f"d{horizon}_mature"] = pd.to_numeric(d[col], errors="coerce").notna().astype(int)

    d["prospective_validation_eligible"] = d["validation_cohort"].eq("PROSPECTIVE_OOS").astype(int)
    d["discovery_rows_never_counted_as_validation"] = 1
    d["used_as_actual_strategy_gate"] = 0

    prospect = d[d["validation_cohort"].eq("PROSPECTIVE_OOS")].copy()

    def med(frame: pd.DataFrame, col: str) -> float:
        x = pd.to_numeric(frame.get(col, pd.Series(dtype=float)), errors="coerce")
        return float(x.median()) if x.notna().any() else float("nan")

    d15m = prospect[prospect["d15_mature"].eq(1)] if not prospect.empty else prospect
    summary = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "candidate_id": R2_CANDIDATE_ID,
        "freeze_date": R2_CANDIDATE_FREEZE_DATE,
        "prospective_start_date": R2_CANDIDATE_PROSPECTIVE_START_DATE,
        "discovery_candidate_events": int(d["validation_cohort"].ne("PROSPECTIVE_OOS").sum()),
        "prospective_candidate_events": int(len(prospect)),
        "prospective_d5_mature": int(prospect["d5_mature"].sum()) if not prospect.empty else 0,
        "prospective_d10_mature": int(prospect["d10_mature"].sum()) if not prospect.empty else 0,
        "prospective_d15_mature": int(prospect["d15_mature"].sum()) if not prospect.empty else 0,
        "prospective_d15_positive": int(
            (pd.to_numeric(d15m.get("d15_close_ret_pct", pd.Series(dtype=float)), errors="coerce") > 0).sum()
        ) if not d15m.empty else 0,
        "prospective_d5_close_median_pct": med(prospect[prospect["d5_mature"].eq(1)], "d5_close_ret_pct") if not prospect.empty else float("nan"),
        "prospective_d10_close_median_pct": med(prospect[prospect["d10_mature"].eq(1)], "d10_close_ret_pct") if not prospect.empty else float("nan"),
        "prospective_d15_close_median_pct": med(d15m, "d15_close_ret_pct") if not d15m.empty else float("nan"),
        "prospective_d15_mfe_median_pct": med(d15m, "d15_mfe_pct") if not d15m.empty else float("nan"),
        "prospective_d15_mae_median_pct": med(d15m, "d15_mae_pct") if not d15m.empty else float("nan"),
        "promotion_status": "WAIT_PROSPECTIVE_OOS",
        "discovery_performance_excluded_from_promotion_decision": 1,
        "used_as_actual_strategy_gate": 0,
    }])
    return definition, d, summary



def build_r2_prospective_control_audit(
    restart_reacceleration_detail: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Prospectively frozen contemporaneous control for R2C1.

    Candidate group:
      existing RESTART + restart close >= prior wave high

    Control group:
      existing RESTART + restart close < prior wave high

    Both groups use the same prospective start date. No historical/pre-freeze
    event can enter OOS comparison. This does not alter R2C1 or actual strategy.
    """
    if restart_reacceleration_detail is None or restart_reacceleration_detail.empty:
        detail = pd.DataFrame(columns=[
            "schema","strategy_id","loader_revision","candidate_id",
            "candidate_event_date","validation_cohort","prospective_group",
            "d5_mature","d10_mature","d15_mature","used_as_actual_strategy_gate",
        ])
        summary = pd.DataFrame([{
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "loader_revision": LOADER_REVISION,
            "candidate_id": R2_CANDIDATE_ID,
            "freeze_date": R2_CANDIDATE_FREEZE_DATE,
            "prospective_start_date": R2_CANDIDATE_PROSPECTIVE_START_DATE,
            "candidate_events": 0,
            "control_events": 0,
            "candidate_d15_mature": 0,
            "control_d15_mature": 0,
            "candidate_d15_positive": 0,
            "control_d15_positive": 0,
            "candidate_d15_close_median_pct": float("nan"),
            "control_d15_close_median_pct": float("nan"),
            "candidate_d15_mfe_median_pct": float("nan"),
            "control_d15_mfe_median_pct": float("nan"),
            "candidate_d15_mae_median_pct": float("nan"),
            "control_d15_mae_median_pct": float("nan"),
            "status": "WAIT_PROSPECTIVE_OOS",
            "used_as_actual_strategy_gate": 0,
        }])
        return detail, summary

    d = restart_reacceleration_detail.copy()
    d["candidate_id"] = R2_CANDIDATE_ID
    d["freeze_date"] = R2_CANDIDATE_FREEZE_DATE
    d["prospective_start_date"] = R2_CANDIDATE_PROSPECTIVE_START_DATE
    d["candidate_event_date"] = pd.to_datetime(d["anchor_date"], errors="coerce").dt.date.astype(str)

    pstart = pd.Timestamp(R2_CANDIDATE_PROSPECTIVE_START_DATE).normalize()
    event_ts = pd.to_datetime(d["anchor_date"], errors="coerce").dt.normalize()
    d["validation_cohort"] = np.where(
        event_ts >= pstart,
        "PROSPECTIVE_OOS",
        "DISCOVERY_OR_PRE_FREEZE_NOT_VALIDATION",
    )
    d["prospective_group"] = np.where(
        d["restart_reclaim_pre_wave_high"].eq(1),
        "R2C1_RECLAIM_CANDIDATE",
        "NO_RECLAIM_CONTEMPORANEOUS_CONTROL",
    )
    for horizon in (5,10,15):
        d[f"d{horizon}_mature"] = pd.to_numeric(
            d[f"d{horizon}_close_ret_pct"], errors="coerce"
        ).notna().astype(int)
    d["used_as_actual_strategy_gate"] = 0

    # Output detail contains all restart events for provenance, but only
    # PROSPECTIVE_OOS rows are used below.
    p = d[d["validation_cohort"].eq("PROSPECTIVE_OOS")].copy()
    cand = p[p["prospective_group"].eq("R2C1_RECLAIM_CANDIDATE")]
    ctrl = p[p["prospective_group"].eq("NO_RECLAIM_CONTEMPORANEOUS_CONTROL")]

    def mature15(g: pd.DataFrame) -> pd.DataFrame:
        return g[g["d15_mature"].eq(1)] if not g.empty else g

    def med(g: pd.DataFrame, col: str) -> float:
        x = pd.to_numeric(g.get(col, pd.Series(dtype=float)), errors="coerce")
        return float(x.median()) if x.notna().any() else float("nan")

    cm = mature15(cand)
    xm = mature15(ctrl)
    summary = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "candidate_id": R2_CANDIDATE_ID,
        "freeze_date": R2_CANDIDATE_FREEZE_DATE,
        "prospective_start_date": R2_CANDIDATE_PROSPECTIVE_START_DATE,
        "candidate_events": int(len(cand)),
        "control_events": int(len(ctrl)),
        "candidate_d5_mature": int(cand["d5_mature"].sum()) if not cand.empty else 0,
        "control_d5_mature": int(ctrl["d5_mature"].sum()) if not ctrl.empty else 0,
        "candidate_d10_mature": int(cand["d10_mature"].sum()) if not cand.empty else 0,
        "control_d10_mature": int(ctrl["d10_mature"].sum()) if not ctrl.empty else 0,
        "candidate_d15_mature": int(cand["d15_mature"].sum()) if not cand.empty else 0,
        "control_d15_mature": int(ctrl["d15_mature"].sum()) if not ctrl.empty else 0,
        "candidate_d15_positive": int(
            (pd.to_numeric(cm.get("d15_close_ret_pct", pd.Series(dtype=float)), errors="coerce") > 0).sum()
        ) if not cm.empty else 0,
        "control_d15_positive": int(
            (pd.to_numeric(xm.get("d15_close_ret_pct", pd.Series(dtype=float)), errors="coerce") > 0).sum()
        ) if not xm.empty else 0,
        "candidate_d15_close_median_pct": med(cm, "d15_close_ret_pct"),
        "control_d15_close_median_pct": med(xm, "d15_close_ret_pct"),
        "candidate_d15_mfe_median_pct": med(cm, "d15_mfe_pct"),
        "control_d15_mfe_median_pct": med(xm, "d15_mfe_pct"),
        "candidate_d15_mae_median_pct": med(cm, "d15_mae_pct"),
        "control_d15_mae_median_pct": med(xm, "d15_mae_pct"),
        "status": "WAIT_PROSPECTIVE_OOS",
        "historical_rows_excluded_from_comparison": 1,
        "used_as_actual_strategy_gate": 0,
    }])
    return d, summary



def build_r2_oos_readiness_audit(
    end: pd.Timestamp,
    universe_dates: List[pd.Timestamp],
    r2_candidate_shadow_summary: pd.DataFrame,
    r2_prospective_control_summary: pd.DataFrame,
    cfg: FrozenConfig,
) -> pd.DataFrame:
    """Operational readiness for prospective OOS interpretation.

    This is data-authority metadata only. It changes no candidate definition,
    chronology, threshold, stage, score, rank, LIVE logic, or order behavior.
    """
    end_ts = pd.Timestamp(end).normalize()
    pstart = pd.Timestamp(R2_CANDIDATE_PROSPECTIVE_START_DATE).normalize()
    normalized_dates = sorted({
        pd.Timestamp(x).normalize()
        for x in (universe_dates or [])
        if pd.notna(x)
    })
    eligible = [x for x in normalized_dates if x <= end_ts]
    latest_eligible = eligible[-1] if eligible else None
    latest_any = normalized_dates[-1] if normalized_dates else None
    age = int((end_ts - latest_eligible).days) if latest_eligible is not None else -1

    data_reached = int(end_ts >= pstart)
    universe_ready = int(
        latest_eligible is not None
        and age >= 0
        and age <= int(cfg.universe_max_calendar_age_days)
    )
    if not data_reached:
        status = "WAIT_DATA_CATCHUP"
    elif latest_eligible is None:
        status = "FAIL_NO_ELIGIBLE_ASOF_UNIVERSE"
    elif not universe_ready:
        status = "FAIL_STALE_ASOF_UNIVERSE"
    else:
        status = "READY_PROSPECTIVE_OOS"

    cand = 0
    if r2_candidate_shadow_summary is not None and not r2_candidate_shadow_summary.empty:
        cand = int(r2_candidate_shadow_summary.iloc[0].get("prospective_candidate_events", 0) or 0)
    ctrl = 0
    cand_d15 = 0
    ctrl_d15 = 0
    if r2_prospective_control_summary is not None and not r2_prospective_control_summary.empty:
        rr = r2_prospective_control_summary.iloc[0]
        ctrl = int(rr.get("control_events", 0) or 0)
        cand_d15 = int(rr.get("candidate_d15_mature", 0) or 0)
        ctrl_d15 = int(rr.get("control_d15_mature", 0) or 0)

    return pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "candidate_id": R2_CANDIDATE_ID,
        "freeze_date": R2_CANDIDATE_FREEZE_DATE,
        "prospective_start_date": R2_CANDIDATE_PROSPECTIVE_START_DATE,
        "research_data_end": end_ts.date().isoformat(),
        "prospective_data_reached": data_reached,
        "asof_snapshot_dates_total": int(len(normalized_dates)),
        "latest_asof_snapshot_any": latest_any.date().isoformat() if latest_any is not None else "",
        "latest_asof_snapshot_le_data_end": latest_eligible.date().isoformat() if latest_eligible is not None else "",
        "asof_age_to_data_end_calendar_days": age,
        "asof_max_calendar_age_days": int(cfg.universe_max_calendar_age_days),
        "asof_fresh_for_data_end": universe_ready,
        "prospective_candidate_events": cand,
        "prospective_control_events": ctrl,
        "candidate_d15_mature": cand_d15,
        "control_d15_mature": ctrl_d15,
        "oos_readiness_status": status,
        "candidate_zero_is_interpretable_as_no_event": int(status == "READY_PROSPECTIVE_OOS"),
        "used_as_actual_strategy_gate": 0,
    }])


def build_forward_outcomes(signals: List[Dict[str, Any]], frames: Dict[str, pd.DataFrame], cfg: FrozenConfig) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for s in signals:
        code = s["code"]
        df = frames.get(code)
        if df is None or df.empty:
            continue
        sig_date = pd.Timestamp(s["event_date"])
        indices = df.index[df["date"] == sig_date].tolist()
        if not indices:
            continue
        si = int(indices[-1])
        entry_i = si + 1
        base = {
            "schema": SCHEMA, "strategy_id": STRATEGY_ID, "episode_id": s["episode_id"], "event_id": s["event_id"],
            "code": code, "signal_date": sig_date.date().isoformat(), "entry_policy": "EVENT_STUDY_D1_OPEN_NOT_PRIMARY",
        }
        if entry_i >= len(df):
            base.update({"censored": 1, "censor_reason": "NO_D1_BAR"})
            rows.append(base); continue
        entry_open = float(df.iloc[entry_i]["open"])
        base.update({"censored": 0, "censor_reason": "", "entry_date": df.iloc[entry_i]["date"].date().isoformat(), "entry_open": entry_open})
        for h in cfg.forward_horizons:
            end_i = entry_i + h - 1
            if end_i >= len(df):
                base[f"d{h}_available"] = 0
                continue
            seg = df.iloc[entry_i:end_i + 1]
            base[f"d{h}_available"] = 1
            base[f"d{h}_close_ret_pct"] = (float(df.iloc[end_i]["close"]) / entry_open - 1.0) * 100.0
            base[f"d{h}_mfe_pct"] = (float(seg["high"].max()) / entry_open - 1.0) * 100.0
            base[f"d{h}_mae_pct"] = (float(seg["low"].min()) / entry_open - 1.0) * 100.0
        rows.append(base)
    if rows:
        return pd.DataFrame(rows)
    cols = ["schema","strategy_id","episode_id","event_id","code","signal_date","entry_policy","censored","censor_reason","entry_date","entry_open"]
    for h in cfg.forward_horizons:
        cols += [f"d{h}_available", f"d{h}_close_ret_pct", f"d{h}_mfe_pct", f"d{h}_mae_pct"]
    return pd.DataFrame(columns=cols)


def stage_chronology_audit(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    order = {s: i for i, s in enumerate(STAGES)}
    if events.empty:
        return pd.DataFrame([{"audit": "CHRONOLOGY", "status": "PASS_EMPTY", "fail_count": 0, "episode_count": 0}])
    fails = 0
    complete = 0
    for ep, g in events.groupby("episode_id", dropna=False):
        g = g.sort_values(["event_date", "stage"])
        seq = list(g["stage"])
        idx = [order.get(x, -1) for x in seq]
        # Same-day stages are allowed only in the correct ordinal order; event append order is preserved by row order.
        ok = all(a < b for a, b in zip(idx, idx[1:])) and seq[:2] == STAGES[:2]
        if "TRI_RESTART" in seq:
            complete += 1
            ok = ok and seq == STAGES
        if not ok:
            fails += 1
    rows.append({"audit": "CHRONOLOGY", "status": "PASS" if fails == 0 else "FAIL", "fail_count": fails, "episode_count": int(events["episode_id"].nunique()), "complete_restart_episodes": complete})
    return pd.DataFrame(rows)


def deterministic_manual_sample(signals: pd.DataFrame, events: pd.DataFrame, limit: int = 40) -> pd.DataFrame:
    if not signals.empty:
        x = signals.sort_values(["event_date", "code"]).reset_index(drop=True)
    else:
        x = events[events["stage"] == "TRI_HEALTHY_PULLBACK"].sort_values(["event_date", "code"]).reset_index(drop=True) if not events.empty else pd.DataFrame()
    if x.empty:
        return pd.DataFrame(columns=["episode_id","event_id","code","event_date","stage","manual_chart_label","manual_note"])
    if len(x) <= limit:
        return x
    positions = np.linspace(0, len(x) - 1, num=limit, dtype=int)
    return x.iloc[sorted(set(positions))].copy()


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _safe_float_summary(series: pd.Series) -> Dict[str, Any]:
    x = pd.to_numeric(series, errors="coerce").dropna()
    if x.empty:
        return {"n": 0, "mean": None, "median": None, "positive_pct": None}
    return {"n": int(len(x)), "mean": float(x.mean()), "median": float(x.median()), "positive_pct": float((x > 0).mean() * 100.0)}


def run(args: argparse.Namespace) -> int:
    cfg = CONFIG
    price_root = Path(args.price_cache_dir)
    amount_root = Path(args.amount_cache_dir)
    asof_root = Path(args.asof_cache_dir)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    universe = UniverseAuthority(asof_root, cfg.universe_max_calendar_age_days)
    if not universe.dates:
        raise RuntimeError("TRIANGLE1PB_FAIL_CLOSED: no causal historical as-of universe snapshots could be parsed")
    amount_auth = AmountAuthority(amount_root, asof_root)

    price_files = sorted(x for x in price_root.rglob("*") if x.is_file()) if price_root.exists() else []
    if not price_files:
        raise RuntimeError("TRIANGLE1PB_FAIL_CLOSED: price cache is empty")

    frames: Dict[str, pd.DataFrame] = {}
    amount_sources: Dict[str, str] = {}
    date_sources: Dict[str, str] = {}
    load_fail = 0
    duplicate_code_files = 0
    load_failure_samples: List[str] = []
    price_container_types: Dict[str, int] = {}
    for p in price_files:
        try:
            raw_obj = _load_any(p)
            typ = type(raw_obj).__name__
            price_container_types[typ] = price_container_types.get(typ, 0) + 1
            z = normalize_price_frame(raw_obj, p, amount_auth)
            if z is None:
                load_fail += 1
                if len(load_failure_samples) < 20:
                    frame_desc = []
                    for cand, meta in _iter_frame_candidates(raw_obj):
                        frame_desc.append({
                            "shape": list(cand.shape),
                            "columns": [str(c) for c in list(cand.columns)[:25]],
                            "index_type": type(cand.index).__name__,
                            "index_name": str(cand.index.name or ""),
                            "meta_keys": sorted(str(k) for k in meta.keys()),
                        })
                        if len(frame_desc) >= 3:
                            break
                    load_failure_samples.append(json.dumps({
                        "file": p.name,
                        "object_type": typ,
                        "frame_candidates": frame_desc,
                    }, ensure_ascii=False, sort_keys=True))
                continue
            code, df, amount_source, date_source = z
            if code in frames:
                duplicate_code_files += 1
                merged = pd.concat([frames[code], df], ignore_index=True).sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
                frames[code] = merged
                if amount_sources.get(code) != amount_source:
                    amount_sources[code] = "MULTI_FILE_MIXED_ACTUAL_AMOUNT"
                if date_sources.get(code) != date_source:
                    date_sources[code] = "MULTI_FILE_MIXED_DATE_SOURCE"
            else:
                frames[code] = df
                amount_sources[code] = amount_source
                date_sources[code] = date_source
        except Exception as exc:
            load_fail += 1
            if len(load_failure_samples) < 20:
                load_failure_samples.append(json.dumps({
                    "file": p.name,
                    "exception": f"{type(exc).__name__}:{str(exc)[:300]}",
                }, ensure_ascii=False, sort_keys=True))
    print(
        "TRIANGLE1PB_PRICE_LOADER",
        "files", len(price_files),
        "usable_codes", len(frames),
        "load_fail", load_fail,
        "duplicate_code_files", duplicate_code_files,
        "container_types", json.dumps(price_container_types, sort_keys=True),
        "date_sources", json.dumps(pd.Series(list(date_sources.values())).value_counts().to_dict(), sort_keys=True),
    )
    print(
        "TRIANGLE1PB_AMOUNT_AUTHORITY",
        "dedicated_files", amount_auth.external_files,
        "asof_files_scanned", amount_auth.asof_files_scanned,
        "asof_explicit_amount_rows", amount_auth.asof_amount_rows,
        "asof_codes_with_amount", len(amount_auth.asof_index),
        "synthetic_close_x_volume_fallback_rows", 0,
    )
    if load_failure_samples:
        for sample in load_failure_samples:
            print("TRIANGLE1PB_PRICE_LOAD_FAIL_SAMPLE", sample)
    if not frames:
        raise RuntimeError("TRIANGLE1PB_FAIL_CLOSED: no usable price frames")

    total_positive_amount_rows = sum(
        int(pd.to_numeric(df["amount"], errors="coerce").gt(0).sum())
        for df in frames.values()
    )
    if total_positive_amount_rows <= 0:
        raise RuntimeError(
            "TRIANGLE1PB_FAIL_CLOSED: explicit actual Amount unavailable "
            "(dedicated cache empty and no explicit Amount in as-of/price cache); "
            "close*volume fallback is forbidden"
        )

    max_data_date = max(df["date"].max() for df in frames.values())
    end = pd.Timestamp(args.end_date).normalize() if args.end_date else pd.Timestamp(max_data_date).normalize()

    # Freeze the discovery left edge after R2C1 hypothesis lock.
    locked_start = pd.Timestamp(R2_DISCOVERY_START_DATE).normalize()
    if args.start_date:
        requested_start = pd.Timestamp(args.start_date).normalize()
        if requested_start != locked_start:
            raise RuntimeError(
                "TRIANGLE1PB_DISCOVERY_WINDOW_LOCK_FAIL "
                f"expected_start={locked_start.date().isoformat()} "
                f"requested_start={requested_start.date().isoformat()}"
            )
    start = locked_start
    if start >= end:
        raise ValueError("locked discovery start must be earlier than end_date")

    codes = sorted(frames)
    if args.max_codes and int(args.max_codes) > 0:
        codes = codes[: int(args.max_codes)]

    all_events: List[Dict[str, Any]] = []
    all_signals: List[Dict[str, Any]] = []
    all_rejects: List[Dict[str, Any]] = []
    all_gate_diags: List[Dict[str, Any]] = []
    all_structure_audit: List[Dict[str, Any]] = []
    per_code_digest: Dict[str, str] = {}
    rerun_fail = 0

    for n, code in enumerate(codes, 1):
        df = frames[code]
        # Keep causal warmup and forward bars; detector itself enforces signal date range.
        e, s, r, gd = detect_code(
            code, df, amount_sources.get(code, "MISSING"), universe, start, end, cfg,
            structure_audit_sink=all_structure_audit,
        )
        all_events.extend(e); all_signals.extend(s); all_rejects.extend(r); all_gate_diags.append(gd)
        payload = json.dumps(e, sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":"))
        per_code_digest[code] = hashlib.sha256(payload.encode()).hexdigest()
        # Determinism invariant: rerun codes that emitted anything, plus a deterministic sparse sample.
        if e or n % max(1, len(codes) // 25 or 1) == 0:
            e2, s2, r2, gd2 = detect_code(code, df, amount_sources.get(code, "MISSING"), universe, start, end, cfg)
            payload2 = json.dumps(e2, sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":"))
            if hashlib.sha256(payload2.encode()).hexdigest() != per_code_digest[code]:
                rerun_fail += 1
        if n % 200 == 0:
            print("TRIANGLE1PB_PROGRESS", n, "/", len(codes), "events", len(all_events), "signals", len(all_signals))

    event_base_cols = [
        "schema","strategy_id","episode_id","event_id","code","stage","event_date","feature_max_date",
        "universe_snapshot_date","universe_age_days","amount_source"
    ]
    signal_base_cols = event_base_cols + ["open","high","low","close","actual_amount","amount20_mean_prior","amount20_ratio",
                                           "pullback_amount_median","amount_vs_pullback_median","wave_high","breakout_reference","drawdown_pct","signal_index"]
    reject_base_cols = ["schema","strategy_id","code","event_date","stage","reason","episode_id"]
    events = pd.DataFrame(all_events) if all_events else pd.DataFrame(columns=event_base_cols)
    signals = pd.DataFrame(all_signals) if all_signals else pd.DataFrame(columns=signal_base_cols)
    rejects = pd.DataFrame(all_rejects) if all_rejects else pd.DataFrame(columns=reject_base_cols)
    if not events.empty:
        events = events.sort_values(["event_date", "code", "episode_id", "stage"]).reset_index(drop=True)
    if not signals.empty:
        signals = signals.sort_values(["event_date", "code"]).drop_duplicates(["code", "event_date"], keep="first").reset_index(drop=True)
    if not rejects.empty:
        rejects = rejects.sort_values(["event_date", "code", "stage", "reason"]).reset_index(drop=True)

    # Strictly separate future outcomes from the signal ledger.
    outcomes = build_forward_outcomes(all_signals, frames, cfg)
    chronology = stage_chronology_audit(events)

    lookahead_fail = 0
    future_universe_fail = 0
    feature_future_fail = 0
    if not events.empty:
        evd = pd.to_datetime(events["event_date"], errors="coerce")
        fmd = pd.to_datetime(events["feature_max_date"], errors="coerce")
        usd = pd.to_datetime(events["universe_snapshot_date"], errors="coerce")
        feature_future_fail = int((fmd > evd).fillna(False).sum())
        future_universe_fail = int((usd > evd).fillna(False).sum())
        lookahead_fail = feature_future_fail + future_universe_fail
    forbidden_future_cols = [c for c in signals.columns if re.search(r"(^d\d+_|mfe|mae|forward|future)", str(c), re.I)] if not signals.empty else []
    lookahead_fail += len(forbidden_future_cols)

    duplicate_event_ids = int(events["event_id"].duplicated().sum()) if not events.empty and "event_id" in events else 0
    chronology_fail = int(pd.to_numeric(chronology.get("fail_count", pd.Series([0])), errors="coerce").fillna(0).sum())
    invariant_fail = duplicate_event_ids + chronology_fail + lookahead_fail + rerun_fail

    amount_rows = sum(len(frames[c]) for c in codes)
    amount_ready_rows = sum(int(pd.to_numeric(frames[c]["amount"], errors="coerce").gt(0).sum()) for c in codes)
    amount_coverage = (amount_ready_rows / amount_rows * 100.0) if amount_rows else 0.0
    amount_source_counts = pd.Series([amount_sources.get(c, "MISSING") for c in codes]).value_counts().to_dict()

    gate_diag = pd.DataFrame(all_gate_diags)
    gate_metric_cols = [
        "windows_tested","upper_falling_windows","lower_rising_windows","both_slope_signs_windows",
        "width_order_windows","contraction_pass_windows","r2_pass_windows","end_width_pass_windows",
        "squeeze_qualifying_windows","squeeze_streak_reached","squeeze_universe_pass","squeeze_universe_fail",
        "squeeze_context_ready","breakout_price_cross","breakout_amount_ready","breakout_amount_expansion_pass",
        "breakout_candle_confirm_pass","breakout_universe_pass","breakout_accepted","first_pullback_accepted",
        "healthy_pullback_accepted","restart_accepted","structure_audit_errors","short_frame",
    ]
    for c in gate_metric_cols:
        if c not in gate_diag:
            gate_diag[c] = 0
        gate_diag[c] = pd.to_numeric(gate_diag[c], errors="coerce").fillna(0).astype(int)

    gate_totals = {c: int(gate_diag[c].sum()) for c in gate_metric_cols}
    gate_totals["codes_scanned"] = int(len(gate_diag))
    gate_totals["codes_with_qualifying_squeeze_window"] = int((gate_diag["squeeze_qualifying_windows"] > 0).sum())
    gate_totals["codes_reaching_squeeze_streak"] = int((gate_diag["max_squeeze_streak"] >= cfg.squeeze_min_consecutive_windows).sum())
    gate_totals["codes_with_breakout_price_cross"] = int((gate_diag["breakout_price_cross"] > 0).sum())
    gate_totals["codes_with_breakout_accepted"] = int((gate_diag["breakout_accepted"] > 0).sum())
    gate_totals["max_squeeze_streak_global"] = int(gate_diag["max_squeeze_streak"].max()) if not gate_diag.empty else 0

    gate_totals_df = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        **gate_totals,
    }])

    structure_audit_raw_rows = int(len(all_structure_audit))
    structure_audit_expected_rows = int(gate_totals.get("squeeze_qualifying_windows", 0))
    structure_audit_error_count = int(gate_totals.get("structure_audit_errors", 0))
    structure_audit_integrity_fail = int(
        structure_audit_error_count != 0
        or structure_audit_raw_rows != structure_audit_expected_rows
    )

    structure_audit = pd.DataFrame(all_structure_audit)
    if not structure_audit.empty:
        structure_audit = (
            structure_audit
            .sort_values(["squeeze_date","code","shape_score"], ascending=[True,True,False])
            .drop_duplicates(["code","squeeze_date"], keep="first")
            .reset_index(drop=True)
        )
    structure_review = build_structure_manual_review_sample(structure_audit, n_each=8)
    structure_review_bars = build_structure_review_bars(structure_review, frames)

    structure_dates = pd.to_datetime(structure_audit.get("squeeze_date", pd.Series(dtype=str)), errors="coerce")
    research_mask = structure_dates.between(start, end, inclusive="both") if not structure_audit.empty else pd.Series(dtype=bool)
    research_structure_audit = structure_audit.loc[research_mask].copy() if not structure_audit.empty else pd.DataFrame()
    warmup_structure_audit = structure_audit.loc[~research_mask].copy() if not structure_audit.empty else pd.DataFrame()
    research_codes_with_squeeze = int(research_structure_audit["code"].nunique()) if not research_structure_audit.empty else 0
    warmup_codes_with_squeeze = int(warmup_structure_audit["code"].nunique()) if not warmup_structure_audit.empty else 0

    counterfactual_detail, counterfactual_summary = build_counterfactual_streak_audit(
        structure_audit, frames, universe, start, end, cfg
    )
    qualified_event_detail, qualified_event_summary, qualified_event_review_bars = (
        build_qualified_event_fidelity_audit(
            counterfactual_detail, structure_audit, frames, cfg
        )
    )
    phase_sequence_detail, phase_sequence_summary = build_phase_sequence_audit(
        qualified_event_detail, structure_audit, frames
    )
    terminal_energy_detail, terminal_energy_summary = build_terminal_energy_profile_audit(
        qualified_event_detail, frames
    )
    precursor_case_control_detail, precursor_case_control_summary = (
        build_causal_precursor_case_control_audit(
            counterfactual_detail, structure_audit, frames
        )
    )
    joint_precursor_detail, joint_precursor_summary, joint_precursor_review_bars = (
        build_joint_precursor_stability_audit(
            precursor_case_control_detail, frames, start, end
        )
    )
    d15_path_detail, d15_path_event_summary, d15_path_summary = build_qualified_d1_d15_path_study(
        qualified_event_detail, precursor_case_control_detail, frames
    )
    long_ma_candidate_detail, long_ma_event_detail, long_ma_summary = build_long_ma_context_audit(
        precursor_case_control_detail, qualified_event_detail, d15_path_event_summary, frames
    )
    stage_anchor_path_detail, stage_anchor_event_summary, stage_anchor_summary = (
        build_stage_anchor_path_audit(
            qualified_event_detail, precursor_case_control_detail, frames
        )
    )
    conversion_quality_summary = build_conversion_vs_quality_audit(
        precursor_case_control_detail, d15_path_event_summary
    )
    healthy_bottoming_detail, restart_reacceleration_detail, stage_quality_anatomy_summary = (
        build_stage_quality_anatomy_audit(
            qualified_event_detail, stage_anchor_event_summary,
            stage_anchor_path_detail, frames, cfg
        )
    )
    restart_reclaim_robustness_summary = build_restart_reclaim_robustness_audit(
        healthy_bottoming_detail, restart_reacceleration_detail, start, end
    )
    r2_candidate_definition, r2_candidate_shadow_detail, r2_candidate_shadow_summary = (
        build_r2_candidate_prospective_shadow_audit(restart_reacceleration_detail)
    )
    r2_prospective_control_detail, r2_prospective_control_summary = (
        build_r2_prospective_control_audit(restart_reacceleration_detail)
    )

    structure_integrity_audit = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "expected_full_squeeze_rows": structure_audit_expected_rows,
        "raw_structure_audit_rows": structure_audit_raw_rows,
        "structure_audit_errors": structure_audit_error_count,
        "integrity_fail": structure_audit_integrity_fail,
    }])

    stage_counts = pd.DataFrame(
        [{"stage": st, "count": int((events["stage"] == st).sum()) if not events.empty else 0} for st in STAGES]
    )
    rejection_counts = rejects.groupby(["stage", "reason"], dropna=False).size().reset_index(name="count") if not rejects.empty else pd.DataFrame(columns=["stage", "reason", "count"])
    lookahead_audit = pd.DataFrame([{
        "schema": SCHEMA, "strategy_id": STRATEGY_ID,
        "status": "PASS" if lookahead_fail == 0 else "FAIL",
        "feature_future_fail": feature_future_fail,
        "future_universe_fail": future_universe_fail,
        "forbidden_future_columns_in_signal_ledger": len(forbidden_future_cols),
        "forbidden_columns": ",".join(forbidden_future_cols),
        "actual_amount_synthetic_fallback_rows": 0,
        "note": "Forward outcomes live only in tri_forward_outcomes.csv",
    }])
    invariant_audit = pd.DataFrame([{
        "schema": SCHEMA, "strategy_id": STRATEGY_ID,
        "status": "PASS" if (invariant_fail == 0 and structure_audit_integrity_fail == 0) else "FAIL",
        "duplicate_event_ids": duplicate_event_ids,
        "chronology_fail": chronology_fail,
        "lookahead_fail": lookahead_fail,
        "deterministic_rerun_fail": rerun_fail,
        "total_fail": invariant_fail,
    }])
    amount_audit = pd.DataFrame([{
        "schema": SCHEMA, "strategy_id": STRATEGY_ID,
        "codes_scanned": len(codes), "price_rows": amount_rows, "actual_amount_ready_rows": amount_ready_rows,
        "actual_amount_coverage_pct": amount_coverage,
        "price_loader_usable_codes": len(frames),
        "price_loader_failed_files": load_fail,
        "price_container_types": price_container_types,
        "price_date_sources": pd.Series(list(date_sources.values())).value_counts().to_dict(),
        "asof_explicit_amount_rows": amount_auth.asof_amount_rows,
        "synthetic_close_x_volume_fallback_rows": 0,
        "source_counts_json": json.dumps(amount_source_counts, ensure_ascii=False, sort_keys=True),
        "date_source_counts_json": json.dumps(pd.Series([date_sources.get(c, "MISSING") for c in codes]).value_counts().to_dict(), ensure_ascii=False, sort_keys=True),
        "dedicated_amount_files": amount_auth.external_files,
        "asof_explicit_amount_rows": amount_auth.asof_amount_rows,
        "asof_codes_with_amount": len(amount_auth.asof_index),
    }])
    universe_audit = pd.DataFrame([{
        "schema": SCHEMA, "strategy_id": STRATEGY_ID,
        "snapshot_dates": len(universe.dates), "files_loaded": universe.files_loaded, "files_failed": universe.files_failed,
        "first_snapshot": universe.dates[0].date().isoformat() if universe.dates else "",
        "last_snapshot": universe.dates[-1].date().isoformat() if universe.dates else "",
        "max_calendar_age_days": cfg.universe_max_calendar_age_days,
        "future_snapshot_fallback_allowed": 0,
    }])
    r2_oos_readiness = build_r2_oos_readiness_audit(
        end, universe.dates, r2_candidate_shadow_summary,
        r2_prospective_control_summary, cfg
    )

    sample = deterministic_manual_sample(signals, events, int(args.manual_sample_limit))
    if not sample.empty:
        keep = [c for c in ["episode_id", "event_id", "code", "event_date", "stage", "close", "drawdown_pct", "amount20_ratio", "amount_vs_pullback_median", "universe_snapshot_date", "amount_source"] if c in sample.columns]
        sample = sample[keep].copy()
        sample["manual_chart_label"] = "UNREVIEWED"
        sample["manual_note"] = ""

    # Neutral event-study summary. No stop/target/PRIMARY is selected.
    outcome_summary = []
    for h in cfg.forward_horizons:
        col = f"d{h}_close_ret_pct"
        if col in outcomes:
            ssum = _safe_float_summary(outcomes[col])
            outcome_summary.append({"horizon": f"D+{h}", **ssum})
    outcome_summary_df = pd.DataFrame(outcome_summary)

    config_df = pd.DataFrame([{"schema": SCHEMA, "strategy_id": STRATEGY_ID, "tuning_allowed": 0, **asdict(cfg)}])
    _write_csv(config_df, out / "tri_state_spec.csv")
    _write_csv(events, out / "tri_stage_ledger.csv")
    _write_csv(signals, out / "tri_signal_ledger.csv")
    _write_csv(outcomes, out / "tri_forward_outcomes.csv")
    _write_csv(stage_counts, out / "tri_stage_counts.csv")
    _write_csv(gate_totals_df, out / "tri_gate_diagnostics.csv")
    _write_csv(structure_audit, out / "tri_structure_fidelity_audit.csv")
    _write_csv(structure_integrity_audit, out / "tri_structure_audit_integrity.csv")
    _write_csv(structure_review, out / "tri_structure_manual_review_sample.csv")
    _write_csv(structure_review_bars, out / "tri_structure_review_bars.csv")
    _write_csv(counterfactual_detail, out / "tri_counterfactual_streak_detail.csv")
    _write_csv(counterfactual_summary, out / "tri_counterfactual_streak_summary.csv")
    _write_csv(qualified_event_detail, out / "tri_qualified_event_fidelity_detail.csv")
    _write_csv(qualified_event_summary, out / "tri_qualified_event_fidelity_summary.csv")
    _write_csv(qualified_event_review_bars, out / "tri_qualified_event_review_bars.csv")
    _write_csv(phase_sequence_detail, out / "tri_phase_sequence_detail.csv")
    _write_csv(phase_sequence_summary, out / "tri_phase_sequence_summary.csv")
    _write_csv(terminal_energy_detail, out / "tri_terminal_energy_profile_detail.csv")
    _write_csv(terminal_energy_summary, out / "tri_terminal_energy_profile_summary.csv")
    _write_csv(precursor_case_control_detail, out / "tri_causal_precursor_case_control_detail.csv")
    _write_csv(precursor_case_control_summary, out / "tri_causal_precursor_case_control_summary.csv")
    _write_csv(joint_precursor_detail, out / "tri_joint_precursor_stability_detail.csv")
    _write_csv(joint_precursor_summary, out / "tri_joint_precursor_stability_summary.csv")
    _write_csv(joint_precursor_review_bars, out / "tri_joint_precursor_review_bars.csv")
    _write_csv(d15_path_detail, out / "tri_qualified_d1_d15_path_detail.csv")
    _write_csv(d15_path_event_summary, out / "tri_qualified_d1_d15_event_summary.csv")
    _write_csv(d15_path_summary, out / "tri_qualified_d1_d15_summary.csv")
    _write_csv(long_ma_candidate_detail, out / "tri_long_ma_candidate_context.csv")
    _write_csv(long_ma_event_detail, out / "tri_long_ma_event_context.csv")
    _write_csv(long_ma_summary, out / "tri_long_ma_summary.csv")
    _write_csv(stage_anchor_path_detail, out / "tri_stage_anchor_d1_d15_path_detail.csv")
    _write_csv(stage_anchor_event_summary, out / "tri_stage_anchor_d1_d15_event_summary.csv")
    _write_csv(stage_anchor_summary, out / "tri_stage_anchor_d1_d15_summary.csv")
    _write_csv(conversion_quality_summary, out / "tri_conversion_vs_quality_summary.csv")
    _write_csv(healthy_bottoming_detail, out / "tri_healthy_bottoming_anatomy.csv")
    _write_csv(restart_reacceleration_detail, out / "tri_restart_reacceleration_anatomy.csv")
    _write_csv(stage_quality_anatomy_summary, out / "tri_stage_quality_anatomy_summary.csv")
    _write_csv(restart_reclaim_robustness_summary, out / "tri_restart_reclaim_robustness_summary.csv")
    _write_csv(r2_candidate_definition, out / "tri_r2_candidate_definition.csv")
    _write_csv(r2_candidate_shadow_detail, out / "tri_r2_candidate_shadow_detail.csv")
    _write_csv(r2_candidate_shadow_summary, out / "tri_r2_candidate_shadow_summary.csv")
    _write_csv(r2_prospective_control_detail, out / "tri_r2_prospective_control_detail.csv")
    _write_csv(r2_prospective_control_summary, out / "tri_r2_prospective_control_summary.csv")
    _write_csv(
        gate_diag.sort_values(
            ["squeeze_qualifying_windows","max_squeeze_streak","breakout_price_cross"],
            ascending=[False,False,False]
        ),
        out / "tri_code_gate_diagnostics.csv"
    )
    _write_csv(rejects, out / "tri_rejection_ledger.csv")
    _write_csv(rejection_counts, out / "tri_rejection_counts.csv")
    _write_csv(chronology, out / "tri_chronology_audit.csv")
    _write_csv(lookahead_audit, out / "tri_lookahead_audit.csv")
    _write_csv(invariant_audit, out / "tri_invariant_audit.csv")
    _write_csv(amount_audit, out / "tri_amount_authority_audit.csv")
    _write_csv(universe_audit, out / "tri_asof_universe_audit.csv")
    _write_csv(r2_oos_readiness, out / "tri_r2_oos_readiness.csv")
    _write_csv(sample, out / "tri_manual_chart_review_sample.csv")
    _write_csv(outcome_summary_df, out / "tri_event_study_summary.csv")

    manifest = {
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "research_authority": RESEARCH_AUTHORITY,
        "core224_dependency": "NONE",
        "core224_logic_changed": False,
        "live_logic_changed": False,
        "score_rank_changed": False,
        "order_logic_changed": False,
        "primary_policy": "NONE_EVENT_STUDY_ONLY",
        "tuning_allowed": False,
        "start_date": start.date().isoformat(),
        "end_date": end.date().isoformat(),
        "research_start": start.date().isoformat(),
        "research_end": end.date().isoformat(),
        "codes_scanned": len(codes),
        "price_cache_files_seen": len(price_files),
        "price_load_fail": load_fail,
        "duplicate_code_files": duplicate_code_files,
        "asof_snapshot_dates": len(universe.dates),
        "stage_counts": {r["stage"]: int(r["count"]) for r in stage_counts.to_dict("records")},
        "gate_diagnostics": gate_totals,
        "structure_fidelity_audit": {
            "rows": int(len(structure_audit)),
            "raw_rows": structure_audit_raw_rows,
            "research_period_rows": int(len(research_structure_audit)),
            "warmup_rows": int(len(warmup_structure_audit)),
            "research_period_codes": research_codes_with_squeeze,
            "warmup_codes": warmup_codes_with_squeeze,
            "expected_full_squeeze_rows": structure_audit_expected_rows,
            "audit_errors": structure_audit_error_count,
            "integrity_fail": structure_audit_integrity_fail,
            "manual_review_rows": int(len(structure_review)),
            "post_event_fields_use_future_data": 1,
            "used_as_strategy_gate": 0,
            "legacy_accumulation_bar_used_as_gate": 0,
            "synthetic_close_x_volume_fallback_rows": 0,
            "wave1_found_rows": int(pd.to_numeric(structure_audit.get("post15_wave1_found", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not structure_audit.empty else 0,
            "first_pullback_found_rows": int(pd.to_numeric(structure_audit.get("first_pullback_found", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not structure_audit.empty else 0,
            "restart_after_pullback_rows": int(pd.to_numeric(structure_audit.get("restart_after_pullback_found", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not structure_audit.empty else 0,
        },
        "counterfactual_streak_audit": {
            "research_only": 1,
            "used_as_strategy_gate": 0,
            "summary": counterfactual_summary.to_dict("records"),
        },
        "qualified_event_fidelity_audit": {
            "research_only": 1,
            "used_as_strategy_gate": 0,
            "summary": qualified_event_summary.to_dict("records"),
        },
        "phase_sequence_audit": {
            "research_only": 1,
            "used_as_strategy_gate": 0,
            "summary": phase_sequence_summary.to_dict("records"),
        },
        "terminal_energy_profile_audit": {
            "research_only": 1,
            "used_as_strategy_gate": 0,
            "summary": terminal_energy_summary.to_dict("records"),
        },
        "causal_precursor_case_control_audit": {
            "research_only": 1,
            "features_causal_through_squeeze_date": 1,
            "future_labels_not_features": 1,
            "used_as_strategy_gate": 0,
            "summary": precursor_case_control_summary.to_dict("records"),
        },
        "joint_precursor_stability_audit": {
            "research_only": 1,
            "hypothesis": "PRE10_OBV_POSITIVE_AND_BB40_CONTRACTING",
            "hypothesis_posthoc_on_same_research_period": 1,
            "prospective_gate_promoted": 0,
            "used_as_strategy_gate": 0,
            "summary": joint_precursor_summary.to_dict("records"),
        },
        "qualified_d1_d15_path_audit": {
            "research_only": 1, "future_outcome_only": 1, "used_as_strategy_gate": 0,
            "summary": d15_path_summary.to_dict("records"),
        },
        "long_ma_context_audit": {
            "research_only": 1, "squeeze_features_causal_through_event_date": 1,
            "qualified_restart_context_event_time_only": 1, "used_as_strategy_gate": 0,
            "summary": long_ma_summary.to_dict("records"),
        },
        "stage_anchor_path_audit": {
            "research_only": 1,
            "anchors": ["QUALIFIED","HEALTHY","RESTART"],
            "future_outcome_only": 1,
            "used_as_strategy_gate": 0,
            "summary": stage_anchor_summary.to_dict("records"),
        },
        "conversion_vs_quality_audit": {
            "research_only": 1,
            "precursor_conversion_and_conditional_quality_separated": 1,
            "used_as_strategy_gate": 0,
            "summary": conversion_quality_summary.to_dict("records"),
        },
        "stage_quality_anatomy_audit": {
            "research_only": 1,
            "healthy_lower_low_is_future_taxonomy_only": 1,
            "restart_wave_high_reclaim_hypothesis_posthoc": 1,
            "used_as_strategy_gate": 0,
            "summary": stage_quality_anatomy_summary.to_dict("records"),
        },
        "restart_reclaim_robustness_audit": {
            "research_only": 1,
            "frozen_posthoc_hypothesis": "RESTART_CLOSE_GE_PRIOR_WAVE_HIGH",
            "natural_threshold_no_parameter_tuning": 1,
            "temporal_half_split_fixed_by_declared_research_span": 1,
            "leave_one_event_out_sensitivity": 1,
            "used_as_strategy_gate": 0,
            "summary": restart_reclaim_robustness_summary.to_dict("records"),
        },
        "r2_candidate_prospective_shadow": {
            "research_only": 1,
            "candidate_id": R2_CANDIDATE_ID,
            "freeze_date": R2_CANDIDATE_FREEZE_DATE,
            "prospective_start_date": R2_CANDIDATE_PROSPECTIVE_START_DATE,
            "actual_strategy_changed": 0,
            "discovery_rows_never_counted_as_validation": 1,
            "used_as_strategy_gate": 0,
            "definition": r2_candidate_definition.to_dict("records"),
            "summary": r2_candidate_shadow_summary.to_dict("records"),
        },
        "r2_prospective_control_audit": {
            "research_only": 1,
            "candidate_id": R2_CANDIDATE_ID,
            "prospective_start_date": R2_CANDIDATE_PROSPECTIVE_START_DATE,
            "candidate_group": "R2C1_RECLAIM_CANDIDATE",
            "control_group": "NO_RECLAIM_CONTEMPORANEOUS_CONTROL",
            "historical_rows_excluded_from_comparison": 1,
            "actual_strategy_changed": 0,
            "used_as_strategy_gate": 0,
            "summary": r2_prospective_control_summary.to_dict("records"),
        },
        "r2_oos_readiness": {
            "research_only": 1,
            "data_authority_only": 1,
            "actual_strategy_changed": 0,
            "used_as_strategy_gate": 0,
            "summary": r2_oos_readiness.to_dict("records"),
        },
        "r2_discovery_window_lock": {
            "research_only": 1,
            "discovery_start_date_locked": R2_DISCOVERY_START_DATE,
            "freeze_date": R2_CANDIDATE_FREEZE_DATE,
            "prospective_start_date": R2_CANDIDATE_PROSPECTIVE_START_DATE,
            "rolling_left_edge_allowed": 0,
            "actual_strategy_changed": 0,
            "used_as_strategy_gate": 0,
        },
        "restart_signals": int(len(signals)),
        "invariant_fail": int(invariant_fail + structure_audit_integrity_fail),
        "structure_audit_integrity_fail": structure_audit_integrity_fail,
        "lookahead_fail": lookahead_fail,
        "actual_amount_coverage_pct": amount_coverage,
        "actual_amount_synthetic_fallback_rows": 0,
        "status": "PASS" if invariant_fail == 0 else "FAIL",
    }
    # Hash only pre-outcome research authority files for stable provenance.
    authority_files = [
        out / "tri_state_spec.csv", out / "tri_stage_ledger.csv", out / "tri_signal_ledger.csv",
        out / "tri_chronology_audit.csv", out / "tri_lookahead_audit.csv", out / "tri_invariant_audit.csv",
        out / "tri_amount_authority_audit.csv", out / "tri_asof_universe_audit.csv",
        out / "tri_gate_diagnostics.csv", out / "tri_code_gate_diagnostics.csv",
        out / "tri_structure_fidelity_audit.csv", out / "tri_structure_audit_integrity.csv",
        out / "tri_structure_manual_review_sample.csv", out / "tri_structure_review_bars.csv",
        out / "tri_counterfactual_streak_detail.csv", out / "tri_counterfactual_streak_summary.csv",
        out / "tri_qualified_event_fidelity_detail.csv", out / "tri_qualified_event_fidelity_summary.csv",
        out / "tri_qualified_event_review_bars.csv",
        out / "tri_phase_sequence_detail.csv", out / "tri_phase_sequence_summary.csv",
        out / "tri_terminal_energy_profile_detail.csv", out / "tri_terminal_energy_profile_summary.csv",
        out / "tri_causal_precursor_case_control_detail.csv", out / "tri_causal_precursor_case_control_summary.csv",
        out / "tri_joint_precursor_stability_detail.csv", out / "tri_joint_precursor_stability_summary.csv",
        out / "tri_joint_precursor_review_bars.csv",
        out / "tri_qualified_d1_d15_path_detail.csv", out / "tri_qualified_d1_d15_event_summary.csv",
        out / "tri_qualified_d1_d15_summary.csv",
        out / "tri_long_ma_candidate_context.csv", out / "tri_long_ma_event_context.csv",
        out / "tri_long_ma_summary.csv",
        out / "tri_stage_anchor_d1_d15_path_detail.csv", out / "tri_stage_anchor_d1_d15_event_summary.csv",
        out / "tri_stage_anchor_d1_d15_summary.csv", out / "tri_conversion_vs_quality_summary.csv",
        out / "tri_healthy_bottoming_anatomy.csv", out / "tri_restart_reacceleration_anatomy.csv",
        out / "tri_stage_quality_anatomy_summary.csv",
        out / "tri_restart_reclaim_robustness_summary.csv",
        out / "tri_r2_candidate_definition.csv",
        out / "tri_r2_candidate_shadow_detail.csv",
        out / "tri_r2_candidate_shadow_summary.csv",
        out / "tri_r2_prospective_control_detail.csv",
        out / "tri_r2_prospective_control_summary.csv",
        out / "tri_r2_oos_readiness.csv",
    ]
    h = hashlib.sha256()
    for p in authority_files:
        h.update(p.name.encode()); h.update(b"\0"); h.update(p.read_bytes()); h.update(b"\0")
    manifest["research_authority_digest"] = h.hexdigest()
    (out / "tri_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    report = [
        "TRIANGLE1PB RESEARCH BOARD · RESEARCH_ONLY",
        f"schema={SCHEMA}", f"strategy={STRATEGY_ID}",
        f"period={start.date().isoformat()}..{end.date().isoformat()} codes={len(codes)}",
        "chronology=TRI_SQUEEZE -> TRI_BREAKOUT_WAVE1 -> TRI_FIRST_PULLBACK -> TRI_HEALTHY_PULLBACK -> TRI_RESTART",
        "execution_policy=NONE; outcomes are neutral D+1 OPEN event-study only",
        f"stage_counts={manifest['stage_counts']}",
        f"gate_diagnostics={gate_totals}",
        f"structure_fidelity_audit={json.dumps(manifest['structure_fidelity_audit'], ensure_ascii=False, sort_keys=True)}",
        (
            f"structure_period_split=research:{len(research_structure_audit)}"
            f" warmup:{len(warmup_structure_audit)}"
            f" research_codes:{research_codes_with_squeeze}"
        ),
        f"counterfactual_streak_summary={json.dumps(counterfactual_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"qualified_event_fidelity_summary={json.dumps(qualified_event_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"phase_sequence_summary={json.dumps(phase_sequence_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"terminal_energy_profile_summary={json.dumps(terminal_energy_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"causal_precursor_case_control_summary={json.dumps(precursor_case_control_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"joint_precursor_stability_summary={json.dumps(joint_precursor_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"qualified_d1_d15_path_summary={json.dumps(d15_path_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"long_ma_context_summary={json.dumps(long_ma_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"stage_anchor_d1_d15_summary={json.dumps(stage_anchor_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"conversion_vs_quality_summary={json.dumps(conversion_quality_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"stage_quality_anatomy_summary={json.dumps(stage_quality_anatomy_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"restart_reclaim_robustness_summary={json.dumps(restart_reclaim_robustness_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"r2_candidate_shadow_summary={json.dumps(r2_candidate_shadow_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"r2_prospective_control_summary={json.dumps(r2_prospective_control_summary.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"r2_oos_readiness={json.dumps(r2_oos_readiness.to_dict('records'), ensure_ascii=False, sort_keys=True)}",
        f"r2_discovery_start_locked={R2_DISCOVERY_START_DATE}",
        (
            f"structure_audit_integrity=expected:{structure_audit_expected_rows}"
            f" raw:{structure_audit_raw_rows} errors:{structure_audit_error_count}"
            f" fail:{structure_audit_integrity_fail}"
        ),
        f"restart_signals={len(signals)}",
        f"actual_amount_coverage_pct={amount_coverage:.2f}; synthetic_close_x_volume_fallback=0",
        f"asof_snapshot_dates={len(universe.dates)}; future_snapshot_fallback=0",
        f"lookahead_fail={lookahead_fail}; invariant_fail={invariant_fail}; deterministic_rerun_fail={rerun_fail}",
        f"status={manifest['status']}",
        (
            "NEXT_GATE=manual chart review + false-positive taxonomy before any threshold/performance tuning"
            if len(signals) > 0 else
            "NEXT_GATE=keep discovery start fixed at 2024-08-27; advance only the right edge and interpret candidate/control only when OOS readiness=READY_PROSPECTIVE_OOS"
        ),
    ]
    (out / "tri_report.txt").write_text("\n".join(report) + "\n", encoding="utf-8")
    print("\n".join(report))
    if not structure_review.empty:
        print("TRIANGLE1PB_STRUCTURE_REVIEW_SAMPLE_BEGIN")
        show_cols = [
            "review_bucket","code","squeeze_date","shape_score",
            "pre60_volume_spike_2p0_count","pre60_amount_spike_2p0_count",
            "pre60_legacy_accumulation_bar_count","pre10_obv_relative_change",
            "bb40_width_pct_at_squeeze","post15_wave1_found","wave1_date",
            "wave1_high_ret_pct","wave1_volume20_ratio","wave1_amount20_ratio",
            "first_pullback_found","pullback_from_wave_high_pct",
            "pullback_volume_vs_wave1","pullback_amount_vs_wave1",
            "restart_after_pullback_found",
        ]
        show_cols = [c for c in show_cols if c in structure_review.columns]
        for rec in structure_review[show_cols].head(32).to_dict("records"):
            print("TRIANGLE1PB_STRUCTURE_REVIEW", json.dumps(rec, ensure_ascii=False, sort_keys=True, default=str))
        print("TRIANGLE1PB_STRUCTURE_REVIEW_SAMPLE_END")
    return 0 if (invariant_fail == 0 and structure_audit_integrity_fail == 0) else 31


def _synthetic_frame() -> pd.DataFrame:
    # Create a deterministic narrowing triangle, amount-backed breakout, healthy pullback and restart.
    dates = pd.bdate_range("2026-01-02", periods=55)
    rows = []
    for i, d in enumerate(dates):
        if i < 35:
            upper = 120.0 - 0.35 * i
            lower = 80.0 + 0.35 * i
            close = (upper + lower) / 2 + math.sin(i) * 0.4
            high = upper + 0.4
            low = lower - 0.4
            op = close - 0.1
            amt = 100.0
        else:
            # flat filler; overridden below around pattern events
            close = 100.0; op = 99.8; high = 100.5; low = 99.0; amt = 100.0
        rows.append([d, op, high, low, close, 1000.0, amt])
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume", "amount"])
    # Rebuild the last 20 bars before breakout as a clean narrowing triangle.
    b = 38
    for j, i in enumerate(range(b - 20, b)):
        upper = 110.0 - 0.28 * j
        lower = 90.0 + 0.28 * j
        df.loc[i, ["open", "high", "low", "close", "amount"]] = [(upper+lower)/2-0.1, upper, lower, (upper+lower)/2, 100.0]
    # breakout / extension / first healthy pullback / restart
    # projected upper ~104.4, breakout at 108 with 2x amount
    df.loc[b, ["open", "high", "low", "close", "amount"]] = [104.5, 108.5, 104.0, 108.0, 220.0]
    df.loc[b+1, ["open", "high", "low", "close", "amount"]] = [108.0, 111.0, 107.5, 110.0, 180.0]
    df.loc[b+2, ["open", "high", "low", "close", "amount"]] = [109.5, 110.0, 106.5, 107.5, 100.0]  # ~3.2% dd, amount <75% breakout
    df.loc[b+3, ["open", "high", "low", "close", "amount"]] = [107.2, 110.2, 107.0, 110.1, 150.0]  # > prior high and >1.2x pullback amt
    return df


class _SyntheticUniverse:
    def lookup(self, event_date: pd.Timestamp, code: str) -> Tuple[bool, str, int, str]:
        return True, event_date.date().isoformat(), 0, "PASS"


def self_test() -> int:
    df = _synthetic_frame()
    start, end = df["date"].min(), df["date"].max()
    structure_sink = []
    e, s, r, gd = detect_code(
        "123456", df, "SYNTHETIC_ACTUAL_AMOUNT", _SyntheticUniverse(), start, end, CONFIG,
        structure_audit_sink=structure_sink,
    )
    stages = [x["stage"] for x in e]
    assert STAGES == stages, ("synthetic chronology mismatch", stages)
    assert len(s) == 1 and s[0]["stage"] == "TRI_RESTART"
    assert gd["squeeze_qualifying_windows"] > 0
    assert gd["breakout_accepted"] == 1
    assert gd["first_pullback_accepted"] == 1
    assert gd["healthy_pullback_accepted"] == 1
    assert gd["restart_accepted"] == 1
    assert gd["structure_audit_errors"] == 0
    assert len(structure_sink) == gd["squeeze_qualifying_windows"] > 0
    assert all(x.get("audit_role") == "DESCRIPTIVE_ONLY_NOT_A_GATE" for x in structure_sink)
    sa_df = pd.DataFrame(structure_sink)
    cf_detail, cf_summary = build_counterfactual_streak_audit(
        sa_df, {"123456": df}, _SyntheticUniverse(), start, end, CONFIG
    )
    assert not cf_summary.empty
    cf1 = cf_summary[cf_summary["streak_threshold"].eq(1)].iloc[0]
    assert int(cf1["research_candidate_runs"]) >= 1
    assert int(cf1["first_cross_exact_with_universe"]) >= 1
    assert int(cf1["first_pullback_existing"]) >= 1
    assert int(cf1["healthy_pullback_existing"]) >= 1
    assert int(cf1["restart_existing"]) >= 1
    assert int(cf1["qualified_first_pullback_existing"]) >= 1
    assert int(cf1["qualified_healthy_pullback_existing"]) >= 1
    assert int(cf1["qualified_restart_existing"]) >= 1
    assert int(cf1["qualified_unique_breakout_events"]) >= 1
    qd, _, _ = build_qualified_event_fidelity_audit(cf_detail, sa_df, {"123456": df}, CONFIG)
    ccd, _ = build_causal_precursor_case_control_audit(cf_detail, sa_df, {"123456": df})
    pd15, pe15, ps15 = build_qualified_d1_d15_path_study(qd, ccd, {"123456": df})
    assert not pd15.empty and not pe15.empty and not ps15.empty
    mac, mae, mas = build_long_ma_context_audit(ccd, qd, pe15, {"123456": df})
    assert not mac.empty and not mae.empty and not mas.empty
    assert int(mac["ma224_ready"].sum()) == 0
    sad, sae, sas = build_stage_anchor_path_audit(qd, ccd, {"123456": df})
    assert not sad.empty and not sae.empty and not sas.empty
    hba, rra, sqa = build_stage_quality_anatomy_audit(qd, sae, sad, {"123456": df}, CONFIG)
    assert not hba.empty and not rra.empty and not sqa.empty
    rrb = build_restart_reclaim_robustness_audit(
        hba, rra, pd.Timestamp("2024-01-01"), pd.Timestamp("2025-12-31")
    )
    assert not rrb.empty
    r2def, r2det, r2sum = build_r2_candidate_prospective_shadow_audit(rra)
    assert not r2def.empty and not r2sum.empty
    assert int(r2sum.iloc[0]["used_as_actual_strategy_gate"]) == 0
    r2cd, r2cs = build_r2_prospective_control_audit(rra)
    assert r2cs is not None and not r2cs.empty
    assert int(r2cs.iloc[0]["used_as_actual_strategy_gate"]) == 0
    rr_wait = build_r2_oos_readiness_audit(
        pd.Timestamp("2026-08-28"), [pd.Timestamp("2026-08-19")], r2sum, r2cs, CONFIG
    )
    assert str(rr_wait.iloc[0]["oos_readiness_status"]) == "WAIT_DATA_CATCHUP"
    rr_ready = build_r2_oos_readiness_audit(
        pd.Timestamp("2026-08-31"), [pd.Timestamp("2026-08-28")], r2sum, r2cs, CONFIG
    )
    assert str(rr_ready.iloc[0]["oos_readiness_status"]) == "READY_PROSPECTIVE_OOS"
    # Determinism.
    e2, s2, r2, gd2 = detect_code("123456", df, "SYNTHETIC_ACTUAL_AMOUNT", _SyntheticUniverse(), start, end, CONFIG)
    assert json.dumps(e, sort_keys=True, default=str) == json.dumps(e2, sort_keys=True, default=str)
    # No amount fallback: remove amount and verify no restart.
    noamt = df.copy(); noamt["amount"] = np.nan
    e3, s3, r3, gd3 = detect_code("123456", noamt, "MISSING", _SyntheticUniverse(), start, end, CONFIG)
    assert not s3
    print("TRIANGLE1PB_SYNTHETIC_TEST PASS stages=" + ">".join(stages) + " no_amount_signal=0 deterministic=1")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--price-cache-dir", default="reports/.cache/v20_price_history")
    ap.add_argument("--asof-cache-dir", default="reports/.cache/v20_asof_snapshots")
    ap.add_argument("--amount-cache-dir", default="reports/.cache/v25_actual_amount_history")
    ap.add_argument("--output-dir", default="reports/triangle1pb")
    ap.add_argument("--start-date", default="")
    ap.add_argument("--end-date", default="")
    ap.add_argument("--max-codes", type=int, default=0)
    ap.add_argument("--manual-sample-limit", type=int, default=40)
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        return self_test()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
