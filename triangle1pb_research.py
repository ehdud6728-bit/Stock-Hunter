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
LOADER_REVISION = "TRIANGLE1PB_R1_6_QUALIFIED_EVENT_FIDELITY_AUDIT"
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
    start = pd.Timestamp(args.start_date).normalize() if args.start_date else (end - pd.Timedelta(days=730))
    if start >= end:
        raise ValueError("start_date must be earlier than end_date")

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
            "NEXT_GATE=qualified event fidelity + full visual review; no strategy promotion until duplicate/discontinuity/structure-intact audit is proven"
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
