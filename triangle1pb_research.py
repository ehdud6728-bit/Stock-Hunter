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
    def __init__(self, root: Path):
        self.root = root
        self.index: Dict[str, List[Path]] = {}
        self._cache: Dict[str, pd.DataFrame] = {}
        if root.exists():
            for p in sorted(x for x in root.rglob("*") if x.is_file()):
                code = _code_from_filename(p)
                if code:
                    self.index.setdefault(code, []).append(p)

    def for_code(self, code: str) -> pd.DataFrame:
        if code in self._cache:
            return self._cache[code]
        frames = []
        for p in self.index.get(code, []):
            try:
                obj = _load_any(p)
                if not isinstance(obj, pd.DataFrame):
                    continue
                dcol = _first_col(obj.columns, DATE_ALIASES)
                acol = _first_col(obj.columns, AMOUNT_ALIASES)
                if not dcol or not acol:
                    continue
                x = pd.DataFrame({"date": pd.to_datetime(obj[dcol], errors="coerce").dt.normalize(), "amount_external": pd.to_numeric(obj[acol], errors="coerce")})
                x = x.dropna(subset=["date"]).drop_duplicates("date", keep="last")
                frames.append(x)
            except Exception:
                pass
        if frames:
            out = pd.concat(frames, ignore_index=True).sort_values("date").drop_duplicates("date", keep="last")
        else:
            out = pd.DataFrame(columns=["date", "amount_external"])
        self._cache[code] = out
        return out


def normalize_price_frame(obj: Any, path: Path, amount_auth: AmountAuthority) -> Optional[Tuple[str, pd.DataFrame, str]]:
    if not isinstance(obj, pd.DataFrame) or obj.empty:
        return None
    code = _code_from_filename(path)
    ccol = _first_col(obj.columns, CODE_ALIASES)
    if not code and ccol and len(obj):
        code = _normalize_code(obj[ccol].dropna().iloc[0]) if obj[ccol].notna().any() else ""
    if not code:
        return None
    dcol = _first_col(obj.columns, DATE_ALIASES)
    ocol = _first_col(obj.columns, OPEN_ALIASES)
    hcol = _first_col(obj.columns, HIGH_ALIASES)
    lcol = _first_col(obj.columns, LOW_ALIASES)
    cclose = _first_col(obj.columns, CLOSE_ALIASES)
    vcol = _first_col(obj.columns, VOLUME_ALIASES)
    acol = _first_col(obj.columns, AMOUNT_ALIASES)
    if not all((dcol, ocol, hcol, lcol, cclose)):
        return None
    x = pd.DataFrame({
        "date": pd.to_datetime(obj[dcol], errors="coerce").dt.normalize(),
        "open": pd.to_numeric(obj[ocol], errors="coerce"),
        "high": pd.to_numeric(obj[hcol], errors="coerce"),
        "low": pd.to_numeric(obj[lcol], errors="coerce"),
        "close": pd.to_numeric(obj[cclose], errors="coerce"),
        "volume": pd.to_numeric(obj[vcol], errors="coerce") if vcol else np.nan,
        "amount_price": pd.to_numeric(obj[acol], errors="coerce") if acol else np.nan,
    })
    x = x.dropna(subset=["date", "open", "high", "low", "close"])
    x = x[(x["open"] > 0) & (x["high"] > 0) & (x["low"] > 0) & (x["close"] > 0)]
    x = x.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
    if x.empty:
        return None
    ext = amount_auth.for_code(code)
    source = "MISSING"
    if not ext.empty:
        x = x.merge(ext, on="date", how="left")
        x["amount"] = x["amount_external"]
        source = "V25_ACTUAL_AMOUNT_HISTORY"
        if acol:
            m = x["amount"].isna() & x["amount_price"].notna()
            x.loc[m, "amount"] = x.loc[m, "amount_price"]
            if m.any():
                source = "MIXED_EXTERNAL_THEN_PRICE_ACTUAL_AMOUNT"
    else:
        x["amount"] = x["amount_price"] if acol else np.nan
        source = "PRICE_CACHE_ACTUAL_AMOUNT" if acol else "MISSING"
    # Deliberately no close*volume fallback.
    return code, x, source


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
    qualifies = bool(
        hs < 0 and ls > 0 and
        upper_start > lower_start and upper_end > lower_end and
        0 < contraction <= cfg.squeeze_max_contraction_ratio and
        upper_r2 >= cfg.squeeze_min_upper_r2 and lower_r2 >= cfg.squeeze_min_lower_r2 and
        cfg.squeeze_min_end_width_pct <= width_end <= cfg.squeeze_max_end_width_pct
    )
    projected_upper_next = float(hs * len(window) + hi0)
    projected_lower_next = float(ls * len(window) + lo0)
    return {
        "qualifies": qualifies,
        "upper_slope_pct_per_bar": hs / mid,
        "lower_slope_pct_per_bar": ls / mid,
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


def detect_code(
    code: str,
    df: pd.DataFrame,
    amount_source: str,
    universe: UniverseAuthority,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cfg: FrozenConfig,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    events: List[Dict[str, Any]] = []
    signals: List[Dict[str, Any]] = []
    rejects: List[Dict[str, Any]] = []
    if len(df) < cfg.squeeze_lookback + 5:
        return events, signals, rejects

    squeeze_streak = 0
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
        if geom.get("qualifies"):
            squeeze_streak += 1
            if squeeze_streak >= cfg.squeeze_min_consecutive_windows:
                sq_date = pd.Timestamp(prev_window["date"].iloc[-1])
                uok, udate, uage, ure = universe.lookup(sq_date, code)
                if uok:
                    squeeze_context = dict(geom)
                    squeeze_context.update({"event_date": sq_date, "universe_snapshot_date": udate, "universe_age_days": uage})
                else:
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
            if not (math.isfinite(amount) and amount > 0 and amt_obs >= cfg.amount20_min_observations and math.isfinite(amt20_ratio)):
                reject(i, "BREAKOUT_ACTUAL_AMOUNT_NOT_READY", "TRI_BREAKOUT_WAVE1", {"amount_source": amount_source, "amount20_obs": amt_obs})
                squeeze_streak = 0; squeeze_context = None
                continue
            if amt20_ratio < cfg.breakout_min_amount20_ratio:
                reject(i, "BREAKOUT_AMOUNT_EXPANSION_TOO_LOW", "TRI_BREAKOUT_WAVE1", {"amount20_ratio": amt20_ratio})
                squeeze_streak = 0; squeeze_context = None
                continue
            if not (row["close"] > row["open"] and row["close"] >= (row["high"] + row["low"]) / 2.0):
                reject(i, "BREAKOUT_CANDLE_NOT_CONFIRMING", "TRI_BREAKOUT_WAVE1")
                squeeze_streak = 0; squeeze_context = None
                continue
            # Universe must be causal at breakout; squeeze context already passed separately.
            uok, udate, uage, ure = universe.lookup(d, code)
            if not uok:
                reject(i, ure, "TRI_BREAKOUT_WAVE1", {"universe_snapshot_date": udate, "universe_age_days": uage})
                squeeze_streak = 0; squeeze_context = None
                continue
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
            cooldown_until = i + cfg.post_signal_cooldown_bars + 1
            state = "IDLE"; ctx = {}; squeeze_streak = 0; squeeze_context = None

    return events, signals, rejects


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
    amount_auth = AmountAuthority(amount_root)

    price_files = sorted(x for x in price_root.rglob("*") if x.is_file()) if price_root.exists() else []
    if not price_files:
        raise RuntimeError("TRIANGLE1PB_FAIL_CLOSED: price cache is empty")

    frames: Dict[str, pd.DataFrame] = {}
    amount_sources: Dict[str, str] = {}
    load_fail = 0
    duplicate_code_files = 0
    for p in price_files:
        try:
            z = normalize_price_frame(_load_any(p), p, amount_auth)
            if z is None:
                load_fail += 1; continue
            code, df, amount_source = z
            if code in frames:
                duplicate_code_files += 1
                merged = pd.concat([frames[code], df], ignore_index=True).sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
                frames[code] = merged
                if amount_sources.get(code) != amount_source:
                    amount_sources[code] = "MULTI_FILE_MIXED_ACTUAL_AMOUNT"
            else:
                frames[code] = df
                amount_sources[code] = amount_source
        except Exception:
            load_fail += 1
    if not frames:
        raise RuntimeError("TRIANGLE1PB_FAIL_CLOSED: no usable price frames")

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
    per_code_digest: Dict[str, str] = {}
    rerun_fail = 0

    for n, code in enumerate(codes, 1):
        df = frames[code]
        # Keep causal warmup and forward bars; detector itself enforces signal date range.
        e, s, r = detect_code(code, df, amount_sources.get(code, "MISSING"), universe, start, end, cfg)
        all_events.extend(e); all_signals.extend(s); all_rejects.extend(r)
        payload = json.dumps(e, sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":"))
        per_code_digest[code] = hashlib.sha256(payload.encode()).hexdigest()
        # Determinism invariant: rerun codes that emitted anything, plus a deterministic sparse sample.
        if e or n % max(1, len(codes) // 25 or 1) == 0:
            e2, s2, r2 = detect_code(code, df, amount_sources.get(code, "MISSING"), universe, start, end, cfg)
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
        "status": "PASS" if invariant_fail == 0 else "FAIL",
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
        "synthetic_close_x_volume_fallback_rows": 0,
        "source_counts_json": json.dumps(amount_source_counts, ensure_ascii=False, sort_keys=True),
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
        "codes_scanned": len(codes),
        "price_cache_files_seen": len(price_files),
        "price_load_fail": load_fail,
        "duplicate_code_files": duplicate_code_files,
        "asof_snapshot_dates": len(universe.dates),
        "stage_counts": {r["stage"]: int(r["count"]) for r in stage_counts.to_dict("records")},
        "restart_signals": int(len(signals)),
        "invariant_fail": invariant_fail,
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
        f"restart_signals={len(signals)}",
        f"actual_amount_coverage_pct={amount_coverage:.2f}; synthetic_close_x_volume_fallback=0",
        f"asof_snapshot_dates={len(universe.dates)}; future_snapshot_fallback=0",
        f"lookahead_fail={lookahead_fail}; invariant_fail={invariant_fail}; deterministic_rerun_fail={rerun_fail}",
        f"status={manifest['status']}",
        "NEXT_GATE=manual chart review + false-positive taxonomy before any threshold/performance tuning",
    ]
    (out / "tri_report.txt").write_text("\n".join(report) + "\n", encoding="utf-8")
    print("\n".join(report))
    return 0 if invariant_fail == 0 else 31


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
    e, s, r = detect_code("123456", df, "SYNTHETIC_ACTUAL_AMOUNT", _SyntheticUniverse(), start, end, CONFIG)
    stages = [x["stage"] for x in e]
    assert STAGES == stages, ("synthetic chronology mismatch", stages)
    assert len(s) == 1 and s[0]["stage"] == "TRI_RESTART"
    # Determinism.
    e2, s2, r2 = detect_code("123456", df, "SYNTHETIC_ACTUAL_AMOUNT", _SyntheticUniverse(), start, end, CONFIG)
    assert json.dumps(e, sort_keys=True, default=str) == json.dumps(e2, sort_keys=True, default=str)
    # No amount fallback: remove amount and verify no restart.
    noamt = df.copy(); noamt["amount"] = np.nan
    e3, s3, r3 = detect_code("123456", noamt, "MISSING", _SyntheticUniverse(), start, end, CONFIG)
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
