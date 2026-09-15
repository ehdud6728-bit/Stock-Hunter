#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import math
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

RESEARCH_ID = "REAL_FULL_STRUCTURE_RESEARCH_R1"
RESEARCH_REVISION = "R1_2_OUTCOME4_V72_SATURATION_TRUE_BLIND"

# These are descriptive research constants, not trading thresholds.
GRADUAL_LOW = 1.20
GRADUAL_HIGH = 2.00
SPIKE_RATIO = 2.00
DEEP_MAE_PCT = -5.0
MFE_WATCH_PCT = 5.0


def num(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else np.nan
    except Exception:
        return np.nan


def code(v) -> str:
    s = re.sub(r"[^0-9A-Za-z]", "", str(v or "").strip())
    return s.zfill(6) if s.isdigit() else s


def read_csv(path: str | Path, **kwargs) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(p, **kwargs)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def read_jsonl(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    rows = []
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return pd.DataFrame(rows)


def load_price_frame(cache_dir: Path, c: str) -> Tuple[pd.DataFrame, str]:
    pats = [str(cache_dir / f"{c}_*.pkl.gz"), str(cache_dir / f"{c}*.pkl*")]
    files: List[str] = []
    for pat in pats:
        files.extend(glob.glob(pat))
    files = sorted(set(files))
    if not files:
        return pd.DataFrame(), "MISSING"
    best = None
    best_len = -1
    for f in files:
        try:
            obj = pd.read_pickle(f)
            fr = obj.get("frame") if isinstance(obj, dict) else obj
            if not isinstance(fr, pd.DataFrame) or fr.empty:
                continue
            if len(fr) > best_len:
                best = fr.copy()
                best_len = len(fr)
        except Exception:
            continue
    if best is None or best.empty:
        return pd.DataFrame(), "INVALID"
    fr = best.copy()
    if "Date" in fr.columns:
        fr["date"] = pd.to_datetime(fr["Date"], errors="coerce")
    elif "date" in fr.columns:
        fr["date"] = pd.to_datetime(fr["date"], errors="coerce")
    else:
        fr["date"] = pd.to_datetime(fr.index, errors="coerce")
    rename = {}
    for col in fr.columns:
        lc = str(col).lower()
        if lc == "open": rename[col] = "open"
        elif lc == "high": rename[col] = "high"
        elif lc == "low": rename[col] = "low"
        elif lc == "close": rename[col] = "close"
        elif lc == "volume": rename[col] = "volume"
        elif lc in {"amount", "value", "tradingvalue", "trading_value", "거래대금"}: rename[col] = "amount"
    fr = fr.rename(columns=rename)
    need = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in fr.columns]
    fr = fr[need + (["amount"] if "amount" in fr.columns else [])].copy()
    fr = fr.dropna(subset=["date"]).sort_values("date").drop_duplicates("date", keep="last")
    for c2 in ["open", "high", "low", "close", "volume", "amount"]:
        if c2 in fr.columns:
            fr[c2] = pd.to_numeric(fr[c2], errors="coerce")
    if "amount" in fr.columns and pd.to_numeric(fr["amount"], errors="coerce").notna().sum() > 0:
        fr["research_amount"] = pd.to_numeric(fr["amount"], errors="coerce")
        amount_source = "ACTUAL_AMOUNT"
    else:
        fr["research_amount"] = pd.to_numeric(fr.get("close"), errors="coerce") * pd.to_numeric(fr.get("volume"), errors="coerce")
        amount_source = "CLOSE_X_VOLUME_PROXY"
    fr["ma224"] = pd.to_numeric(fr.get("close"), errors="coerce").rolling(224, min_periods=224).mean()
    return fr.reset_index(drop=True), amount_source


def safe_ratio(a, b):
    a = num(a); b = num(b)
    return a / b if math.isfinite(a) and math.isfinite(b) and b != 0 else np.nan


def slope_norm(values: pd.Series) -> float:
    s = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(s) < 5:
        return np.nan
    med = float(s.median())
    if not math.isfinite(med) or med == 0:
        return np.nan
    x = np.arange(len(s), dtype=float)
    try:
        sl = float(np.polyfit(x, s.values, 1)[0])
    except Exception:
        return np.nan
    return sl / med


def boolish(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)) and math.isfinite(float(v)):
        return float(v) != 0.0
    return str(v).strip().lower() in {"1","true","yes","y","on","pass","ok"}


def parse_v72_debug(v) -> Dict[str, float]:
    """Parse only fields already present in the causal V72 debug string."""
    txt = str(v or "")
    out = {
        "v72_debug_score": np.nan,
        "v72_debug_impulse": np.nan,
        "v72_debug_support": np.nan,
        "v72_debug_rsi": np.nan,
        "v72_debug_disp": np.nan,
        "v72_debug_wick": np.nan,
        "v72_debug_k": np.nan,
        "v72_debug_d": np.nan,
    }
    pats = {
        "v72_debug_score": r"(?:^|[,\s])score=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_impulse": r"(?:^|[,\s])impulse=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_support": r"(?:^|[,\s])support=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_rsi": r"(?:^|[,\s])rsi=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_disp": r"(?:^|[,\s])disp=([+-]?\d+(?:\.\d+)?)",
        "v72_debug_wick": r"(?:^|[,\s])wick=([+-]?\d+(?:\.\d+)?)",
    }
    for k, pat in pats.items():
        m = re.search(pat, txt, flags=re.I)
        if m:
            out[k] = num(m.group(1))
    mk = re.search(r"(?:^|[,\s])k=([+-]?\d+(?:\.\d+)?)/([+-]?\d+(?:\.\d+)?)", txt, flags=re.I)
    if mk:
        out["v72_debug_k"] = num(mk.group(1))
        out["v72_debug_d"] = num(mk.group(2))
    return out


def assign_outcome4(df: pd.DataFrame) -> pd.Series:
    """Fixed research outcome taxonomy. Future outcomes are labels only."""
    hit = pd.to_numeric(df.get("origin_d5_hit_plus5"), errors="coerce").fillna(0).eq(1)
    close = pd.to_numeric(df.get("origin_d5_close_ret_pct"), errors="coerce")
    mae = pd.to_numeric(df.get("origin_d5_mae_pct"), errors="coerce")
    out = pd.Series("NO_HIT", index=df.index, dtype=object)
    giveback = hit & close.le(0)
    deep = hit & close.gt(0) & mae.le(DEEP_MAE_PCT)
    clean = hit & close.gt(0) & mae.gt(DEEP_MAE_PCT)
    out.loc[giveback] = "GIVEBACK"
    out.loc[deep] = "DEEP_MAE_WIN"
    out.loc[clean] = "CLEAN_PATH_WIN"
    return out


def structural_features(fr: pd.DataFrame, origin_date: str, pullback_days) -> Dict[str, float | int | str]:
    out: Dict[str, float | int | str] = {}
    if fr is None or fr.empty:
        out["price_history_status"] = "MISSING"
        return out
    d = pd.Timestamp(origin_date).normalize()
    f = fr.copy()
    f["date"] = pd.to_datetime(f["date"], errors="coerce").dt.normalize()
    hist = f[f["date"].le(d)].copy()
    if hist.empty or not hist["date"].eq(d).any():
        out["price_history_status"] = "ORIGIN_DATE_MISSING"
        return out
    cur = hist[hist["date"].eq(d)].iloc[-1]
    prior = hist[hist["date"].lt(d)].copy()
    out["price_history_status"] = "READY"
    close = num(cur.get("close")); low = num(cur.get("low")); amount = num(cur.get("research_amount")); volume = num(cur.get("volume"))
    ma224 = num(cur.get("ma224"))
    out["origin_close_cache"] = close
    out["ma224"] = ma224
    out["ma224_ready"] = int(math.isfinite(ma224) and ma224 > 0)
    out["ma224_distance_pct"] = ((close / ma224) - 1) * 100 if math.isfinite(close) and math.isfinite(ma224) and ma224 > 0 else np.nan
    out["below_ma224"] = int(math.isfinite(close) and math.isfinite(ma224) and close < ma224) if math.isfinite(ma224) else np.nan

    p20 = prior.tail(20)
    p60 = prior.tail(60)
    if len(p20):
        valid = p20[p20["ma224"].notna()]
        out["prior20_below_ma224_rate"] = float((valid["close"] < valid["ma224"]).mean()) if len(valid) else np.nan
        base_amount = float(pd.to_numeric(p60.get("research_amount"), errors="coerce").dropna().median()) if len(p60) else np.nan
        p20_amount = pd.to_numeric(p20.get("research_amount"), errors="coerce")
        p20_volume = pd.to_numeric(p20.get("volume"), errors="coerce")
        med20_amount = float(p20_amount.dropna().median()) if p20_amount.notna().any() else np.nan
        med20_volume = float(p20_volume.dropna().median()) if p20_volume.notna().any() else np.nan
        out["signal_amount_vs_pre20_median"] = safe_ratio(amount, med20_amount)
        out["signal_volume_vs_pre20_median"] = safe_ratio(volume, med20_volume)
        out["pre5_amount_vs_pre20_median"] = safe_ratio(pd.to_numeric(prior.tail(5).get("research_amount"), errors="coerce").median(), med20_amount)
        out["pre10_amount_vs_pre20_median"] = safe_ratio(pd.to_numeric(prior.tail(10).get("research_amount"), errors="coerce").median(), med20_amount)
        out["pre20_amount_slope_norm"] = slope_norm(p20_amount)
        out["pre20_volume_slope_norm"] = slope_norm(p20_volume)
        if math.isfinite(base_amount) and base_amount > 0:
            ratios = p20_amount / base_amount
            out["pre20_gradual_pulse_days_1p2_2x"] = int(((ratios >= GRADUAL_LOW) & (ratios < GRADUAL_HIGH)).sum())
            out["pre20_spike_days_ge2x"] = int((ratios >= SPIKE_RATIO).sum())
            out["pre20_amount_ratio_to_prior60_median"] = safe_ratio(med20_amount, base_amount)
        else:
            out["pre20_gradual_pulse_days_1p2_2x"] = np.nan
            out["pre20_spike_days_ge2x"] = np.nan
            out["pre20_amount_ratio_to_prior60_median"] = np.nan
    else:
        for k in ["prior20_below_ma224_rate","signal_amount_vs_pre20_median","signal_volume_vs_pre20_median",
                  "pre5_amount_vs_pre20_median","pre10_amount_vs_pre20_median","pre20_amount_slope_norm",
                  "pre20_volume_slope_norm","pre20_gradual_pulse_days_1p2_2x","pre20_spike_days_ge2x",
                  "pre20_amount_ratio_to_prior60_median"]:
            out[k] = np.nan

    # Position in recent range. Causal: only bars known by origin close are used.
    h60 = pd.to_numeric(hist.tail(60).get("high"), errors="coerce").max()
    l60 = pd.to_numeric(hist.tail(60).get("low"), errors="coerce").min()
    out["range60_position_pct"] = ((close-l60)/(h60-l60)*100) if all(math.isfinite(x) for x in [close,h60,l60]) and h60>l60 else np.nan

    # Inferred pullback anatomy: existing causal pullback_days defines only a duration descriptor.
    # We do NOT claim this is the scanner's exact Wave1 anchor. Names are deliberately prefixed inferred_.
    n = int(round(num(pullback_days))) if math.isfinite(num(pullback_days)) else 0
    if 1 <= n <= 15 and len(prior) >= n + 3:
        pb = prior.tail(n)
        prepb = prior.iloc[:-n]
        wavewin = prepb.tail(5)
        wave_high = pd.to_numeric(wavewin.get("high"), errors="coerce").max() if len(wavewin) else np.nan
        wave_amt_peak = pd.to_numeric(wavewin.get("research_amount"), errors="coerce").max() if len(wavewin) else np.nan
        wave_vol_peak = pd.to_numeric(wavewin.get("volume"), errors="coerce").max() if len(wavewin) else np.nan
        pb_amt_med = pd.to_numeric(pb.get("research_amount"), errors="coerce").median()
        pb_vol_med = pd.to_numeric(pb.get("volume"), errors="coerce").median()
        pb_low = pd.to_numeric(pb.get("low"), errors="coerce").min()
        out["inferred_pb_ready"] = 1
        out["inferred_wave1_high"] = wave_high
        out["inferred_pb_amount_vs_wave_peak"] = safe_ratio(pb_amt_med, wave_amt_peak)
        out["inferred_pb_volume_vs_wave_peak"] = safe_ratio(pb_vol_med, wave_vol_peak)
        out["inferred_pb_drawdown_from_wave_high_pct"] = ((pb_low/wave_high)-1)*100 if math.isfinite(pb_low) and math.isfinite(wave_high) and wave_high>0 else np.nan
        out["inferred_origin_close_vs_wave_high_pct"] = ((close/wave_high)-1)*100 if math.isfinite(close) and math.isfinite(wave_high) and wave_high>0 else np.nan
        out["inferred_wave_high_reclaim_at_origin"] = int(close >= wave_high) if math.isfinite(close) and math.isfinite(wave_high) else np.nan
        out["origin_low_holds_inferred_pb_low"] = int(low >= pb_low) if math.isfinite(low) and math.isfinite(pb_low) else np.nan
    else:
        out["inferred_pb_ready"] = 0
        for k in ["inferred_wave1_high","inferred_pb_amount_vs_wave_peak","inferred_pb_volume_vs_wave_peak",
                  "inferred_pb_drawdown_from_wave_high_pct","inferred_origin_close_vs_wave_high_pct",
                  "inferred_wave_high_reclaim_at_origin","origin_low_holds_inferred_pb_low"]:
            out[k] = np.nan
    return out


def med(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna()
    return float(x.median()) if len(x) else np.nan


def rate(s: pd.Series, val=1) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna()
    return float((x == val).mean()) if len(x) else np.nan


def outcome_stats(g: pd.DataFrame) -> Dict[str, float | int]:
    return {
        "events": int(len(g)),
        "d5_plus5_rate": rate(g.get("origin_d5_hit_plus5", pd.Series(dtype=float))),
        "d5_close_median": med(g.get("origin_d5_close_ret_pct", pd.Series(dtype=float))),
        "d5_mfe_median": med(g.get("origin_d5_mfe_pct", pd.Series(dtype=float))),
        "d5_mae_median": med(g.get("origin_d5_mae_pct", pd.Series(dtype=float))),
        "d10_close_median": med(g.get("origin_d10_close_ret_pct", pd.Series(dtype=float))),
        "d10_mfe_median": med(g.get("origin_d10_mfe_pct", pd.Series(dtype=float))),
        "d10_mae_median": med(g.get("origin_d10_mae_pct", pd.Series(dtype=float))),
    }


def horizon_summary(df: pd.DataFrame, label: str) -> pd.DataFrame:
    rows = []
    for h in (1,3,5):
        rec = {"cohort":label, "horizon":h, "events":int(len(df))}
        for hit in ("hit_plus3","hit_plus5"):
            col=f"origin_d{h}_{hit}"
            x=pd.to_numeric(df.get(col),errors="coerce").dropna() if col in df.columns else pd.Series(dtype=float)
            rec[f"{hit}_rate"] = float(x.mean()) if len(x) else np.nan
        for m in ("close_ret_pct","mfe_pct","mae_pct"):
            col=f"origin_d{h}_{m}"
            x=pd.to_numeric(df.get(col),errors="coerce").dropna() if col in df.columns else pd.Series(dtype=float)
            rec[f"{m}_median"] = float(x.median()) if len(x) else np.nan
        rows.append(rec)
    return pd.DataFrame(rows)


def comparison_summary(df: pd.DataFrame, group_col: str, feature_cols: List[str]) -> pd.DataFrame:
    rows = []
    if group_col not in df.columns:
        return pd.DataFrame()
    for gname, g in df.groupby(group_col, dropna=False):
        rec = {"comparison": group_col, "group": str(gname), **outcome_stats(g)}
        for c in feature_cols:
            if c not in g.columns:
                continue
            vals = pd.to_numeric(g[c], errors="coerce")
            rec[f"{c}__n"] = int(vals.notna().sum())
            rec[f"{c}__median"] = med(vals)
            if c in {"below_ma224", "inferred_wave_high_reclaim_at_origin", "origin_low_holds_inferred_pb_low"}:
                rec[f"{c}__rate1"] = rate(vals)
        rows.append(rec)
    return pd.DataFrame(rows)


def deterministic_sample(df: pd.DataFrame, limit: int = 40) -> pd.DataFrame:
    if df.empty:
        return df
    groups = [
        ("WIN_PLUS5_D5", 12),
        ("NO_PLUS5_D5", 12),
        ("MFE5_CLOSE_NONPOS", 8),
        ("DEEP_MAE_D5", 8),
    ]
    picks = []
    used = set()
    for name, n in groups:
        z = df[df["primary_outcome_group"].eq(name)].copy() if name in {"WIN_PLUS5_D5", "NO_PLUS5_D5"} else df[df[name].eq(1)].copy()
        if z.empty:
            continue
        z["_hash"] = z["event_id"].fillna(z["origin_date"].astype(str)+"|"+z["code"]).astype(str).map(lambda x: hashlib.sha256(x.encode()).hexdigest())
        z = z.sort_values("_hash")
        taken = 0
        for _, r in z.iterrows():
            eid = str(r.get("event_id") or f"{r.get('origin_date')}|{r.get('code')}|{r.get('origin_rank')}")
            if eid in used:
                continue
            used.add(eid)
            rr = r.drop(labels=["_hash"], errors="ignore").copy()
            rr["sample_stratum"] = name
            picks.append(rr)
            taken += 1
            if taken >= n or len(picks) >= limit:
                break
        if len(picks) >= limit:
            break
    if not picks:
        return df.head(limit).copy()
    out = pd.DataFrame(picks).head(limit).copy()
    return out



def deterministic_blind_sample(df: pd.DataFrame, limit: int = 40) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Outcome-stratified selection for coverage, then independently shuffled.
    Reviewer-facing sample contains NO future outcome labels. Key is separate.
    """
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()
    target = min(int(limit), len(df))
    quotas = {"CLEAN_PATH_WIN":10, "GIVEBACK":10, "DEEP_MAE_WIN":10, "NO_HIT":10}
    picks = []
    used = set()
    for grp, q in quotas.items():
        z = df[df.get("outcome4", pd.Series("", index=df.index)).eq(grp)].copy()
        if z.empty:
            continue
        z["_sel_hash"] = z.apply(
            lambda r: hashlib.sha256(
                ("R1_2_SELECT|"+str(r.get("event_id") or f"{r.get('origin_date')}|{r.get('code')}|{r.get('origin_rank')}")).encode()
            ).hexdigest(), axis=1
        )
        for _, r in z.sort_values("_sel_hash").head(q).iterrows():
            eid = str(r.get("event_id") or f"{r.get('origin_date')}|{r.get('code')}|{r.get('origin_rank')}")
            if eid in used:
                continue
            used.add(eid); picks.append(r.drop(labels=["_sel_hash"], errors="ignore"))
    # Fill any shortfall from the remaining population without changing labels/thresholds.
    if len(picks) < target:
        rest = df.copy()
        rest["_eid"] = rest.apply(lambda r: str(r.get("event_id") or f"{r.get('origin_date')}|{r.get('code')}|{r.get('origin_rank')}"), axis=1)
        rest = rest[~rest["_eid"].isin(used)].copy()
        rest["_sel_hash"] = rest["_eid"].map(lambda x: hashlib.sha256(("R1_2_FILL|"+x).encode()).hexdigest())
        for _, r in rest.sort_values("_sel_hash").head(target-len(picks)).iterrows():
            picks.append(r.drop(labels=["_sel_hash","_eid"], errors="ignore"))
    chosen = pd.DataFrame(picks).head(target).copy()
    if chosen.empty:
        return pd.DataFrame(), pd.DataFrame()
    chosen["_blind_hash"] = chosen.apply(
        lambda r: hashlib.sha256(
            ("R1_2_BLIND_ORDER|"+str(r.get("event_id") or f"{r.get('origin_date')}|{r.get('code')}|{r.get('origin_rank')}")).encode()
        ).hexdigest(), axis=1
    )
    chosen = chosen.sort_values("_blind_hash").reset_index(drop=True)
    chosen["blind_id"] = [f"B{i:02d}" for i in range(1, len(chosen)+1)]

    key_cols = [
        "blind_id","event_id","origin_date","code","name","origin_rank","outcome4",
        "origin_d5_hit_plus5","origin_d5_close_ret_pct","origin_d5_mfe_pct","origin_d5_mae_pct",
        "origin_d10_close_ret_pct","origin_d10_mfe_pct","origin_d10_mae_pct",
        "v72_score100","v72_pullback_restart_score","v72_pullback_restart_score_raw","v72_pullback_restart_grade"
    ]
    key = chosen[[c for c in key_cols if c in chosen.columns]].copy()

    # Reviewer-facing table: causal metadata only. Explicitly strip all future/outcome fields.
    exclude_prefix = ("origin_d1_","origin_d3_","origin_d5_","origin_d10_","origin_d15_","ready_d")
    exclude_exact = {
        "outcome4","primary_outcome_group","MFE5_CLOSE_NONPOS","MFE5_CLOSE_POS","DEEP_MAE_D5",
        "_blind_hash","sample_stratum"
    }
    causal_cols = []
    for c in chosen.columns:
        if c in exclude_exact or c.startswith(exclude_prefix):
            continue
        causal_cols.append(c)
    blind = chosen[causal_cols].copy()
    front = [c for c in ["blind_id","origin_date","code","name","origin_rank"] if c in blind.columns]
    blind = blind[front + [c for c in blind.columns if c not in front]]
    return blind, key


def make_blind_review_bars(blind_sample: pd.DataFrame, frames: Dict[str, pd.DataFrame], pre: int = 60) -> pd.DataFrame:
    """True causal blind pack: only pre-origin bars and the origin bar. No D+1+ bars, no outcome labels."""
    rows = []
    for _, r in blind_sample.iterrows():
        c = code(r.get("code")); d = pd.Timestamp(r.get("origin_date")).normalize()
        f = frames.get(c)
        if f is None or f.empty:
            continue
        z = f.copy(); z["date"] = pd.to_datetime(z["date"], errors="coerce").dt.normalize()
        before = z[z["date"].lt(d)].tail(pre)
        origin = z[z["date"].eq(d)].tail(1)
        q = pd.concat([before, origin], ignore_index=True)
        if q.empty:
            continue
        origin_pos = len(before)
        for i, bar in q.iterrows():
            rows.append({
                "blind_id": r.get("blind_id"),
                "origin_date": r.get("origin_date"),
                "code": c,
                "name": r.get("name"),
                "bar_offset": int(i-origin_pos),
                "date": bar.get("date"),
                "open": bar.get("open"),
                "high": bar.get("high"),
                "low": bar.get("low"),
                "close": bar.get("close"),
                "volume": bar.get("volume"),
                "research_amount": bar.get("research_amount"),
                "ma224": bar.get("ma224"),
            })
    out = pd.DataFrame(rows)
    if not out.empty:
        assert int(pd.to_numeric(out["bar_offset"], errors="coerce").max()) <= 0, "BLIND_PACK_FUTURE_BAR_LEAK"
    return out


def make_review_bars(sample: pd.DataFrame, frames: Dict[str, pd.DataFrame], pre: int = 60, post: int = 15) -> pd.DataFrame:
    rows = []
    for _, r in sample.iterrows():
        c = code(r.get("code")); d = pd.Timestamp(r.get("origin_date")).normalize()
        f = frames.get(c)
        if f is None or f.empty:
            continue
        z = f.copy(); z["date"] = pd.to_datetime(z["date"], errors="coerce").dt.normalize()
        before = z[z["date"].lt(d)].tail(pre)
        origin = z[z["date"].eq(d)].tail(1)
        after = z[z["date"].gt(d)].head(post)
        q = pd.concat([before, origin, after], ignore_index=True)
        if q.empty: continue
        origin_pos = len(before)
        for i, bar in q.iterrows():
            rows.append({
                "event_id": r.get("event_id"), "origin_date": r.get("origin_date"), "code": c, "name": r.get("name"),
                "primary_outcome_group": r.get("primary_outcome_group"), "bar_offset": int(i-origin_pos),
                "date": bar.get("date"), "open": bar.get("open"), "high": bar.get("high"), "low": bar.get("low"),
                "close": bar.get("close"), "volume": bar.get("volume"), "research_amount": bar.get("research_amount"),
                "ma224": bar.get("ma224"),
            })
    return pd.DataFrame(rows)


def parse_matrices(context_report: Path, context_dir: Path) -> int:
    if context_report.exists():
        txt = context_report.read_text(encoding="utf-8", errors="ignore")
        m = re.search(r"matrices\s*=\s*(\d+)", txt)
        if m: return int(m.group(1))
    if context_dir.exists():
        files = [p for p in context_dir.glob("context_matrix_*.csv") if p.name != "context_matrix_all.csv"]
        return len(files)
    return 0


def fmt_pct(v) -> str:
    x = num(v)
    return "NA" if not math.isfinite(x) else f"{x*100:.1f}%"


def fmt_num(v, suffix="") -> str:
    x = num(v)
    return "NA" if not math.isfinite(x) else f"{x:.2f}{suffix}"


def run(a) -> int:
    outdir = Path(a.output_dir); outdir.mkdir(parents=True, exist_ok=True)
    events = read_csv(a.event_ledger, dtype={"code": str})
    raw = read_jsonl(a.raw_snapshots)
    readiness = read_csv(a.readiness_csv)
    if events.empty:
        raise SystemExit("REAL_FULL_STRUCTURE_RESEARCH_EVENT_LEDGER_EMPTY")
    events["code"] = events["code"].map(code)
    if not raw.empty:
        raw["code"] = raw["code"].map(code)
        raw["_snapshot_date"] = raw["_snapshot_date"].astype(str)
        raw["_source_order"] = pd.to_numeric(raw["_source_order"], errors="coerce")
        merged = events.merge(raw, left_on=["origin_date","origin_rank","code"], right_on=["_snapshot_date","_source_order","code"], how="left", suffixes=("", "_raw"), indicator=True)
        raw_match = int((merged["_merge"] == "both").sum())
        merged = merged.drop(columns=["_merge"])
    else:
        merged = events.copy(); raw_match = 0

    # Identity guard: performance interpretation is forbidden if this does not match.
    snapshots = int(pd.to_numeric(readiness.get("snapshot_dates", pd.Series([0])), errors="coerce").fillna(0).iloc[-1]) if not readiness.empty else 0
    top15 = int(pd.to_numeric(readiness.get("top15_events", pd.Series([len(events)])), errors="coerce").fillna(0).iloc[-1]) if not readiness.empty else len(events)
    d5 = int(pd.to_numeric(readiness.get("d5_mature_events", pd.Series([0])), errors="coerce").fillna(0).iloc[-1]) if not readiness.empty else int(pd.to_numeric(events.get("origin_d5_complete"), errors="coerce").fillna(0).eq(1).sum())
    matrices = parse_matrices(Path(a.context_report), Path(a.context_dir))
    identity = {
        "snapshot_dates": snapshots, "top15_events": top15, "d5_mature_events": d5, "context_pattern_matrices": matrices,
        "expected_snapshot_dates": a.expected_snapshots, "expected_top15_events": a.expected_events,
        "expected_d5_mature_events": a.expected_d5, "expected_context_pattern_matrices": a.expected_matrices,
    }
    checks = []
    for actual, expected in [(snapshots,a.expected_snapshots),(top15,a.expected_events),(d5,a.expected_d5),(matrices,a.expected_matrices)]:
        checks.append(True if int(expected) <= 0 else int(actual) == int(expected))
    identity_pass = all(checks)
    identity["identity_pass"] = int(identity_pass)
    pd.DataFrame([identity]).to_csv(outdir/"identity_audit.csv", index=False, encoding="utf-8-sig")
    if not identity_pass:
        (outdir/"real_full_structure_research_report.txt").write_text(
            f"🧪 [REAL_FULL STRUCTURE RESEARCH R1]\nIDENTITY FAIL-CLOSED · actual={snapshots}/{top15}/{d5}/{matrices}\n성과 해석 중단. 검색식/점수/랭킹 변경 0.\n", encoding="utf-8")
        print("REAL_FULL_STRUCTURE_RESEARCH_IDENTITY_FAIL", identity)
        return 41

    cache = Path(a.price_cache_dir)
    frames: Dict[str, pd.DataFrame] = {}
    amount_sources: Dict[str, str] = {}
    feat_rows = []
    for _, r in merged.iterrows():
        c = code(r.get("code"))
        if c not in frames:
            frames[c], amount_sources[c] = load_price_frame(cache, c)
        f = frames[c]
        feat = structural_features(f, str(r.get("origin_date")), r.get("origin_pullback_days"))
        feat["amount_source"] = amount_sources.get(c, "MISSING")
        feat_rows.append(feat)
    features = pd.DataFrame(feat_rows, index=merged.index)
    research = pd.concat([merged.reset_index(drop=True), features.reset_index(drop=True)], axis=1)

    # Fixed outcome labels. These are labels only; they are never fed back into the detector.
    d5hit = pd.to_numeric(research.get("origin_d5_hit_plus5"), errors="coerce").fillna(0).eq(1)
    research["primary_outcome_group"] = np.where(d5hit, "WIN_PLUS5_D5", "NO_PLUS5_D5")
    d5mfe = pd.to_numeric(research.get("origin_d5_mfe_pct"), errors="coerce")
    d5close = pd.to_numeric(research.get("origin_d5_close_ret_pct"), errors="coerce")
    d5mae = pd.to_numeric(research.get("origin_d5_mae_pct"), errors="coerce")
    research["MFE5_CLOSE_NONPOS"] = ((d5mfe >= MFE_WATCH_PCT) & (d5close <= 0)).astype(int)
    research["MFE5_CLOSE_POS"] = ((d5mfe >= MFE_WATCH_PCT) & (d5close > 0)).astype(int)
    research["DEEP_MAE_D5"] = (d5mae <= DEEP_MAE_PCT).astype(int)
    research["outcome4"] = assign_outcome4(research)

    # R1.2: preserve and parse the detector's own causal V72 diagnostics.
    dbg_rows = [parse_v72_debug(v) for v in research.get("v72_pullback_restart_debug", pd.Series("", index=research.index))]
    dbg = pd.DataFrame(dbg_rows, index=research.index)
    for c in dbg.columns:
        research[c] = pd.to_numeric(dbg[c], errors="coerce")
    v72_score = pd.to_numeric(research.get("v72_pullback_restart_score"), errors="coerce")
    v72_raw = pd.to_numeric(research.get("v72_pullback_restart_score_raw"), errors="coerce")
    research["v72_score100"] = v72_score.ge(99.999).astype(int)

    research["ma224_context"] = np.where(pd.to_numeric(research.get("ma224_ready"), errors="coerce").eq(1), np.where(pd.to_numeric(research.get("below_ma224"), errors="coerce").eq(1), "BELOW224", "AT_OR_ABOVE224"), "MA224_NOT_READY")
    research["inferred_reclaim_context"] = np.where(pd.to_numeric(research.get("inferred_pb_ready"), errors="coerce").eq(1), np.where(pd.to_numeric(research.get("inferred_wave_high_reclaim_at_origin"), errors="coerce").eq(1), "INFERRED_RECLAIM", "INFERRED_NO_RECLAIM"), "INFERRED_PB_NOT_READY")
    research.to_csv(outdir/"structure_event_ledger.csv", index=False, encoding="utf-8-sig")

    feature_cols = [
        "ma224_distance_pct","prior20_below_ma224_rate","range60_position_pct",
        "pre20_amount_ratio_to_prior60_median","pre20_gradual_pulse_days_1p2_2x","pre20_spike_days_ge2x",
        "pre20_amount_slope_norm","pre20_volume_slope_norm","pre5_amount_vs_pre20_median","pre10_amount_vs_pre20_median",
        "signal_amount_vs_pre20_median","signal_volume_vs_pre20_median",
        "origin_impulse_pct","origin_pullback_days","origin_support_count","origin_volume_ratio20","origin_headroom_pct",
        "origin_bb40","origin_obv_slope",
        "v72_pullback_restart_score","v72_pullback_restart_score_raw","v72_impulse_pct","v72_pullback_days",
        "v72_support_count","v72_volume_ratio20","v72_headroom_pct","v72_stop_distance_pct",
        "v72_debug_k","v72_debug_d","v72_debug_rsi","v72_debug_disp","v72_debug_wick",
        "inferred_pb_amount_vs_wave_peak","inferred_pb_volume_vs_wave_peak","inferred_pb_drawdown_from_wave_high_pct",
        "inferred_origin_close_vs_wave_high_pct","below_ma224","inferred_wave_high_reclaim_at_origin","origin_low_holds_inferred_pb_low",
    ]
    comps = []
    for gcol in ["primary_outcome_group","outcome4","ma224_context","inferred_reclaim_context","MFE5_CLOSE_NONPOS","DEEP_MAE_D5"]:
        s = comparison_summary(research, gcol, feature_cols)
        if not s.empty: comps.append(s)
    all_comp = pd.concat(comps, ignore_index=True) if comps else pd.DataFrame()
    all_comp.to_csv(outdir/"winner_loser_structure_comparison.csv", index=False, encoding="utf-8-sig")

    # R1.2 V72 saturation audit: quantify whether the legacy detector still separates candidates.
    restart_series = research.get("v72_pullback_restart", pd.Series(False, index=research.index)).map(boolish)
    grade_series = research.get("v72_pullback_restart_grade", pd.Series("", index=research.index)).astype(str).str.strip().str.upper()
    patt_series = research.get("origin_search_pattern", pd.Series("", index=research.index)).astype(str)
    sat_rows = [
        {"metric":"v72_restart_true","count":int(restart_series.sum()),"total":len(research),"rate":float(restart_series.mean()) if len(research) else np.nan},
        {"metric":"v72_grade_A","count":int(grade_series.eq("A").sum()),"total":len(research),"rate":float(grade_series.eq("A").mean()) if len(research) else np.nan},
        {"metric":"v72_score_100","count":int(research["v72_score100"].eq(1).sum()),"total":len(research),"rate":float(research["v72_score100"].eq(1).mean()) if len(research) else np.nan},
        {"metric":"v72_raw_score_ge100","count":int(v72_raw.ge(100).sum()),"total":len(research),"rate":float(v72_raw.ge(100).mean()) if len(research) else np.nan},
        {"metric":"first_pullback_pattern","count":int(patt_series.str.contains("첫눌림|PULLBACK_RESTART", regex=True, na=False).sum()),"total":len(research),"rate":float(patt_series.str.contains("첫눌림|PULLBACK_RESTART", regex=True, na=False).mean()) if len(research) else np.nan},
    ]
    pd.DataFrame(sat_rows).to_csv(outdir/"v72_saturation_audit.csv", index=False, encoding="utf-8-sig")
    score100 = research[research["v72_score100"].eq(1)].copy()
    score100_cmp = comparison_summary(score100, "outcome4", feature_cols)
    score100_cmp.to_csv(outdir/"v72_score100_outcome4_comparison.csv", index=False, encoding="utf-8-sig")

    # V72 is an existing short-horizon closing-bet detector. Keep D1/D3 reaction separate from D5 hold-risk.
    v72_mask = restart_series | patt_series.str.contains("첫눌림|PULLBACK_RESTART", regex=True, na=False)
    v72_events = research[v72_mask].copy()
    v72_horizon = horizon_summary(v72_events, "V72_PULLBACK_RESTART")
    v72_horizon.to_csv(outdir/"v72_horizon_alignment.csv", index=False, encoding="utf-8-sig")

    # Compact fixed-context outcome tables; descriptive only.
    context_rows = []
    for ccol in ["ma224_context","inferred_reclaim_context","origin_td_label","origin_pullback_grade","origin_search_pattern","origin_canonical_phase"]:
        if ccol not in research.columns: continue
        for name, g in research.groupby(ccol, dropna=False):
            context_rows.append({"context":ccol,"group":str(name),**outcome_stats(g)})
    pd.DataFrame(context_rows).to_csv(outdir/"fixed_context_outcomes.csv", index=False, encoding="utf-8-sig")

    # Manual review pack, deterministic and separate from strategy authority.
    sample = deterministic_sample(research, limit=a.manual_sample)
    sample.to_csv(outdir/"manual_review_sample.csv", index=False, encoding="utf-8-sig")
    bars = make_review_bars(sample, frames)
    bars.to_csv(outdir/"manual_review_bars.csv", index=False, encoding="utf-8-sig")

    # R1.2 true blind pack. Outcome key is deliberately isolated from the reviewer-facing files.
    blind_sample, blind_key = deterministic_blind_sample(research, limit=a.manual_sample)
    blind_sample.to_csv(outdir/"blind_review_sample.csv", index=False, encoding="utf-8-sig")
    blind_bars = make_blind_review_bars(blind_sample, frames, pre=60)
    blind_bars.to_csv(outdir/"blind_review_bars.csv", index=False, encoding="utf-8-sig")
    blind_key.to_csv(outdir/"blind_review_key_DO_NOT_OPEN_UNTIL_REVIEW.csv", index=False, encoding="utf-8-sig")

    hist_ready = int(research.get("price_history_status", pd.Series(dtype=str)).eq("READY").sum())
    unique_codes = int(research["code"].nunique())
    hist_codes = int(sum(1 for c in set(research["code"]) if c in frames and not frames[c].empty))
    actual_amount_events = int(research.get("amount_source", pd.Series(dtype=str)).eq("ACTUAL_AMOUNT").sum())
    proxy_amount_events = int(research.get("amount_source", pd.Series(dtype=str)).eq("CLOSE_X_VOLUME_PROXY").sum())
    ma_ready = research[pd.to_numeric(research.get("ma224_ready"), errors="coerce").eq(1)]
    below = ma_ready[pd.to_numeric(ma_ready.get("below_ma224"), errors="coerce").eq(1)]
    above = ma_ready[pd.to_numeric(ma_ready.get("below_ma224"), errors="coerce").eq(0)]
    win = research[research["primary_outcome_group"].eq("WIN_PLUS5_D5")]
    lose = research[research["primary_outcome_group"].eq("NO_PLUS5_D5")]
    giveback = research[research["MFE5_CLOSE_NONPOS"].eq(1)]
    clean = research[research["outcome4"].eq("CLEAN_PATH_WIN")]
    deepwin = research[research["outcome4"].eq("DEEP_MAE_WIN")]
    nohit = research[research["outcome4"].eq("NO_HIT")]
    score100_counts = score100["outcome4"].value_counts().to_dict()
    reclaim = research[research["inferred_reclaim_context"].eq("INFERRED_RECLAIM")]
    noreclaim = research[research["inferred_reclaim_context"].eq("INFERRED_NO_RECLAIM")]

    def line_group(label, g):
        st = outcome_stats(g)
        return f"{label} n={st['events']} · +5% D5={fmt_pct(st['d5_plus5_rate'])} · D5 Close={fmt_num(st['d5_close_median'],'%')} · MFE={fmt_num(st['d5_mfe_median'],'%')} · MAE={fmt_num(st['d5_mae_median'],'%')}"

    report = "\n".join([
        "🧪 [REAL_FULL STRUCTURE RESEARCH R1.2]",
        "RESEARCH ONLY · 본 검색식/점수/랭킹/주문 변경 0 · same-sample tuning 금지",
        f"revision={RESEARCH_REVISION}",
        f"identity={snapshots}/{top15}/{d5}/{matrices} PASS · raw snapshot match={raw_match}/{len(events)}",
        f"price history events={hist_ready}/{len(research)} · unique codes={hist_codes}/{unique_codes}",
        f"Amount provenance: actual={actual_amount_events} · Close×Volume proxy={proxy_amount_events} · proxy는 실제 거래대금과 동일하다고 해석 금지",
        "",
        "🎯 [Fixed D+5 Outcome Split]",
        line_group("WIN(+5%≤D5)", win),
        line_group("NO-WIN", lose),
        f"MFE≥+5%인데 D5 Close≤0 giveback n={len(giveback)} · 연구용 실패경로",
        "",
        "🧭 [R1.2 Fixed Outcome Taxonomy · outcome label only]",
        line_group("CLEAN_PATH_WIN (+5% hit · D5 Close>0 · MAE>-5%)", clean),
        line_group("DEEP_MAE_WIN (+5% hit · D5 Close>0 · MAE≤-5%)", deepwin),
        line_group("GIVEBACK (+5% hit · D5 Close≤0)", giveback),
        line_group("NO_HIT", nohit),
        "※ 이 4분류는 연구 outcome label이며 detector/gate/score에 피드백하지 않음.",
        "",
        "🧱 [V72 Saturation Audit]",
        f"restart TRUE={int(restart_series.sum())}/{len(research)} · Grade A={int(grade_series.eq('A').sum())}/{len(research)} · score=100={int(research['v72_score100'].eq(1).sum())}/{len(research)} · raw≥100={int(v72_raw.ge(100).sum())}/{len(research)}",
        f"score100 내부: CLEAN={int(score100_counts.get('CLEAN_PATH_WIN',0))} · GIVEBACK={int(score100_counts.get('GIVEBACK',0))} · DEEP_MAE_WIN={int(score100_counts.get('DEEP_MAE_WIN',0))} · NO_HIT={int(score100_counts.get('NO_HIT',0))}",
        "※ score=100/Grade A가 성과군을 충분히 분리하지 못하는지 감사하는 연구판. 본 V72 판정은 수정하지 않음.",
        "",
        "⏱️ [V72 Horizon Alignment · detector quality와 hold-risk 분리]",
        f"V72 n={len(v72_events)} · D1 +3={fmt_pct(pd.to_numeric(v72_events.get('origin_d1_hit_plus3'),errors='coerce').mean())} / +5={fmt_pct(pd.to_numeric(v72_events.get('origin_d1_hit_plus5'),errors='coerce').mean())} · Close={fmt_num(med(v72_events.get('origin_d1_close_ret_pct',pd.Series(dtype=float))),'%')} · MFE={fmt_num(med(v72_events.get('origin_d1_mfe_pct',pd.Series(dtype=float))),'%')} · MAE={fmt_num(med(v72_events.get('origin_d1_mae_pct',pd.Series(dtype=float))),'%')}",
        f"D3 +3={fmt_pct(pd.to_numeric(v72_events.get('origin_d3_hit_plus3'),errors='coerce').mean())} / +5={fmt_pct(pd.to_numeric(v72_events.get('origin_d3_hit_plus5'),errors='coerce').mean())} · Close={fmt_num(med(v72_events.get('origin_d3_close_ret_pct',pd.Series(dtype=float))),'%')} · MFE={fmt_num(med(v72_events.get('origin_d3_mfe_pct',pd.Series(dtype=float))),'%')} · MAE={fmt_num(med(v72_events.get('origin_d3_mae_pct',pd.Series(dtype=float))),'%')}",
        f"D5 +3={fmt_pct(pd.to_numeric(v72_events.get('origin_d5_hit_plus3'),errors='coerce').mean())} / +5={fmt_pct(pd.to_numeric(v72_events.get('origin_d5_hit_plus5'),errors='coerce').mean())} · Close={fmt_num(med(v72_events.get('origin_d5_close_ret_pct',pd.Series(dtype=float))),'%')} · MFE={fmt_num(med(v72_events.get('origin_d5_mfe_pct',pd.Series(dtype=float))),'%')} · MAE={fmt_num(med(v72_events.get('origin_d5_mae_pct',pd.Series(dtype=float))),'%')}",
        "※ 기존 V72 운용은 +3 우선익절·5일 보유 금지. 따라서 GIVEBACK/D5 Close는 장기보유 위험 label이지 곧바로 detector false-positive 판정이 아님.",
        "",
        "📏 [MA224 Context · signal close 시점 causal]",
        line_group("BELOW224", below),
        line_group("AT/ABOVE224", above),
        "※ MA224는 origin 종가까지의 과거 224 bars로 계산. 조건 승격 금지.",
        "",
        "🌱 [Accumulation Descriptors · causal]",
        f"winner pre20 gradual-pulse median={fmt_num(med(win.get('pre20_gradual_pulse_days_1p2_2x',pd.Series(dtype=float))))}d · spike≥2x={fmt_num(med(win.get('pre20_spike_days_ge2x',pd.Series(dtype=float))))}d · amount-slope={fmt_num(med(win.get('pre20_amount_slope_norm',pd.Series(dtype=float))))}",
        f"loser  pre20 gradual-pulse median={fmt_num(med(lose.get('pre20_gradual_pulse_days_1p2_2x',pd.Series(dtype=float))))}d · spike≥2x={fmt_num(med(lose.get('pre20_spike_days_ge2x',pd.Series(dtype=float))))}d · amount-slope={fmt_num(med(lose.get('pre20_amount_slope_norm',pd.Series(dtype=float))))}",
        "※ 1.2~2.0x/≥2.0x는 고정 설명구간이며 매매 threshold가 아님.",
        "",
        "🪂 [Pullback Anatomy · inferred audit only]",
        f"inferred PB ready={int(pd.to_numeric(research.get('inferred_pb_ready'),errors='coerce').eq(1).sum())}/{len(research)}",
        line_group("INFERRED_RECLAIM", reclaim),
        line_group("INFERRED_NO_RECLAIM", noreclaim),
        "※ existing pullback_days로 PB 구간을 역산한 감사용 proxy. scanner의 정확한 Wave1 high라고 주장하지 않음.",
        "",
        "🧭 [Winner vs Loser Anatomy files]",
        "structure_event_ledger.csv · winner_loser_structure_comparison.csv · fixed_context_outcomes.csv",
        f"legacy manual sample={len(sample)} · review bars={len(bars)}",
        f"TRUE BLIND sample={len(blind_sample)} · causal bars={len(blind_bars)} · max bar_offset={int(pd.to_numeric(blind_bars.get('bar_offset'),errors='coerce').max()) if len(blind_bars) else 'NA'}",
        "blind_review_sample/bars에는 outcome·D+1 이후 봉을 제거. 정답은 blind_review_key_DO_NOT_OPEN_UNTIL_REVIEW.csv에만 분리.",
        "",
        "🔒 [Authority]",
        "모든 구조 feature는 origin_date 이하 데이터만 사용. D+5/D+10은 outcome label로만 사용.",
        "이번 R1.2 결과만 보고 조건 추가/삭제/가중치 변경 금지. CORE224/TRIANGLE1PB/LOW224와 독립.",
        "➡️ NEXT: TRUE BLIND 40개를 outcome 미공개 상태로 판정한 뒤 key와 결합. V72=100 내부 반복 causal 차이 1개가 재현될 때만 Shadow 후보 검토.",
    ])
    (outdir/"real_full_structure_research_report.txt").write_text(report, encoding="utf-8")
    provenance = {
        "research_id":RESEARCH_ID,"revision":RESEARCH_REVISION,"identity":identity,
        "event_ledger":str(a.event_ledger),"raw_snapshots":str(a.raw_snapshots),"price_cache_dir":str(a.price_cache_dir),
        "research_only":1,"search_logic_changed":0,"score_changed":0,"ranking_changed":0,"order_changed":0,
        "same_sample_tuning_allowed":0,"actual_amount_events":actual_amount_events,"proxy_amount_events":proxy_amount_events,
        "inferred_pullback_anchor_is_exact_strategy_anchor":0,
        "outcome4_is_research_label_only":1,
        "true_blind_future_bars_removed":1,
        "true_blind_outcome_labels_removed":1,
        "v72_detector_changed":0,
        "v72_horizon_alignment_separated":1,
        "d5_giveback_is_hold_risk_not_detector_truth":1,
    }
    (outdir/"provenance.json").write_text(json.dumps(provenance, ensure_ascii=False, indent=2), encoding="utf-8")
    print(report)
    return 0


def self_test() -> int:
    dates = pd.bdate_range("2025-01-01", periods=260)
    close = np.linspace(100, 150, len(dates))
    fr = pd.DataFrame({"date":dates,"open":close,"high":close*1.01,"low":close*.99,"close":close,"volume":np.arange(len(dates))+1000})
    fr["research_amount"] = fr["close"]*fr["volume"]
    fr["ma224"] = fr["close"].rolling(224,min_periods=224).mean()
    x=structural_features(fr,str(dates[-1].date()),4)
    assert x["price_history_status"]=="READY" and x["ma224_ready"]==1 and math.isfinite(num(x["ma224_distance_pct"]))
    t = pd.DataFrame([
        {"origin_d5_hit_plus5":1,"origin_d5_close_ret_pct":3,"origin_d5_mae_pct":-2},
        {"origin_d5_hit_plus5":1,"origin_d5_close_ret_pct":2,"origin_d5_mae_pct":-8},
        {"origin_d5_hit_plus5":1,"origin_d5_close_ret_pct":-1,"origin_d5_mae_pct":-7},
        {"origin_d5_hit_plus5":0,"origin_d5_close_ret_pct":1,"origin_d5_mae_pct":-1},
    ])
    assert list(assign_outcome4(t)) == ["CLEAN_PATH_WIN","DEEP_MAE_WIN","GIVEBACK","NO_HIT"]
    dbg=parse_v72_debug("score=112, impulse=45.8, support=4, k=88.1/46.6, rsi=58.1, disp=106.9, wick=0.17")
    assert dbg["v72_debug_score"]==112 and abs(dbg["v72_debug_k"]-88.1)<1e-9 and abs(dbg["v72_debug_d"]-46.6)<1e-9
    print("REAL_FULL_STRUCTURE_RESEARCH_SELF_TEST PASS")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--event-ledger", default="source/reports/real_full_validation_backtest/analysis/pattern_backtest_event_ledger.csv")
    ap.add_argument("--raw-snapshots", default="source/reports/real_full_validation_backtest/materialized_adapter/materialized_candidate_snapshots.jsonl")
    ap.add_argument("--readiness-csv", default="source/reports/real_full_validation_backtest/analysis/real_full_validation_readiness.csv")
    ap.add_argument("--context-report", default="source/reports/real_full_validation_backtest/context_risk/context_pattern_risk_report.txt")
    ap.add_argument("--context-dir", default="source/reports/real_full_validation_backtest/context_risk")
    ap.add_argument("--price-cache-dir", default="source/reports/.cache/v20_price_history")
    ap.add_argument("--output-dir", default="reports/real_full_structure_research_r1")
    ap.add_argument("--expected-snapshots", type=int, default=24)
    ap.add_argument("--expected-events", type=int, default=115)
    ap.add_argument("--expected-d5", type=int, default=115)
    ap.add_argument("--expected-matrices", type=int, default=13)
    ap.add_argument("--manual-sample", type=int, default=40)
    ap.add_argument("--self-test", action="store_true")
    a=ap.parse_args()
    return self_test() if a.self_test else run(a)


if __name__ == "__main__":
    raise SystemExit(main())
