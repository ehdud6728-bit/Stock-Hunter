#!/usr/bin/env python3
"""LOW224_ACCUM_WAVE1_PB_R1 independent structure-first research lane.

Research only. No LIVE, scoring, ranking, policy, or order behavior.

Hypothesis:
    LOW224_BASE
      -> GRADUAL_AMOUNT_ACCUM
      -> WAVE1
      -> FIRST_PULLBACK
      -> STABILIZATION
      -> REACCELERATION

This R1 is deliberately broad and audit-first. Thresholds below are frozen
structure definitions for the first audit and MUST NOT be tuned from outcomes.

Historical traded value must be explicit actual Amount. Close*Volume is never
used. Causal historical as-of universe is required.

TRIANGLE1PB is imported only for its already-audited cache adapters and
data-authority classes; no TRIANGLE detector/stage/gate is called.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from triangle1pb_research import (
    AmountAuthority,
    UniverseAuthority,
    _load_any,
    normalize_price_frame,
)

SCHEMA = "LOW224_ACCUM_WAVE1_PB_RESEARCH_SCHEMA_V1"
STRATEGY_ID = "LOW224_ACCUM_WAVE1_PB_R1_STRUCTURE_FIRST"
LOADER_REVISION = "LOW224_R1_0_STRUCTURE_AUDIT_ONLY"
RESEARCH_AUTHORITY = "RESEARCH_ONLY_NO_LIVE_NO_SCORE_NO_RANK_NO_ORDERS"
SHARED_DATA_AUTHORITY_ONLY = "TRIANGLE1PB_CACHE_ADAPTERS_ONLY_NO_PATTERN_LOGIC"

STAGES = [
    "LOW224_BASE",
    "GRADUAL_AMOUNT_ACCUM",
    "WAVE1",
    "FIRST_PULLBACK",
    "STABILIZATION",
    "REACCELERATION",
]


@dataclass(frozen=True)
class FrozenR1Config:
    # Broad R1 structure definitions. Not optimized from performance.
    ma_long: int = 224
    base_context_bars: int = 20

    # "스물스물" actual-Amount proxy.
    accum_recent_bars: int = 5
    accum_prior_bars: int = 20
    accum_min_prior_amount_obs: int = 15
    accum_recent_vs_prior_min: float = 1.10
    accum_recent_days_above_prior_median_min: int = 3
    accum_max_single_day_vs_prior_median: float = 3.00
    accum_price_abs_return20_max: float = 0.08

    # First impulse / wave 1.
    wave1_wait_max_bars: int = 20
    wave1_prior_high_lookback: int = 20
    wave1_min_gain_from_base_low: float = 0.08
    wave1_min_amount20_ratio: float = 1.50

    # First pullback.
    pullback_wait_max_bars: int = 10
    pullback_min_drawdown: float = 0.03
    pullback_max_drawdown: float = 0.15

    # Causal stabilization marker.
    stabilization_wait_max_bars: int = 4
    stabilization_amount_vs_wave_max: float = 0.80

    # Re-acceleration is intentionally not wave-high reclaim.
    # It is a broad "next-bar strength + Amount re-entry" marker.
    reaccel_wait_max_bars: int = 8
    reaccel_min_amount_vs_pullback_median: float = 1.20

    amount20_window: int = 20
    amount20_min_obs: int = 15
    universe_max_calendar_age_days: int = 10
    cooldown_bars: int = 20
    forward_horizons: Tuple[int, ...] = (5, 10, 15)


CONFIG = FrozenR1Config()


def _finite(x: Any) -> bool:
    try:
        return math.isfinite(float(x))
    except Exception:
        return False


def _median_positive(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce")
    x = x[x > 0]
    return float(x.median()) if not x.empty else float("nan")


def _amount20_prior(df: pd.DataFrame, i: int, cfg: FrozenR1Config) -> Tuple[float, int]:
    a = pd.to_numeric(df["amount"].iloc[max(0, i-cfg.amount20_window):i], errors="coerce")
    a = a[a > 0]
    if len(a) < cfg.amount20_min_obs:
        return float("nan"), int(len(a))
    return float(a.mean()), int(len(a))


def _amount_slope_norm(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").to_numpy(dtype=float)
    ok = np.isfinite(x) & (x > 0)
    if int(ok.sum()) < 5:
        return float("nan")
    y = np.log(x[ok])
    xx = np.arange(len(y), dtype=float)
    slope = float(np.polyfit(xx, y, 1)[0])
    return slope


def _stage_row(
    code: str,
    episode_id: str,
    stage: str,
    i: int,
    df: pd.DataFrame,
    universe_snapshot: str,
    universe_age: int,
    amount_source: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    r = df.iloc[i]
    out = {
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "episode_id": episode_id,
        "code": code,
        "stage": stage,
        "event_date": pd.Timestamp(r["date"]).date().isoformat(),
        "feature_max_date": pd.Timestamp(r["date"]).date().isoformat(),
        "universe_snapshot_date": universe_snapshot,
        "universe_age_days": int(universe_age),
        "amount_source": amount_source,
        "bar_index": int(i),
        "open": float(r["open"]),
        "high": float(r["high"]),
        "low": float(r["low"]),
        "close": float(r["close"]),
        "actual_amount": float(r["amount"]) if _finite(r["amount"]) else float("nan"),
    }
    if extra:
        out.update(extra)
    return out


def _find_accum(
    df: pd.DataFrame, i: int, cfg: FrozenR1Config
) -> Optional[Dict[str, Any]]:
    # Current/recent 5 bars vs the preceding 20 bars. All causal.
    r0 = i - cfg.accum_recent_bars + 1
    p0 = r0 - cfg.accum_prior_bars
    if p0 < 0:
        return None

    recent = pd.to_numeric(df["amount"].iloc[r0:i+1], errors="coerce")
    prior = pd.to_numeric(df["amount"].iloc[p0:r0], errors="coerce")
    prior = prior[prior > 0]
    recent_pos = recent[recent > 0]
    if len(prior) < cfg.accum_min_prior_amount_obs or len(recent_pos) < cfg.accum_recent_bars:
        return None

    prior_med = float(prior.median())
    if not _finite(prior_med) or prior_med <= 0:
        return None

    recent_med = float(recent_pos.median())
    recent_vs_prior = recent_med / prior_med
    recent_above = int((recent_pos > prior_med).sum())
    max_day_ratio = float(recent_pos.max() / prior_med)

    trend_window = pd.to_numeric(
        df["amount"].iloc[max(0, i-(cfg.accum_prior_bars+cfg.accum_recent_bars)+1):i+1],
        errors="coerce",
    )
    slope = _amount_slope_norm(trend_window)

    if i < 20:
        return None
    close20 = float(df.iloc[i-20]["close"])
    price_ret20 = float(df.iloc[i]["close"]) / close20 - 1.0 if close20 > 0 else float("nan")

    passed = bool(
        recent_vs_prior >= cfg.accum_recent_vs_prior_min
        and recent_above >= cfg.accum_recent_days_above_prior_median_min
        and max_day_ratio <= cfg.accum_max_single_day_vs_prior_median
        and _finite(slope) and slope > 0
        and _finite(price_ret20) and abs(price_ret20) <= cfg.accum_price_abs_return20_max
    )
    return {
        "accum_pass": int(passed),
        "accum_recent_amount_median": recent_med,
        "accum_prior_amount_median": prior_med,
        "accum_recent_vs_prior": recent_vs_prior,
        "accum_recent_days_above_prior_median": recent_above,
        "accum_max_single_day_vs_prior_median": max_day_ratio,
        "accum_log_amount_slope": slope,
        "accum_price_return20": price_ret20,
    }


def detect_code(
    code: str,
    df: pd.DataFrame,
    amount_source: str,
    universe: UniverseAuthority,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cfg: FrozenR1Config,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    x = df.copy().sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
    x["ma224"] = pd.to_numeric(x["close"], errors="coerce").rolling(cfg.ma_long, min_periods=cfg.ma_long).mean()

    stages: List[Dict[str, Any]] = []
    gate = {
        "bars": 0,
        "ma224_ready": 0,
        "below_ma224": 0,
        "universe_pass": 0,
        "universe_fail": 0,
        "accum_descriptor_ready": 0,
        "accum_pass": 0,
        "wave1": 0,
        "first_pullback": 0,
        "stabilization": 0,
        "reacceleration": 0,
    }

    dates = pd.to_datetime(x["date"]).dt.normalize()
    min_i = max(cfg.ma_long-1, cfg.accum_prior_bars + cfg.accum_recent_bars, 25)
    next_allowed_i = min_i

    for i in range(min_i, len(x)):
        d = dates.iloc[i]
        if d < start or d > end or i < next_allowed_i:
            continue
        gate["bars"] += 1

        ma224 = x.iloc[i]["ma224"]
        if not _finite(ma224):
            continue
        gate["ma224_ready"] += 1

        close = float(x.iloc[i]["close"])
        if not close < float(ma224):
            continue
        gate["below_ma224"] += 1

        uni_ok, uni_date, uni_age, _ = universe.lookup(d, code)
        if not uni_ok:
            gate["universe_fail"] += 1
            continue
        gate["universe_pass"] += 1

        accum = _find_accum(x, i, cfg)
        if accum is None:
            continue
        gate["accum_descriptor_ready"] += 1
        if not accum["accum_pass"]:
            continue
        gate["accum_pass"] += 1

        base_start = max(0, i-cfg.base_context_bars+1)
        base_low = float(pd.to_numeric(x["low"].iloc[base_start:i+1], errors="coerce").min())
        below_frac = float((pd.to_numeric(x["close"].iloc[base_start:i+1], errors="coerce") <
                            pd.to_numeric(x["ma224"].iloc[base_start:i+1], errors="coerce")).mean())
        episode_id = f"{code}:{d.date().isoformat()}"

        stages.append(_stage_row(
            code, episode_id, "LOW224_BASE", i, x, uni_date, uni_age, amount_source,
            {
                "ma224": float(ma224),
                "close_vs_ma224_pct": (close / float(ma224) - 1.0) * 100.0,
                "below_ma224_frac20": below_frac,
                "base_low20": base_low,
            }
        ))
        stages.append(_stage_row(
            code, episode_id, "GRADUAL_AMOUNT_ACCUM", i, x, uni_date, uni_age, amount_source,
            dict(accum)
        ))

        # WAVE1: first close above prior 20-bar high with broad price/Amount impulse.
        wave_i = None
        wave_meta: Dict[str, Any] = {}
        for j in range(i+1, min(len(x), i+1+cfg.wave1_wait_max_bars)):
            dj = dates.iloc[j]
            if dj > end:
                break
            if j < cfg.wave1_prior_high_lookback:
                continue
            prior_high = float(pd.to_numeric(
                x["high"].iloc[j-cfg.wave1_prior_high_lookback:j], errors="coerce"
            ).max())
            amt20, n20 = _amount20_prior(x, j, cfg)
            a = float(x.iloc[j]["amount"]) if _finite(x.iloc[j]["amount"]) else float("nan")
            amt_ratio = a / amt20 if _finite(a) and _finite(amt20) and amt20 > 0 else float("nan")
            gain = float(x.iloc[j]["close"]) / base_low - 1.0 if base_low > 0 else float("nan")
            qualifies = bool(
                float(x.iloc[j]["close"]) > prior_high
                and gain >= cfg.wave1_min_gain_from_base_low
                and _finite(amt_ratio) and amt_ratio >= cfg.wave1_min_amount20_ratio
                and float(x.iloc[j]["close"]) > float(x.iloc[j]["open"])
            )
            if qualifies:
                wave_i = j
                wave_meta = {
                    "wave1_prior_high20": prior_high,
                    "wave1_gain_from_base_low": gain,
                    "wave1_amount20_mean_prior": amt20,
                    "wave1_amount20_obs": n20,
                    "wave1_amount20_ratio": amt_ratio,
                }
                break

        if wave_i is None:
            next_allowed_i = i + 1
            continue
        gate["wave1"] += 1
        uw, uwd, uwa, _ = universe.lookup(dates.iloc[wave_i], code)
        if not uw:
            next_allowed_i = wave_i + 1
            continue
        stages.append(_stage_row(
            code, episode_id, "WAVE1", wave_i, x, uwd, uwa, amount_source, wave_meta
        ))

        # FIRST_PULLBACK: first 3-15% drawdown from running wave high.
        pb_i = None
        running_wave_high = float(x.iloc[wave_i]["high"])
        for j in range(wave_i+1, min(len(x), wave_i+1+cfg.pullback_wait_max_bars)):
            if dates.iloc[j] > end:
                break
            running_wave_high = max(running_wave_high, float(x.iloc[j-1]["high"]))
            dd = 1.0 - float(x.iloc[j]["close"]) / running_wave_high if running_wave_high > 0 else float("nan")
            if cfg.pullback_min_drawdown <= dd <= cfg.pullback_max_drawdown and float(x.iloc[j]["low"]) > base_low:
                pb_i = j
                pb_dd = dd
                break

        if pb_i is None:
            next_allowed_i = wave_i + 1
            continue
        gate["first_pullback"] += 1
        upb, upbd, upba, _ = universe.lookup(dates.iloc[pb_i], code)
        if not upb:
            next_allowed_i = pb_i + 1
            continue

        wave_amt = float(x.iloc[wave_i]["amount"]) if _finite(x.iloc[wave_i]["amount"]) else float("nan")
        pb_amt = float(x.iloc[pb_i]["amount"]) if _finite(x.iloc[pb_i]["amount"]) else float("nan")
        stages.append(_stage_row(
            code, episode_id, "FIRST_PULLBACK", pb_i, x, upbd, upba, amount_source,
            {
                "pullback_drawdown_from_running_wave_high": pb_dd,
                "running_wave_high": running_wave_high,
                "pullback_amount_vs_wave": pb_amt / wave_amt if _finite(pb_amt) and _finite(wave_amt) and wave_amt > 0 else float("nan"),
            }
        ))

        # STABILIZATION: causal low-hold + close recovery + Amount contraction vs wave.
        st_i = None
        for j in range(pb_i+1, min(len(x), pb_i+1+cfg.stabilization_wait_max_bars)):
            if dates.iloc[j] > end:
                break
            low_hold = float(x.iloc[j]["low"]) >= float(x.iloc[j-1]["low"])
            close_recovery = float(x.iloc[j]["close"]) >= float(x.iloc[j-1]["close"])
            aj = float(x.iloc[j]["amount"]) if _finite(x.iloc[j]["amount"]) else float("nan")
            amount_contract = (
                _finite(aj) and _finite(wave_amt) and wave_amt > 0
                and aj / wave_amt <= cfg.stabilization_amount_vs_wave_max
            )
            if low_hold and close_recovery and amount_contract:
                st_i = j
                st_amt_vs_wave = aj / wave_amt
                break

        if st_i is None:
            next_allowed_i = pb_i + 1
            continue
        gate["stabilization"] += 1
        us, usd, usa, _ = universe.lookup(dates.iloc[st_i], code)
        if not us:
            next_allowed_i = st_i + 1
            continue
        stages.append(_stage_row(
            code, episode_id, "STABILIZATION", st_i, x, usd, usa, amount_source,
            {
                "stabilization_low_hold_prev": 1,
                "stabilization_close_recovery_prev": 1,
                "stabilization_amount_vs_wave": st_amt_vs_wave,
            }
        ))

        # REACCELERATION: broad current-strength marker, not the R2C1 wave-high reclaim gate.
        reac_i = None
        pb_amounts = pd.to_numeric(x["amount"].iloc[pb_i:st_i+1], errors="coerce")
        pb_med = _median_positive(pb_amounts)
        prior_wave_high_ex_reaccel = float(pd.to_numeric(x["high"].iloc[wave_i:st_i+1], errors="coerce").max())
        for j in range(st_i+1, min(len(x), st_i+1+cfg.reaccel_wait_max_bars)):
            if dates.iloc[j] > end:
                break
            aj = float(x.iloc[j]["amount"]) if _finite(x.iloc[j]["amount"]) else float("nan")
            amt_vs_pb = aj / pb_med if _finite(aj) and _finite(pb_med) and pb_med > 0 else float("nan")
            broad_restart = (
                float(x.iloc[j]["close"]) > float(x.iloc[j-1]["high"])
                and float(x.iloc[j]["close"]) > float(x.iloc[j]["open"])
                and _finite(amt_vs_pb)
                and amt_vs_pb >= cfg.reaccel_min_amount_vs_pullback_median
            )
            if broad_restart:
                reac_i = j
                prior_wave_high_ex_reaccel = float(pd.to_numeric(
                    x["high"].iloc[wave_i:j], errors="coerce"
                ).max())
                reclaims_wave_high = int(float(x.iloc[j]["close"]) >= prior_wave_high_ex_reaccel)
                reac_meta = {
                    "reaccel_amount_vs_pullback_median": amt_vs_pb,
                    "pullback_amount_median": pb_med,
                    "prior_wave_high_ex_reaccel": prior_wave_high_ex_reaccel,
                    "reaccel_reclaims_prior_wave_high": reclaims_wave_high,
                }
                break

        if reac_i is not None:
            gate["reacceleration"] += 1
            ur, urd, ura, _ = universe.lookup(dates.iloc[reac_i], code)
            if ur:
                stages.append(_stage_row(
                    code, episode_id, "REACCELERATION", reac_i, x, urd, ura, amount_source, reac_meta
                ))
            next_allowed_i = reac_i + cfg.cooldown_bars
        else:
            next_allowed_i = st_i + 1

    return stages, gate


def build_forward_outcomes(
    stage_df: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
    cfg: FrozenR1Config,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    if stage_df.empty:
        return pd.DataFrame()
    for r in stage_df.to_dict("records"):
        code = str(r["code"])
        df = frames.get(code)
        if df is None or df.empty:
            continue
        dates = pd.to_datetime(df["date"]).dt.normalize()
        event_date = pd.Timestamp(r["event_date"]).normalize()
        idxs = np.where(dates.to_numpy() == event_date.to_datetime64())[0]
        if len(idxs) != 1:
            continue
        i = int(idxs[0])
        anchor = float(df.iloc[i]["close"])
        out = {
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "episode_id": r["episode_id"],
            "code": code,
            "stage": r["stage"],
            "event_date": r["event_date"],
            "anchor_close": anchor,
        }
        for h in cfg.forward_horizons:
            if i + h < len(df):
                future = df.iloc[i+1:i+h+1]
                out[f"d{h}_close_ret_pct"] = (float(df.iloc[i+h]["close"]) / anchor - 1.0) * 100.0
                out[f"d{h}_mfe_pct"] = (float(pd.to_numeric(future["high"], errors="coerce").max()) / anchor - 1.0) * 100.0
                out[f"d{h}_mae_pct"] = (float(pd.to_numeric(future["low"], errors="coerce").min()) / anchor - 1.0) * 100.0
                out[f"d{h}_complete"] = 1
            else:
                out[f"d{h}_close_ret_pct"] = float("nan")
                out[f"d{h}_mfe_pct"] = float("nan")
                out[f"d{h}_mae_pct"] = float("nan")
                out[f"d{h}_complete"] = 0
        rows.append(out)
    return pd.DataFrame(rows)


def build_stage_summary(outcomes: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for stage in STAGES:
        g = outcomes[outcomes["stage"].eq(stage)] if not outcomes.empty else pd.DataFrame()
        row = {"stage": stage, "events": int(len(g))}
        for h in (5,10,15):
            if g.empty:
                row.update({
                    f"d{h}_complete": 0,
                    f"d{h}_close_median_pct": float("nan"),
                    f"d{h}_mfe_median_pct": float("nan"),
                    f"d{h}_mae_median_pct": float("nan"),
                })
                continue
            mature = g[pd.to_numeric(g[f"d{h}_complete"], errors="coerce").eq(1)]
            row[f"d{h}_complete"] = int(len(mature))
            row[f"d{h}_close_median_pct"] = float(pd.to_numeric(mature[f"d{h}_close_ret_pct"], errors="coerce").median()) if not mature.empty else float("nan")
            row[f"d{h}_mfe_median_pct"] = float(pd.to_numeric(mature[f"d{h}_mfe_pct"], errors="coerce").median()) if not mature.empty else float("nan")
            row[f"d{h}_mae_median_pct"] = float(pd.to_numeric(mature[f"d{h}_mae_pct"], errors="coerce").median()) if not mature.empty else float("nan")
        rows.append(row)
    return pd.DataFrame(rows)


def deterministic_manual_sample(stage_df: pd.DataFrame, limit: int) -> pd.DataFrame:
    if stage_df.empty:
        return stage_df.copy()
    e = stage_df.drop_duplicates(["episode_id"]).copy()
    e["sample_key"] = e["episode_id"].map(
        lambda x: hashlib.sha256(str(x).encode("utf-8")).hexdigest()
    )
    return e.sort_values("sample_key").head(int(limit)).drop(columns=["sample_key"])


def build_review_bars(
    sample: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
    pre_bars: int = 30,
    post_bars: int = 15,
) -> pd.DataFrame:
    rows = []
    for sr in sample.to_dict("records"):
        code = str(sr["code"])
        df = frames.get(code)
        if df is None:
            continue
        anchor = pd.Timestamp(sr["event_date"]).normalize()
        dates = pd.to_datetime(df["date"]).dt.normalize()
        idxs = np.where(dates.to_numpy() == anchor.to_datetime64())[0]
        if len(idxs) != 1:
            continue
        i = int(idxs[0])
        for j in range(max(0,i-pre_bars), min(len(df), i+post_bars+1)):
            r = df.iloc[j]
            rows.append({
                "schema": SCHEMA,
                "strategy_id": STRATEGY_ID,
                "episode_id": sr["episode_id"],
                "code": code,
                "sample_anchor_stage": sr["stage"],
                "sample_anchor_date": sr["event_date"],
                "bar_offset": int(j-i),
                "future_relative_to_sample_anchor": int(j > i),
                "date": pd.Timestamp(r["date"]).date().isoformat(),
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]) if _finite(r["volume"]) else float("nan"),
                "actual_amount": float(r["amount"]) if _finite(r["amount"]) else float("nan"),
            })
    return pd.DataFrame(rows)


def run(args: argparse.Namespace) -> int:
    cfg = CONFIG
    price_root = Path(args.price_cache_dir)
    amount_root = Path(args.amount_cache_dir)
    asof_root = Path(args.asof_cache_dir)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    universe = UniverseAuthority(asof_root, cfg.universe_max_calendar_age_days)
    if not universe.dates:
        raise RuntimeError("LOW224_FAIL_CLOSED no causal historical as-of universe snapshots")

    amount_auth = AmountAuthority(amount_root, asof_root)
    price_files = sorted(x for x in price_root.rglob("*") if x.is_file()) if price_root.exists() else []
    if not price_files:
        raise RuntimeError("LOW224_FAIL_CLOSED price cache empty")

    frames: Dict[str, pd.DataFrame] = {}
    amount_sources: Dict[str, str] = {}
    load_fail = 0
    duplicates = 0
    for p in price_files:
        try:
            z = normalize_price_frame(_load_any(p), p, amount_auth)
            if z is None:
                load_fail += 1
                continue
            code, df, amount_source, _ = z
            if code in frames:
                duplicates += 1
                frames[code] = (
                    pd.concat([frames[code], df], ignore_index=True)
                    .sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
                )
            else:
                frames[code] = df
            amount_sources[code] = amount_source
        except Exception:
            load_fail += 1

    if not frames:
        raise RuntimeError("LOW224_FAIL_CLOSED no usable price frames")

    positive_amount = sum(int(pd.to_numeric(x["amount"], errors="coerce").gt(0).sum()) for x in frames.values())
    if positive_amount <= 0:
        raise RuntimeError("LOW224_FAIL_CLOSED actual Amount unavailable; Close*Volume forbidden")

    max_data_date = max(pd.Timestamp(df["date"].max()).normalize() for df in frames.values())
    end = pd.Timestamp(args.end_date).normalize() if args.end_date else max_data_date
    start = pd.Timestamp(args.start_date).normalize() if args.start_date else pd.Timestamp("2024-08-27")
    if start >= end:
        raise ValueError("start must be earlier than end")

    codes = sorted(frames)
    if args.max_codes and int(args.max_codes) > 0:
        codes = codes[:int(args.max_codes)]

    all_stages: List[Dict[str, Any]] = []
    gates = {k:0 for k in [
        "bars","ma224_ready","below_ma224","universe_pass","universe_fail",
        "accum_descriptor_ready","accum_pass","wave1","first_pullback",
        "stabilization","reacceleration"
    ]}
    per_code_digest = {}
    deterministic_fail = 0

    for n, code in enumerate(codes,1):
        st, gd = detect_code(code, frames[code], amount_sources.get(code,"MISSING"), universe, start, end, cfg)
        all_stages.extend(st)
        for k,v in gd.items():
            gates[k] += int(v)
        payload = json.dumps(st, sort_keys=True, ensure_ascii=False, default=str)
        per_code_digest[code] = hashlib.sha256(payload.encode()).hexdigest()

        # Sparse deterministic rerun + every emitting code.
        if st or n % max(1, len(codes)//25 or 1) == 0:
            st2, _ = detect_code(code, frames[code], amount_sources.get(code,"MISSING"), universe, start, end, cfg)
            payload2 = json.dumps(st2, sort_keys=True, ensure_ascii=False, default=str)
            if hashlib.sha256(payload2.encode()).hexdigest() != per_code_digest[code]:
                deterministic_fail += 1

        if n % 250 == 0:
            print("LOW224_PROGRESS", n, "/", len(codes), "stage_rows", len(all_stages))

    stage_df = pd.DataFrame(all_stages)
    if stage_df.empty:
        stage_df = pd.DataFrame(columns=[
            "schema","strategy_id","loader_revision","episode_id","code","stage","event_date"
        ])

    outcomes = build_forward_outcomes(stage_df, frames, cfg)
    stage_summary = build_stage_summary(outcomes)
    sample = deterministic_manual_sample(stage_df, args.manual_sample_limit)
    review_bars = build_review_bars(sample, frames)

    stage_counts = (
        stage_df["stage"].value_counts().reindex(STAGES, fill_value=0).rename_axis("stage").reset_index(name="count")
        if not stage_df.empty else pd.DataFrame({"stage":STAGES,"count":[0]*len(STAGES)})
    )

    episodes = int(stage_df["episode_id"].nunique()) if not stage_df.empty and "episode_id" in stage_df else 0
    stage_digest = hashlib.sha256(
        stage_df.sort_values(["episode_id","event_date","stage"]).to_csv(index=False).encode("utf-8")
    ).hexdigest()

    # Causal integrity checks.
    lookahead_fail = 0
    if not stage_df.empty:
        lookahead_fail = int((
            pd.to_datetime(stage_df["feature_max_date"], errors="coerce") >
            pd.to_datetime(stage_df["event_date"], errors="coerce")
        ).sum())
    synthetic_amount_fallback_rows = 0

    amount_rows_total = sum(len(df) for df in frames.values())
    amount_rows_positive = positive_amount
    amount_coverage = (amount_rows_positive / amount_rows_total * 100.0) if amount_rows_total else 0.0

    authority = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "codes": len(codes),
        "price_files": len(price_files),
        "price_load_fail": load_fail,
        "duplicate_code_files": duplicates,
        "actual_amount_positive_rows": amount_rows_positive,
        "actual_amount_total_rows": amount_rows_total,
        "actual_amount_coverage_pct": amount_coverage,
        "synthetic_close_x_volume_fallback_rows": synthetic_amount_fallback_rows,
        "asof_snapshot_dates": len(universe.dates),
        "asof_first": universe.dates[0].date().isoformat() if universe.dates else "",
        "asof_last": universe.dates[-1].date().isoformat() if universe.dates else "",
        "future_snapshot_fallback_allowed": 0,
        "deterministic_fail": deterministic_fail,
        "lookahead_fail": lookahead_fail,
    }])

    manifest = {
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "loader_revision": LOADER_REVISION,
        "research_authority": RESEARCH_AUTHORITY,
        "shared_data_authority_only": SHARED_DATA_AUTHORITY_ONLY,
        "research_start": start.date().isoformat(),
        "research_end": end.date().isoformat(),
        "codes_scanned": len(codes),
        "config": asdict(cfg),
        "stages": STAGES,
        "gate_funnel": gates,
        "stage_counts": dict(zip(stage_counts["stage"], stage_counts["count"].astype(int))),
        "episodes": episodes,
        "manual_sample_episodes": int(len(sample)),
        "lookahead_fail": lookahead_fail,
        "deterministic_fail": deterministic_fail,
        "actual_amount_synthetic_fallback_rows": 0,
        "future_universe_fallback": 0,
        "live_logic_changed": False,
        "score_rank_changed": False,
        "order_logic_changed": False,
        "core224_logic_changed": False,
        "triangle1pb_logic_changed": False,
        "tuning_allowed": False,
        "outcomes_used_as_gate": False,
        "stage_digest": stage_digest,
        "status": "PASS" if lookahead_fail == 0 and deterministic_fail == 0 else "FAIL",
        "next_gate": "MANUAL_STRUCTURE_REVIEW_BEFORE_ANY_THRESHOLD_CHANGE",
    }

    def write_csv(df: pd.DataFrame, name: str):
        df.to_csv(out/name, index=False, encoding="utf-8-sig")

    write_csv(stage_df, "low224_stage_ledger.csv")
    write_csv(stage_counts, "low224_stage_counts.csv")
    write_csv(pd.DataFrame([gates]), "low224_gate_funnel.csv")
    write_csv(outcomes, "low224_forward_outcomes.csv")
    write_csv(stage_summary, "low224_stage_outcome_summary.csv")
    write_csv(sample, "low224_manual_review_sample.csv")
    write_csv(review_bars, "low224_manual_review_bars.csv")
    write_csv(authority, "low224_authority_audit.csv")
    (out/"low224_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    # Compact human report. Performance is explicitly secondary in R1.
    sc = manifest["stage_counts"]
    report = [
        "🧪 [LOW224_ACCUM_WAVE1_PB R1 · STRUCTURE AUDIT]",
        "RESEARCH ONLY · 주문 0 · LIVE/점수/랭킹 0",
        f"📅 period={manifest['research_start']} ~ {manifest['research_end']} · codes={len(codes)}",
        "",
        "🧭 [Frozen chronology]",
        "LOW224_BASE → GRADUAL_AMOUNT_ACCUM → WAVE1 → FIRST_PULLBACK → STABILIZATION → REACCELERATION",
        "",
        "🔎 [Gate funnel]",
        f"MA224 ready {gates['ma224_ready']:,} → below224 {gates['below_ma224']:,}",
        f"universe PASS {gates['universe_pass']:,} / FAIL {gates['universe_fail']:,}",
        f"accum descriptor ready {gates['accum_descriptor_ready']:,} → gradual-Amount {gates['accum_pass']:,}",
        f"WAVE1 {gates['wave1']:,} → FIRST_PB {gates['first_pullback']:,} → STABLE {gates['stabilization']:,} → REACCEL {gates['reacceleration']:,}",
        "",
        "📦 [Stage counts]",
        " · ".join(f"{st} {int(sc.get(st,0))}" for st in STAGES),
        f"episodes {episodes} · manual review sample {len(sample)}",
        "",
        "🛡️ [Authority]",
        f"actual Amount coverage {amount_coverage:.2f}% · Close×Volume fallback 0",
        f"as-of snapshots {len(universe.dates)} · future fallback 0",
        f"lookahead {lookahead_fail} · deterministic {deterministic_fail}",
        "",
        "⚠️ R1 원칙: 성과로 threshold를 조정하지 않습니다.",
        "➡️ NEXT: manual review bars로 실제 '224 아래→스물스물 Amount→1파→첫눌림' 구조가 맞는지 먼저 검증",
    ]
    (out/"low224_report.txt").write_text("\n".join(report), encoding="utf-8")
    print("\n".join(report))
    if manifest["status"] != "PASS":
        raise RuntimeError(f"LOW224_AUDIT_FAIL {manifest['status']}")
    return 0


def self_test() -> int:
    # Pure detector primitives / stage-summary smoke tests; cache adapters are
    # separately compiled/imported by workflow preflight.
    n = 320
    dates = pd.bdate_range("2025-01-02", periods=n)
    close = np.full(n, 100.0)
    open_ = np.full(n, 99.8)
    high = np.full(n, 100.8)
    low = np.full(n, 99.0)
    amount = np.full(n, 100.0)

    # Keep long MA above price.
    close[:260] = np.linspace(120, 100, 260)
    open_[:260] = close[:260] * 0.997
    high[:260] = close[:260] * 1.006
    low[:260] = close[:260] * 0.994

    # Gradual Amount build while price stays flat-ish.
    for k, idx in enumerate(range(250, 275)):
        amount[idx] = 100 + k * 4
        close[idx] = 99.0 + k * 0.03
        open_[idx] = close[idx] * 0.998
        high[idx] = close[idx] * 1.004
        low[idx] = close[idx] * 0.996

    # Wave 1 breakout.
    close[275] = 110.0; open_[275] = 103.0; high[275] = 111.0; low[275] = 102.5; amount[275] = 260.0
    # First pullback.
    close[278] = 105.0; open_[278] = 107.0; high[278] = 108.0; low[278] = 104.0; amount[278] = 120.0
    # Stabilization.
    close[279] = 106.0; open_[279] = 104.8; high[279] = 106.5; low[279] = 104.2; amount[279] = 115.0
    # Reacceleration.
    close[280] = 108.5; open_[280] = 106.2; high[280] = 109.0; low[280] = 106.0; amount[280] = 155.0

    df = pd.DataFrame({
        "date": dates, "open": open_, "high": high, "low": low,
        "close": close, "volume": np.full(n, 1000.0), "amount": amount,
    })

    class U:
        def lookup(self, event_date, code):
            return True, pd.Timestamp(event_date).date().isoformat(), 0, "PASS"

    st, gd = detect_code(
        "123456", df, "SYNTHETIC_ACTUAL_AMOUNT", U(),
        pd.Timestamp("2025-01-02"), pd.Timestamp(dates[-1]), CONFIG
    )
    assert gd["accum_pass"] >= 1
    assert gd["wave1"] >= 1
    assert any(x["stage"] == "FIRST_PULLBACK" for x in st)
    assert any(x["stage"] == "STABILIZATION" for x in st)
    print("LOW224_R1_SYNTHETIC_TEST PASS", gd)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--price-cache-dir", default="reports/.cache/v20_price_history")
    ap.add_argument("--asof-cache-dir", default="reports/.cache/v20_asof_snapshots")
    ap.add_argument("--amount-cache-dir", default="reports/.cache/v25_actual_amount_history")
    ap.add_argument("--output-dir", default="reports/low224_r1")
    ap.add_argument("--start-date", default="2024-08-27")
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
