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
LOADER_REVISION = "LOW224_R1_0_5_STABILIZATION_ANATOMY_AUDIT"
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



def build_episode_overlap_audit(stage_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Audit repeated accumulation detections without changing any detector gate."""
    if stage_df.empty:
        empty = pd.DataFrame()
        return empty, pd.DataFrame([{
            "episodes": 0, "consecutive_runs": 0, "episodes_in_multi_day_runs": 0,
            "episodes_in_multi_day_runs_pct": 0.0, "median_run_len": float("nan"),
            "p90_run_len": float("nan"), "max_run_len": 0,
            "used_as_gate": 0,
        }])

    base = stage_df[stage_df["stage"].eq("LOW224_BASE")].copy()
    base = base.sort_values(["code", "bar_index", "event_date"])
    rows = []
    for code, g in base.groupby("code", sort=True):
        local_run = 0
        prev_bar = None
        for r in g.to_dict("records"):
            bi = int(r["bar_index"])
            if prev_bar is None or bi - prev_bar > 1:
                local_run += 1
            rows.append({
                "episode_id": r["episode_id"],
                "code": str(code),
                "event_date": r["event_date"],
                "bar_index": bi,
                "consecutive_accum_run_id": f"{code}:RUN{local_run}",
            })
            prev_bar = bi

    detail = pd.DataFrame(rows)
    sizes = (
        detail.groupby(["code", "consecutive_accum_run_id"])
        .size().rename("run_length").reset_index()
    )
    detail = detail.merge(sizes, on=["code", "consecutive_accum_run_id"], how="left")
    multi_episodes = int(detail.loc[detail["run_length"].gt(1)].shape[0])
    n = int(len(detail))
    summ = pd.DataFrame([{
        "episodes": n,
        "consecutive_runs": int(len(sizes)),
        "episodes_in_multi_day_runs": multi_episodes,
        "episodes_in_multi_day_runs_pct": (multi_episodes / n * 100.0) if n else 0.0,
        "median_run_len": float(sizes["run_length"].median()) if not sizes.empty else float("nan"),
        "p90_run_len": float(sizes["run_length"].quantile(0.90)) if not sizes.empty else float("nan"),
        "max_run_len": int(sizes["run_length"].max()) if not sizes.empty else 0,
        "used_as_gate": 0,
    }])
    return detail, summ


def build_stratified_manual_sample(
    stage_df: pd.DataFrame,
    per_stratum: int = 8,
) -> pd.DataFrame:
    """Deterministic structural sample across terminal progression strata."""
    if stage_df.empty:
        return pd.DataFrame()

    order = {s: i for i, s in enumerate(STAGES)}
    rows = []
    for episode_id, g in stage_df.groupby("episode_id", sort=False):
        gg = g.sort_values(["bar_index", "stage"])
        stages = list(gg["stage"])
        max_stage = max(stages, key=lambda x: order[x])
        rr = gg[gg["stage"].eq("REACCELERATION")]
        reclaim = (
            int(pd.to_numeric(rr.iloc[0].get("reaccel_reclaims_prior_wave_high"), errors="coerce"))
            if not rr.empty and pd.notna(rr.iloc[0].get("reaccel_reclaims_prior_wave_high"))
            else -1
        )
        if max_stage == "GRADUAL_AMOUNT_ACCUM":
            stratum = "ACCUM_NO_WAVE"
        elif max_stage == "WAVE1":
            stratum = "WAVE_NO_PB"
        elif max_stage == "FIRST_PULLBACK":
            stratum = "PB_NO_STABLE"
        elif max_stage == "STABILIZATION":
            stratum = "STABLE_NO_REACCEL"
        elif max_stage == "REACCELERATION" and reclaim == 1:
            stratum = "REACCEL_RECLAIM"
        else:
            stratum = "REACCEL_NO_RECLAIM"

        base = gg[gg["stage"].eq("LOW224_BASE")].iloc[0]
        terminal = gg.iloc[-1]
        rows.append({
            "episode_id": episode_id,
            "code": str(base["code"]),
            "base_date": base["event_date"],
            "terminal_stage": max_stage,
            "terminal_date": terminal["event_date"],
            "stratum": stratum,
            "reaccel_reclaims_prior_wave_high": reclaim,
            "sample_key": hashlib.sha256(str(episode_id).encode("utf-8")).hexdigest(),
        })

    ep = pd.DataFrame(rows)
    out = []
    for stratum, g in ep.groupby("stratum", sort=True):
        out.append(g.sort_values("sample_key").head(int(per_stratum)))
    ans = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    return ans.drop(columns=["sample_key"], errors="ignore")


def build_episode_review_bars(
    stratified_sample: pd.DataFrame,
    stage_df: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
    pre_base_bars: int = 30,
    post_terminal_bars: int = 15,
) -> pd.DataFrame:
    """Review each sampled episode from pre-base through post-terminal.

    Stage labels are attached to the exact bar where they occurred. Future bars
    after terminal are explicit visual-audit rows only.
    """
    if stratified_sample.empty:
        return pd.DataFrame()
    rows = []
    for sr in stratified_sample.to_dict("records"):
        episode_id = sr["episode_id"]
        code = str(sr["code"])
        df = frames.get(code)
        if df is None or df.empty:
            continue
        eg = stage_df[stage_df["episode_id"].eq(episode_id)].copy()
        if eg.empty:
            continue

        dates = pd.to_datetime(df["date"]).dt.normalize()
        base_date = pd.Timestamp(eg.loc[eg["stage"].eq("LOW224_BASE"), "event_date"].iloc[0]).normalize()
        terminal_date = pd.Timestamp(eg.sort_values("bar_index").iloc[-1]["event_date"]).normalize()
        bidx = np.where(dates.to_numpy() == base_date.to_datetime64())[0]
        tidx = np.where(dates.to_numpy() == terminal_date.to_datetime64())[0]
        if len(bidx) != 1 or len(tidx) != 1:
            continue
        bidx = int(bidx[0]); tidx = int(tidx[0])

        stage_map: Dict[str, List[str]] = {}
        for er in eg.to_dict("records"):
            stage_map.setdefault(str(er["event_date"]), []).append(str(er["stage"]))

        for j in range(max(0, bidx-pre_base_bars), min(len(df), tidx+post_terminal_bars+1)):
            r = df.iloc[j]
            d = pd.Timestamp(r["date"]).date().isoformat()
            rows.append({
                "schema": SCHEMA,
                "strategy_id": STRATEGY_ID,
                "episode_id": episode_id,
                "code": code,
                "stratum": sr["stratum"],
                "base_date": base_date.date().isoformat(),
                "terminal_stage": sr["terminal_stage"],
                "terminal_date": terminal_date.date().isoformat(),
                "bar_offset_from_base": int(j-bidx),
                "bar_offset_from_terminal": int(j-tidx),
                "future_after_terminal_for_visual_audit": int(j > tidx),
                "stage_labels_on_bar": "|".join(stage_map.get(d, [])),
                "date": d,
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]) if _finite(r["volume"]) else float("nan"),
                "actual_amount": float(r["amount"]) if _finite(r["amount"]) else float("nan"),
            })
    return pd.DataFrame(rows)



def build_run_level_funnel(
    stage_df: pd.DataFrame,
    overlap_detail: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Collapse consecutive accumulation detections for audit only.

    This does NOT change the detector or episode ledger. It asks how many
    independent-looking accumulation *runs* existed and whether any raw episode
    inside each run later reached each stage.
    """
    if stage_df.empty or overlap_detail.empty:
        empty = pd.DataFrame()
        return empty, pd.DataFrame([{
            "runs": 0, "wave1_runs": 0, "first_pullback_runs": 0,
            "stabilization_runs": 0, "reacceleration_runs": 0,
            "used_as_gate": 0,
        }])

    base = stage_df[stage_df["stage"].eq("LOW224_BASE")][
        ["episode_id", "code", "event_date", "bar_index"]
    ].copy()
    m = base.merge(
        overlap_detail[[
            "episode_id", "consecutive_accum_run_id", "run_length"
        ]],
        on="episode_id", how="left"
    )
    presence = (
        stage_df.assign(v=1)
        .pivot_table(index="episode_id", columns="stage", values="v", aggfunc="max", fill_value=0)
        .reset_index()
    )
    dates = (
        stage_df[stage_df["stage"].isin(["WAVE1","FIRST_PULLBACK","STABILIZATION","REACCELERATION"])]
        .pivot_table(index="episode_id", columns="stage", values="event_date", aggfunc="min")
        .reset_index()
    )
    m = m.merge(presence, on="episode_id", how="left").merge(dates, on="episode_id", how="left", suffixes=("","_date"))

    rows = []
    for rid, g in m.groupby("consecutive_accum_run_id", sort=True):
        run_start = pd.to_datetime(g["event_date"]).min()
        run_end = pd.to_datetime(g["event_date"]).max()
        rr = {
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "consecutive_accum_run_id": rid,
            "code": str(g["code"].iloc[0]),
            "run_start_date": run_start.date().isoformat(),
            "run_end_date": run_end.date().isoformat(),
            "run_length_episodes": int(len(g)),
            "used_as_gate": 0,
        }
        for st in ["WAVE1","FIRST_PULLBACK","STABILIZATION","REACCELERATION"]:
            has = int(pd.to_numeric(g.get(st, 0), errors="coerce").fillna(0).gt(0).any())
            rr[f"{st.lower()}_any"] = has
            vals = pd.to_datetime(g.get(f"{st}_date"), errors="coerce").dropna() if f"{st}_date" in g else pd.Series(dtype="datetime64[ns]")
            if len(vals):
                first = vals.min()
                rr[f"{st.lower()}_first_date"] = first.date().isoformat()
                rr[f"{st.lower()}_calendar_days_from_run_start"] = int((first-run_start).days)
                rr[f"{st.lower()}_calendar_days_from_run_end"] = int((first-run_end).days)
            else:
                rr[f"{st.lower()}_first_date"] = ""
                rr[f"{st.lower()}_calendar_days_from_run_start"] = -1
                rr[f"{st.lower()}_calendar_days_from_run_end"] = -1
        rows.append(rr)

    detail = pd.DataFrame(rows)
    summ = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "runs": int(len(detail)),
        "wave1_runs": int(detail["wave1_any"].sum()),
        "first_pullback_runs": int(detail["first_pullback_any"].sum()),
        "stabilization_runs": int(detail["stabilization_any"].sum()),
        "reacceleration_runs": int(detail["reacceleration_any"].sum()),
        "wave1_rate_pct": float(detail["wave1_any"].mean()*100.0) if len(detail) else 0.0,
        "first_pullback_rate_pct": float(detail["first_pullback_any"].mean()*100.0) if len(detail) else 0.0,
        "stabilization_rate_pct": float(detail["stabilization_any"].mean()*100.0) if len(detail) else 0.0,
        "reacceleration_rate_pct": float(detail["reacceleration_any"].mean()*100.0) if len(detail) else 0.0,
        "wave1_days_from_run_start_median": float(
            pd.to_numeric(detail.loc[detail["wave1_any"].eq(1),"wave1_calendar_days_from_run_start"], errors="coerce").median()
        ) if int(detail["wave1_any"].sum()) else float("nan"),
        "wave1_days_from_run_start_p90": float(
            pd.to_numeric(detail.loc[detail["wave1_any"].eq(1),"wave1_calendar_days_from_run_start"], errors="coerce").quantile(.90)
        ) if int(detail["wave1_any"].sum()) else float("nan"),
        "used_as_gate": 0,
    }])
    return detail, summ


def build_structure_context_audit(
    stage_df: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Add causal context descriptors without changing any detector gate."""
    rows = []
    if stage_df.empty:
        return pd.DataFrame()

    episode_groups = stage_df.groupby("episode_id", sort=False)
    for episode_id, eg in episode_groups:
        eg = eg.sort_values("bar_index")
        base = eg[eg["stage"].eq("LOW224_BASE")]
        accum = eg[eg["stage"].eq("GRADUAL_AMOUNT_ACCUM")]
        if base.empty or accum.empty:
            continue
        b = base.iloc[0]
        code = str(b["code"])
        df = frames.get(code)
        if df is None or df.empty:
            continue
        bi = int(b["bar_index"])
        if bi >= len(df):
            continue

        close = float(df.iloc[bi]["close"])
        lo60 = float(pd.to_numeric(df["low"].iloc[max(0,bi-59):bi+1], errors="coerce").min())
        hi60 = float(pd.to_numeric(df["high"].iloc[max(0,bi-59):bi+1], errors="coerce").max())
        pos60 = (close-lo60)/(hi60-lo60) if hi60 > lo60 else float("nan")
        dd60 = close/hi60-1.0 if hi60 > 0 else float("nan")

        # Causal actual-Amount shape around accumulation anchor.
        recent = pd.to_numeric(df["amount"].iloc[max(0,bi-4):bi+1], errors="coerce")
        recent = recent[recent > 0]
        prior = pd.to_numeric(df["amount"].iloc[max(0,bi-24):max(0,bi-4)], errors="coerce")
        prior = prior[prior > 0]
        up_steps = int((recent.diff().dropna() > 0).sum()) if len(recent) >= 2 else -1
        recent_cv = float(recent.std(ddof=0)/recent.mean()) if len(recent) >= 2 and float(recent.mean()) > 0 else float("nan")
        recent_max_med = float(recent.max()/recent.median()) if len(recent) and float(recent.median()) > 0 else float("nan")
        prior_med = float(prior.median()) if len(prior) else float("nan")
        prior_spike2 = int((prior > prior_med*2.0).sum()) if _finite(prior_med) and prior_med > 0 else -1

        rr = {
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "episode_id": episode_id,
            "code": code,
            "base_date": b["event_date"],
            "base_close_vs_ma224_pct": float(b.get("close_vs_ma224_pct")) if _finite(b.get("close_vs_ma224_pct")) else float("nan"),
            "base_below_ma224_frac20": float(b.get("below_ma224_frac20")) if _finite(b.get("below_ma224_frac20")) else float("nan"),
            "base_range_position60": pos60,
            "base_drawdown_from_high60": dd60,
            "accum_recent5_up_steps": up_steps,
            "accum_recent5_cv": recent_cv,
            "accum_recent5_max_over_median": recent_max_med,
            "accum_prior20_spike2_count": prior_spike2,
            "used_as_gate": 0,
        }

        wave = eg[eg["stage"].eq("WAVE1")]
        if not wave.empty:
            w = wave.iloc[0]
            wi = int(w["bar_index"])
            prev_close = float(df.iloc[wi-1]["close"]) if wi >= 1 else float("nan")
            op = float(df.iloc[wi]["open"])
            cl = float(df.iloc[wi]["close"])
            rr.update({
                "wave1_present": 1,
                "wave1_date": w["event_date"],
                "wave1_trading_bars_from_base": int(wi-bi),
                "wave1_gain_from_base_low": float(w.get("wave1_gain_from_base_low")) if _finite(w.get("wave1_gain_from_base_low")) else float("nan"),
                "wave1_amount20_ratio": float(w.get("wave1_amount20_ratio")) if _finite(w.get("wave1_amount20_ratio")) else float("nan"),
                "wave1_gap_pct": (op/prev_close-1.0)*100.0 if _finite(prev_close) and prev_close>0 else float("nan"),
                "wave1_day_close_ret_pct": (cl/prev_close-1.0)*100.0 if _finite(prev_close) and prev_close>0 else float("nan"),
                "wave1_body_pct": (cl/op-1.0)*100.0 if op>0 else float("nan"),
            })
        else:
            rr["wave1_present"] = 0

        rows.append(rr)
    return pd.DataFrame(rows)


def build_structure_context_summary(ctx: pd.DataFrame) -> pd.DataFrame:
    if ctx.empty:
        return pd.DataFrame()
    wave = ctx[ctx["wave1_present"].eq(1)].copy()
    def q(series, p):
        return float(pd.to_numeric(series, errors="coerce").quantile(p)) if len(series) else float("nan")
    return pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "episodes": int(len(ctx)),
        "base_below_ma224_frac20_lt50_pct": float(
            (pd.to_numeric(ctx["base_below_ma224_frac20"], errors="coerce") < 0.5).mean()*100.0
        ),
        "base_range_position60_median": q(ctx["base_range_position60"], .5),
        "base_range_position60_p75": q(ctx["base_range_position60"], .75),
        "accum_recent5_up_steps_median": q(ctx["accum_recent5_up_steps"], .5),
        "accum_recent5_cv_median": q(ctx["accum_recent5_cv"], .5),
        "accum_recent5_max_over_median_median": q(ctx["accum_recent5_max_over_median"], .5),
        "wave1_events": int(len(wave)),
        "wave1_trading_bars_from_base_median": q(wave["wave1_trading_bars_from_base"], .5),
        "wave1_amount20_ratio_median": q(wave["wave1_amount20_ratio"], .5),
        "wave1_amount20_ratio_p75": q(wave["wave1_amount20_ratio"], .75),
        "wave1_amount20_ratio_p90": q(wave["wave1_amount20_ratio"], .90),
        "wave1_amount20_ratio_ge10_pct": float(
            (pd.to_numeric(wave["wave1_amount20_ratio"], errors="coerce") >= 10.0).mean()*100.0
        ) if len(wave) else 0.0,
        "wave1_gain_from_base_low_median": q(wave["wave1_gain_from_base_low"], .5),
        "wave1_gain_from_base_low_ge30_pct": float(
            (pd.to_numeric(wave["wave1_gain_from_base_low"], errors="coerce") >= 0.30).mean()*100.0
        ) if len(wave) else 0.0,
        "wave1_gap_ge5_pct": float(
            (pd.to_numeric(wave["wave1_gap_pct"], errors="coerce") >= 5.0).mean()*100.0
        ) if len(wave) else 0.0,
        "used_as_gate": 0,
    }])



def _longest_true_streak(flags: List[int]) -> int:
    best = cur = 0
    for v in flags:
        if int(v):
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return int(best)


def build_raw_accum_persistence_audit(
    frames: Dict[str, pd.DataFrame],
    universe: UniverseAuthority,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cfg: FrozenR1Config,
    stage_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Measure the raw daily accumulation mask independent of detector jumps.

    This is deliberately recomputed on every eligible bar. It does not use
    next_allowed_i, cooldown, Wave1/PB/Stable/Reaccel state, or future outcome.
    Therefore raw accumulation persistence is not shortened merely because a
    later stage was found by the detector.

    Returns:
      raw_pass_detail: every causal bar where the existing R1 accumulation
                       descriptor passes.
      raw_run_summary: consecutive raw-pass run distribution.
      episode_context: maps existing emitted episodes to the independent raw
                       mask around BASE and, when present, before WAVE1.
    """
    raw_rows: List[Dict[str, Any]] = []
    pass_sets: Dict[str, set] = {}
    bar_date_maps: Dict[str, Dict[int, str]] = {}

    min_i = max(
        cfg.ma_long - 1,
        cfg.accum_prior_bars + cfg.accum_recent_bars,
        25,
    )

    for code, source_df in frames.items():
        x = (
            source_df.copy()
            .sort_values("date")
            .drop_duplicates("date", keep="last")
            .reset_index(drop=True)
        )
        x["ma224"] = (
            pd.to_numeric(x["close"], errors="coerce")
            .rolling(cfg.ma_long, min_periods=cfg.ma_long)
            .mean()
        )
        dates = pd.to_datetime(x["date"]).dt.normalize()
        pset = set()
        dmap = {}

        for i in range(min_i, len(x)):
            d = dates.iloc[i]
            if d < start or d > end:
                continue
            dmap[int(i)] = d.date().isoformat()

            ma224 = x.iloc[i]["ma224"]
            if not _finite(ma224):
                continue
            close = float(x.iloc[i]["close"])
            if not close < float(ma224):
                continue

            uni_ok, uni_date, uni_age, _ = universe.lookup(d, code)
            if not uni_ok:
                continue

            accum = _find_accum(x, i, cfg)
            if accum is None or not int(accum.get("accum_pass", 0)):
                continue

            pset.add(int(i))
            raw_rows.append({
                "schema": SCHEMA,
                "strategy_id": STRATEGY_ID,
                "code": str(code),
                "bar_index": int(i),
                "date": d.date().isoformat(),
                "universe_snapshot_date": uni_date,
                "universe_age_days": int(uni_age),
                "ma224": float(ma224),
                "close": close,
                "close_vs_ma224_pct": (close / float(ma224) - 1.0) * 100.0,
                "accum_recent_vs_prior": float(accum["accum_recent_vs_prior"]),
                "accum_recent_days_above_prior_median": int(
                    accum["accum_recent_days_above_prior_median"]
                ),
                "accum_max_single_day_vs_prior_median": float(
                    accum["accum_max_single_day_vs_prior_median"]
                ),
                "accum_log_amount_slope": float(accum["accum_log_amount_slope"]),
                "accum_price_return20": float(accum["accum_price_return20"]),
                "used_as_gate": 0,
            })

        pass_sets[str(code)] = pset
        bar_date_maps[str(code)] = dmap

    raw = pd.DataFrame(raw_rows)
    run_rows: List[Dict[str, Any]] = []
    run_lookup: Dict[Tuple[str, int], str] = {}

    for code, pset in pass_sets.items():
        idxs = sorted(pset)
        local = 0
        current: List[int] = []
        prev = None

        def flush(run_indices: List[int], run_no: int):
            if not run_indices:
                return
            rid = f"{code}:RAW{run_no}"
            for bi in run_indices:
                run_lookup[(code, int(bi))] = rid
            dmap = bar_date_maps.get(code, {})
            run_rows.append({
                "schema": SCHEMA,
                "strategy_id": STRATEGY_ID,
                "raw_accum_run_id": rid,
                "code": code,
                "run_start_bar_index": int(run_indices[0]),
                "run_end_bar_index": int(run_indices[-1]),
                "run_start_date": dmap.get(int(run_indices[0]), ""),
                "run_end_date": dmap.get(int(run_indices[-1]), ""),
                "run_length_pass_bars": int(len(run_indices)),
                "used_as_gate": 0,
            })

        for bi in idxs:
            if prev is None or bi == prev + 1:
                current.append(int(bi))
            else:
                local += 1
                flush(current, local)
                current = [int(bi)]
            prev = int(bi)
        if current:
            local += 1
            flush(current, local)

    runs = pd.DataFrame(run_rows)

    # Map every existing episode to the independent raw mask.
    ep_rows: List[Dict[str, Any]] = []
    if not stage_df.empty:
        for episode_id, eg in stage_df.groupby("episode_id", sort=False):
            eg = eg.sort_values("bar_index")
            base = eg[eg["stage"].eq("LOW224_BASE")]
            if base.empty:
                continue
            b = base.iloc[0]
            code = str(b["code"])
            bi = int(b["bar_index"])
            pset = pass_sets.get(code, set())

            wave = eg[eg["stage"].eq("WAVE1")]
            wi = int(wave.iloc[0]["bar_index"]) if not wave.empty else None

            next10 = [int((bi+k) in pset) for k in range(0, 10)]
            next20 = [int((bi+k) in pset) for k in range(0, 20)]

            if wi is not None and wi > bi:
                pre20_start = max(0, wi - 20)
                pre10_start = max(0, wi - 10)
                pre20_flags = [int(k in pset) for k in range(pre20_start, wi)]
                pre10_flags = [int(k in pset) for k in range(pre10_start, wi)]
                between_flags = [int(k in pset) for k in range(bi, wi)]
                prewave20_count = int(sum(pre20_flags))
                prewave10_count = int(sum(pre10_flags))
                between_count = int(sum(between_flags))
                prewave20_longest = _longest_true_streak(pre20_flags)
                trading_bars_to_wave = int(wi - bi)
            else:
                prewave20_count = -1
                prewave10_count = -1
                between_count = -1
                prewave20_longest = -1
                trading_bars_to_wave = -1

            rid = run_lookup.get((code, bi), "")
            run_len = 0
            if rid and not runs.empty:
                rg = runs[runs["raw_accum_run_id"].eq(rid)]
                if not rg.empty:
                    run_len = int(rg.iloc[0]["run_length_pass_bars"])

            ep_rows.append({
                "schema": SCHEMA,
                "strategy_id": STRATEGY_ID,
                "episode_id": episode_id,
                "code": code,
                "base_date": b["event_date"],
                "wave1_present": int(wi is not None),
                "wave1_date": wave.iloc[0]["event_date"] if wi is not None else "",
                "trading_bars_base_to_wave": trading_bars_to_wave,
                "raw_run_id_at_base": rid,
                "raw_run_length_at_base": run_len,
                "raw_accum_pass_count_base_next10": int(sum(next10)),
                "raw_accum_pass_longest_streak_base_next10": _longest_true_streak(next10),
                "raw_accum_pass_count_base_next20": int(sum(next20)),
                "raw_accum_pass_longest_streak_base_next20": _longest_true_streak(next20),
                "raw_accum_pass_count_between_base_and_wave": between_count,
                "raw_accum_pass_count_prewave10": prewave10_count,
                "raw_accum_pass_count_prewave20": prewave20_count,
                "raw_accum_pass_longest_streak_prewave20": prewave20_longest,
                "used_as_gate": 0,
            })

    ep = pd.DataFrame(ep_rows)

    def _q(series: pd.Series, p: float) -> float:
        x = pd.to_numeric(series, errors="coerce").dropna()
        x = x[x >= 0]
        return float(x.quantile(p)) if len(x) else float("nan")

    wave_ep = ep[ep["wave1_present"].eq(1)] if not ep.empty else pd.DataFrame()
    summary = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "raw_pass_bars": int(len(raw)),
        "raw_runs": int(len(runs)),
        "raw_run_length_median": _q(runs["run_length_pass_bars"], .50) if not runs.empty else float("nan"),
        "raw_run_length_p75": _q(runs["run_length_pass_bars"], .75) if not runs.empty else float("nan"),
        "raw_run_length_p90": _q(runs["run_length_pass_bars"], .90) if not runs.empty else float("nan"),
        "raw_run_length_max": int(runs["run_length_pass_bars"].max()) if not runs.empty else 0,
        "raw_runs_ge2_pct": float(
            pd.to_numeric(runs["run_length_pass_bars"], errors="coerce").ge(2).mean() * 100.0
        ) if not runs.empty else 0.0,
        "raw_runs_ge3_pct": float(
            pd.to_numeric(runs["run_length_pass_bars"], errors="coerce").ge(3).mean() * 100.0
        ) if not runs.empty else 0.0,
        "episodes": int(len(ep)),
        "wave1_episodes": int(len(wave_ep)),
        "all_episode_base_next10_pass_count_median": _q(
            ep["raw_accum_pass_count_base_next10"], .50
        ) if not ep.empty else float("nan"),
        "wave1_base_next10_pass_count_median": _q(
            wave_ep["raw_accum_pass_count_base_next10"], .50
        ) if not wave_ep.empty else float("nan"),
        "wave1_base_next20_pass_count_median": _q(
            wave_ep["raw_accum_pass_count_base_next20"], .50
        ) if not wave_ep.empty else float("nan"),
        "wave1_prewave10_pass_count_median": _q(
            wave_ep["raw_accum_pass_count_prewave10"], .50
        ) if not wave_ep.empty else float("nan"),
        "wave1_prewave20_pass_count_median": _q(
            wave_ep["raw_accum_pass_count_prewave20"], .50
        ) if not wave_ep.empty else float("nan"),
        "wave1_prewave20_longest_streak_median": _q(
            wave_ep["raw_accum_pass_longest_streak_prewave20"], .50
        ) if not wave_ep.empty else float("nan"),
        "used_as_gate": 0,
        "detector_state_independent": 1,
    }])
    return raw, runs, ep, summary



def build_first_pullback_anatomy_audit(
    stage_df: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Causal anatomy of the existing FIRST_PULLBACK stage.

    No pullback rule is changed. Every descriptor in the main anatomy columns
    uses only information available on or before the FIRST_PULLBACK bar.
    A small explicitly named `future_taxonomy_*` block is also saved for
    structural review only and is never a gate.
    """
    if stage_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for episode_id, eg in stage_df.groupby("episode_id", sort=False):
        eg = eg.sort_values("bar_index")
        wave = eg[eg["stage"].eq("WAVE1")]
        pb = eg[eg["stage"].eq("FIRST_PULLBACK")]
        if wave.empty or pb.empty:
            continue

        w = wave.iloc[0]
        p = pb.iloc[0]
        code = str(w["code"])
        df = frames.get(code)
        if df is None or df.empty:
            continue

        wi = int(w["bar_index"])
        pi = int(p["bar_index"])
        if wi < 0 or pi <= wi or pi >= len(df):
            continue

        wave_amt = float(w["actual_amount"]) if _finite(w.get("actual_amount")) else float("nan")
        pb_amt = float(p["actual_amount"]) if _finite(p.get("actual_amount")) else float("nan")
        prior_breakout_high = (
            float(w.get("wave1_prior_high20"))
            if _finite(w.get("wave1_prior_high20"))
            else float("nan")
        )
        wave_close = float(w["close"])
        pb_close = float(p["close"])
        pb_low = float(p["low"])
        pb_open = float(p["open"])
        running_wave_high = (
            float(p.get("running_wave_high"))
            if _finite(p.get("running_wave_high"))
            else float("nan")
        )

        # Actual Amount during bars after WAVE1 through FIRST_PULLBACK,
        # inclusive. This is causal at FIRST_PULLBACK.
        path = df.iloc[wi+1:pi+1]
        path_amt = pd.to_numeric(path["amount"], errors="coerce")
        path_amt = path_amt[path_amt > 0]
        path_close = pd.to_numeric(path["close"], errors="coerce")
        path_low = pd.to_numeric(path["low"], errors="coerce")

        wave_amt20_prior = (
            float(w.get("wave1_amount20_mean_prior"))
            if _finite(w.get("wave1_amount20_mean_prior"))
            else float("nan")
        )

        rr = {
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "episode_id": episode_id,
            "code": code,
            "wave1_date": w["event_date"],
            "first_pullback_date": p["event_date"],
            "wave_to_pb_trading_bars": int(pi - wi),
            "pullback_drawdown_from_running_wave_high": (
                float(p.get("pullback_drawdown_from_running_wave_high"))
                if _finite(p.get("pullback_drawdown_from_running_wave_high"))
                else float("nan")
            ),
            "pb_close_vs_wave_close_pct": (
                (pb_close / wave_close - 1.0) * 100.0 if wave_close > 0 else float("nan")
            ),
            "pb_amount_vs_wave": (
                pb_amt / wave_amt
                if _finite(pb_amt) and _finite(wave_amt) and wave_amt > 0
                else float("nan")
            ),
            "pb_amount_vs_wave_amount20_prior": (
                pb_amt / wave_amt20_prior
                if _finite(pb_amt) and _finite(wave_amt20_prior) and wave_amt20_prior > 0
                else float("nan")
            ),
            "pb_path_amount_median_vs_wave": (
                float(path_amt.median()) / wave_amt
                if len(path_amt) and _finite(wave_amt) and wave_amt > 0
                else float("nan")
            ),
            "pb_path_amount_max_vs_wave": (
                float(path_amt.max()) / wave_amt
                if len(path_amt) and _finite(wave_amt) and wave_amt > 0
                else float("nan")
            ),
            "pb_close_above_prior_breakout_high": (
                int(pb_close >= prior_breakout_high)
                if _finite(prior_breakout_high) else -1
            ),
            "pb_low_above_prior_breakout_high": (
                int(pb_low >= prior_breakout_high)
                if _finite(prior_breakout_high) else -1
            ),
            "pb_close_vs_prior_breakout_high_pct": (
                (pb_close / prior_breakout_high - 1.0) * 100.0
                if _finite(prior_breakout_high) and prior_breakout_high > 0
                else float("nan")
            ),
            "pb_low_vs_prior_breakout_high_pct": (
                (pb_low / prior_breakout_high - 1.0) * 100.0
                if _finite(prior_breakout_high) and prior_breakout_high > 0
                else float("nan")
            ),
            "pb_green_candle": int(pb_close > pb_open),
            "pb_path_red_close_fraction": (
                float((path_close.diff().dropna() < 0).mean())
                if len(path_close) >= 2 else float("nan")
            ),
            "used_as_gate": 0,
        }

        # Explicit future taxonomy: subsequent 5 bars only for structural audit.
        future = df.iloc[pi+1:min(len(df), pi+6)]
        if not future.empty:
            future_lows = pd.to_numeric(future["low"], errors="coerce")
            future_closes = pd.to_numeric(future["close"], errors="coerce")
            rr.update({
                "future_taxonomy_lower_low_within5": int(
                    bool((future_lows < pb_low).any())
                ),
                "future_taxonomy_close_reclaim_pb_high_within5": int(
                    bool((future_closes > float(p["high"])).any())
                ),
                "future_taxonomy_only_not_gate": 1,
            })
        else:
            rr.update({
                "future_taxonomy_lower_low_within5": -1,
                "future_taxonomy_close_reclaim_pb_high_within5": -1,
                "future_taxonomy_only_not_gate": 1,
            })
        rows.append(rr)

    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail, pd.DataFrame()

    def q(col: str, p: float) -> float:
        x = pd.to_numeric(detail[col], errors="coerce").dropna()
        return float(x.quantile(p)) if len(x) else float("nan")

    valid_support = detail[pd.to_numeric(
        detail["pb_close_above_prior_breakout_high"], errors="coerce"
    ).ge(0)]
    valid_future = detail[pd.to_numeric(
        detail["future_taxonomy_lower_low_within5"], errors="coerce"
    ).ge(0)]

    summary = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "pullback_events": int(len(detail)),
        "wave_to_pb_bars_median": q("wave_to_pb_trading_bars", .50),
        "wave_to_pb_bars_p75": q("wave_to_pb_trading_bars", .75),
        "wave_to_pb_1bar_pct": float(
            pd.to_numeric(detail["wave_to_pb_trading_bars"], errors="coerce").eq(1).mean() * 100.0
        ),
        "drawdown_median_pct": q("pullback_drawdown_from_running_wave_high", .50) * 100.0,
        "drawdown_p75_pct": q("pullback_drawdown_from_running_wave_high", .75) * 100.0,
        "pb_amount_vs_wave_median": q("pb_amount_vs_wave", .50),
        "pb_amount_vs_wave_p75": q("pb_amount_vs_wave", .75),
        "pb_amount_vs_wave_ge1_pct": float(
            pd.to_numeric(detail["pb_amount_vs_wave"], errors="coerce").ge(1.0).mean() * 100.0
        ),
        "pb_path_amount_median_vs_wave_median": q("pb_path_amount_median_vs_wave", .50),
        "pb_close_above_prior_breakout_high_pct": float(
            pd.to_numeric(valid_support["pb_close_above_prior_breakout_high"], errors="coerce").eq(1).mean() * 100.0
        ) if len(valid_support) else float("nan"),
        "pb_low_above_prior_breakout_high_pct": float(
            pd.to_numeric(valid_support["pb_low_above_prior_breakout_high"], errors="coerce").eq(1).mean() * 100.0
        ) if len(valid_support) else float("nan"),
        "future_taxonomy_lower_low_within5_pct": float(
            pd.to_numeric(valid_future["future_taxonomy_lower_low_within5"], errors="coerce").eq(1).mean() * 100.0
        ) if len(valid_future) else float("nan"),
        "future_taxonomy_only_not_gate": 1,
        "used_as_gate": 0,
    }])
    return detail, summary


def build_pullback_stratified_sample(
    anatomy: pd.DataFrame,
    per_group: int = 8,
) -> pd.DataFrame:
    """Deterministic pullback anatomy sample; no outcome fields used."""
    if anatomy.empty:
        return pd.DataFrame()

    x = anatomy.copy()
    amt = pd.to_numeric(x["pb_amount_vs_wave"], errors="coerce")
    support = pd.to_numeric(x["pb_close_above_prior_breakout_high"], errors="coerce")
    bars = pd.to_numeric(x["wave_to_pb_trading_bars"], errors="coerce")

    def classify(i: int) -> str:
        # Semantic review buckets only; not gates.
        if support.iloc[i] == 1 and amt.iloc[i] <= 0.5:
            return "SUPPORT_HOLD_DRY"
        if support.iloc[i] == 1 and amt.iloc[i] > 0.5:
            return "SUPPORT_HOLD_ACTIVE"
        if support.iloc[i] == 0 and amt.iloc[i] <= 0.5:
            return "SUPPORT_UNDERCUT_DRY"
        if support.iloc[i] == 0 and amt.iloc[i] > 0.5:
            return "SUPPORT_UNDERCUT_ACTIVE"
        if bars.iloc[i] == 1:
            return "IMMEDIATE_PB"
        return "OTHER"

    x["pb_review_stratum"] = [classify(i) for i in range(len(x))]
    x["sample_key"] = x["episode_id"].map(
        lambda v: hashlib.sha256(str(v).encode("utf-8")).hexdigest()
    )
    out = []
    for st, g in x.groupby("pb_review_stratum", sort=True):
        out.append(g.sort_values("sample_key").head(int(per_group)))
    ans = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    return ans.drop(columns=["sample_key"], errors="ignore")



def build_stabilization_anatomy_audit(
    stage_df: pd.DataFrame,
    frames: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Causal anatomy of the existing STABILIZATION stage.

    No stabilization rule is changed. Main anatomy columns use only data
    available on or before STABILIZATION. Explicitly named future taxonomy
    columns inspect the next 5/8 bars only for structural review and are never
    detector gates.
    """
    if stage_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    rows: List[Dict[str, Any]] = []

    for episode_id, eg in stage_df.groupby("episode_id", sort=False):
        eg = eg.sort_values("bar_index")
        wave = eg[eg["stage"].eq("WAVE1")]
        pb = eg[eg["stage"].eq("FIRST_PULLBACK")]
        st = eg[eg["stage"].eq("STABILIZATION")]
        if wave.empty or pb.empty or st.empty:
            continue

        w = wave.iloc[0]
        p = pb.iloc[0]
        q = st.iloc[0]
        code = str(w["code"])
        df = frames.get(code)
        if df is None or df.empty:
            continue

        wi = int(w["bar_index"])
        pi = int(p["bar_index"])
        si = int(q["bar_index"])
        if not (0 <= wi < pi < si < len(df)):
            continue

        wave_amt = float(w["actual_amount"]) if _finite(w.get("actual_amount")) else float("nan")
        pb_amt = float(p["actual_amount"]) if _finite(p.get("actual_amount")) else float("nan")
        st_amt = float(q["actual_amount"]) if _finite(q.get("actual_amount")) else float("nan")

        wave_close = float(w["close"])
        wave_high = float(w["high"])
        pb_close = float(p["close"])
        pb_low = float(p["low"])
        pb_high = float(p["high"])
        st_close = float(q["close"])
        st_low = float(q["low"])
        st_high = float(q["high"])

        breakout_high = (
            float(w.get("wave1_prior_high20"))
            if _finite(w.get("wave1_prior_high20"))
            else float("nan")
        )

        path = df.iloc[pi:si+1]
        path_low = pd.to_numeric(path["low"], errors="coerce")
        path_close = pd.to_numeric(path["close"], errors="coerce")
        path_amt = pd.to_numeric(path["amount"], errors="coerce")
        path_amt_pos = path_amt[path_amt > 0]

        path_min_low = float(path_low.min()) if len(path_low) else float("nan")
        path_max_close = float(path_close.max()) if len(path_close) else float("nan")
        path_min_close = float(path_close.min()) if len(path_close) else float("nan")

        rr = {
            "schema": SCHEMA,
            "strategy_id": STRATEGY_ID,
            "episode_id": episode_id,
            "code": code,
            "wave1_date": w["event_date"],
            "first_pullback_date": p["event_date"],
            "stabilization_date": q["event_date"],
            "wave_to_pb_trading_bars": int(pi - wi),
            "pb_to_stable_trading_bars": int(si - pi),
            "wave_to_stable_trading_bars": int(si - wi),

            "stabilization_amount_vs_wave": (
                st_amt / wave_amt
                if _finite(st_amt) and _finite(wave_amt) and wave_amt > 0
                else float("nan")
            ),
            "stabilization_amount_vs_pb": (
                st_amt / pb_amt
                if _finite(st_amt) and _finite(pb_amt) and pb_amt > 0
                else float("nan")
            ),
            "pb_to_stable_path_amount_median_vs_wave": (
                float(path_amt_pos.median()) / wave_amt
                if len(path_amt_pos) and _finite(wave_amt) and wave_amt > 0
                else float("nan")
            ),
            "pb_to_stable_path_amount_max_vs_wave": (
                float(path_amt_pos.max()) / wave_amt
                if len(path_amt_pos) and _finite(wave_amt) and wave_amt > 0
                else float("nan")
            ),

            "stabilization_close_vs_pb_close_pct": (
                (st_close / pb_close - 1.0) * 100.0
                if pb_close > 0 else float("nan")
            ),
            "stabilization_low_vs_pb_low_pct": (
                (st_low / pb_low - 1.0) * 100.0
                if pb_low > 0 else float("nan")
            ),
            "stabilization_close_above_pb_high": int(st_close > pb_high),
            "stabilization_close_above_wave_close": int(st_close >= wave_close),
            "stabilization_close_above_breakout_high": (
                int(st_close >= breakout_high)
                if _finite(breakout_high) else -1
            ),
            "stabilization_low_above_breakout_high": (
                int(st_low >= breakout_high)
                if _finite(breakout_high) else -1
            ),
            "stabilization_low_equals_path_min": (
                int(_finite(path_min_low) and abs(st_low - path_min_low) <= max(1e-12, abs(path_min_low) * 1e-10))
            ),
            "path_min_low_vs_pb_low_pct": (
                (path_min_low / pb_low - 1.0) * 100.0
                if _finite(path_min_low) and pb_low > 0 else float("nan")
            ),
            "path_close_recovery_from_min_pct": (
                (st_close / path_min_close - 1.0) * 100.0
                if _finite(path_min_close) and path_min_close > 0 else float("nan")
            ),
            "path_close_recovery_vs_path_range_pct": (
                (st_close - path_min_close) / (path_max_close - path_min_close) * 100.0
                if _finite(path_min_close) and _finite(path_max_close) and path_max_close > path_min_close
                else float("nan")
            ),
            "used_as_gate": 0,
        }

        # Future taxonomy only: structural validation, never a gate.
        future5 = df.iloc[si+1:min(len(df), si+6)]
        future8 = df.iloc[si+1:min(len(df), si+9)]

        if not future5.empty:
            f5_low = pd.to_numeric(future5["low"], errors="coerce")
            f5_close = pd.to_numeric(future5["close"], errors="coerce")
            rr["future_taxonomy_lower_low_vs_stable_within5"] = int(
                bool((f5_low < st_low).any())
            )
            rr["future_taxonomy_break_pb_low_within5"] = int(
                bool((f5_low < pb_low).any())
            )
            rr["future_taxonomy_close_above_stable_high_within5"] = int(
                bool((f5_close > st_high).any())
            )
        else:
            rr["future_taxonomy_lower_low_vs_stable_within5"] = -1
            rr["future_taxonomy_break_pb_low_within5"] = -1
            rr["future_taxonomy_close_above_stable_high_within5"] = -1

        if not future8.empty:
            f8_close = pd.to_numeric(future8["close"], errors="coerce")
            rr["future_taxonomy_wave_high_reclaim_within8"] = int(
                bool((f8_close >= wave_high).any())
            )
        else:
            rr["future_taxonomy_wave_high_reclaim_within8"] = -1

        reac = eg[eg["stage"].eq("REACCELERATION")]
        rr["future_taxonomy_existing_reaccel_stage_present"] = int(not reac.empty)
        rr["future_taxonomy_only_not_gate"] = 1
        rows.append(rr)

    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail, pd.DataFrame()

    def q(col: str, p: float) -> float:
        x = pd.to_numeric(detail[col], errors="coerce").dropna()
        return float(x.quantile(p)) if len(x) else float("nan")

    f5 = detail[
        pd.to_numeric(
            detail["future_taxonomy_lower_low_vs_stable_within5"],
            errors="coerce"
        ).ge(0)
    ]
    f8 = detail[
        pd.to_numeric(
            detail["future_taxonomy_wave_high_reclaim_within8"],
            errors="coerce"
        ).ge(0)
    ]
    support = detail[
        pd.to_numeric(
            detail["stabilization_close_above_breakout_high"],
            errors="coerce"
        ).ge(0)
    ]

    summary = pd.DataFrame([{
        "schema": SCHEMA,
        "strategy_id": STRATEGY_ID,
        "stabilization_events": int(len(detail)),
        "pb_to_stable_bars_median": q("pb_to_stable_trading_bars", .50),
        "pb_to_stable_bars_p75": q("pb_to_stable_trading_bars", .75),
        "pb_to_stable_1bar_pct": float(
            pd.to_numeric(
                detail["pb_to_stable_trading_bars"], errors="coerce"
            ).eq(1).mean() * 100.0
        ),
        "stabilization_amount_vs_wave_median": q(
            "stabilization_amount_vs_wave", .50
        ),
        "stabilization_amount_vs_pb_median": q(
            "stabilization_amount_vs_pb", .50
        ),
        "stabilization_low_above_pb_low_pct": float(
            pd.to_numeric(
                detail["stabilization_low_vs_pb_low_pct"], errors="coerce"
            ).ge(0).mean() * 100.0
        ),
        "stabilization_close_above_breakout_high_pct": float(
            pd.to_numeric(
                support["stabilization_close_above_breakout_high"],
                errors="coerce"
            ).eq(1).mean() * 100.0
        ) if len(support) else float("nan"),
        "stabilization_low_above_breakout_high_pct": float(
            pd.to_numeric(
                support["stabilization_low_above_breakout_high"],
                errors="coerce"
            ).eq(1).mean() * 100.0
        ) if len(support) else float("nan"),
        "future_taxonomy_lower_low_vs_stable_within5_pct": float(
            pd.to_numeric(
                f5["future_taxonomy_lower_low_vs_stable_within5"],
                errors="coerce"
            ).eq(1).mean() * 100.0
        ) if len(f5) else float("nan"),
        "future_taxonomy_break_pb_low_within5_pct": float(
            pd.to_numeric(
                f5["future_taxonomy_break_pb_low_within5"],
                errors="coerce"
            ).eq(1).mean() * 100.0
        ) if len(f5) else float("nan"),
        "future_taxonomy_wave_high_reclaim_within8_pct": float(
            pd.to_numeric(
                f8["future_taxonomy_wave_high_reclaim_within8"],
                errors="coerce"
            ).eq(1).mean() * 100.0
        ) if len(f8) else float("nan"),
        "future_taxonomy_existing_reaccel_stage_pct": float(
            pd.to_numeric(
                detail["future_taxonomy_existing_reaccel_stage_present"],
                errors="coerce"
            ).eq(1).mean() * 100.0
        ),
        "future_taxonomy_only_not_gate": 1,
        "used_as_gate": 0,
    }])

    return detail, summary


def build_stabilization_review_sample(
    anatomy: pd.DataFrame,
    per_group: int = 8,
) -> pd.DataFrame:
    """Deterministic causal anatomy sample; future fields are not classifiers."""
    if anatomy.empty:
        return pd.DataFrame()

    x = anatomy.copy()
    support = pd.to_numeric(
        x["stabilization_close_above_breakout_high"], errors="coerce"
    )
    amt = pd.to_numeric(
        x["stabilization_amount_vs_wave"], errors="coerce"
    )
    pb_low = pd.to_numeric(
        x["stabilization_low_vs_pb_low_pct"], errors="coerce"
    )

    strata = []
    for i in range(len(x)):
        if support.iloc[i] == 1 and amt.iloc[i] <= 0.5 and pb_low.iloc[i] >= 0:
            st = "SUPPORT_HOLD_DRY_LOW_HOLD"
        elif support.iloc[i] == 1 and pb_low.iloc[i] >= 0:
            st = "SUPPORT_HOLD_LOW_HOLD"
        elif support.iloc[i] == 0 and pb_low.iloc[i] >= 0:
            st = "BREAKOUT_UNDERCUT_PB_LOW_HOLD"
        elif pb_low.iloc[i] < 0:
            st = "PB_LOW_UNDERCUT_BEFORE_STABLE"
        else:
            st = "OTHER"
        strata.append(st)

    x["stabilization_review_stratum"] = strata
    x["sample_key"] = x["episode_id"].map(
        lambda v: hashlib.sha256(str(v).encode("utf-8")).hexdigest()
    )

    out = []
    for st, g in x.groupby("stabilization_review_stratum", sort=True):
        out.append(g.sort_values("sample_key").head(int(per_group)))

    ans = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    return ans.drop(columns=["sample_key"], errors="ignore")


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

    overlap_detail, overlap_summary = build_episode_overlap_audit(stage_df)
    run_funnel_detail, run_funnel_summary = build_run_level_funnel(
        stage_df, overlap_detail
    )
    structure_context = build_structure_context_audit(stage_df, frames)
    structure_context_summary = build_structure_context_summary(structure_context)

    raw_accum_pass, raw_accum_runs, raw_accum_episode_context, raw_accum_summary = (
        build_raw_accum_persistence_audit(
            frames, universe, start, end, cfg, stage_df
        )
    )

    pullback_anatomy, pullback_anatomy_summary = (
        build_first_pullback_anatomy_audit(stage_df, frames)
    )
    pullback_review_sample = build_pullback_stratified_sample(
        pullback_anatomy, per_group=8
    )

    stabilization_anatomy, stabilization_anatomy_summary = (
        build_stabilization_anatomy_audit(stage_df, frames)
    )
    stabilization_review_sample = build_stabilization_review_sample(
        stabilization_anatomy, per_group=8
    )

    stratified_sample = build_stratified_manual_sample(
        stage_df, per_stratum=int(args.stratified_sample_per_group)
    )
    episode_review_bars = build_episode_review_bars(
        stratified_sample, stage_df, frames
    )

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
        "stratified_manual_sample_episodes": int(len(stratified_sample)),
        "episode_overlap_audit": overlap_summary.to_dict("records"),
        "run_level_funnel_audit": run_funnel_summary.to_dict("records"),
        "structure_context_audit": structure_context_summary.to_dict("records"),
        "raw_accum_persistence_audit": raw_accum_summary.to_dict("records"),
        "first_pullback_anatomy_audit": pullback_anatomy_summary.to_dict("records"),
        "stabilization_anatomy_audit": stabilization_anatomy_summary.to_dict("records"),
        "sample_fidelity_audit_only": True,
        "run_context_wave_character_audit_only": True,
        "raw_accum_persistence_audit_only": True,
        "first_pullback_anatomy_audit_only": True,
        "stabilization_anatomy_audit_only": True,
        "detector_gate_changed": False,
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
    write_csv(overlap_detail, "low224_episode_overlap_audit.csv")
    write_csv(overlap_summary, "low224_episode_overlap_summary.csv")
    write_csv(run_funnel_detail, "low224_run_level_funnel_detail.csv")
    write_csv(run_funnel_summary, "low224_run_level_funnel_summary.csv")
    write_csv(structure_context, "low224_structure_context_audit.csv")
    write_csv(structure_context_summary, "low224_structure_context_summary.csv")
    write_csv(raw_accum_pass, "low224_raw_accum_pass_detail.csv")
    write_csv(raw_accum_runs, "low224_raw_accum_run_detail.csv")
    write_csv(raw_accum_episode_context, "low224_raw_accum_episode_context.csv")
    write_csv(raw_accum_summary, "low224_raw_accum_persistence_summary.csv")
    write_csv(pullback_anatomy, "low224_first_pullback_anatomy_detail.csv")
    write_csv(pullback_anatomy_summary, "low224_first_pullback_anatomy_summary.csv")
    write_csv(pullback_review_sample, "low224_first_pullback_review_sample.csv")
    write_csv(stabilization_anatomy, "low224_stabilization_anatomy_detail.csv")
    write_csv(stabilization_anatomy_summary, "low224_stabilization_anatomy_summary.csv")
    write_csv(stabilization_review_sample, "low224_stabilization_review_sample.csv")
    write_csv(stratified_sample, "low224_stratified_manual_review_sample.csv")
    write_csv(episode_review_bars, "low224_episode_review_bars.csv")
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
        f"episodes {episodes} · legacy manual sample {len(sample)}",
        "",
        "🧬 [Sample Fidelity Audit · detector 변경 없음]",
        (
            f"consecutive accumulation runs {int(overlap_summary.iloc[0]['consecutive_runs']):,}"
            f" / raw episodes {int(overlap_summary.iloc[0]['episodes']):,}"
        ),
        (
            f"multi-day run 내부 중복 episode "
            f"{int(overlap_summary.iloc[0]['episodes_in_multi_day_runs']):,}"
            f" ({float(overlap_summary.iloc[0]['episodes_in_multi_day_runs_pct']):.1f}%)"
        ),
        f"stratified manual sample {len(stratified_sample)} · episode 전체구간 review bars 저장",
        "※ 이 audit는 threshold/gate를 바꾸지 않고 표본 독립성·육안검증 품질만 측정",
        "",
        "🧭 [Run-level Funnel · audit only]",
        (
            f"runs {int(run_funnel_summary.iloc[0]['runs']):,}"
            f" → WAVE1 {int(run_funnel_summary.iloc[0]['wave1_runs']):,}"
            f" → PB {int(run_funnel_summary.iloc[0]['first_pullback_runs']):,}"
            f" → STABLE {int(run_funnel_summary.iloc[0]['stabilization_runs']):,}"
            f" → REACCEL {int(run_funnel_summary.iloc[0]['reacceleration_runs']):,}"
        ),
        (
            f"run→WAVE1 {float(run_funnel_summary.iloc[0]['wave1_rate_pct']):.1f}%"
            f" · WAVE1 latency median {float(run_funnel_summary.iloc[0]['wave1_days_from_run_start_median']):.1f}d"
        ),
        "※ raw episode를 제거하지 않음 · 독립 표본 단위 후보를 보기 위한 counterfactual audit",
        "",
        "🔬 [Base/Accum/Wave Character · audit only]",
        (
            f"BASE prior20 MA224 아래<50% "
            f"{float(structure_context_summary.iloc[0]['base_below_ma224_frac20_lt50_pct']):.1f}%"
            f" · 60bar range position median {float(structure_context_summary.iloc[0]['base_range_position60_median'])*100:.1f}%"
        ),
        (
            f"WAVE1 Amount20 median {float(structure_context_summary.iloc[0]['wave1_amount20_ratio_median']):.2f}x"
            f" · p75 {float(structure_context_summary.iloc[0]['wave1_amount20_ratio_p75']):.2f}x"
            f" · ≥10x {float(structure_context_summary.iloc[0]['wave1_amount20_ratio_ge10_pct']):.1f}%"
        ),
        "※ gap/폭발성/장기하단 여부는 설명변수로만 저장 · gate 미사용",
        "",
        "🌱 [Raw Accum Persistence · detector-state independent]",
        (
            f"raw pass bars {int(raw_accum_summary.iloc[0]['raw_pass_bars']):,}"
            f" · raw runs {int(raw_accum_summary.iloc[0]['raw_runs']):,}"
            f" · run length median {float(raw_accum_summary.iloc[0]['raw_run_length_median']):.1f}"
            f" / p90 {float(raw_accum_summary.iloc[0]['raw_run_length_p90']):.1f}"
        ),
        (
            f"WAVE1 pre10 accum-pass median "
            f"{float(raw_accum_summary.iloc[0]['wave1_prewave10_pass_count_median']):.1f}/10"
            f" · pre20 {float(raw_accum_summary.iloc[0]['wave1_prewave20_pass_count_median']):.1f}/20"
            f" · longest streak {float(raw_accum_summary.iloc[0]['wave1_prewave20_longest_streak_median']):.1f}"
        ),
        "※ next_allowed/cooldown/WAVE 결과와 무관하게 모든 eligible bar에서 raw mask 재계산 · gate 미사용",
        "",
        "🪂 [First Pullback Anatomy · audit only]",
        (
            f"PB {int(pullback_anatomy_summary.iloc[0]['pullback_events']):,}"
            f" · WAVE→PB median {float(pullback_anatomy_summary.iloc[0]['wave_to_pb_bars_median']):.1f} bars"
            f" · immediate1bar {float(pullback_anatomy_summary.iloc[0]['wave_to_pb_1bar_pct']):.1f}%"
        ),
        (
            f"drawdown median {float(pullback_anatomy_summary.iloc[0]['drawdown_median_pct']):.1f}%"
            f" · PB Amount/WAVE median {float(pullback_anatomy_summary.iloc[0]['pb_amount_vs_wave_median']):.2f}x"
            f" · PB Amount≥WAVE {float(pullback_anatomy_summary.iloc[0]['pb_amount_vs_wave_ge1_pct']):.1f}%"
        ),
        (
            f"close keeps breakout-high {float(pullback_anatomy_summary.iloc[0]['pb_close_above_prior_breakout_high_pct']):.1f}%"
            f" · low keeps breakout-high {float(pullback_anatomy_summary.iloc[0]['pb_low_above_prior_breakout_high_pct']):.1f}%"
        ),
        (
            f"future taxonomy lower-low≤5 "
            f"{float(pullback_anatomy_summary.iloc[0]['future_taxonomy_lower_low_within5_pct']):.1f}%"
            " (미래 audit only)"
        ),
        "※ support/Amount contraction/future taxonomy 모두 아직 FIRST_PB gate 미사용",
        "",
        "🧱 [Stabilization Anatomy · audit only]",
        (
            f"STABLE {int(stabilization_anatomy_summary.iloc[0]['stabilization_events']):,}"
            f" · PB→STABLE median {float(stabilization_anatomy_summary.iloc[0]['pb_to_stable_bars_median']):.1f} bars"
            f" · immediate1bar {float(stabilization_anatomy_summary.iloc[0]['pb_to_stable_1bar_pct']):.1f}%"
        ),
        (
            f"STABLE Amount/WAVE median {float(stabilization_anatomy_summary.iloc[0]['stabilization_amount_vs_wave_median']):.2f}x"
            f" · Amount/PB median {float(stabilization_anatomy_summary.iloc[0]['stabilization_amount_vs_pb_median']):.2f}x"
            f" · low≥PB low {float(stabilization_anatomy_summary.iloc[0]['stabilization_low_above_pb_low_pct']):.1f}%"
        ),
        (
            f"close keeps breakout-high {float(stabilization_anatomy_summary.iloc[0]['stabilization_close_above_breakout_high_pct']):.1f}%"
            f" · low keeps breakout-high {float(stabilization_anatomy_summary.iloc[0]['stabilization_low_above_breakout_high_pct']):.1f}%"
        ),
        (
            f"future lower-low≤5 vs STABLE "
            f"{float(stabilization_anatomy_summary.iloc[0]['future_taxonomy_lower_low_vs_stable_within5_pct']):.1f}%"
            f" · break original PB low≤5 "
            f"{float(stabilization_anatomy_summary.iloc[0]['future_taxonomy_break_pb_low_within5_pct']):.1f}%"
        ),
        (
            f"future wave-high reclaim≤8 "
            f"{float(stabilization_anatomy_summary.iloc[0]['future_taxonomy_wave_high_reclaim_within8_pct']):.1f}%"
            f" · existing REACCEL stage "
            f"{float(stabilization_anatomy_summary.iloc[0]['future_taxonomy_existing_reaccel_stage_pct']):.1f}%"
        ),
        "※ future taxonomy는 구조검증용 미래 label · STABILIZATION/REACCEL gate 미사용",
        "",
        "🛡️ [Authority]",
        f"actual Amount coverage {amount_coverage:.2f}% · Close×Volume fallback 0",
        f"as-of snapshots {len(universe.dates)} · future fallback 0",
        f"lookahead {lookahead_fail} · deterministic {deterministic_fail}",
        "",
        "⚠️ R1 원칙: 성과로 threshold를 조정하지 않습니다.",
        "➡️ NEXT: FIRST_PB는 눌림 시작 marker로 유지 · STABILIZATION이 실제 바닥 안정화를 확인하는지 검증 후 R1.1 수정 여부 결정",
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
    ap.add_argument("--stratified-sample-per-group", type=int, default=8)
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        return self_test()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
