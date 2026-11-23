#!/usr/bin/env python3
"""R22 frozen previous-55-day breakout-line exits and D+5 reclaim audit.

RESEARCH_ONLY. No LIVE/SEARCH/WATCH, KIS secrets, or orders.
All actions use a day-end signal and the *next* session open.
The D+5 landmark and hindsight MFE are never inputs to any exit policy.
"""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import statistics
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "trendline_true55_r20_ma_exit_20261123"))
sys.path.insert(0, str(ROOT / "trendline_true55_r21_phase_exit_20261123"))
import r20_ma_exit as r20
import r21_phase_exit as r21

MA5 = "MA5_CLOSE_BELOW_NEXT_OPEN"
MA5_TWO = "MA5_TWO_CONSECUTIVE_CLOSES"
LEVEL_ONE = "FROZEN55_LEVEL_LOSS_1C_NEXT_OPEN"
LEVEL_TWO = "FROZEN55_LEVEL_LOSS_2C_NEXT_OPEN"
GUARD_TWO = "FROZEN55_LEVEL_OR_MA5_TWO_NEXT_OPEN"
METHODS = (MA5, MA5_TWO, LEVEL_ONE, LEVEL_TWO, GUARD_TWO)


def frozen_prior55_level(g: pd.DataFrame, t: int) -> float:
    """Use 55 *prior* daily highs only, excluding today's breakout bar."""
    if t < 55 or t + r20.HORIZON >= len(g):
        raise ValueError("R22_INSUFFICIENT_PRE55_OR_FUTURE60")
    level = float(g.high.iloc[t - 55:t].max())
    if not np.isfinite(level) or level <= 0:
        raise ValueError("R22_INVALID_FROZEN_BREAKOUT_LEVEL")
    return level


def policy(g: pd.DataFrame, t: int, method: str) -> dict:
    if method not in METHODS:
        raise ValueError("R22_UNKNOWN_POLICY")
    level = frozen_prior55_level(g, t)
    entry = float(g.open.iloc[t + 1])
    if not np.isfinite(entry) or entry <= level:
        raise ValueError("R22_ENTRY_MUST_EXCEED_FROZEN_PRIOR55_HIGH")
    # Use original implementations as immutable controls, not reimplementations.
    if method == MA5:
        return r20.ma_close_break_next_open(g, t, 5)
    if method == MA5_TWO:
        return r21.simulate(g, t, MA5_TWO)
    prev_level_lost, prev_ma5_below = False, False
    for i in range(t + 1, t + r20.HORIZON):
        close = float(g.close.iloc[i])
        level_lost = close <= level
        ma5_below = close < float(g.close.iloc[i - 4:i + 1].mean())
        if method == LEVEL_ONE:
            trigger = level_lost
            reason = "FROZEN55_FIRST_CLOSE_AT_OR_BELOW_LEVEL"
        elif method == LEVEL_TWO:
            trigger = level_lost and prev_level_lost
            reason = "FROZEN55_TWO_CONSECUTIVE_CLOSES_AT_OR_BELOW_LEVEL"
        else:
            # Risk guard: loss of the fixed breakout level OR two MA5 closes.
            # Unlike R21's two-MA5 baseline, a single level loss exits immediately.
            trigger = level_lost or (ma5_below and prev_ma5_below)
            reason = ("FROZEN55_FIRST_LOSS" if level_lost
                      else "TWO_CONSECUTIVE_MA5_BREAKS")
        if trigger:
            return {
                "net_pct": round(100 * (float(g.open.iloc[i + 1]) / entry - 1)
                                 - r20.COST_PCT, 6),
                "exit_day": i + 1 - t,
                "exit_reason": reason,
            }
        prev_level_lost, prev_ma5_below = level_lost, ma5_below
    return {"net_pct": round(
        100 * (float(g.close.iloc[t + r20.HORIZON]) / entry - 1)
        - r20.COST_PCT, 6),
        "exit_day": r20.HORIZON, "exit_reason": "WINDOW_END_CLOSE"}


def day5_landmark(g: pd.DataFrame, t: int) -> dict:
    """D+5 end-of-day classification; forward stats measured from D+6 open.

    Entire output is descriptive, separate from policy(), and cannot be used
    to infer that an earlier exit could have known this later information.
    """
    level = frozen_prior55_level(g, t)
    closes = [float(v) for v in g.close.iloc[t + 1:t + 6]]
    lost = [v <= level for v in closes]
    if not any(lost):
        state = "NEVER_LOST_BY_D5"
    elif closes[-1] > level:
        state = "LOST_THEN_RECLAIMED_BY_D5"
    else:
        state = "AT_OR_BELOW_LEVEL_ON_D5"
    forward_entry = float(g.open.iloc[t + 6])
    if not np.isfinite(forward_entry) or forward_entry <= 0:
        raise ValueError("R22_INVALID_D6_LANDMARK_ENTRY")
    # These future outcomes are never read by policy().
    future = g.iloc[t + 6:t + r20.HORIZON + 1]
    return {
        "asof_d5_state": state,
        "lost_first5": any(lost),
        "level_lost_twice_consecutive_by_d5":
            any(lost[j] and lost[j - 1] for j in range(1, 5)),
        "asof_d5_close_above_frozen_level": closes[-1] > level,
        "d6_to_d60_close_hold_net_pct_EXPOST": round(
            100 * (float(g.close.iloc[t + r20.HORIZON]) / forward_entry - 1)
            - r20.COST_PCT, 6),
        "d6_to_d60_best_intraday_mfe_pct_EXPOST": round(
            100 * (float(future.high.max()) / forward_entry - 1), 6),
    }


def audit(prices: pd.DataFrame, events: list[dict],
          ma_rows: list[dict], r21_rows: list[dict]) -> list[dict]:
    identities = lambda rows: [(str(x["code"]), str(x["date"])) for x in rows]
    if (len(events) != 149 or len(ma_rows) != 149 or len(r21_rows) != 149
            or identities(events) != identities(ma_rows)
            or identities(events) != identities(r21_rows)):
        raise ValueError("R22_R19_R20_R21_COHORT_IDENTITY_MISMATCH")
    frames = {str(code): f.sort_values("date").reset_index(drop=True)
              for code, f in prices.groupby("code")}
    positions = {code: {day: k for k, day in enumerate(g.date)}
                 for code, g in frames.items()}
    result = []
    for event, oldma, oldr21 in zip(events, ma_rows, r21_rows):
        code, date = str(event["code"]), str(event["date"])
        if code not in positions or date not in positions[code]:
            raise ValueError("R22_FROZEN_EVENT_MISSING_PRICE")
        g, t = frames[code], positions[code][date]
        level = frozen_prior55_level(g, t)
        outcomes = {m: policy(g, t, m) for m in METHODS}
        # Compare baseline return and execution day to separately reproduced R20/R21.
        for method, previous in (
            (MA5, oldma["outcomes"][MA5]),
            (MA5, oldr21["outcomes"][MA5]),
            (MA5_TWO, oldr21["outcomes"][MA5_TWO]),
        ):
            for key in ("net_pct", "exit_day"):
                if outcomes[method][key] != previous[key]:
                    raise ValueError("R22_PRIOR_EXIT_EVENT_REPLAY_MISMATCH")
        entry = float(g.open.iloc[t + 1])
        path = g.high.iloc[t + 1:t + r20.HORIZON + 1]
        peak_index = int(path.idxmax())
        full_mfe_pct = float(event["expost_not_tradable"]["max_favorable_60_pct"])
        if abs(full_mfe_pct - round(100 * (float(path.max()) / entry - 1), 6)) > 1e-5:
            raise ValueError("R22_R19_MFE_PROVENANCE_MISMATCH")
        result.append({
            "code": code, "date": date, "year": int(date[:4]),
            "frozen_prior55_high": level, "entry_price": entry,
            "outcomes": outcomes,
            "landmark_d5_DESCRIPTIVE_NOT_TRADING": day5_landmark(g, t),
            "expost_full60_not_a_trade_signal": {
                "max_favorable_pct": full_mfe_pct,
                "peak_day": peak_index - t,
                "opportunity_30pct": full_mfe_pct >= 30,
            },
        })
    return result


def ticker_cluster_bootstrap(rows: list[dict], method: str,
                             repetitions: int = 1200) -> dict:
    """Descriptive paired-mean uncertainty; 26 convenience tickers are not random."""
    codes = sorted({r["code"] for r in rows})
    deltas = {c: [r["outcomes"][method]["net_pct"] -
                  r["outcomes"][MA5]["net_pct"]
                  for r in rows if r["code"] == c] for c in codes}
    rng = np.random.default_rng(20261123)
    boots = []
    for _ in range(repetitions):
        sampled = rng.integers(0, len(codes), size=len(codes))
        values = [v for j in sampled for v in deltas[codes[int(j)]]]
        boots.append(statistics.mean(values))
    lo, hi = np.quantile(boots, [.025, .975])
    return {"ticker_clusters": len(codes),
            "paired_mean_delta_95pct_cluster_bootstrap_interval": [
                round(float(lo), 6), round(float(hi), 6)],
            "random_seed": 20261123, "resamples": repetitions,
            "descriptive_only_not_population_inference": True}


def summarize(rows: list[dict]) -> dict:
    if len(rows) != 149:
        raise ValueError("R22_149_EVENTS_EXPECTED")
    methods = {m: r20.describe(rows, m) for m in METHODS}
    paired = {m: {**r20.paired(rows, m, MA5),
                  "ticker_cluster_uncertainty": ticker_cluster_bootstrap(rows, m)}
              for m in METHODS if m != MA5}
    legend = ("NEVER_LOST_BY_D5", "LOST_THEN_RECLAIMED_BY_D5",
              "AT_OR_BELOW_LEVEL_ON_D5")
    groups = {}
    for state in legend:
        sample = [r for r in rows if
                  r["landmark_d5_DESCRIPTIVE_NOT_TRADING"]["asof_d5_state"] == state]
        hold = [r["landmark_d5_DESCRIPTIVE_NOT_TRADING"]
                ["d6_to_d60_close_hold_net_pct_EXPOST"] for r in sample]
        mfe = [r["landmark_d5_DESCRIPTIVE_NOT_TRADING"]
               ["d6_to_d60_best_intraday_mfe_pct_EXPOST"] for r in sample]
        groups[state] = {
            "n": len(sample),
            "d6_to_d60_close_hold_mean_net_pct_EXPOST":
                round(statistics.mean(hold), 6) if hold else None,
            "d6_to_d60_close_hold_median_net_pct_EXPOST":
                round(statistics.median(hold), 6) if hold else None,
            "d6_to_d60_intraday_mfe_median_pct_EXPOST":
                round(statistics.median(mfe), 6) if mfe else None,
            "d6_to_d60_mfe_at_least_30pct_n_EXPOST": sum(x >= 30 for x in mfe),
        }
    opportunity = [r for r in rows
                   if r["expost_full60_not_a_trade_signal"]["opportunity_30pct"]]
    return {
        "revision": "R22_FROZEN55_LEVEL_LOSS_RECLAIM_AND_MA5_GUARD_AUDIT",
        "research_only": True, "source_kis_run_id": r20.KIS_RUN,
        "same_frozen_r19_events": len(rows),
        "max_horizon_bars": r20.HORIZON, "cost_roundtrip_pct": r20.COST_PCT,
        "entry": "R19_FROZEN_DPLUS1_OPEN",
        "frozen_level": "MAX_PRIOR_55_HIGH_EXCLUDING_D0",
        "exit": "CLOSE_DECISION_FOLLOWING_OPEN_EXECUTION",
        "method_statistics": methods,
        "paired_against_unmodified_r20_ma5": paired,
        "yearly_method_statistics": {
            str(y): {m: r20.describe([r for r in rows if r["year"] == y], m)
                     for m in METHODS} for y in range(2021, 2026)},
        "exit_reason_counts": {
            m: dict(Counter(r["outcomes"][m]["exit_reason"] for r in rows))
            for m in METHODS},
        "day5_landmark_ASOF_ONLY_forward_outcomes_EXPOST": groups,
        "expost_30pct_full60_opportunity_count": len(opportunity),
        "expost_30pct_opportunity_exits_before_peak": {
            m: sum(r["outcomes"][m]["exit_day"] <
                   r["expost_full60_not_a_trade_signal"]["peak_day"]
                   for r in opportunity) for m in METHODS},
        "reclaim_by_d5_never_used_for_d1_through_d5_exits": True,
        "no_future_high_or_expost_mfe_used_by_exit_policies": True,
        "selected_26_ticker_convenience_sample_and_nonpristine_review": True,
        "corporate_actions_not_independently_certified": True,
        "can_publish_trade_edge": False,
        "live_search_watch_changed": False, "orders_placed": 0,
    }


def run(r16_out: Path, r19_out: Path, r20_ma_out: Path,
        r21_out: Path, out: Path) -> dict:
    prices, events = r20.read_sources(r16_out, r19_out)
    ma_summary = json.loads((r20_ma_out / "R20_MA_EXIT_SUMMARY.json").read_text())
    r21_summary = json.loads((r21_out / "R21_PHASE_EXIT_SUMMARY.json").read_text())
    if (ma_summary.get("same_149_r19_entries") != 149
            or r21_summary.get("same_149_events") != 149
            or r21_summary.get("source_kis_run_id") != r20.KIS_RUN):
        raise ValueError("R22_SOURCE_COHORT_SUMMARY_NOT_FROZEN")
    ma_rows = json.loads((r20_ma_out / "R20_MA_PAIRED_EVENTS.json").read_text())
    r21_rows = json.loads((r21_out / "R21_PHASE_PAIRED_EVENTS.json").read_text())
    rows = audit(prices, events, ma_rows, r21_rows)
    summary = summarize(rows)
    out.mkdir(parents=True, exist_ok=True)
    (out / "R22_LEVEL_EXIT_SUMMARY.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8")
    (out / "R22_LEVEL_PAIRED_EVENTS.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8")
    return summary


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--r16-out", type=Path, required=True)
    p.add_argument("--r19-out", type=Path, required=True)
    p.add_argument("--r20-ma-out", type=Path, required=True)
    p.add_argument("--r21-out", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    print(json.dumps(run(a.r16_out, a.r19_out, a.r20_ma_out,
                         a.r21_out, a.out), ensure_ascii=False, indent=2,
                     allow_nan=False))
