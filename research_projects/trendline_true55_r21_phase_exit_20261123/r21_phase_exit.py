#!/usr/bin/env python3
"""R21 causal exit transitions: frozen R19 cohort, RESEARCH_ONLY.

None of the algorithms reads future OHLCV when deciding an exit; same-day
close decisions execute only at the following session's open. Ex-post
opportunity labels are reported separately and never drive the policies.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "trendline_true55_r20_ma_exit_20261123"))
import r20_ma_exit as r20

BASE = "MA5_CLOSE_BELOW_NEXT_OPEN"
METHODS = (BASE, "MA5_TWO_CONSECUTIVE_CLOSES", "D0_LOW_THEN_MA5_AFTER_1ATR",
           "D0_LOW_THEN_MA5_AFTER_CONFIRMED_HIGHER_LOW")


def atr14_at_signal(g: pd.DataFrame, t: int) -> float:
    """Arithmetic ATR14 at D0, including D0 only, not a future bar."""
    if t < 14:
        raise ValueError("INSUFFICIENT_PRE_SIGNAL_ATR_HISTORY")
    tr = []
    for k in range(t - 13, t + 1):
        high, low = float(g.high.iloc[k]), float(g.low.iloc[k])
        prev_close = float(g.close.iloc[k - 1])
        tr.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    atr = statistics.mean(tr)
    if not np.isfinite(atr) or atr <= 0:
        raise ValueError("INVALID_PRE_SIGNAL_ATR")
    return atr


def confirmed_higher_low_at_close(g: pd.DataFrame, t: int, i: int, floor: float) -> bool:
    """New post-entry 2L/2R pivot low can first be known at close i=j+2."""
    j = i - 2
    if j < t + 1 or j < 2:
        return False
    low = float(g.low.iloc[j])
    return (low > floor and low < float(g.low.iloc[j-2:j].min())
            and low <= float(g.low.iloc[j+1:i+1].min()))


def simulate(g: pd.DataFrame, t: int, method: str) -> dict:
    if method not in METHODS or t < 55 or t + r20.HORIZON >= len(g):
        raise ValueError("INVALID_METHOD_OR_INCOMPLETE_FROZEN_HORIZON")
    entry = float(g.open.iloc[t+1]); floor = float(g.low.iloc[t])
    if entry <= 0 or floor <= 0:
        raise ValueError("INVALID_ENTRY_OR_D0_LOW")
    atr = atr14_at_signal(g, t)
    switched = False
    switch_day = None
    last_below = False
    for i in range(t+1, t+r20.HORIZON):
        close = float(g.close.iloc[i])
        ma = float(g.close.iloc[i-4:i+1].mean())
        below = close < ma
        stop = False
        reason = None
        if method == BASE:
            stop, reason = below, "MA5_CLOSE_BREAK"
        elif method == "MA5_TWO_CONSECUTIVE_CLOSES":
            stop, reason = (below and last_below), "TWO_CONSECUTIVE_MA5_BREAKS"
        else:
            # Initial D0 low is an independently confirmed, pre-entry level.
            # It is an emergency close-based structural failure exit in both phases.
            if close < floor:
                stop, reason = True, "D0_LOW_CLOSE_BREAK"
            elif switched and below:
                stop, reason = True, "POST_CONFIRMATION_MA5_CLOSE_BREAK"
            elif not switched:
                if method == "D0_LOW_THEN_MA5_AFTER_1ATR":
                    activation = close >= entry + atr
                else:
                    activation = (close > entry and
                                  confirmed_higher_low_at_close(g, t, i, floor))
                if activation:
                    switched = True
                    switch_day = i - t
                    # The newly activated exit regime is enforced from NEXT close.
        if stop:
            next_open = float(g.open.iloc[i+1])
            return {"net_pct": round(100*(next_open/entry-1)-r20.COST_PCT,6),
                    "exit_day": i+1-t, "exit_reason": reason,
                    "switch_day": switch_day}
        last_below = below
    return {"net_pct": round(100*(float(g.close.iloc[t+r20.HORIZON])/entry-1)-r20.COST_PCT,6),
            "exit_day": r20.HORIZON, "exit_reason": "WINDOW_END_CLOSE",
            "switch_day": switch_day}


def early_path_diagnostics(g: pd.DataFrame, t: int) -> dict:
    """D+1..5 features descriptive only, NEVER passed to simulate()."""
    entry = float(g.open.iloc[t+1]); floor = float(g.low.iloc[t])
    prior55 = float(g.high.iloc[t-55:t].max())
    flags = {"d0_low_close_break": False, "lost_prior55_level_at_close": False,
             "ma5_close_break": False, "negative_volume_expansion": False}
    for i in range(t+1,t+6):
        close = float(g.close.iloc[i])
        flags["d0_low_close_break"] |= close < floor
        flags["lost_prior55_level_at_close"] |= close <= prior55
        flags["ma5_close_break"] |= close < float(g.close.iloc[i-4:i+1].mean())
        v20 = float(g.volume.iloc[i-20:i].mean())
        flags["negative_volume_expansion"] |= (v20 > 0 and
          float(g.volume.iloc[i]) >= 1.5*v20 and close < float(g.open.iloc[i]))
    post_d5_max = float(g.high.iloc[t+6:t+r20.HORIZON+1].max())
    return {"first5_known_flags": flags,
            "future_after_d5_mfe_pct_EXPOST_ONLY": round(100*(post_d5_max/entry-1),6)}


def audit(prices: pd.DataFrame, frozen_events: list[dict], r20_rows: list[dict]) -> list[dict]:
    if (len(frozen_events) != 149 or len(r20_rows) != 149 or
        [(r["code"],r["date"]) for r in frozen_events] !=
        [(r["code"],r["date"]) for r in r20_rows]):
        raise ValueError("R21_FROZEN_COHORT_NOT_IDENTICAL_TO_R19_R20")
    frames = {str(code): g.sort_values("date").reset_index(drop=True)
              for code,g in prices.groupby("code")}
    locs = {code:{date:i for i,date in enumerate(g.date)} for code,g in frames.items()}
    rows = []
    for event, old in zip(frozen_events, r20_rows):
        code, date = str(event["code"]),str(event["date"])
        if code not in locs or date not in locs[code]:
            raise ValueError("FROZEN_EVENT_NOT_IN_PRICE_ARCHIVE")
        g, t = frames[code], locs[code][date]
        outcomes = {method:simulate(g,t,method) for method in METHODS}
        base_old = old["outcomes"][BASE]
        if any(outcomes[BASE][k] != base_old[k] for k in ("net_pct","exit_day")):
            raise ValueError("R20_MA5_BASELINE_REPLAY_MISMATCH")
        entry = float(g.open.iloc[t+1])
        future = g.high.iloc[t+1:t+r20.HORIZON+1].astype(float)
        peak_index = int(future.idxmax())
        full_mfe = 100*(float(future.max())/entry-1)
        early = early_path_diagnostics(g,t)
        rows.append({"code":code,"date":date,"year":int(date[:4]),
                     "outcomes":outcomes, "first5_diagnostic":early,
                     "expost_never_trading_decision":{
                       "full60_mfe_pct":round(full_mfe,6),
                       "peak_day":peak_index-t,
                       "mfe_30pct_opportunity":full_mfe>=30,
                       "exit_before_expost_peak":{m:outcomes[m]["exit_day"]<peak_index-t
                                                  for m in METHODS}}})
    return rows


def aggregate(rows: list[dict]) -> dict:
    cohort = len(rows)
    methods = {m:r20.describe(rows,m) for m in METHODS}
    pairs = {m:r20.paired(rows,m,BASE) for m in METHODS if m != BASE}
    opportunities = [r for r in rows if r["expost_never_trading_decision"]["mfe_30pct_opportunity"]]
    first5 = {}
    for flag in ("d0_low_close_break", "lost_prior55_level_at_close",
                 "ma5_close_break", "negative_volume_expansion"):
        yes = [r for r in rows if r["first5_diagnostic"]["first5_known_flags"][flag]]
        no = [r for r in rows if not r["first5_diagnostic"]["first5_known_flags"][flag]]
        first5[flag] = {"yes_n":len(yes),"no_n":len(no),
          "post_d5_mfe_median_yes_EXPOST_ONLY":(statistics.median(r["first5_diagnostic"]["future_after_d5_mfe_pct_EXPOST_ONLY"] for r in yes) if yes else None),
          "post_d5_mfe_median_no_EXPOST_ONLY":(statistics.median(r["first5_diagnostic"]["future_after_d5_mfe_pct_EXPOST_ONLY"] for r in no) if no else None)}
    return {"revision":"R21_FIXED149_PHASED_EXIT_AND_EARLY_PULLBACK_AUDIT",
            "research_only":True,"source_kis_run_id":r20.KIS_RUN,
            "same_149_events":cohort,"cost_round_trip_pct":r20.COST_PCT,
            "max_horizon":r20.HORIZON,
            "d0_atr14_precomputed_not_fitted":True,
            "entry":"FROZEN_R19_DPLUS1_OPEN",
            "exits":"DAY_CLOSE_SIGNAL_NEXT_DAY_OPEN",
            "methods":methods,"paired_against_ma5":pairs,
            "by_year":{str(y):{m:r20.describe([r for r in rows if r["year"]==y],m)
                              for m in METHODS} for y in range(2021,2026)},
            "first_five_days_descriptive_only":first5,
            "expost_30pct_mfe_cohort_n":len(opportunities),
            "expost_30pct_exits_before_peak":{m:sum(r["expost_never_trading_decision"]["exit_before_expost_peak"][m] for r in opportunities) for m in METHODS},
            "no_expost_target_or_future_feature_used_to_trade":True,
            "not_pristine_oos_or_full_market_random_sample":True,
            "corporate_actions_not_independently_certified":True,
            "can_publish_trade_edge":False,"live_search_watch_changed":False,"orders_placed":0}


def run(r16_out:Path,r19_out:Path,r20_ma_out:Path,out:Path) -> dict:
    prices,events=r20.read_sources(r16_out,r19_out)
    old=json.loads((r20_ma_out/"R20_MA_PAIRED_EVENTS.json").read_text(encoding="utf-8"))
    summary=json.loads((r20_ma_out/"R20_MA_EXIT_SUMMARY.json").read_text(encoding="utf-8"))
    if summary.get("same_149_r19_entries") != 149:
        raise ValueError("R20_SUMMARY_NOT_FROZEN_149")
    rows=audit(prices,events,old)
    report=aggregate(rows)
    out.mkdir(parents=True,exist_ok=True)
    (out/"R21_PHASE_EXIT_SUMMARY.json").write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    (out/"R21_PHASE_PAIRED_EVENTS.json").write_text(json.dumps(rows,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    return report

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--r16-out",type=Path,required=True)
    parser.add_argument("--r19-out",type=Path,required=True)
    parser.add_argument("--r20-ma-out",type=Path,required=True)
    parser.add_argument("--out",type=Path,required=True)
    a=parser.parse_args()
    print(json.dumps(run(a.r16_out,a.r19_out,a.r20_ma_out,a.out),ensure_ascii=False,indent=2,allow_nan=False))
