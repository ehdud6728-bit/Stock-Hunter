#!/usr/bin/env python3
"""R23 RESEARCH_ONLY entry-quality audit; 149 frozen R19 entries plus 500 raw D0 signals.

Features are knowable no later than D0 CLOSE. All post-D0 prices/outcomes
are LABELS ONLY, never used as feature or to tune thresholds. The handpicked
26 ticker KIS archive and 2024-25 review are NOT pristine out-of-sample.
No LIVE/SEARCH/WATCH, orders, or secret access.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import statistics
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "trendline_true55_r19_20261122"))
sys.path.insert(0, str(ROOT / "trendline_true55_r20_ma_exit_20261123"))
sys.path.insert(0, str(ROOT / "trendline_true55_r22_level_reclaim_20261123"))
import r19_true55_transfer as r19
import r20_ma_exit as r20
import r22_level_reclaim as r22

TRAIN_END = "2023-12-31"
REVIEW_START = "2024-01-01"
MA5 = r22.MA5
LEVEL_TWO = r22.LEVEL_TWO

# Predeclared descriptive proxies, not optimized trading cutoffs.
VOLUME_ACCUM_RATIO = 1.25
AMOUNT_ACCUM_RATIO = 1.25
WAVE_MIN_PCT = 12.0
PULLBACK_MIN_PCT = 3.0
PULLBACK_MAX_PCT = 25.0
D0_AMOUNT_SPIKE = 1.5
CONTRACTION_RATIO = 0.8
FACTORS = (
    "below_ma224_at_d0",
    "prior20_volume_and_amount_accumulation",
    "first_wave_then_pre_d0_pullback",
    "pre_d0_pullback_volume_contraction",
    "d0_breakout_amount_spike",
    "original_224_accum_wave_pullback_proxy",
)


def finite_positive(value) -> bool:
    try:
        return math.isfinite(float(value)) and float(value) > 0
    except (TypeError, ValueError):
        return False


def valid_ratio(num, den):
    if not finite_positive(den):
        return None
    result = float(num) / float(den)
    return round(result, 8) if math.isfinite(result) and result >= 0 else None


def state_bool(value) -> str:
    return "YES" if value is True else ("NO" if value is False else "UNKNOWN")


def feature_asof_d0(g: pd.DataFrame, t: int) -> dict:
    """Only price/volume/amount at or *before* signal-day t. No D+1 access."""
    if t < 55 or t >= len(g):
        raise ValueError("R23_INVALID_SIGNAL_INDEX")
    d0_close = float(g.close.iloc[t])
    prior55 = float(g.high.iloc[t-55:t].max())
    if not finite_positive(prior55) or not d0_close > prior55:
        raise ValueError("R23_NOT_GENUINE_PRIOR55_HIGH_CLOSE_BREAKOUT")

    ma224 = float(g.close.iloc[t-223:t+1].mean()) if t >= 223 else None
    below224 = (d0_close < ma224) if ma224 is not None and finite_positive(ma224) else None
    amount20 = float(g.amount.iloc[t-20:t].mean()) if t >= 20 else None
    volume20 = float(g.volume.iloc[t-20:t].mean()) if t >= 20 else None
    d0_amount_ratio = valid_ratio(g.amount.iloc[t], amount20)
    d0_volume_ratio = valid_ratio(g.volume.iloc[t], volume20)

    v_acc = a_acc = None
    if t >= 80:
        v_acc = valid_ratio(g.volume.iloc[t-20:t].mean(),
                            g.volume.iloc[t-80:t-20].mean())
        a_acc = valid_ratio(g.amount.iloc[t-20:t].mean(),
                            g.amount.iloc[t-80:t-20].mean())
    accumulation = ((v_acc >= VOLUME_ACCUM_RATIO) and
                    (a_acc >= AMOUNT_ACCUM_RATIO)) if v_acc is not None and a_acc is not None else None

    amplitude = drawdown = None
    pattern = higher_low = None
    contraction = None
    if t >= 60:
        # Ordered windows: base D-60..D-31, wave D-30..D-11,
        # pullback D-10..D-1. No D0 or future bar in these windows.
        base = g.iloc[t-60:t-30]
        wave = g.iloc[t-30:t-10]
        pullback = g.iloc[t-10:t]
        base_low = float(base.low.min())
        base_high = float(base.high.max())
        wave_high = float(wave.high.max())
        pb_low = float(pullback.low.min())
        if all(finite_positive(v) for v in (base_low, base_high, wave_high, pb_low)):
            amplitude = round(100 * (wave_high / base_low - 1), 6)
            drawdown = round(100 * (1 - pb_low / wave_high), 6)
            higher_low = pb_low > base_low
            pattern = (wave_high > base_high and amplitude >= WAVE_MIN_PCT
                       and PULLBACK_MIN_PCT <= drawdown <= PULLBACK_MAX_PCT
                       and higher_low)
        pre_wave_volume = float(g.volume.iloc[t-30:t-10].mean())
        recent_volume = float(g.volume.iloc[t-10:t].mean())
        contraction_ratio = valid_ratio(recent_volume, pre_wave_volume)
        contraction = (contraction_ratio <= CONTRACTION_RATIO) if contraction_ratio is not None else None
    else:
        contraction_ratio = None

    spike = d0_amount_ratio >= D0_AMOUNT_SPIKE if d0_amount_ratio is not None else None
    complete = (below224 and accumulation and pattern) if all(
        v is not None for v in (below224, accumulation, pattern)) else None

    return {
        "asof": str(g.date.iloc[t]), "prior55_high_frozen": prior55,
        "d0_close_vs_ma224_pct": round(100*(d0_close/ma224-1),6)
             if ma224 is not None and finite_positive(ma224) else None,
        "pre20_volume_vs_previous60_ratio": v_acc,
        "pre20_amount_vs_previous60_ratio": a_acc,
        "d0_volume_vs_prior20_ratio": d0_volume_ratio,
        "d0_amount_vs_prior20_ratio": d0_amount_ratio,
        "first_wave_amplitude_pct": amplitude,
        "pre_d0_pullback_depth_pct": drawdown,
        "pre_d0_pullback_has_higher_low": higher_low,
        "pre_d0_pullback_volume_ratio": contraction_ratio,
        "factors": {
            "below_ma224_at_d0": state_bool(below224),
            "prior20_volume_and_amount_accumulation": state_bool(accumulation),
            "first_wave_then_pre_d0_pullback": state_bool(pattern),
            "pre_d0_pullback_volume_contraction": state_bool(contraction),
            "d0_breakout_amount_spike": state_bool(spike),
            "original_224_accum_wave_pullback_proxy": state_bool(complete),
        },
    }


def describe_outcomes(rows: list[dict], method: str) -> dict:
    """Robust against zero/one-event groups. Never fabricate a zero return."""
    values = [float(r["outcomes"][method]["net_pct"]) for r in rows]
    if not values:
        return {"n": 0, "mean_net_pct": None, "median_net_pct": None,
                "win_rate_pct": None, "p10_net_pct": None,
                "worst_net_pct": None, "mean_excluding_best_pct": None}
    ordered = sorted(values)
    return {
        "n": len(values),
        "mean_net_pct": round(statistics.mean(ordered),6),
        "median_net_pct": round(statistics.median(ordered),6),
        "win_rate_pct": round(100*sum(v>0 for v in values)/len(values),3),
        "p10_net_pct": round(float(np.quantile(ordered,.10)),6),
        "worst_net_pct": round(min(ordered),6),
        "mean_excluding_best_pct": round(statistics.mean(ordered[:-1]),6)
            if len(ordered)>1 else None,
    }


def factor_split(rows: list[dict], factor: str) -> dict:
    output = {}
    for value in ("YES","NO","UNKNOWN"):
        sample = [r for r in rows if r["features_d0"]["factors"][factor] == value]
        big = sum(r["labels_EXPOST"]["mfe30_full60"] for r in sample)
        output[value] = {
            "n":len(sample),
            "unique_tickers":len({r["code"] for r in sample}),
            "mfe_at_least_30pct_n_EXPOST":big,
            "mfe_at_least_30pct_rate_EXPOST":round(100*big/len(sample),3)
                if sample else None,
            "r20_ma5_realized":describe_outcomes(sample,MA5),
            "r22_two_close_level_realized":describe_outcomes(sample,LEVEL_TWO),
        }
    return output


def report_groups(rows: list[dict]) -> dict:
    return {
        "TRAIN_2021_2023": {
            factor:factor_split([r for r in rows if r["date"]<=TRAIN_END],factor)
            for factor in FACTORS},
        "REVIEW_2024_2025_NON_PRISTINE": {
            factor:factor_split([r for r in rows if r["date"]>=REVIEW_START],factor)
            for factor in FACTORS},
        "ALL_149_DESCRIPTIVE": {
            factor:factor_split(rows,factor) for factor in FACTORS},
    }


def build(prices: pd.DataFrame, frozen_events: list[dict],
          r22_rows: list[dict], r22_summary: dict) -> tuple[dict,list[dict]]:
    ids = lambda rs:[(str(r["code"]),str(r["date"])) for r in rs]
    if (len(frozen_events) != 149 or len(r22_rows) != 149 or
            ids(frozen_events) != ids(r22_rows) or
            r22_summary.get("same_frozen_r19_events") != 149):
        raise ValueError("R23_IDENTITIES_NOT_FROZEN_R19_R22_149")
    frames={str(code):g.sort_values("date").reset_index(drop=True)
            for code,g in prices.groupby("code")}
    positions={c:{d:t for t,d in enumerate(g.date)} for c,g in frames.items()}
    # Raw D0 candidate population is exactly R19 before embargo or D+1 gate.
    # It has no outcomes: overlapping and incomplete signals cannot be labeled.
    raw=[]
    for code,g in frames.items():
        indexes,_=r19.prior55_candidates(g)
        for t in indexes:
            f=feature_asof_d0(g,t)
            raw.append({"code":code,"date":str(g.date.iloc[t]),"features_d0":f})
    if len(raw)!=500 or len({(r["code"],r["date"]) for r in raw})!=500:
        raise ValueError("R23_RAW_PRIOR55_POPULATION_NOT_500")
    raw_by_id={(r["code"],r["date"]):r for r in raw}
    rows=[]
    for old,r22row in zip(frozen_events,r22_rows):
        code,date=str(old["code"]),str(old["date"])
        if code not in positions or date not in positions[code]:
            raise ValueError("R23_EVENT_NOT_IN_KIS_ARCHIVE")
        g,t=frames[code],positions[code][date]
        f=feature_asof_d0(g,t)
        if f != raw_by_id[(code,date)]["features_d0"]:
            raise ValueError("R23_D0_FEATURE_REPLAY_DRIFT")
        old_feats=old["features"]
        for old_name,new_name in (("volume_vs_prev20","d0_volume_vs_prior20_ratio"),
                                  ("amount_vs_prev20","d0_amount_vs_prior20_ratio"),
                                  ("close_vs_ma224_pct","d0_close_vs_ma224_pct")):
            old_v,new_v=old_feats.get(old_name),f[new_name]
            if (old_v is None)!=(new_v is None):
                raise ValueError("R23_R19_CAUSAL_FEATURE_MISSINGNESS_MISMATCH")
            if old_v is not None and abs(float(old_v)-float(new_v))>1e-5:
                raise ValueError("R23_R19_CAUSAL_FEATURE_REPLAY_MISMATCH")
        outcomes={m:r22row["outcomes"][m] for m in (MA5,LEVEL_TWO)}
        path_mfe=float(old["expost_not_tradable"]["max_favorable_60_pct"])
        compare_mfe=float(r22row["expost_full60_not_a_trade_signal"]["max_favorable_pct"])
        if abs(path_mfe-compare_mfe)>1e-6:
            raise ValueError("R23_R22_OUTCOME_MFE_DRIFT")
        rows.append({
            "code":code,"date":date,"year":int(date[:4]),
            "features_d0":f,"outcomes":outcomes,
            "labels_EXPOST":{"mfe30_full60":path_mfe>=30,
                             "full60_mfe_pct":path_mfe},
        })
    # Show how event selection changed the feature composition, but never
    # assign returns or win rates to 500 overlapping raw breakouts.
    denom={}
    for factor in FACTORS:
        denom[factor]={
            "raw500_counts":{state:sum(r["features_d0"]["factors"][factor]==state for r in raw)
                            for state in ("YES","NO","UNKNOWN")},
            "frozen149_counts":{state:sum(r["features_d0"]["factors"][factor]==state for r in rows)
                                for state in ("YES","NO","UNKNOWN")},
        }
    report={
        "revision":"R23_ENTRY_QUALITY_ASOF_D0_ORIGINAL224_WAVE_PULLBACK_PROXIES",
        "research_only":True,"source_kis_run_id":r20.KIS_RUN,
        "raw_d0_prior55_breakout_candidates":len(raw),
        "frozen_r19_after_embargo_dplus1_entry_events":len(rows),
        "sample_ticker_count":len(frames),
        "entry":"R19_DPLUS1_OPEN_UNCHANGED",
        "exit_methods_compared":[MA5,LEVEL_TWO],
        "cost_round_trip_pct":r20.COST_PCT,
        "signal_features_only_through":"D0_CLOSE",
        "predeclared_descriptive_thresholds":{
            "volume_pre20_vs_prior60_min":VOLUME_ACCUM_RATIO,
            "amount_pre20_vs_prior60_min":AMOUNT_ACCUM_RATIO,
            "wave_min_amplitude_pct":WAVE_MIN_PCT,
            "pullback_depth_min_pct":PULLBACK_MIN_PCT,
            "pullback_depth_max_pct":PULLBACK_MAX_PCT,
            "pullback_volume_contraction_max":CONTRACTION_RATIO,
            "d0_amount_vs_previous20_min":D0_AMOUNT_SPIKE,
        },
        "feature_temporal_windows":{
            "pre_base":"Dminus60_through_Dminus31",
            "pre_first_wave":"Dminus30_through_Dminus11",
            "pre_pullback":"Dminus10_through_Dminus1",
            "pre_accumulation":"Dminus20_through_Dminus1_vs_Dminus80_through_Dminus21",
            "ma224":"224_closes_ending_at_D0",
        },
        "feature_composition_and_selection_warning":denom,
        "descriptive_subgroup_summaries":report_groups(rows),
        "frozen_full149_baselines":{
            MA5:describe_outcomes(rows,MA5),
            LEVEL_TWO:describe_outcomes(rows,LEVEL_TWO)},
        "no_model_fit_or_parameter_tuning":True,
        "no_features_from_dplus1_or_future_labels":True,
        "raw500_no_fake_outcomes":True,
        "r19_149_dplus1_gate_is_selection_bias":True,
        "review_2024_25_previously_seen_not_pristine_oos":True,
        "not_full_market_or_random_tickers":True,
        "corporate_actions_not_independently_certified":True,
        "orders_placed":0,"live_search_watch_changed":False,
        "can_publish_trade_edge":False,
    }
    return report,rows


def run(r16_out:Path,r19_out:Path,r22_out:Path,out:Path)->dict:
    prices,events=r20.read_sources(r16_out,r19_out)
    r22s=json.loads((r22_out/"R22_LEVEL_EXIT_SUMMARY.json").read_text(encoding="utf-8"))
    r22rows=json.loads((r22_out/"R22_LEVEL_PAIRED_EVENTS.json").read_text(encoding="utf-8"))
    report,rows=build(prices,events,r22rows,r22s)
    out.mkdir(parents=True,exist_ok=True)
    (out/"R23_ENTRY_QUALITY_SUMMARY.json").write_text(
        json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    (out/"R23_149_ASOF_D0_FEATURE_AUDIT.json").write_text(
        json.dumps(rows,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    return report


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--r16-out",type=Path,required=True)
    p.add_argument("--r19-out",type=Path,required=True)
    p.add_argument("--r22-out",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(run(a.r16_out,a.r19_out,a.r22_out,a.out),
                     ensure_ascii=False,indent=2,allow_nan=False))
