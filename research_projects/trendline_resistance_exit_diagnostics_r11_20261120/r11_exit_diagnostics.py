#!/usr/bin/env python3
"""R11 RESEARCH_ONLY: why R10 target exits change win rate without mean return.

Pure ex-post diagnostics of the immutable R10 3-stock paired cohort.
No new entry or exit decisions and no post-exit highs fed to a rule.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
R10_SHA256 = "5cb75edbd0a5b5644fb5d837f7237ec2899bf11e21fb814fb68864cc62672f88"
HORIZON = 20
METHODS = ("PRIOR_PIVOT_120", "DAILY_AMOUNT_HLC3_PROXY_120")
TARGET_RULE = "TARGET_ATR_INITIAL_3"
ATR_ONLY = "ATR_INITIAL_3"
TARGET_REASONS = ("FROZEN_TARGET_LIMIT_TOUCH", "FROZEN_TARGET_LIMIT_GAP_CONSERVATIVE")
LOSS_PCT_CUTOFFS = (-5., -10., -15.)


def frozen_r10(repo: Path = ROOT):
    source = repo / "research_projects/trendline_resistance_targets_r10_20261120/r10_resistance_targets.py"
    if hashlib.sha256(source.read_bytes()).hexdigest() != R10_SHA256:
        raise ValueError("CANONICAL_R10_SOURCE_SHA256_MISMATCH")
    sys.path.insert(0, str(source.parent))
    import r10_resistance_targets
    return r10_resistance_targets


def _stats(values: list[float]) -> dict:
    if not values:
        return {"n": 0, "mean": None, "median": None, "p10": None, "p90": None,
                "min": None, "max": None}
    v = sorted(values)
    return {"n": len(v), "mean": round(statistics.mean(v), 6),
            "median": round(statistics.median(v), 6),
            "p10": round(v[int(.1*(len(v)-1))], 6),
            "p90": round(v[int(.9*(len(v)-1))], 6),
            "min": round(v[0], 6), "max": round(v[-1], 6)}


def diagnose_event(event: dict, method: str, prices: pd.DataFrame) -> dict:
    """Paired ATR-only vs target+ATR, plus strictly EXPOST after-exit path.

    The day of target execution is *excluded* from post-exit high diagnostics:
    intraday ordering is unknown from daily bars. D+21 is never inspected.
    """
    if method not in METHODS:
        raise ValueError("UNREGISTERED_METHOD")
    model = event["models"][method + "/" + TARGET_RULE]
    baseline = event["models"][ATR_ONLY]
    out = {"code": event["code"], "date": event["date"], "year": int(event["date"][:4]),
           "idx": int(event["idx"]), "line_id": event["line_id"],
           "method": method, "nonoverlap_20d": bool(event["nonoverlap_20d"]),
           "r10_model_status": model["status"], "atr_only_status": baseline["status"],
           "pair_status": "UNKNOWN_INCOMPLETE_PAIRED_MODEL", "outcome_bucket": None,
           "target_d0": event["resistance"][method].get("target"),
           "post_exit_expost": None}
    if not (model["status"].startswith("PROVISIONAL_") and
            baseline["status"].startswith("PROVISIONAL_")):
        return out
    t = int(event["idx"])
    if t + HORIZON >= len(prices):
        raise AssertionError("PAIRED_COMPLETE_BUT_PATH_MISSING")
    entry = float(event["entry_dplus1_open"])
    target = float(event["resistance"][method]["target"])
    if min(entry, target) <= 0:
        raise AssertionError("PAIRED_INVALID_ENTRY_OR_TARGET")
    target_net = float(model["net_total20bps_pct"])
    atr_net = float(baseline["net_total20bps_pct"])
    delta = round(target_net-atr_net, 6)
    hit_target = model["reason"] in TARGET_REASONS
    if not hit_target and abs(delta) > 1e-6:
        raise AssertionError("NON_TARGET_EXIT_MUST_EQUAL_FROZEN_ATR_ONLY")
    if hit_target:
        bucket = "FROZEN_TARGET_EXIT"
    elif model["reason"] in ("INITIAL_ATR_STOP", "STOP_GAP_DOWN_OPEN"):
        bucket = "ATR_STOP_BEFORE_TARGET"
    elif model["reason"] == "TIME_D20_FALLBACK":
        bucket = "D20_FALLBACK_NO_TARGET_OR_ATR_STOP"
    else:
        raise AssertionError("UNREGISTERED_R10_EXIT_REASON")
    out.update({"pair_status": "PROVISIONAL_PAIRED_3STOCK_NOT_OOS",
                "outcome_bucket": bucket,
                "target_distance_from_entry_pct": round(100*(target/entry-1), 6),
                "r10_exit_day": int(model["exit_day"]), "r10_exit_reason": model["reason"],
                "atr_only_exit_day": int(baseline["exit_day"]),
                "atr_only_exit_reason": baseline["reason"],
                "r10_net_pct": target_net, "atr_only_net_pct": atr_net,
                "paired_difference_pp": delta,
                "target_wins_pair": delta > 1e-6, "target_loses_pair": delta < -1e-6,
                "sacrificed_pct_points_vs_atr_only_EXPOST": round(max(0., -delta), 6),
                "saved_pct_points_vs_atr_only_EXPOST": round(max(0., delta), 6)})
    if hit_target:
        h = int(model["exit_day"])
        remaining = prices.iloc[t+h+1:t+HORIZON+1]
        if len(remaining) != HORIZON-h:
            raise AssertionError("POST_EXIT_D20_WINDOW_MISMATCH")
        future_max = max(float(x) for x in remaining.high) if len(remaining) else None
        d20_close = float(prices.iloc[t+HORIZON].close)
        out["post_exit_expost"] = {
            "asof": "POST_R10_EXECUTION_DESCRIPTIVE_ONLY_NOT_SIGNAL",
            "excluded_exit_day_high_due_to_unknown_intraday_order": True,
            "remaining_days_until_D20": len(remaining),
            "later_high": round(future_max, 6) if future_max is not None else None,
            "later_high_above_frozen_target_pct":
                round(100*(future_max/target-1), 6) if future_max is not None else None,
            "later_unrealized_high_gain_pct_nonnegative":
                round(max(0., 100*(future_max/target-1)), 6) if future_max is not None else None,
            "D20_close_vs_exit_target_pct": round(100*(d20_close/target-1), 6),
            "later_high_gain_at_least_5pct": bool(future_max > target*1.05 or
                math.isclose(future_max,target*1.05,rel_tol=1e-12))
                if future_max is not None else None,
            "later_high_gain_at_least_10pct": bool(future_max > target*1.10 or
                math.isclose(future_max,target*1.10,rel_tol=1e-12))
                if future_max is not None else None,
        }
    return out


def _group(records: list[dict]) -> dict:
    paired = [r for r in records if r["pair_status"].startswith("PROVISIONAL_")]
    touches = [r for r in paired if r["outcome_bucket"] == "FROZEN_TARGET_EXIT"]
    post = [r["post_exit_expost"] for r in touches if r["post_exit_expost"] is not None]
    later = [x for x in post if x["later_high"] is not None]
    delta = [r["paired_difference_pp"] for r in paired]
    method_set = {r["method"] for r in records}
    if len(method_set) > 1:
        raise ValueError("MIXED_METHODS_MUST_NOT_BE_AGGREGATED")
    return {
        "input_n": len(records), "paired_n": len(paired),
        "unknown_n": len(records)-len(paired),
        "outcome_buckets": dict(Counter(r["outcome_bucket"] for r in paired)),
        "paired_difference_pp": _stats(delta),
        "paired_positive_n": sum(x>1e-6 for x in delta),
        "paired_negative_n": sum(x< -1e-6 for x in delta),
        "paired_equal_n": sum(abs(x)<=1e-6 for x in delta),
        "mean_sacrificed_vs_ATR_only_EXPOST_pp": _stats([
            r["sacrificed_pct_points_vs_atr_only_EXPOST"] for r in paired]),
        "mean_saved_vs_ATR_only_EXPOST_pp": _stats([
            r["saved_pct_points_vs_atr_only_EXPOST"] for r in paired]),
        "R10_target_plus_ATR_net": _stats([r["r10_net_pct"] for r in paired]),
        "R7_ATR_only_net": _stats([r["atr_only_net_pct"] for r in paired]),
        "R10_loss_tail": {str(k): {
            "r10_n": sum(r["r10_net_pct"] <= k for r in paired),
            "atr_only_n": sum(r["atr_only_net_pct"] <= k for r in paired)}
            for k in LOSS_PCT_CUTOFFS},
        "post_target_exit_expost": {
            "target_exits_n": len(touches), "with_subsequent_days_n": len(later),
            "exit_on_D20_no_subsequent_days_n": len(post)-len(later),
            "subsequent_max_high_gain_from_target_pct": _stats([
                x["later_unrealized_high_gain_pct_nonnegative"] for x in later]),
            "later_high_at_least_5pct_n": sum(x["later_high_gain_at_least_5pct"] for x in later),
            "later_high_at_least_10pct_n": sum(x["later_high_gain_at_least_10pct"] for x in later),
            "D20_close_vs_target_pct": _stats([x["D20_close_vs_exit_target_pct"] for x in post]),
            "paired_difference_on_target_exit_pp": _stats([
                r["paired_difference_pp"] for r in touches]),
        },
        "largest_five_regrets_EXPOST": [
            {"code": x["code"], "date": x["date"],
             "paired_difference_pp": x["paired_difference_pp"],
             "r10_exit_day": x["r10_exit_day"]}
            for x in sorted(paired,key=lambda r:r["paired_difference_pp"])[:5]],
    }


def study(repo: Path = ROOT) -> dict:
    r10 = frozen_r10(repo)
    parent = r10.study(repo)
    r7 = r10.frozen_r7(repo)
    r6 = r7.frozen_r6(repo)
    r5 = r6.r5_module(repo)
    prepare = r5._modules(repo)[0]
    prices = {c: prepare(r5.frozen_data(repo,c)) for c in r5.SOURCES}
    if (parent["raw_line_events"], parent["unique_stock_dates"],
            parent["nonoverlap_20d_count"]) != (506,368,94):
        raise AssertionError("R10_FROZEN_POPULATION_CHANGED")
    records = [diagnose_event(e,m,prices[e["code"]]) for e in parent["records"] for m in METHODS]
    summary = {
        "revision": "R11_R10_EXIT_REGRET_DISTRIBUTION_RESEARCH_ONLY_3STOCK",
        "research_only": True,
        "source_revision": parent["revision"],
        "stock_codes": sorted(prices),
        "raw_line_events": 506, "unique_stock_dates": 368,
        "nonoverlap_20d_count": 94,
        "predeclared_analysis": {
            "fixed_target_rule": TARGET_RULE,
            "counterfactual_rule": ATR_ONLY,
            "same_signal_only": True,
            "target_exit_reasons": list(TARGET_REASONS),
            "post_exit_range": "D_AFTER_EXIT_THROUGH_DPLUS20_ONLY",
            "post_exit_high_excludes_day_of_exit": True,
            "future_high_is_not_executable_or_predictive": True,
            "post_exit_high_thresholds_pct": [5,10],
            "loss_tail_cuts_pct": list(LOSS_PCT_CUTOFFS),
            "first_signal_20day_same_stock_nonoverlap": True,
            "additional_parameter_tuning": False,
        },
        "primary": {m: _group([r for r in records if r["method"]==m]) for m in METHODS},
        "nonoverlap_20d": {m: _group([r for r in records if r["method"]==m and r["nonoverlap_20d"]]) for m in METHODS},
        "by_code": {c: {m: _group([r for r in records if r["code"]==c and r["method"]==m]) for m in METHODS} for c in prices},
        "by_year": {str(y): {m: _group([r for r in records if r["year"]==y and r["method"]==m]) for m in METHODS} for y in range(2021,2025)},
        "real_price_by_volume_available": False,
        "independent_adjustment_lineage_certified": False,
        "marketwide_point_in_time_universe": False,
        "out_of_sample": False,
        "strategy_winner": None,
        "can_publish_strategy_performance": False,
        "can_modify_live_search_watch": False,
        "records": records,
    }
    # This comparison is a stable cross-study contract, not a winner test.
    for method in METHODS:
        expected = parent["primary"]["methods"][method]["rules"][TARGET_RULE]
        got = summary["primary"][method]
        if got["paired_n"] != expected["identical_cohort_n"]:
            raise AssertionError("R10_R11_SAME_COHORT_N_MISMATCH")
        expected_diff = expected["paired_mean_difference_vs_baselines_pp"][ATR_ONLY]
        got_diff = got["paired_difference_pp"]["mean"]
        if got_diff != expected_diff:
            raise AssertionError("R10_R11_PAIRED_MEAN_DIFF_MISMATCH")
    return summary


def run(repo: Path, out: Path) -> dict:
    result = study(repo)
    out.mkdir(parents=True,exist_ok=True)
    (out/"R11_FULL_AUDIT.json").write_text(json.dumps(
        result,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    (out/"R11_SUMMARY.json").write_text(json.dumps(
        {k:v for k,v in result.items() if k!="records"},
        ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    flat = []
    for e in result["records"]:
        row = {k:v for k,v in e.items() if k!="post_exit_expost"}
        for k,v in (e["post_exit_expost"] or {}).items():
            row["EXPOST_"+k] = v
        flat.append(row)
    pd.DataFrame(flat).to_csv(out/"R11_PAIRED_EXIT_DIAGNOSTICS.csv",index=False,encoding="utf-8-sig")
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo",default=".")
    p.add_argument("--out",required=True)
    args = p.parse_args()
    result = run(Path(args.repo).resolve(),Path(args.out))
    print(json.dumps({k:v for k,v in result.items() if k not in ("records","by_code","by_year")},ensure_ascii=False))
