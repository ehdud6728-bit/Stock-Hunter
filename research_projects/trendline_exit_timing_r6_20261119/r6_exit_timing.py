#!/usr/bin/env python3
"""R6 independent RESEARCH_ONLY maximum opportunity and *causal* exit study.

Replays the frozen R5 three-stock breakout population; does not touch operations.
Daily OHLC is not intraday order data. Ambiguous same-bar outcomes take the worse
stop fill, and a trailing stop uses only preceding *completed* days' highs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
R5_ROOT = ROOT / "research_projects/trendline_breakout_quality_20261119"
R5_SCRIPT_SHA256 = "c1245ef44d13b0252cc1215cdf4d58ee5593277546a88a6dbd35f9a15a8e6d69"
MAX_HORIZON = 20
COST_BPS = 20
PRIMARY_PRIORITY = {"MEDIUM": 0, "LONG": 1, "SHORT": 2}
EXIT_RULES = (
    "TIME_D5", "TIME_D10", "TIME_D20",
    "TARGET_5", "TARGET_10", "TARGET_15",
    "TRAIL_5", "TRAIL_8", "TRAIL_10",
    "TARGET_10_TRAIL_8", "TARGET_15_TRAIL_8",
    "LINE_RELOSS_NEXT_OPEN",
)


def r5_module(repo: Path = ROOT):
    source = repo / "research_projects/trendline_breakout_quality_20261119/r5_breakout_paths.py"
    if hashlib.sha256(source.read_bytes()).hexdigest() != R5_SCRIPT_SHA256:
        raise ValueError("CANONICAL_R5_SOURCE_SHA256_MISMATCH")
    sys.path.insert(0, str(source.parent))
    import r5_breakout_paths as r5
    return r5


def pct(exit_price: float, entry_price: float) -> float:
    return round(100. * (exit_price / entry_price - 1.), 6)


def position_exit(bars: pd.DataFrame, entry_price: float, day: int,
                  price: float, reason: str, *, ambiguous: bool = False,
                  gap_stop: bool = False) -> dict:
    gross = pct(price, entry_price)
    return {
        "status": "PROVISIONAL_DAILY_OHLC_3_STOCK_NOT_OOS",
        "day": day,
        "date": str(bars.iloc[day-1].date.date()),
        "price": round(float(price), 6),
        "reason": reason,
        "gross_pct": gross,
        "net_total20bps_pct": round(gross - COST_BPS / 100, 6),
        "same_bar_stop_target_ambiguous": bool(ambiguous),
        "gap_through_stop_open_fill": bool(gap_stop),
    }


def simulate_exit(bars: pd.DataFrame, entry: float, rule: str,
                  *, line_level_d0: float | None = None,
                  line_slope: float | None = None) -> dict:
    """Predeclared rules; no future peak/low is ever used to choose exit.

    All thresholds are entry-relative, not optimized on these test outcomes.
    When a daily bar hits target and stop without intraday ordering, STOP FIRST.
    For a gapped-down stop, fills at the worse opening price, not the stop.
    """
    if rule not in EXIT_RULES:
        raise ValueError("UNREGISTERED_EXIT_RULE " + rule)
    if len(bars) != MAX_HORIZON:
        return {"status": "UNKNOWN_INCOMPLETE_DPLUS20_PATH"}
    if entry <= 0 or (bars[["open", "high", "low", "close"]] <= 0).any().any():
        return {"status": "UNKNOWN_INVALID_PRICE"}

    if rule.startswith("TIME_D"):
        h = int(rule.removeprefix("TIME_D"))
        return position_exit(bars, entry, h, float(bars.iloc[h-1].close), "TIME_CLOSE")

    if rule == "LINE_RELOSS_NEXT_OPEN":
        if line_level_d0 is None or line_slope is None:
            return {"status": "UNKNOWN_MISSING_ORIGINAL_LINE_GEOMETRY"}
        for h in range(1, MAX_HORIZON):
            close = float(bars.iloc[h-1].close)
            frozen_level = line_level_d0 + line_slope * h
            if close < frozen_level * .996:
                return position_exit(bars, entry, h+1, float(bars.iloc[h].open),
                                     "PREVIOUS_CLOSE_BELOW_FROZEN_LINE_NEXT_OPEN")
        return position_exit(bars, entry, MAX_HORIZON, float(bars.iloc[-1].close),
                             "TIME_D20_FALLBACK")

    target = None
    trailing = None
    if rule.startswith("TARGET_"):
        if "_TRAIL_" in rule:
            target_str, trail_str = rule.split("_TRAIL_")
            target = int(target_str.removeprefix("TARGET_")) / 100.
            trailing = int(trail_str) / 100.
        else:
            target = int(rule.removeprefix("TARGET_")) / 100.
    elif rule.startswith("TRAIL_"):
        trailing = int(rule.removeprefix("TRAIL_")) / 100.

    prior_complete_high = entry
    for h, row in enumerate(bars.itertuples(index=False), start=1):
        op, hi, lo = float(row.open), float(row.high), float(row.low)
        target_price = entry * (1 + target) if target is not None else None
        stop_price = prior_complete_high * (1 - trailing) if trailing is not None else None

        # The session opening price occurs before today's high/low.
        if stop_price is not None and op <= stop_price:
            return position_exit(bars, entry, h, op, "STOP_GAP_DOWN_OPEN",
                                 gap_stop=True)
        if target_price is not None and op >= target_price:
            return position_exit(bars, entry, h, target_price,
                                 "TARGET_LIMIT_CONSERVATIVE_PRICE_ON_GAP_UP")

        hit_stop = stop_price is not None and lo <= stop_price
        hit_target = target_price is not None and hi >= target_price
        if hit_stop:
            return position_exit(bars, entry, h, stop_price,
                                 "STOP_FIRST_ON_DAILY_BAR" if hit_target else "TRAILING_STOP",
                                 ambiguous=bool(hit_target))
        if hit_target:
            return position_exit(bars, entry, h, target_price, "TARGET_LIMIT_TOUCH")

        # Only a *completed* previous bar may raise tomorrow's trailing stop.
        prior_complete_high = max(prior_complete_high, hi)
    return position_exit(bars, entry, MAX_HORIZON, float(bars.iloc[-1].close),
                         "TIME_D20_FALLBACK")


def opportunity(bars: pd.DataFrame, entry: float) -> dict:
    """EXPOST descriptive potential only; peak date is never fed to exit models."""
    if len(bars) != MAX_HORIZON:
        return {"status": "UNKNOWN_INCOMPLETE_DPLUS20_PATH"}
    if entry <= 0:
        return {"status": "UNKNOWN_INVALID_PRICE"}
    # Ties are consistently assigned to the earliest peak day.
    peak_day = max(range(1, MAX_HORIZON+1),
                   key=lambda day: (float(bars.iloc[day-1].high), -day))
    peak_price = float(bars.iloc[peak_day-1].high)
    peak_gross = pct(peak_price, entry)
    d20_gross = pct(float(bars.iloc[-1].close), entry)
    giveback = round(peak_gross - d20_gross, 6)
    running_close_high = entry
    max_pre_peak_close_drawdown = 0.
    for day in range(1, peak_day):
        close = float(bars.iloc[day-1].close)
        running_close_high = max(running_close_high, close)
        max_pre_peak_close_drawdown = max(
            max_pre_peak_close_drawdown, 100. * (1 - close / running_close_high))
    first_touch = {
        str(p): next((day for day in range(1, MAX_HORIZON+1)
                      if float(bars.iloc[day-1].high) >= entry * (1 + p/100)), None)
        for p in (5, 10, 15)
    }
    return {
        "status": "EXPOST_OPPORTUNITY_NOT_EXECUTABLE_EXIT",
        "peak_day": peak_day,
        "peak_date": str(bars.iloc[peak_day-1].date.date()),
        "peak_high": round(peak_price, 6),
        "mfe_gross_pct": peak_gross,
        "dplus20_gross_pct": d20_gross,
        "dplus20_giveback_percentage_points": giveback,
        "giveback_fraction_of_positive_mfe": round(giveback / peak_gross, 6)
        if peak_gross > 0 else None,
        "pre_peak_prior_close_high_max_dd_pct": round(max_pre_peak_close_drawdown, 6),
        "first_target_touch_day_expost": first_touch,
        "intraday_order_of_peak_and_low_known": False,
    }


def scan_event(event: dict, prices: pd.DataFrame) -> dict:
    t = int(event["idx"])
    if str(prices.iloc[t].date.date()) != event["date"]:
        raise AssertionError("R5_EVENT_RAW_PRICE_INDEX_MISMATCH")
    bars = prices.iloc[t+1:t+MAX_HORIZON+1].copy()
    identity = {k: event[k] for k in ("code", "date", "idx", "scale", "line_id")}
    identity["selection_asof"] = "AFTER_D0_CLOSE"
    if len(bars) != MAX_HORIZON:
        identity.update({
            "status": "UNKNOWN_INCOMPLETE_DPLUS20_PATH",
            "opportunity_EXPOST": {"status": "UNKNOWN_INCOMPLETE_DPLUS20_PATH"},
            "exit_models": {r:{"status":"UNKNOWN_INCOMPLETE_DPLUS20_PATH"} for r in EXIT_RULES},
        })
        return identity
    entry = float(bars.iloc[0].open)
    if entry <= 0:
        raise AssertionError("INVALID_EVENT_ENTRY_PRICE")
    identity["status"] = "PROVISIONAL_DAILY_OHLC_3_STOCK_NOT_OOS"
    identity["entry_date"] = str(bars.iloc[0].date.date())
    identity["entry_dplus1_open"] = entry
    identity["opportunity_EXPOST"] = opportunity(bars, entry)
    line_level_d0 = event.get("break_line_level_d0")
    slope = event.get("line_slope_per_bar")
    identity["exit_models"] = {
        rule: simulate_exit(bars, entry, rule,
                            line_level_d0=line_level_d0, line_slope=slope)
        for rule in EXIT_RULES
    }
    d20 = identity["exit_models"]["TIME_D20"]
    r5_d20 = event["future_paths_EXPOST"]["20"]
    if abs(d20["net_total20bps_pct"] -
           r5_d20["net_pct_by_total_cost_bps"]["20"]) > 1e-5:
        raise AssertionError("R5_R6_DPLUS20_PARITY_FAILURE")
    if abs(identity["opportunity_EXPOST"]["mfe_gross_pct"] -
           r5_d20["mfe_from_entry_pct"]) > 1e-5:
        raise AssertionError("R5_R6_MFE_PARITY_FAILURE")
    return identity


def primary_events(event_results: list[dict]) -> list[dict]:
    """Precommitted medium, then long, then short; NEVER best later outcome."""
    dates = {}
    for e in sorted(event_results,
                    key=lambda q: (q["code"], q["date"], PRIMARY_PRIORITY[q["scale"]], q["line_id"])):
        dates.setdefault((e["code"], e["date"]), e)
    return list(dates.values())


def _numbers(vals: list[float]) -> dict:
    if not vals:
        return {"n":0, "mean":None, "median":None, "p10":None, "ex_top1_mean":None}
    arr = sorted(vals)
    p10_idx = max(0, int((len(arr)-1) * .1))
    return {
        "n":len(vals), "mean":round(statistics.mean(vals), 6),
        "median":round(statistics.median(vals), 6),
        "p10_nearest_rank_conservative":round(arr[p10_idx], 6),
        "ex_top1_mean":round(statistics.mean(arr[:-1]), 6) if len(arr)>1 else None,
    }


def summarize(events: list[dict]) -> dict:
    available = [x for x in events if x["status"].startswith("PROVISIONAL_")]
    peak_days = [x["opportunity_EXPOST"]["peak_day"] for x in available]
    mfe = [x["opportunity_EXPOST"]["mfe_gross_pct"] for x in available]
    giveback = [x["opportunity_EXPOST"]["dplus20_giveback_percentage_points"] for x in available]
    def model_stats(rule: str) -> dict:
        completed = [e["exit_models"][rule] for e in available
                     if e["exit_models"][rule]["status"].startswith("PROVISIONAL_")]
        return {
            **_numbers([m["net_total20bps_pct"] for m in completed]),
            "unknown_exit_count":len(available)-len(completed),
            "win_count":sum(m["net_total20bps_pct"]>0 for m in completed),
            "median_exit_day":round(statistics.median([m["day"] for m in completed]), 2)
            if completed else None,
            "ambiguous_same_bar":sum(m["same_bar_stop_target_ambiguous"] for m in completed),
            "gap_stop":sum(m["gap_through_stop_open_fill"] for m in completed),
            "exit_reason_counts":dict(Counter(m["reason"] for m in completed)),
        }
    return {
        "input_events":len(events), "complete_dplus20":len(available),
        "unknown_incomplete":len(events)-len(available),
        "peak_day":_numbers(peak_days),
        "mfe_gross_pct":_numbers(mfe),
        "giveback_percentage_points":_numbers(giveback),
        "peak_day_counts":{str(day):peak_days.count(day) for day in range(1, MAX_HORIZON+1)},
        "target_touch_counts_expost":{
            str(p):sum(e["opportunity_EXPOST"]["first_target_touch_day_expost"][str(p)]
                       is not None for e in available)
            for p in (5, 10, 15)},
        "exit_models":{rule:model_stats(rule) for rule in EXIT_RULES},
    }


def study(repo: Path = ROOT) -> dict:
    r5 = r5_module(repo)
    frozen = r5.study(repo)
    prepare = r5._modules(repo)[0]
    prices = {code:prepare(r5.frozen_data(repo, code)) for code in r5.SOURCES}
    events = [scan_event(e, prices[e["code"]]) for e in frozen["events"]]
    unique = primary_events(events)
    assert len(events) == frozen["line_events"] == 506
    assert len(unique) == frozen["unique_stock_dates"] == 368
    return {
        "revision":"R6_DAILY_BAR_MAX_OPPORTUNITY_AND_PRECOMMITTED_EXIT_DIAGNOSTIC",
        "source_r5_revision":frozen["revision"],
        "research_only":True, "d0_selection_future_independent":True,
        "scenario_definition_frozen_before_results":True,
        "test_stock_codes":sorted(r5.SOURCES),
        "source_population":"THREE_HANDPICKED_HISTORICAL_STOCKS_NOT_ALL_MARKET",
        "line_events":len(events), "unique_stock_dates":len(unique),
        "primary_selection":"MEDIUM_ELSE_LONG_ELSE_SHORT_NOT_PERFORMANCE_SELECTED",
        "exit_rules_predeclared":list(EXIT_RULES),
        "execution_assumptions":{
            "entry":"D+1_OPEN", "max_horizon":MAX_HORIZON,
            "total_cost_bps":COST_BPS,
            "daily_bar_sequence":"UNKNOWN_1DAY_HIGH_LOW_ORDER",
            "simultaneous_target_and_stop":"PESSIMISTIC_STOP_FIRST_FLAGGED",
            "trailing_high":"ONLY_PRIOR_COMPLETED_BARS",
            "stop_gap":"OPEN_IF_GAPPED_BELOW_STOP",
            "limit_gap":"CONSERVATIVE_TARGET_PRICE_IF_GAPPED_ABOVE",
            "line_reloss":"DECIDE_AFTER_D+h_CLOSE_EXIT_D+h+1_OPEN",
        },
        "all_lines_summary_OVERLAPS":summarize(events),
        "unique_stock_dates_primary_summary":summarize(unique),
        "by_scale_overlapping":{scale:summarize([e for e in events if e["scale"] == scale])
                                for scale in ("SHORT", "MEDIUM", "LONG")},
        "events":events,
        "out_of_sample":False, "market_regime_pit":"UNKNOWN",
        "independent_price_source_certified":False, "survivorship_bias_resolved":False,
        "daily_ohlc_intrabar_order_known":False,
        "strategy_winner":None, "can_publish_strategy_performance":False,
        "can_modify_live_search_watch":False, "env_lower_touch_events_merged":False,
    }


def run(repo: Path, out: Path) -> dict:
    result = study(repo)
    out.mkdir(parents=True, exist_ok=True)
    (out/"R6_EXIT_TIMING_FULL_AUDIT.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)+"\n", encoding="utf-8")
    summary = {k:v for k,v in result.items() if k != "events"}
    (out/"R6_EXIT_TIMING_SUMMARY.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False)+"\n", encoding="utf-8")
    rows = []
    for e in primary_events(result["events"]):
        for rule in EXIT_RULES:
            v = e["exit_models"][rule]
            rows.append({
                "code":e["code"], "signal_date":e["date"], "scale":e["scale"],
                "line_id":e["line_id"], "entry_date":e.get("entry_date"),
                "entry_open":e.get("entry_dplus1_open"), "rule":rule,
                "status":v["status"], "exit_day":v.get("day"),
                "exit_date":v.get("date"), "exit_reason":v.get("reason"),
                "net20bps_pct_EXPOST":v.get("net_total20bps_pct"),
                "same_bar_ambiguous":v.get("same_bar_stop_target_ambiguous"),
                "gap_stop":v.get("gap_through_stop_open_fill"),
                "peak_day_EXPOST":e["opportunity_EXPOST"].get("peak_day"),
                "mfe_pct_EXPOST":e["opportunity_EXPOST"].get("mfe_gross_pct"),
                "dplus20_giveback_pp_EXPOST":e["opportunity_EXPOST"].get("dplus20_giveback_percentage_points"),
            })
    pd.DataFrame(rows).to_csv(out/"R6_PRIMARY_STOCK_DATE_EXIT_RULES.csv",
                              encoding="utf-8-sig", index=False)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    result = run(Path(args.repo).resolve(), Path(args.out))
    print(json.dumps({k:v for k,v in result.items() if k not in ("events", "all_lines_summary_OVERLAPS", "by_scale_overlapping")},
                     ensure_ascii=False, indent=2, allow_nan=False))
