#!/usr/bin/env python3
"""R14: price-structure driven exits without preset % take-profit/stop-loss.

Breakout line and confirmed rising swing lows define invalidation/trailing
levels. Full daily OHLC path is used to score results, NEVER to move a stop
before the day has finished. Research-only; not independent OOS.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import statistics
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
R12_REL = "research_projects/trendline_signal_expectancy_r12_20261121/r12_signal_expectancy.py"
R12_SHA256 = "42d10240928169612d5ea9f97bc9f8eb3bbd5d3b14c3230255aae029612adcda"
R7_REL = "research_projects/trendline_multifactor_exit_r7_20261119/r7_multifactor.py"
R7_SHA256 = "d3ed459f5dd0c4c747502a450c38c08d305c3f28e5f828cdeeb0f7af65c88e6a"
COHORTS = ("FROZEN_R5_THREE", "ADDITIONAL_EXISTING_TWO")
HORIZON = 20  # fixed evaluation interval, NOT a % target or stop
COST_BPS = 20
PIVOT_LEFT = 2  # same confirmed local-extremum convention as existing R10
PIVOT_RIGHT = 2
MODELS = ("LINE_AND_CONFIRMED_PIVOT_INTRADAY", "LINE_AND_CONFIRMED_PIVOT_CLOSE_NEXT_OPEN")


def _module(repo: Path, relative: str, sha: str, name: str):
    source = repo / relative
    if hashlib.sha256(source.read_bytes()).hexdigest() != sha:
        raise ValueError("CANONICAL_RESEARCH_SOURCE_DRIFT " + relative)
    spec = importlib.util.spec_from_file_location(name, source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parent_modules(repo: Path = ROOT):
    return (_module(repo, R12_REL, R12_SHA256, "r12_signal_expectancy"),
            _module(repo, R7_REL, R7_SHA256, "r7_multifactor"))


def confirmed_low_before_day(prices, candidate_idx: int, last_known_idx: int):
    """Pivot needs two completed right-side bars before today's session.

    The current trading day's low is never in this calculation.
    """
    j = candidate_idx
    if j - PIVOT_LEFT < 0 or j + PIVOT_RIGHT > last_known_idx:
        return None
    center = float(prices.iloc[j].low)
    neighbors = [float(prices.iloc[i].low)
                 for i in range(j-PIVOT_LEFT, j+PIVOT_RIGHT+1) if i != j]
    return center if all(center < x for x in neighbors) else None


def _exit(prices, t: int, day: int, entry: float, price: float,
          reason: str, stop_before_open: float | None = None, confirmed_raises: int = 0) -> dict:
    gross = 100 * (price / entry - 1)
    return {
        "status": "PROVISIONAL_STRUCTURE_EXIT_DAILY_OHLC_NOT_OOS",
        "exit_day": day, "exit_date": prices.iloc[t+day].date.strftime("%Y-%m-%d"),
        "exit_price": round(price, 6), "exit_reason": reason,
        "gross_pct": round(gross, 6),
        "net_total20bps_pct": round(gross - COST_BPS/100, 6),
        "stop_before_open": round(stop_before_open, 6) if stop_before_open is not None else None,
        "confirmed_pivot_raises_before_exit": confirmed_raises,
        "gap_through_stop": reason == "GAP_BELOW_PREKNOWN_STRUCTURE_STOP_OPEN",
        "intraday_high_low_order_assumed": False,
    }


def simulate_structure(prices, t: int, line_d0: float, line_slope: float,
                       model: str, *, horizon: int = HORIZON) -> dict:
    """Entry after D0 close, exits use an advance-known level only.

    Intraday mode: D+h LOW <= premarket stop, with gap-down OPEN fill.
    Close mode: D+h CLOSE < premarket level, sell at D+h+1 OPEN. All pivot
    confirmation has finished by previous day's CLOSE; no retrospective peak.
    """
    if model not in MODELS:
        raise ValueError("INVALID_STRUCTURAL_EXIT_MODEL")
    if len(prices.iloc[t+1:t+horizon+1]) != horizon:
        return {"status": "UNKNOWN_INCOMPLETE_DPLUS20_PATH"}
    if not all(math.isfinite(v) and v > 0 for v in (line_d0, float(prices.iloc[t+1].open))):
        return {"status": "UNKNOWN_INVALID_ENTRY_OR_LINE"}
    entry = float(prices.iloc[t+1].open)
    initial_line = line_d0 + line_slope
    if not math.isfinite(initial_line) or initial_line <= 0:
        return {"status": "UNKNOWN_INVALID_FROZEN_LINE_PROJECTION"}
    if entry <= initial_line:
        return {"status": "SKIPPED_DPLUS1_OPEN_NOT_ABOVE_ORIGINAL_BREAKOUT_LINE",
                "entry_open_if_forced": round(entry, 6),
                "original_line_at_entry": round(initial_line, 6)}
    stop, raises = initial_line, 0
    for h in range(1, horizon+1):
        i = t+h
        # A line's slope is frozen at D0; at most it can raise today's stop.
        projected = line_d0 + h*line_slope
        if projected <= 0 or not math.isfinite(projected):
            return {"status": "UNKNOWN_INVALID_LINE_PROJECTION"}
        stop = max(stop, projected)
        # Candidate j=i-3 has TWO completed right bars, ending at i-1.
        # Only pivots formed after entry are allowed to ratchet the stop.
        j = i-(PIVOT_RIGHT+1)
        if j >= t+1:
            candidate = confirmed_low_before_day(prices, j, i-1)
            if candidate is not None and candidate > stop:
                stop, raises = candidate, raises+1
        row = prices.iloc[i]
        op, hi, lo, close = (float(row[k]) for k in ("open", "high", "low", "close"))
        if min(op, hi, lo, close) <= 0 or lo > min(op, close, hi) or hi < max(op, close, lo):
            return {"status": "UNKNOWN_INVALID_FUTURE_OHLC"}
        if model == "LINE_AND_CONFIRMED_PIVOT_INTRADAY":
            if op <= stop:
                return _exit(prices,t,h,entry,op,"GAP_BELOW_PREKNOWN_STRUCTURE_STOP_OPEN",stop,raises)
            if lo <= stop:
                return _exit(prices,t,h,entry,stop,"TOUCHED_PREKNOWN_STRUCTURE_STOP",stop,raises)
        else:
            # Price is judged only after the completed session's close.
            if close < stop and h < horizon:
                return _exit(prices,t,h+1,entry,float(prices.iloc[i+1].open),
                             "PRIOR_CLOSE_BELOW_STRUCTURE_NEXT_OPEN",stop,raises)
    return _exit(prices,t,horizon,entry,float(prices.iloc[t+horizon].close),
                 "EVALUATION_WINDOW_D20_CLOSE",stop,raises)


def event_result(event: dict, prices, atr, r7) -> dict:
    t = int(event["idx"])
    if prices.iloc[t].date.strftime("%Y-%m-%d") != event["date"]:
        raise AssertionError("FROZEN_EVENT_DATE_INDEX_DRIFT")
    d0_level = event.get("break_line_level_d0")
    slope = event.get("line_slope_per_bar")
    if d0_level is None or slope is None:
        return {"status": "UNKNOWN_MISSING_FROZEN_R5_LINE_GEOMETRY",
                "models": {name: {"status": "UNKNOWN_MISSING_FROZEN_R5_LINE_GEOMETRY"} for name in MODELS}}
    d0_level, slope = float(d0_level), float(slope)
    if t+1 >= len(prices):
        return {"status": "UNKNOWN_INCOMPLETE_NEXT_OPEN",
                "models": {name: {"status": "UNKNOWN_INCOMPLETE_NEXT_OPEN"} for name in MODELS}}
    entry = float(prices.iloc[t+1].open)
    line_at_entry = d0_level+slope
    if not (math.isfinite(entry) and entry > 0):
        raise AssertionError("INVALID_EVENT_ENTRY")
    models = {name: simulate_structure(prices,t,d0_level,slope,name) for name in MODELS}
    atr_d0 = float(atr.iloc[t]) if math.isfinite(float(atr.iloc[t])) else None
    r7_models = {
        name: r7.simulate(prices,t,atr,rule)
        for name,rule in (("R7_ATR_CHANDELIER_3", "ATR_CHANDELIER_3"),
                          ("D20_HOLD_BASELINE", "R6_TIME_D20_BASELINE"))
    }
    return {
        "status": "STRUCTURE_GEOMETRY_KNOWN_AFTER_D0_CLOSE",
        "code": event["code"], "date": event["date"], "year": event["year"],
        "idx": t, "scale": event["scale"], "line_id": event["line_id"],
        "entry_dplus1_open": entry,
        "d0_frozen_breakout_line": d0_level, "d0_frozen_line_slope_per_bar": slope,
        "projected_line_at_next_open": round(line_at_entry,6),
        "initial_structural_risk_pct": round(100*(entry-line_at_entry)/entry,6),
        "atr14_d0": round(atr_d0,6) if atr_d0 is not None else None,
        "initial_structural_risk_atr_units": round((entry-line_at_entry)/atr_d0,6)
        if atr_d0 is not None and atr_d0>0 else None,
        "pre20_swing_low_asof_d0": event.get("prior20_low"),
        "d0_volume_ratio": event.get("d0_volume_vs_pre20"),
        "d0_close_location": event.get("d0_close_location"),
        "models": models, "benchmarks": r7_models,
        "expost_mfe_gross_pct": event["future_paths_EXPOST"]["20"].get("mfe_from_entry_pct"),
    }


def _stats(models: list[dict]) -> dict:
    good=[x for x in models if x["status"].startswith("PROVISIONAL_")]
    vals=[x["net_total20bps_pct"] for x in good]
    ordered=sorted(vals)
    trim=max(1,int(len(ordered)*.1)) if len(ordered)>=10 else 0
    trimmed=ordered[trim:-trim] if trim else ordered
    return {
        "n_selected": len(models), "n_complete": len(good),
        "n_unknown_or_skipped": len(models)-len(good),
        "mean_net_pct": round(statistics.mean(vals),6) if vals else None,
        "median_net_pct": round(statistics.median(vals),6) if vals else None,
        "p10_net_pct": ordered[max(0,int((len(ordered)-1)*.1))] if vals else None,
        "best_net_pct": max(vals) if vals else None,
        "worst_net_pct": min(vals) if vals else None,
        "mean_excluding_single_best_pct": round(statistics.mean(ordered[:-1]),6)
            if len(ordered)>1 else None,
        "symmetric_10pct_trimmed_mean_pct": round(statistics.mean(trimmed),6)
            if trimmed else None,
        "win_rate_pct": round(100*sum(v>0 for v in vals)/len(vals),3) if vals else None,
        "median_exit_day": round(statistics.median(x["exit_day"] for x in good),2) if good else None,
        "exit_reason_counts": dict(Counter(x["exit_reason"] if "exit_reason" in x else x["reason"] for x in good)),
        "confirmed_pivot_raises": sum(x.get("confirmed_pivot_raises_before_exit",0) for x in good),
        "gap_stop_count": sum(x.get("gap_through_stop",False) for x in good),
    }


def summarize(records: list[dict]) -> dict:
    known=[r for r in records if r["status"].startswith("STRUCTURE_")]
    models={name:[r["models"][name] for r in known] for name in MODELS}
    # Compare with existing models on the EXACT structural-eligible events.
    eligible=[r for r in known if all(r["models"][name]["status"].startswith("PROVISIONAL_")
                                     for name in MODELS)]
    result={
        "n_input": len(records), "n_line_known": len(known),
        "n_missing_line": len(records)-len(known),
        "n_structural_gap_skipped": sum(any(r["models"][n]["status"].startswith("SKIPPED_")
                                         for n in MODELS) for r in known),
        "models_on_all_signals": {name:_stats(m) for name,m in models.items()},
        "same_structurally_eligible_cohort_n": len(eligible),
        "same_cohort_models": {name:_stats([r["models"][name] for r in eligible]) for name in MODELS},
        "same_cohort_R7_and_hold": {name:_stats([r["benchmarks"][name] for r in eligible])
                                     for name in ("R7_ATR_CHANDELIER_3", "D20_HOLD_BASELINE")},
        "by_code_on_exact_eligible":{
            code:{name:_stats([r["models"][name] for r in eligible if r["code"]==code])
                  for name in MODELS}
            for code in sorted({r["code"] for r in eligible})},
    }
    if eligible:
        for name in MODELS:
            modelvals=[r["models"][name]["net_total20bps_pct"] for r in eligible]
            baselinevals=[r["benchmarks"]["R7_ATR_CHANDELIER_3"]["net_total20bps_pct"]
                          for r in eligible]
            result.setdefault("paired_delta_vs_R7_ATR_chandelier_pp", {})[name]=round(
                statistics.mean(a-b for a,b in zip(modelvals,baselinevals)),6)
    # The skipped group is never quietly discarded from population counts.
    return result


def study(repo: Path = ROOT) -> dict:
    r12,r7=parent_modules(repo)
    r5=r12.r5_module(repo)
    prepare=r5._modules(repo)[0]
    raw={c:r5.frozen_data(repo,c) for c in r5.SOURCES}
    raw.update({c:r12.extra_frozen_prices(repo,c) for c in r12.ADDITIONAL_SOURCES})
    price={c:prepare(x) for c,x in raw.items()}
    atr={c:r7.wilder_atr(p) for c,p in price.items()}
    grouped={
        "FROZEN_R5_THREE":r12.primary_signals([e for c in r5.SOURCES for e in r5.scan_stock(raw[c],repo)]),
        "ADDITIONAL_EXISTING_TWO":r12.primary_signals([
            e for c in r12.ADDITIONAL_SOURCES for e in r5.scan_stock(raw[c],repo)]),
    }
    if len(grouped["FROZEN_R5_THREE"])!=368:
        raise AssertionError("FROZEN_R5_UNIQUE_COUNT_CHANGED")
    per_cohort={}
    rows=[]
    for cohort in COHORTS:
        selected=r12.nonoverlap_signals(grouped[cohort])
        scanned=[event_result(e,price[e["code"]],atr[e["code"]],r7) for e in selected]
        if cohort=="FROZEN_R5_THREE" and len(selected)!=94:
            raise AssertionError("FROZEN_R7_NONOVERLAP_COUNT_CHANGED")
        per_cohort[cohort]={
            "unique_stock_dates":len(grouped[cohort]),
            "nonoverlap_d20_signals":len(selected),
            "nonoverlap_results":summarize(scanned),
            "by_year":{str(y):summarize([e for e in scanned if e.get("year")==y])
                       for y in sorted({e["year"] for e in scanned if e.get("year") is not None})},
        }
        rows.extend([{"cohort":cohort,**e} for e in scanned])
    return {
        "revision":"R14_R116_BREAKOUT_LINE_CONFIRMED_HIGHER_LOW_TRAIL_RESEARCH_ONLY",
        "research_only":True, "mode":"NO_FIXED_PERCENT_TARGET_OR_STOP",
        "r12_sha256":R12_SHA256, "r7_sha256":R7_SHA256,
        "entry":"D0_SIGNAL_AFTER_CLOSE_THEN_DPLUS1_OPEN_IF_ABOVE_PROJECTED_FROZEN_LINE",
        "initial_invalidation":"PROJECTED_ORIGINAL_R116_BROKEN_RESISTANCE_LINE",
        "trail":"MAX_PRIOR_STOP_PROJECTED_ORIGINAL_LINE_CONFIRMED_LOCAL_LOW_2LEFT_2RIGHT",
        "pivot_confirmation":"2_RIGHT_BARS_MUST_CLOSE_BEFORE_NEXT_SESSION",
        "exit_models":list(MODELS),
        "evaluation_horizon_days":HORIZON, "total_roundtrip_cost_bps":COST_BPS,
        "comparison":"EXACT_ELIGIBLE_EVENT_R7_ATR_CHANDELIER_3_AND_D20_HOLD",
        "cohorts":per_cohort,"records":rows,
        "r7_r10_method_overlap_avoided":"NO_NEW_FIXED_ATR3_OR_PIVOT120_TARGET_RULE_PROPOSED",
        "independent_out_of_sample":False,
        "selection_not_based_on_future_path":True,
        "intraday_data_available":False,
        "historical_corporate_action_provenance_independently_certified":False,
        "strategy_winner":None,"can_publish_trade_edge":False,
        "can_modify_live_search_watch":False,
    }


def run(repo: Path, out: Path) -> dict:
    result=study(repo)
    out.mkdir(parents=True,exist_ok=True)
    (out/"R14_STRUCTURE_EXIT_FULL_AUDIT.json").write_text(
        json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    summary={k:v for k,v in result.items() if k!="records"}
    (out/"R14_STRUCTURE_EXIT_SUMMARY.json").write_text(
        json.dumps(summary,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    return summary


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo",default=".")
    p.add_argument("--out",required=True)
    args=p.parse_args()
    result=run(Path(args.repo).resolve(),Path(args.out).resolve())
    print(json.dumps({"revision":result["revision"],"cohorts":{
        c:result["cohorts"][c]["nonoverlap_results"] for c in COHORTS}},
        ensure_ascii=False,indent=2))
