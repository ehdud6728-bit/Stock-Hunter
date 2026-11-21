#!/usr/bin/env python3
"""R15: no-lookahead R14 exits scored against EX-POST available upside/downside.

Do not infer the peak is tradable. D+20/D+60 are observation horizons, NOT
predefined percentage take-profit points. Retrospective selected five tickers,
no independent OOS, no LIVE/SEARCH/WATCH modifications.
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
R14_REL = "research_projects/trendline_structure_exits_r14_20261121/r14_structure_exits.py"
R14_SHA256 = "2aae19c3d8a2132fa590a36f1df8ed7cddace4df894595b5158477caa517ef54"
COHORTS = ("FROZEN_R5_THREE", "ADDITIONAL_EXISTING_TWO")
HORIZONS = (20, 60)
NONOVERLAP_BARS = 60  # match the *longest* evaluated future window


def r14_module(repo: Path = ROOT):
    src = repo / R14_REL
    if hashlib.sha256(src.read_bytes()).hexdigest() != R14_SHA256:
        raise ValueError("FROZEN_R14_SOURCE_DRIFT")
    spec = importlib.util.spec_from_file_location("frozen_r14_structure_exits", src)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def label_incomplete_horizon(result: dict, horizon: int) -> dict:
    """R14's legacy unknown string says D20 even when called with D60.

    Keep the original status for provenance; do not alter any model decision.
    """
    if horizon != 20 and result.get("status")=="UNKNOWN_INCOMPLETE_DPLUS20_PATH":
        return {**result,"r14_original_status":result["status"],
                "status":f"UNKNOWN_INCOMPLETE_DPLUS{horizon}_PATH"}
    return result


def ex_post_capture(prices, t: int, exit_result: dict, horizon: int, *, cost_bps=20) -> dict:
    """Score an already-caused exit. Future OHLC must never feed the exit engine.

    - Same-day post-exit extrema are excluded, because their ordering is unknown.
    - Both later upside missed and later downside avoided can be true.
    - Peak-to-exit giveback is a hindsight *opportunity* metric, not an order.
    """
    if not exit_result["status"].startswith("PROVISIONAL_"):
        return {"status": exit_result["status"]}
    h = int(exit_result["exit_day"])
    if not 1 <= h <= horizon or len(prices.iloc[t+1:t+horizon+1]) != horizon:
        raise ValueError("CAPTURE_PATH_NOT_COMPLETE")
    entry = float(prices.iloc[t+1].open)
    if not math.isfinite(entry) or entry <= 0:
        raise ValueError("CAPTURE_INVALID_ENTRY")
    path = prices.iloc[t+1:t+horizon+1]
    peak = max(float(v) for v in path.high)
    floor = min(float(v) for v in path.low)
    peak_idx = next(i+1 for i,v in enumerate(path.high) if float(v)==peak)
    gross = float(exit_result["gross_pct"])
    net = float(exit_result["net_total20bps_pct"])
    mfe = 100*(peak/entry-1)
    mae = 100*(floor/entry-1)
    max_hypothetical_net = mfe-cost_bps/100
    if gross > mfe + 0.00001:
        raise AssertionError("EXIT_BETTER_THAN_PERIOD_HIGH_IMPOSSIBLE")

    # A no-profit excursion is mechanically not captureable after cost;
    # never report divide-by-zero as 0% capture.
    capture = (100*max(0,net)/max_hypothetical_net
               if max_hypothetical_net>0 else None)
    after = prices.iloc[t+h+1:t+horizon+1]  # strictly AFTER exit day
    missed = avoided = None
    if len(after):
        highest_later = 100*(max(float(v) for v in after.high)/entry-1)
        lowest_later = 100*(min(float(v) for v in after.low)/entry-1)
        missed = max(0,highest_later-gross)
        avoided = max(0,gross-lowest_later)
    return {
        "status":"EXPOST_DIAGNOSTIC_NOT_ACTIONABLE_OR_PIT",
        "model_exit_day":h, "exit_net_pct":net, "exit_gross_pct":gross,
        "period_best_intraday_high_day_EXPOST":peak_idx,
        "max_favorable_excursion_pct_EXPOST":round(mfe,6),
        "max_adverse_excursion_pct_EXPOST":round(mae,6),
        "peak_to_realized_giveback_pp_EXPOST":round(max(0,mfe-gross),6),
        "net_capture_of_theoretical_positive_mfe_pct_EXPOST":round(capture,6)
            if capture is not None else None,
        "no_positive_theoretical_net_opportunity":max_hypothetical_net<=0,
        "post_exit_later_upside_missed_pp_EXPOST":round(missed,6)
            if missed is not None else None,
        "post_exit_later_downside_avoided_pp_EXPOST":round(avoided,6)
            if avoided is not None else None,
        "hindsight_peak_is_not_a_tradable_order":True,
        "post_exit_same_day_high_low_excluded":True,
    }


def event_diagnostics(event: dict, prices, r14) -> dict:
    t = int(event["idx"])
    if prices.iloc[t].date.strftime("%Y-%m-%d") != event["date"]:
        raise AssertionError("FROZEN_R5_EVENT_INDEX_DRIFT")
    line, slope = event.get("break_line_level_d0"), event.get("line_slope_per_bar")
    if line is None or slope is None:
        return {"code":event["code"],"date":event["date"],"year":event["year"],
                "status":"UNKNOWN_MISSING_FROZEN_R5_LINE_GEOMETRY"}
    result = {"code":event["code"],"date":event["date"],"year":event["year"],
              "scale":event["scale"],"line_id":event["line_id"],
              "status":"EVALUATED_R14_FROZEN_POLICY", "horizons":{}}
    for horizon in HORIZONS:
        models = {}
        for name in r14.MODELS:
            exit_result = label_incomplete_horizon(
                r14.simulate_structure(prices,t,float(line),float(slope),name,horizon=horizon),
                horizon)
            models[name] = {"exit":exit_result,
                            "diagnostic":ex_post_capture(prices,t,exit_result,horizon)}
        result["horizons"][str(horizon)] = models
    return result


def _mean(items):
    return round(statistics.mean(items),6) if items else None


def _median(items):
    return round(statistics.median(items),6) if items else None


def stats(rows: list[dict], horizon: int, model: str) -> dict:
    outcomes=[r["horizons"][str(horizon)][model] for r in rows
              if r["status"]=="EVALUATED_R14_FROZEN_POLICY"]
    good=[x for x in outcomes if x["exit"]["status"].startswith("PROVISIONAL_")]
    d=[x["diagnostic"] for x in good]
    vals=[x["exit"]["net_total20bps_pct"] for x in good]
    ordered=sorted(vals)
    trim=max(1,int(len(ordered)*.1)) if len(ordered)>=10 else 0
    trimmed=ordered[trim:-trim] if trim else ordered
    captures=[x["net_capture_of_theoretical_positive_mfe_pct_EXPOST"] for x in d
              if x["net_capture_of_theoretical_positive_mfe_pct_EXPOST"] is not None]
    missed=[x["post_exit_later_upside_missed_pp_EXPOST"] for x in d
            if x["post_exit_later_upside_missed_pp_EXPOST"] is not None]
    avoided=[x["post_exit_later_downside_avoided_pp_EXPOST"] for x in d
             if x["post_exit_later_downside_avoided_pp_EXPOST"] is not None]
    return {
        "n_model_checked":len(outcomes),"n_complete":len(good),
        "n_unknown_or_skipped":len(outcomes)-len(good),
        "mean_net_pct":_mean(vals),"median_net_pct":_median(vals),
        "mean_excluding_best_pct":_mean(ordered[:-1]) if len(ordered)>1 else None,
        "symmetric_10pct_trimmed_mean_net_pct":_mean(trimmed),
        "win_rate_pct":round(100*sum(v>0 for v in vals)/len(vals),3) if vals else None,
        "median_exit_day":_median([x["exit"]["exit_day"] for x in good]),
        "n_exited_before_window_end":sum(x["exit"]["exit_day"]<horizon for x in good),
        "n_exited_by_day5":sum(x["exit"]["exit_day"]<=5 for x in good),
        "exit_reasons":dict(Counter(x["exit"]["exit_reason"] for x in good)),
        "median_offered_mfe_gross_pct_EXPOST":_median([x["max_favorable_excursion_pct_EXPOST"] for x in d]),
        "median_offered_mae_gross_pct_EXPOST":_median([x["max_adverse_excursion_pct_EXPOST"] for x in d]),
        "median_peak_day_EXPOST":_median([x["period_best_intraday_high_day_EXPOST"] for x in d]),
        "median_peak_to_exit_giveback_pp_EXPOST":_median([x["peak_to_realized_giveback_pp_EXPOST"] for x in d]),
        "n_offered_positive_net_profit_at_peak_EXPOST":len(captures),
        "mean_captured_possible_net_profit_pct_EXPOST":_mean(captures),
        "median_captured_possible_net_profit_pct_EXPOST":_median(captures),
        "n_scored_post_exit_days":len(missed),
        "mean_later_upside_missed_pp_EXPOST":_mean(missed),
        "mean_later_downside_avoided_pp_EXPOST":_mean(avoided),
        "n_both_missed_upside_and_avoided_downside":sum(a>0 and b>0 for a,b in zip(missed,avoided)),
    }


def cohort_summary(rows: list[dict], r14) -> dict:
    eligible=[r for r in rows if r["status"]=="EVALUATED_R14_FROZEN_POLICY"
              and all(r["horizons"][str(h)][m]["exit"]["status"].startswith("PROVISIONAL_")
                      for h in HORIZONS for m in r14.MODELS)]
    out={
        "n_selected_60bar_nonoverlap":len(rows),
        "n_missing_frozen_line":sum(r["status"].startswith("UNKNOWN_") for r in rows),
        "n_complete_same_events_D20_and_D60":len(eligible),
        "status_counts_on_all":dict(Counter(
            r["status"] if r["status"].startswith("UNKNOWN_") else
            r["horizons"]["60"][r14.MODELS[0]]["exit"]["status"] for r in rows)),
        "on_same_complete_60day_cohort":{
            str(h):{m:stats(eligible,h,m) for m in r14.MODELS} for h in HORIZONS},
        "by_code_60day":{code:{m:stats([r for r in eligible if r["code"]==code],60,m)
                              for m in r14.MODELS}
                          for code in sorted({r["code"] for r in eligible})},
        "by_year_60day":{str(year):{m:stats([r for r in eligible if r["year"]==year],60,m)
                                   for m in r14.MODELS}
                           for year in sorted({r["year"] for r in eligible})},
    }
    # How extending D20 to D60 affected only the cases still held at D20.
    out["D20_window_survivors_extended_to_D60"]={}
    for m in r14.MODELS:
        survivors=[r for r in eligible
                   if r["horizons"]["20"][m]["exit"]["exit_reason"]=="EVALUATION_WINDOW_D20_CLOSE"]
        delta=[r["horizons"]["60"][m]["exit"]["net_total20bps_pct"]-
               r["horizons"]["20"][m]["exit"]["net_total20bps_pct"] for r in survivors]
        out["D20_window_survivors_extended_to_D60"][m]={
            "n_survivors":len(survivors),
            "mean_D60_minus_D20_net_pp":_mean(delta),
            "median_D60_minus_D20_net_pp":_median(delta),
            "n_extension_improved":sum(x>0 for x in delta),
        }
    return out


def study(repo: Path=ROOT) -> dict:
    r14=r14_module(repo)
    r12,_r7=r14.parent_modules(repo)
    r5=r12.r5_module(repo)
    prepare=r5._modules(repo)[0]
    raw={c:r5.frozen_data(repo,c) for c in r5.SOURCES}
    raw.update({c:r12.extra_frozen_prices(repo,c) for c in r12.ADDITIONAL_SOURCES})
    prices={code:prepare(df) for code,df in raw.items()}
    cohorts={
        "FROZEN_R5_THREE":r12.primary_signals([e for c in r5.SOURCES for e in r5.scan_stock(raw[c],repo)]),
        "ADDITIONAL_EXISTING_TWO":r12.primary_signals([
            e for c in r12.ADDITIONAL_SOURCES for e in r5.scan_stock(raw[c],repo)]),
    }
    if len(cohorts["FROZEN_R5_THREE"])!=368:
        raise AssertionError("R5_FROZEN_UNIQUE_SIGNALS_DRIFT")
    summaries={};all_records=[]
    for cohort in COHORTS:
        selected=r12.nonoverlap_signals(cohorts[cohort],bars=NONOVERLAP_BARS)
        rows=[event_diagnostics(e,prices[e["code"]],r14) for e in selected]
        summaries[cohort]={"unique_events":len(cohorts[cohort]),**cohort_summary(rows,r14)}
        all_records.extend([{"cohort":cohort,**x} for x in rows])
    return {
        "revision":"R15_R14_EXIT_CAPTURE_GIVEBACK_VS_AVOIDED_DOWNSIDE_RESEARCH_ONLY",
        "source_r14_sha256":R14_SHA256,
        "frozen_exit_policy":list(r14.MODELS),
        "horizons_Dplus":list(HORIZONS),"same_stock_embargo_bars":NONOVERLAP_BARS,
        "cost_total_roundtrip_bps":r14.COST_BPS,
        "peak_measurement":"EXPOST_INTRADAY_MAX_HIGH_NOT_TRADABLE_TARGET",
        "after_exit_measurement":"STRICTLY_LATER_DAYS_ONLY_TO_AVOID_INTRADAY_ORDER",
        "independent_out_of_sample":False,
        "five_selected_stocks_development_only":True,
        "frozen_parameter_search_performed":False,
        "can_modify_live_search_watch":False,
        "can_publish_trade_edge":False,
        "cohorts":summaries,"records":all_records,
    }


def run(repo: Path, out: Path) -> dict:
    result=study(repo)
    out.mkdir(parents=True,exist_ok=True)
    (out/"R15_EXIT_CAPTURE_FULL_AUDIT.json").write_text(
        json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    summary={k:v for k,v in result.items() if k!="records"}
    (out/"R15_EXIT_CAPTURE_SUMMARY.json").write_text(
        json.dumps(summary,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    return summary


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo",default=".")
    p.add_argument("--out",required=True)
    a=p.parse_args()
    result=run(Path(a.repo).resolve(),Path(a.out).resolve())
    print(json.dumps({"revision":result["revision"],"cohorts":result["cohorts"]},ensure_ascii=False,indent=2))
