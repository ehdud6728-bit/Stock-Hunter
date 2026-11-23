#!/usr/bin/env python3
"""R26 true55 +30% opportunity preservation versus realized left-tail exits.

RESEARCH_ONLY. Preserve original R19 149 D+1 entries, R25 +30 opportunity
labels and R25 D0 model selection. All exit guards use close i, fill at open
i+1. Historical +30 highs/MAE are labels and diagnostics ONLY, never signals.
Fixed rule definitions, no parameter tuning or LIVE/SEARCH/WATCH changes.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"trendline_true55_r25_big30_20261123"))
sys.path.insert(0,str(ROOT/"trendline_true55_r21_phase_exit_20261123"))
import r25_big30 as r25
import r21_phase_exit as r21

COST_PCT=r25.r20.COST_PCT
HORIZON=r25.r20.HORIZON
BASE=r25.FLOOR_30
MA5=r25.MA5
LEVEL2=r25.LEVEL_TWO
MA5_TARGET30="MA5_CLOSE_BREAK_OR_CLOSE30_NEXT_OPEN"
MA10_TARGET30="MA10_CLOSE_BREAK_OR_CLOSE30_NEXT_OPEN"
MA20_TARGET30="MA20_CLOSE_BREAK_OR_CLOSE30_NEXT_OPEN"
LEVEL2_TARGET30="LEVEL55_2C_BREAK_OR_CLOSE30_NEXT_OPEN"
ATR2_TARGET30="D0_FLOOR_TRAIL2_ATR14_CLOSE_OR_CLOSE30_NEXT_OPEN"
POLICIES=(BASE,MA5_TARGET30,MA10_TARGET30,MA20_TARGET30,
          LEVEL2_TARGET30,ATR2_TARGET30)
REFERENCE=(MA5,LEVEL2)
CUTS=(10.,15.,20.)
TRAIL_ATR_MULTIPLIER=2.0


def net(exit_price:float,entry:float)->float:
    return round(100*(exit_price/entry-1)-COST_PCT,6)


def simulate(g:pd.DataFrame,t:int,method:str)->dict:
    """Known-by-close guards, next-session open fills, never intraday high."""
    if method not in POLICIES or t<55 or t+HORIZON>=len(g):
        raise ValueError("R26_INVALID_POLICY_OR_HORIZON")
    if method==BASE:
        return r25.simulate(g,t,BASE)
    entry=float(g.open.iloc[t+1])
    d0low=float(g.low.iloc[t])
    frozen55=float(g.high.iloc[t-55:t].max())
    if not all(np.isfinite(v) and v>0 for v in (entry,d0low,frozen55)):
        raise ValueError("R26_INVALID_ASOF_ENTRY")
    if entry<=frozen55:
        raise ValueError("R26_R19_DPLUS1_ENTRY_NOT_ABOVE_FROZEN55")
    atr=r21.atr14_at_signal(g,t) if method==ATR2_TARGET30 else None
    max_close=entry
    prev_lost=False
    for i in range(t+1,t+HORIZON):
        c=float(g.close.iloc[i])
        if not np.isfinite(c) or c<=0:
            raise ValueError("R26_INVALID_OHLCV_CLOSE")
        reason=None
        if method==MA5_TARGET30:
            n=5
            if c<float(g.close.iloc[i-n+1:i+1].mean()):
                reason="MA5_CLOSE_BREAK"
        elif method==MA10_TARGET30:
            n=10
            if c<float(g.close.iloc[i-n+1:i+1].mean()):
                reason="MA10_CLOSE_BREAK"
        elif method==MA20_TARGET30:
            n=20
            if c<float(g.close.iloc[i-n+1:i+1].mean()):
                reason="MA20_CLOSE_BREAK"
        elif method==LEVEL2_TARGET30:
            lost=c<=frozen55
            if lost and prev_lost:
                reason="FROZEN55_TWO_CLOSES_BREAK"
            prev_lost=lost
        else:
            max_close=max(max_close,c)
            trail=max(d0low,entry-TRAIL_ATR_MULTIPLIER*atr,
                      max_close-TRAIL_ATR_MULTIPLIER*atr)
            if c<trail:
                reason="D0_FLOOR_OR_2ATR_CLOSE_TRAIL"
        if reason is None and c>=entry*1.30:
            reason="CLOSE30_TARGET"
        if reason is not None:
            open_next=float(g.open.iloc[i+1])
            if not np.isfinite(open_next) or open_next<=0:
                raise ValueError("R26_INVALID_NEXT_OPEN")
            return {"net_pct":net(open_next,entry),"exit_day":i+1-t,
                    "exit_reason":reason,"trigger_day":i-t,
                    "trigger_close":round(c,6),
                    "fill_next_open":round(open_next,6),
                    "fill_gap_from_trigger_close_pct":round(
                        100*(open_next/c-1),6)}
    # D+60 close is a known end-of-window mark; not a next-open order.
    close_end=float(g.close.iloc[t+HORIZON])
    return {"net_pct":net(close_end,entry),"exit_day":HORIZON,
            "exit_reason":"WINDOW_END_CLOSE","trigger_day":HORIZON,
            "trigger_close":round(close_end,6),
            "fill_next_open":None,"fill_gap_from_trigger_close_pct":None}


def normalize_reference(o:dict)->dict:
    """Reproduce the original R25 return/day without rewriting its policy."""
    return {"net_pct":float(o["net_pct"]),"exit_day":int(o["exit_day"]),
            "exit_reason":str(o["exit_reason"])}


def before_first30(row:dict,method:str)->bool:
    """Exit at day k OPEN happens BEFORE that day's intraday +30% HIGH."""
    day=row["labels_EXPOST_NOT_FEATURES"]["first_intraday_high30_day"]
    if day is None:
        return False
    out=row["outcomes"][method]
    if out["exit_reason"]=="WINDOW_END_CLOSE":
        return False  # D+60 close remains invested through D+60 intraday high
    return int(out["exit_day"])<=int(day)


def build(prices:pd.DataFrame,original:list[dict],old25:list[dict])->list[dict]:
    ids=lambda a:[(str(r["code"]),str(r["date"])) for r in a]
    if len(original)!=149 or len(old25)!=149 or ids(original)!=ids(old25):
        raise ValueError("R26_FROZEN_R19_R25_149_EVENT_DRIFT")
    frames={str(c):f.sort_values("date").reset_index(drop=True)
            for c,f in prices.groupby("code")}
    idxs={c:{d:i for i,d in enumerate(g.date)} for c,g in frames.items()}
    result=[]
    for original_row,prior in zip(original,old25):
        code,date=str(prior["code"]),str(prior["date"])
        if code not in idxs or date not in idxs[code]:
            raise ValueError("R26_FROZEN_EVENT_NOT_IN_KIS_ARCHIVE")
        g,t=frames[code],idxs[code][date]
        out={m:simulate(g,t,m) for m in POLICIES}
        for method in (BASE,MA5,LEVEL2):
            old=prior["outcomes"][method]
            newer=out[method] if method==BASE else normalize_reference(
                r25.simulate(g,t,method))
            if any(float(newer[k])!=float(old[k]) for k in ("net_pct","exit_day")):
                raise ValueError("R26_R25_CAUSAL_BASELINE_REPLAY_DRIFT")
            if newer["exit_reason"]!=old["exit_reason"]:
                raise ValueError("R26_R25_BASELINE_REASON_DRIFT")
            out[method]=newer
        if set(out)!=set(POLICIES)|set(REFERENCE):
            raise ValueError("R26_POLICY_SCHEMA_DRIFT")
        label=prior["labels_EXPOST_NOT_FEATURES"]
        raw_label=original_row["expost_not_tradable"]
        if (abs(float(label["full60_mfe_pct"])-
                float(raw_label["max_favorable_60_pct"]))>1e-5 or
                bool(label["big30_full60"])!=(float(raw_label["max_favorable_60_pct"])>=30)):
            raise ValueError("R26_R19_R25_OPPORTUNITY_LABEL_MISMATCH")
        entry=float(g.open.iloc[t+1])
        # Ex-post MAE to exit, including day of exit only for WINDOW_END_CLOSE.
        # On next-open exits, session D+exit_day intraday low occurred AFTER exit;
        # exclude it. All lows here are descriptive, not pre-trade knowledge.
        mae={}
        for m,o in out.items():
            d=int(o["exit_day"])
            last=d if o["exit_reason"]=="WINDOW_END_CLOSE" else d-1
            lows=g.low.iloc[t+1:t+last+1]
            mae[m]=round(100*(float(lows.min())/entry-1),6) if len(lows) else None
        result.append({
          "code":code,"date":date,"year":int(date[:4]),
          "outcomes":out,
          "labels_EXPOST_NOT_FEATURES":label,
          "mae_through_held_sessions_EXPOST_ONLY":mae,
          # Frozen as-of-D0 inputs pass through for audit, not R26 fitting.
          "d0_features_original":prior["d0_features_only"],
        })
    return result


def describe(rows:list[dict],method:str)->dict:
    if not rows:
        return {"n":0,"mean_net_pct":None,"median_net_pct":None,
                "p10_net_pct":None,"worst_net_pct":None,
                "loss_le_10_n":0,"loss_le_15_n":0,"loss_le_20_n":0,
                "realized_net30_n":0,"big30_opportunities_n_EXPOST":0}
    vals=sorted(float(r["outcomes"][method]["net_pct"]) for r in rows)
    big=[r for r in rows if r["labels_EXPOST_NOT_FEATURES"]["big30_full60"]]
    gapvals=[r["outcomes"][method].get("fill_gap_from_trigger_close_pct")
             for r in rows]
    gapvals=[x for x in gapvals if x is not None]
    mae=[r["mae_through_held_sessions_EXPOST_ONLY"][method] for r in rows]
    mae=[x for x in mae if x is not None]
    return {
      "n":len(rows),"unique_tickers":len({r["code"] for r in rows}),
      "mean_net_pct":round(statistics.mean(vals),6),
      "median_net_pct":round(statistics.median(vals),6),
      "p10_net_pct":round(float(np.quantile(vals,.1)),6),
      "worst_net_pct":round(min(vals),6),
      "mean_excluding_best_pct":round(statistics.mean(vals[:-1]),6)
                                  if len(vals)>1 else None,
      "win_rate_pct":round(100*sum(v>0 for v in vals)/len(vals),3),
      "realized_net30_n":sum(v>=30 for v in vals),
      "realized_net15_n":sum(v>=15 for v in vals),
      "loss_le_10_n":sum(v<=-10 for v in vals),
      "loss_le_15_n":sum(v<=-15 for v in vals),
      "loss_le_20_n":sum(v<=-20 for v in vals),
      "big30_opportunities_n_EXPOST":len(big),
      "big30_held_through_first_intraday30_n_EXPOST":sum(
          not before_first30(r,method) for r in big),
      "big30_exited_at_or_before_first_intraday30_open_n_EXPOST":sum(
          before_first30(r,method) for r in big),
      "big30_realized_net30_n":sum(
          r["outcomes"][method]["net_pct"]>=30 for r in big),
      "median_MAE_through_held_sessions_EXPOST":round(
          statistics.median(mae),6) if mae else None,
      "next_open_gap_from_trigger_close_p10_pct":round(
          float(np.quantile(gapvals,.1)),6) if gapvals else None,
      "next_open_gap_from_trigger_close_worst_pct":round(
          min(gapvals),6) if gapvals else None,
    }


def paired(rows:list[dict],method:str,baseline:str=BASE)->dict:
    b=[r["outcomes"][baseline]["net_pct"] for r in rows]
    t=[r["outcomes"][method]["net_pct"] for r in rows]
    delta=[x-y for x,y in zip(t,b)]
    a30=[r for r in rows if r["labels_EXPOST_NOT_FEATURES"]["big30_full60"]]
    return {
      "n":len(rows),
      "mean_paired_delta_net_pct_points":round(statistics.mean(delta),6)
                                         if delta else None,
      "median_paired_delta_net_pct_points":round(statistics.median(delta),6)
                                           if delta else None,
      "improved_n":sum(v>1e-9 for v in delta),
      "worsened_n":sum(v< -1e-9 for v in delta),
      "unchanged_n":sum(abs(v)<=1e-9 for v in delta),
      "baseline_loss_le15_but_new_above_minus15_n":sum(
          old<=-15 and new>-15 for old,new in zip(b,t)),
      "baseline_above_minus15_but_new_loss_le15_n":sum(
          old>-15 and new<=-15 for old,new in zip(b,t)),
      "big30_opportunities_preserved_through_first_high_n_EXPOST":sum(
          not before_first30(r,method) for r in a30),
      "big30_realized_30_preserved_n":sum(
          r["outcomes"][baseline]["net_pct"]>=30 and
          r["outcomes"][method]["net_pct"]>=30 for r in a30),
      "big30_realized_30_lost_n":sum(
          r["outcomes"][baseline]["net_pct"]>=30 and
          r["outcomes"][method]["net_pct"]<30 for r in a30),
      "big30_realized_30_new_n":sum(
          r["outcomes"][baseline]["net_pct"]<30 and
          r["outcomes"][method]["net_pct"]>=30 for r in a30),
    }


def survivor_landmark(rows:list[dict],old25:list[dict],day:int,
                      policy:str=BASE)->dict:
    """Descriptive-only: exclude those exited or already signalled to exit."""
    if day not in (1,3,5):
        raise ValueError("R26_BAD_LANDMARK")
    by_id={(str(r["code"]),str(r["date"])):r for r in old25}
    survivors=[r for r in rows if (
        r["outcomes"][policy]["exit_reason"]=="WINDOW_END_CLOSE" or
        int(r["outcomes"][policy]["exit_day"])>day+1)]
    res={}
    for field in ("asof_close_above_entry","asof_close_above_frozen55",
                  "asof_close_above_ma5"):
        res[field]={}
        for state in (True,False):
            cohort=[r for r in survivors if
               by_id[(r["code"],r["date"])]["landmarks_DIAGNOSTIC_ONLY"]
                    ["D%d"%day][field] is state]
            res[field]["YES" if state else "NO"]={
              "n":len(cohort),
              "after_d%d_actual_R25_base_loss_le15_n_EXPOST"%day:sum(
                   r["outcomes"][policy]["net_pct"]<=-15 for r in cohort),
              "future_big30_full60_n_EXPOST":sum(
                   r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in cohort),
            }
    return {"day":"D+%d_CLOSE"%day,"policy":policy,
            "still_in_position_and_no_pending_next_open_exit_n":len(survivors),
            "conditional_survivorship_bias_warning":True,
            "observations_NOT_new_trading_conditions":res}


def summarize(rows:list[dict],old25:list[dict],scores25:list[dict],
              summary25:dict)->dict:
    if len(rows)!=149 or len(old25)!=149 or len(scores25)!=59:
        raise ValueError("R26_WRONG_COHORT_COUNTS")
    scores={(str(x["code"]),str(x["date"])):x for x in scores25}
    if len(scores)!=59:
        raise ValueError("R26_SCORE_DUPLICATE_IDENTITIES")
    review=[r for r in rows if r["date"]>=r25.REVIEW_START]
    if len(review)!=59 or {(r["code"],r["date"]) for r in review}!=set(scores):
        raise ValueError("R26_REVIEW_59_MISMATCH")
    selected=[r for r in review if
              scores[(r["code"],r["date"])]["d0_selected_TRAINONLY"]]
    if len(selected)!=44:
        raise ValueError("R26_R25_SELECTED_44_COHORT_DRIFT")
    for r in review:
        k=(r["code"],r["date"])
        if (r["labels_EXPOST_NOT_FEATURES"]["big30_full60"]!=
           scores[k]["labels_EXPOST_NOT_FEATURES"]["big30_full60"]):
            raise ValueError("R26_R25_BIG30_REVIEW_DRIFT")
    if (sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in rows)!=40
        or sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in review)!=20
        or sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in selected)!=17
        or summary25.get("frozen_r19_events")!=149):
        raise ValueError("R26_R25_FROZEN_BIG30_COUNTS_DRIFT")
    cohorts={"ALL_149":rows,"REVIEW_2024_25_59":review,
             "R25_SELECTED_44":selected,
             "REVIEW_2024":[r for r in review if r["year"]==2024],
             "REVIEW_2025":[r for r in review if r["year"]==2025]}
    report={
       "revision":"R26_GENUINE55_BIG30_OPPORTUNITY_AND_LEFT_TAIL_GUARDS",
       "research_only":True,"source_kis_run_id":r25.r20.KIS_RUN,
       "n_fixed_R19":len(rows),"fixed_R25_opportunities_149":40,
       "previously_inspected_review_n":59,
       "r25_prior_D0_selected_n":44,"r25_prior_D0_selected_big30_n":17,
       "methods":list(POLICIES),"reference_only_methods":list(REFERENCE),
       "predeclared_loss_cutoffs_pct":list(CUTS),
       "frozenATR14_D0_multiple":TRAIL_ATR_MULTIPLIER,
       "takeprofit_close_threshold_pct":30.,
       "entry":"FROZEN_R19_DPLUS1_OPEN_ABOVE_PRIOR55",
       "decision_and_fill":"DPLUS_I_CLOSE_TRIGGER_THEN_DPLUS_I_PLUS1_OPEN",
       "opportunity_30_is_post_entry_60bar_intraday_HIGH_EXPOST_NOT_EXECUTABLE":True,
       "same_day_exit_at_OPEN_is_before_that_day_intraday_HIGH":True,
       "cohort_reports":{cohort:{
           "n":len(subset),
           "big30_opportunities_EXPOST_n":sum(
             r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in subset),
           "by_method":{m:describe(subset,m) for m in POLICIES+REFERENCE},
           "paired_against_R25_D0LOW_CLOSE30":{
             m:paired(subset,m) for m in POLICIES+REFERENCE if m!=BASE},
           } for cohort,subset in cohorts.items()},
       "landmarks_D1_D3_D5_R25_BASE_SURVIVORS_ONLY_DIAGNOSTIC":{
           "D%d"%d:survivor_landmark(review,old25,d) for d in (1,3,5)},
       "precommitted_method_grid_no_review_tuning":True,
       "R25_D0_selection_unchanged":True,
       "2024_25_not_pristine_OOS":True,
       "26_ticker_convenience_sample_not_full_market":True,
       "survivor_bias_landmark_explicit":True,
       "gap_losses_can_exceed_close_trigger":True,
       "corporate_action_price_adjustment_not_independently_certified":True,
       "not_an_approved_trading_strategy":True,
       "live_search_watch_changed":False,"orders_placed":0,
       "can_publish_trade_edge":False,
    }
    return report


def run(r16_out:Path,r19_out:Path,r25_out:Path,out:Path)->dict:
    prices,original=r25.r20.read_sources(r16_out,r19_out)
    old25=json.loads((r25_out/"R25_149_PAIRED_EXIT_AND_LANDMARK_EVENTS.json")
                     .read_text(encoding="utf-8"))
    scores25=json.loads((r25_out/"R25_2024_2025_D0_REVIEW_SCORES.json")
                        .read_text(encoding="utf-8"))
    summary25=json.loads((r25_out/"R25_BIG30_RESEARCH_SUMMARY.json")
                         .read_text(encoding="utf-8"))
    if (summary25.get("source_kis_run_id")!=r25.r20.KIS_RUN or
        summary25.get("all149_big30_n")!=40 or
        summary25.get("d0_selection_model",{}).get("training_cutoff_selection",{})
        .get("captured_opportunities_EXPOST_n")!=17):
        raise ValueError("R26_R25_SUMMARY_SOURCE_PIN_MISMATCH")
    rows=build(prices,original,old25)
    report=summarize(rows,old25,scores25,summary25)
    out.mkdir(parents=True,exist_ok=True)
    (out/"R26_LEFT_TAIL_GUARD_SUMMARY.json").write_text(
        json.dumps(report,indent=2,ensure_ascii=False,allow_nan=False)+"\n",encoding="utf-8")
    (out/"R26_FIXED149_PAIRED_GUARD_EVENTS.json").write_text(
        json.dumps(rows,indent=2,ensure_ascii=False,allow_nan=False)+"\n",encoding="utf-8")
    return report


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    for name in ("r16-out","r19-out","r25-out","out"):
        p.add_argument("--"+name,type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(run(a.r16_out,a.r19_out,a.r25_out,a.out),
                     indent=2,ensure_ascii=False,allow_nan=False))
