#!/usr/bin/env python3
"""R27: early 3/5-close true55 failure guards, then wider hold for +30%.

RESEARCH_ONLY. Every exit signal is known at a completed day's close,
executed only at the next open. Existing R19/R25/R26 cohorts, policies,
labels, and D0 selection are replayed and checked; no live path changes.
D+1/3/5 forward metrics use STRICTLY POST-landmark OHLCV and are
descriptive only; review 2024-25 is previously inspected, not pristine OOS.
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
sys.path.insert(0,str(ROOT/"trendline_true55_r26_tail_guard_20261123"))
import r26_tail_guard as r26

r25=r26.r25
BASE=r26.BASE
HORIZON=r26.HORIZON
MA5_FIRST3="EARLY_MA5_D1_D3_THEN_D0FLOOR_CLOSE30"
MA10_FIRST5="EARLY_MA10_D1_D5_THEN_D0FLOOR_CLOSE30"
ATR2_FIRST5="EARLY_ATR2_D1_D5_THEN_D0FLOOR_CLOSE30"
CONFIRM_D3="D3_CLOSE_ENTRY_AND_LEVEL_CONFIRM_ELSE_EXIT"
CONFIRM_D5="D5_CLOSE_ENTRY_AND_LEVEL_CONFIRM_ELSE_EXIT"
NEW_METHODS=(MA5_FIRST3,MA10_FIRST5,ATR2_FIRST5,CONFIRM_D3,CONFIRM_D5)
COMPARE=(BASE,r26.MA5_TARGET30,r26.MA20_TARGET30,r26.ATR2_TARGET30)
ALL_METHODS=COMPARE+NEW_METHODS
DAYS=(1,3,5)
LOSS_CUTS=(10.,15.,20.)


def phase_day_limit(method:str)->int:
    if method in (MA5_FIRST3,CONFIRM_D3):
        return 3
    if method in (MA10_FIRST5,ATR2_FIRST5,CONFIRM_D5):
        return 5
    raise ValueError("R27_UNKNOWN_PHASE_POLICY")


def simulate(g:pd.DataFrame,t:int,method:str)->dict:
    """No lookahead. D+k close only; sells are at OPEN D+k+1.

    The post-early stage reverts to R25 original frozen D0 floor and close+30
    rules. No D+5 features are applied to events already sold by day 5.
    """
    if method not in NEW_METHODS or t<55 or t+HORIZON>=len(g):
        raise ValueError("R27_INVALID_METHOD_OR_INCOMPLETE_HORIZON")
    entry=float(g.open.iloc[t+1])
    floor=float(g.low.iloc[t])
    level=float(g.high.iloc[t-55:t].max())
    if not all(np.isfinite(v) and v>0 for v in (entry,floor,level)):
        raise ValueError("R27_INVALID_ENTRY_LEVELS")
    if entry<=level:
        raise ValueError("R27_ENTRY_MUST_EXCEED_FROZEN55")
    limit=phase_day_limit(method)
    frozen_atr=(r26.r21.atr14_at_signal(g,t) if method==ATR2_FIRST5 else None)
    highest_close=entry
    for i in range(t+1,t+HORIZON):
        day=i-t
        close=float(g.close.iloc[i])
        if not np.isfinite(close) or close<=0:
            raise ValueError("R27_INVALID_DAY_CLOSE")
        stage="EARLY" if day<=limit else "LATE"
        reason=None

        # The original R25 stop floor operates throughout the position.
        if close<floor:
            reason="D0_LOW_CLOSE_BREAK"
        if reason is None and day<=limit:
            if method==MA5_FIRST3:
                ma5=float(g.close.iloc[i-4:i+1].mean())
                if close<ma5:
                    reason="EARLY_MA5_CLOSE_BREAK"
            elif method==MA10_FIRST5:
                ma10=float(g.close.iloc[i-9:i+1].mean())
                if close<ma10:
                    reason="EARLY_MA10_CLOSE_BREAK"
            elif method==ATR2_FIRST5:
                # Only as-of-D0 ATR; high-close is updated at each close.
                highest_close=max(highest_close,close)
                trail=max(floor,entry-2*frozen_atr,
                          highest_close-2*frozen_atr)
                if close<trail:
                    reason="EARLY_D0ATR2_CLOSE_TRAIL"
            elif method==CONFIRM_D3 and day==3:
                if close<=entry or close<=level:
                    reason="D3_CONFIRMATION_FAILED"
            elif method==CONFIRM_D5 and day==5:
                if close<=entry or close<=level:
                    reason="D5_CONFIRMATION_FAILED"
        if reason is None and close>=entry*1.30:
            reason="CLOSE30_TARGET"
        if reason is not None:
            next_open=float(g.open.iloc[i+1])
            if not np.isfinite(next_open) or next_open<=0:
                raise ValueError("R27_INVALID_NEXT_OPEN")
            return {
                "net_pct":r26.net(next_open,entry),
                "exit_day":day+1,"exit_reason":reason,
                "trigger_day":day,"phase_at_trigger":stage,
                "trigger_close":round(close,6),
                "fill_next_open":round(next_open,6),
                "fill_gap_from_trigger_close_pct":round(
                    100*(next_open/close-1),6),
            }

    final=float(g.close.iloc[t+HORIZON])
    return {"net_pct":r26.net(final,entry),"exit_day":HORIZON,
            "exit_reason":"WINDOW_END_CLOSE","trigger_day":HORIZON,
            "phase_at_trigger":"LATE","trigger_close":round(final,6),
            "fill_next_open":None,"fill_gap_from_trigger_close_pct":None}


def event_id(row:dict)->tuple[str,str]:
    return str(row["code"]),str(row["date"])


def build(prices:pd.DataFrame,original:list[dict],
          r25_rows:list[dict],r26_rows:list[dict])->tuple[list[dict],dict]:
    """Recompute immutable R26 and compare every source event before R27."""
    keys=lambda rows:[event_id(r) for r in rows]
    if (len(original)!=149 or len(r25_rows)!=149 or len(r26_rows)!=149
            or keys(original)!=keys(r25_rows) or keys(original)!=keys(r26_rows)
            or len(set(keys(original)))!=149):
        raise ValueError("R27_R19_R25_R26_FIXED149_IDENTITY_MISMATCH")
    replay=r26.build(prices,original,r25_rows)
    for ref,existing in zip(replay,r26_rows):
        if (ref["outcomes"]!=existing["outcomes"] or
                ref["labels_EXPOST_NOT_FEATURES"]!=existing["labels_EXPOST_NOT_FEATURES"]):
            raise ValueError("R27_R26_EVENT_POLICY_OR_LABEL_REPLAY_DRIFT")
    frames={str(code):frame.sort_values("date").reset_index(drop=True)
            for code,frame in prices.groupby("code")}
    positions={code:{date:i for i,date in enumerate(g.date)}
               for code,g in frames.items()}
    extended=[]
    summary={"new_methods":list(NEW_METHODS),"frozen149_replayed":149,
             "new_method_count":len(NEW_METHODS)}
    for old in replay:
        code,date=event_id(old)
        if code not in positions or date not in positions[code]:
            raise ValueError("R27_EVENT_NOT_IN_LOCKED_ARCHIVE")
        g,t=frames[code],positions[code][date]
        row={**old,"outcomes":dict(old["outcomes"]),
             "mae_through_held_sessions_EXPOST_ONLY":dict(
                 old["mae_through_held_sessions_EXPOST_ONLY"])}
        entry=float(g.open.iloc[t+1])
        for m in NEW_METHODS:
            o=simulate(g,t,m)
            row["outcomes"][m]=o
            # On next-open sale, day-of-exit intraday lows occur AFTER sale.
            last=o["exit_day"] if o["exit_reason"]=="WINDOW_END_CLOSE" else o["exit_day"]-1
            lows=g.low.iloc[t+1:t+last+1]
            row["mae_through_held_sessions_EXPOST_ONLY"][m]=round(
                100*(float(lows.min())/entry-1),6) if len(lows) else None
        extended.append(row)
    return extended,summary


def still_open_at_landmark(row:dict,method:str,day:int)->bool:
    """A pending D+(k+1) open sell means no new decision after D+k close."""
    out=row["outcomes"][method]
    return (out["exit_reason"]=="WINDOW_END_CLOSE" or
            int(out["exit_day"])>day+1)


def forward_after_landmark(g:pd.DataFrame,t:int,day:int,
                           entry:float)->dict:
    """EXPOST labels from D+k+1 only. NEVER available by close D+k."""
    if day not in DAYS or t+HORIZON>=len(g):
        raise ValueError("R27_BAD_POST_LANDMARK_WINDOW")
    forward=g.iloc[t+day+1:t+HORIZON+1]
    if len(forward)!=HORIZON-day or forward.empty:
        raise ValueError("R27_INCOMPLETE_POST_LANDMARK")
    hypothetical_start=float(g.open.iloc[t+day+1])
    if hypothetical_start<=0:
        raise ValueError("R27_INVALID_FUTURE_REFERENCE_OPEN")
    highest=float(forward.high.max())
    lowest=float(forward.low.min())
    final=float(g.close.iloc[t+HORIZON])
    return {
      "window_start":"DPLUS%d_OPEN"%(day+1),
      "window_end":"DPLUS60_CLOSE",
      "forward_high30_vs_D1_entry_EXPOST":highest>=entry*1.30,
      "forward_high30_vs_next_open_EXPOST":highest>=hypothetical_start*1.30,
      "forward_max_favorable_vs_next_open_pct_EXPOST":round(
          100*(highest/hypothetical_start-1),6),
      "forward_max_adverse_vs_next_open_pct_EXPOST":round(
          100*(lowest/hypothetical_start-1),6),
      "forward_to_day60_close_vs_next_open_net_pct_EXPOST":round(
          100*(final/hypothetical_start-1)-r26.COST_PCT,6),
      "future_hypothetical_entry_not_an_executed_trade":True,
    }


def landmark_audit(rows:list[dict],prices:pd.DataFrame,
                   r25_rows:list[dict],methods:tuple[str,...])->dict:
    """Only survivor trades + future bars AFTER landmark; not 60d full labels."""
    by_old={event_id(r):r for r in r25_rows}
    frames={str(c):g.sort_values("date").reset_index(drop=True)
            for c,g in prices.groupby("code")}
    locs={code:{date:i for i,date in enumerate(g.date)}
          for code,g in frames.items()}
    result={}
    for day in DAYS:
        day_result={}
        for method in methods:
            survivors=[r for r in rows if still_open_at_landmark(r,method,day)]
            decorated=[]
            for r in survivors:
                code,date=event_id(r)
                g,t=frames[code],locs[code][date]
                entry=float(g.open.iloc[t+1])
                future=forward_after_landmark(g,t,day,entry)
                old=by_old[(code,date)]["landmarks_DIAGNOSTIC_ONLY"]["D%d"%day]
                # no use of future label before this point and no model fitting
                decorated.append((r,old,future))
            grouped={}
            for known_flag in ("asof_close_above_entry",
                               "asof_close_above_frozen55",
                               "asof_close_above_ma5"):
                grouped[known_flag]={}
                for flag in (True,False):
                    sample=[(r,f) for r,k,f in decorated if k[known_flag] is flag]
                    count=len(sample)
                    group_key="YES" if flag else "NO"
                    forwards=[f["forward_to_day60_close_vs_next_open_net_pct_EXPOST"]
                              for _,f in sample]
                    grouped[known_flag][group_key]={
                       "n":count,
                       "future_Dkplus1_to_D60_high30_vs_next_open_n_EXPOST":
                         sum(f["forward_high30_vs_next_open_EXPOST"] for _,f in sample),
                       "future_Dkplus1_to_D60_high30_vs_D1_entry_n_EXPOST":
                         sum(f["forward_high30_vs_D1_entry_EXPOST"] for _,f in sample),
                       "future_Dkplus1_to_D60_close_net_median_EXPOST":
                         round(statistics.median(forwards),6) if forwards else None,
                       "future_Dkplus1_to_D60_close_net_p10_EXPOST":
                         round(float(np.quantile(forwards,.1)),6) if forwards else None,
                    }
            day_result[method]={
              "survivors_without_pending_next_open_exit_n":len(survivors),
              "only_post_landmark_future_prices_used_for_outcomes":True,
              "conditional_survivorship_bias":True,
              "asof_groups":grouped,
            }
        result["D%d"%day]=day_result
    return result


def compare_cohorts(rows:list[dict],scores25:list[dict],summary25:dict)->dict:
    if len(rows)!=149 or len(scores25)!=59:
        raise ValueError("R27_WRONG_R25_COUNTS")
    scoremap={event_id(r):r for r in scores25}
    if len(scoremap)!=59:
        raise ValueError("R27_DUPLICATE_R25_SCORE_IDS")
    review=[r for r in rows if r["date"]>=r25.REVIEW_START]
    if len(review)!=59 or {event_id(r) for r in review}!=set(scoremap):
        raise ValueError("R27_REVIEW_COHORT_IDENTITY_DRIFT")
    chosen=[r for r in review if scoremap[event_id(r)]["d0_selected_TRAINONLY"]]
    if (len(chosen)!=44 or
        sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in rows)!=40 or
        sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in review)!=20 or
        sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in chosen)!=17 or
        summary25.get("d0_selection_model",{}).get("training_cutoff_selection",{})
         .get("selected_n")!=44):
        raise ValueError("R27_R25_OPPORTUNITY_OR_SELECTION_DRIFT")
    cohort={"ALL_149":rows,"REVIEW_2024_2025_59":review,
            "R25_SELECTED_44":chosen,
            "REVIEW_2024":[r for r in review if r["year"]==2024],
            "REVIEW_2025":[r for r in review if r["year"]==2025]}
    return {key:{
      "n":len(vals),
      "big30_60d_EXPOST_n":sum(
          r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in vals),
      "methods":{m:r26.describe(vals,m) for m in ALL_METHODS},
      "paired_to_original_R25":{
        m:r26.paired(vals,m,BASE) for m in ALL_METHODS if m!=BASE}
      } for key,vals in cohort.items()}


def oos_readiness_manifest()->dict:
    """This is a REQUIREMENTS contract, NOT proof new data were acquired."""
    return {
      "status":"BLOCKED_NO_NEW_INDEPENDENT_OHLCV_ATTACHED",
      "required_before_promoting_any_candidate":[
        "timestamped_provenance_and_source_SHA256",
        "new_unseen_tickers_AND_unseen_evaluation_period",
        "minimum_55_prior_high_plus_60_future_sessions_per_event",
        "point_in_time_membership_and_suspension_delisting_handling",
        "corporate_actions_and_adjusted_raw_price_audit",
        "same_predeclared_R19_entry_and_cost_slippage_assumptions",
        "R27_rules_frozen_before_first_independent_result_inspection",
      ],
      "current_source":"26_CONVENIENCE_TICKERS_PREVIOUSLY_REUSED",
      "new_independent_market_data_rows":0,
      "new_independent_results_validated":False,
    }


def run(r16_out:Path,r19_out:Path,r25_out:Path,r26_out:Path,out:Path)->dict:
    prices,original=r25.r20.read_sources(r16_out,r19_out)
    old25=json.loads((r25_out/"R25_149_PAIRED_EXIT_AND_LANDMARK_EVENTS.json")
                     .read_text(encoding="utf-8"))
    scores25=json.loads((r25_out/"R25_2024_2025_D0_REVIEW_SCORES.json")
                        .read_text(encoding="utf-8"))
    summary25=json.loads((r25_out/"R25_BIG30_RESEARCH_SUMMARY.json")
                         .read_text(encoding="utf-8"))
    old26=json.loads((r26_out/"R26_FIXED149_PAIRED_GUARD_EVENTS.json")
                     .read_text(encoding="utf-8"))
    summary26=json.loads((r26_out/"R26_LEFT_TAIL_GUARD_SUMMARY.json")
                         .read_text(encoding="utf-8"))
    if (summary25.get("source_kis_run_id")!=r25.r20.KIS_RUN or
        summary26.get("source_kis_run_id")!=r25.r20.KIS_RUN or
        summary26.get("n_fixed_R19")!=149):
        raise ValueError("R27_FROZEN_SOURCE_RESEARCH_GATE_FAIL")
    rows,_=build(prices,original,old25,old26)
    cohorts=compare_cohorts(rows,scores25,summary25)
    review=[r for r in rows if r["date"]>=r25.REVIEW_START]
    report={
       "revision":"R27_TRUE55_CAUSAL_EARLY_PHASE_FAILURE_VS_LATE_30_CAPTURE",
       "research_only":True,"source_kis_run_id":r25.r20.KIS_RUN,
       "frozen_R19_entries":149,"big30_expost_all":40,
       "review_2024_25_n":59,"selected_R25_D0_n":44,
       "selected_R25_D0_big30_opportunities_n":17,
       "predeclared_early_phase_methods":list(NEW_METHODS),
       "comparators":list(COMPARE),
       "early_phases_in_sessions":{m:phase_day_limit(m) for m in NEW_METHODS},
       "early_phase_decision_only_asof_completed_close":True,
       "post_phase_exit_guard":"FROZEN_D0_LOW_CLOSE_BREAK_OR_CLOSE30_TARGET",
       "execution":"DPLUS_I_CLOSE_TRIGGER_NEXT_OPEN",
       "gap_risk_unbounded_by_close_trigger":True,
       "cohort_reports":cohorts,
       "landmark_exposure_adjusted_REVIEW59_EXPOST_DESCRIPTIVE":
          landmark_audit(review,prices,old25,(BASE,MA5_FIRST3,MA10_FIRST5,
                                              ATR2_FIRST5,CONFIRM_D3,CONFIRM_D5)),
       "external_oos_readiness":oos_readiness_manifest(),
       "labels_plus30_MFE_ONLY_EXPOST_NEVER_LIVE_SIGNAL":True,
       "Dplus1_Dplus3_Dplus5_only_forward_after_landmark":True,
       "survivorship_bias_not_removed":True,
       "predeclared_policies_not_tuned_on_2024_25":True,
       "2024_25_previously_inspected_not_pristine_oos":True,
       "26_ticker_convenience_sample_not_full_universe":True,
       "corporate_actions_not_independently_certified":True,
       "live_search_watch_changed":False,"orders_placed":0,
       "can_publish_trade_edge":False,
    }
    out.mkdir(parents=True,exist_ok=True)
    (out/"R27_PHASED_GUARDS_SUMMARY.json").write_text(
        json.dumps(report,indent=2,ensure_ascii=False,allow_nan=False)+"\n",encoding="utf-8")
    (out/"R27_FIXED149_PAIRED_PHASE_EVENTS.json").write_text(
        json.dumps(rows,indent=2,ensure_ascii=False,allow_nan=False)+"\n",encoding="utf-8")
    return report


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    for name in ("r16-out","r19-out","r25-out","r26-out","out"):
        p.add_argument("--"+name,type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(run(a.r16_out,a.r19_out,a.r25_out,a.r26_out,a.out),
                     indent=2,ensure_ascii=False,allow_nan=False))
