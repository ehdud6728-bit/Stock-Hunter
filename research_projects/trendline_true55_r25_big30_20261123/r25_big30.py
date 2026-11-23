#!/usr/bin/env python3
"""R25: true55 +30% opportunity RECALL versus ACTUAL exit profit capture.

RESEARCH_ONLY on frozen 149 R19 signals. D0 scoring fitted on train through
2023-09-29; Q4 2023 purged; 2024-25 previously inspected, NOT pristine OOS.
The +30% high watermark is ex-post, NEVER an input to a decision or fill.
All non-baseline sell signals occur after CLOSE, executed NEXT session OPEN.
No changes to LIVE, SEARCH, WATCH, orders, scheduler or existing signals.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, brier_score_loss
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"trendline_true55_r20_ma_exit_20261123"))
sys.path.insert(0,str(ROOT/"trendline_true55_r22_level_reclaim_20261123"))
sys.path.insert(0,str(ROOT/"trendline_true55_r24_entry_risk_20261123"))
import r20_ma_exit as r20
import r22_level_reclaim as r22
import r24_entry_risk as r24

MA5=r22.MA5
LEVEL_TWO=r22.LEVEL_TWO
FLOOR_30="D0_LOW_CLOSE_BREAK_OR_CLOSE30_NEXT_OPEN"
HALF_15="MA5_OR_HALF_CLOSE15_THEN_MA5_NEXT_OPEN"
METHODS=(MA5,LEVEL_TWO,FLOOR_30,HALF_15)
TRAIN_END=r24.TRAIN_END
REVIEW_START=r24.REVIEW_START
PRESET_TRAIN_RETAIN_QUANTILE=.25  # score>=train 25th pct (~75% selected TRAIN)
MFE_THRESHOLD_PCT=30.0
SEED=20261123


def net_pct(exit_price:float,entry:float)->float:
    return 100*(exit_price/entry-1)-r20.COST_PCT


def simulate(g:pd.DataFrame,t:int,method:str)->dict:
    """No intraday hindsight: use close i to sell only at open i+1."""
    if method not in METHODS or t<55 or t+r20.HORIZON>=len(g):
        raise ValueError("R25_INVALID_EXIT_OR_INCOMPLETE_HORIZON")
    entry=float(g.open.iloc[t+1])
    floor=float(g.low.iloc[t])
    if not np.isfinite(entry) or not np.isfinite(floor) or min(entry,floor)<=0:
        raise ValueError("R25_BAD_ENTRY_OR_FROZEN_FLOOR")
    if method==MA5:
        return r20.ma_close_break_next_open(g,t,5)
    if method==LEVEL_TWO:
        return r22.policy(g,t,LEVEL_TWO)
    if method==FLOOR_30:
        for i in range(t+1,t+r20.HORIZON):
            close=float(g.close.iloc[i])
            reason=("D0_LOW_CLOSE_BREAK" if close<floor
                    else ("CLOSE30_TARGET" if close>=entry*1.30 else None))
            if reason is not None:
                return {"net_pct":round(net_pct(float(g.open.iloc[i+1]),entry),6),
                        "exit_day":i+1-t,"exit_reason":reason,
                        "close30_trigger":reason=="CLOSE30_TARGET"}
        return {"net_pct":round(net_pct(float(g.close.iloc[t+r20.HORIZON]),entry),6),
                "exit_day":r20.HORIZON,"exit_reason":"WINDOW_END_CLOSE",
                "close30_trigger":False}
    # HALF_15: predeclared half at NEXT open following CLOSE+15%;
    # remainder always uses immutable MA5 close-break. If the same close
    # satisfies MA5 exit and +15%, MA5 full exit takes precedence.
    first_take_price=None
    first_take_day=None
    for i in range(t+1,t+r20.HORIZON):
        close=float(g.close.iloc[i])
        below=close<float(g.close.iloc[i-4:i+1].mean())
        if below:
            px=float(g.open.iloc[i+1])
            ret=net_pct(px,entry) if first_take_price is None else (
                .5*net_pct(first_take_price,entry)+.5*net_pct(px,entry))
            return {"net_pct":round(ret,6),"exit_day":i+1-t,
                    "exit_reason":"MA5_CLOSE_BREAK",
                    "first_take_day":first_take_day}
        if first_take_price is None and close>=entry*1.15:
            first_take_price=float(g.open.iloc[i+1])
            first_take_day=i+1-t
    final=float(g.close.iloc[t+r20.HORIZON])
    ret=net_pct(final,entry) if first_take_price is None else (
        .5*net_pct(first_take_price,entry)+.5*net_pct(final,entry))
    return {"net_pct":round(ret,6),"exit_day":r20.HORIZON,
            "exit_reason":"WINDOW_END_CLOSE","first_take_day":first_take_day}


def first_high30_day(g:pd.DataFrame,t:int,entry:float)->int|None:
    """EX-POST label only; not a fill, target order, or feature."""
    for i in range(t+1,t+r20.HORIZON+1):
        if float(g.high.iloc[i])>=entry*1.30:
            return i-t
    return None


def landmark_asof(g:pd.DataFrame,t:int,k:int,entry:float,level:float)->dict:
    """D+k close-known facts; all forward highs separately EXPOST_ONLY."""
    if k not in (1,3,5) or t+r20.HORIZON>=len(g):
        raise ValueError("R25_INVALID_LANDMARK")
    i=t+k
    close=float(g.close.iloc[i])
    next_open=float(g.open.iloc[i+1])
    if next_open<=0:
        raise ValueError("R25_LANDMARK_NEXT_OPEN_INVALID")
    ma5=float(g.close.iloc[i-4:i+1].mean())
    fut=float(g.high.iloc[i+1:t+r20.HORIZON+1].max())
    return {
        "landmark_day":k,"known_at":"DPLUS%d_CLOSE"%k,
        "asof_return_from_d1_open_pct":round(100*(close/entry-1),6),
        "asof_close_above_entry":close>entry,
        "asof_close_above_frozen55":close>level,
        "asof_close_above_ma5":close>=ma5,
        # The next-open price is an ex-post reference only, NEVER same-day fill.
        "future_mfe_from_next_open_pct_EXPOST":round(100*(fut/next_open-1),6),
        "future30_from_next_open_EXPOST":fut>=next_open*1.30,
        "future30_from_original_entry_EXPOST":fut>=entry*1.30,
    }


def build(prices:pd.DataFrame,events:list[dict],r20_rows:list[dict],
          r22_rows:list[dict])->list[dict]:
    ids=lambda a:[(str(r["code"]),str(r["date"])) for r in a]
    if (len(events)!=149 or len(r20_rows)!=149 or len(r22_rows)!=149
        or ids(events)!=ids(r20_rows) or ids(events)!=ids(r22_rows)):
        raise ValueError("R25_FROZEN149_SOURCE_IDENTITY_MISMATCH")
    frames={str(c):f.sort_values("date").reset_index(drop=True)
            for c,f in prices.groupby("code")}
    idxs={c:{d:i for i,d in enumerate(g.date)} for c,g in frames.items()}
    result=[]
    for original,old20,old22 in zip(events,r20_rows,r22_rows):
        code,date=str(original["code"]),str(original["date"])
        if code not in idxs or date not in idxs[code]:
            raise ValueError("R25_EVENT_NOT_IN_LOCKED_OHLCV")
        g,t=frames[code],idxs[code][date]
        outcomes={m:simulate(g,t,m) for m in METHODS}
        for ref,method in ((old20,MA5),(old22,MA5),(old22,LEVEL_TWO)):
            if any(outcomes[method][key]!=ref["outcomes"][method][key]
                   for key in ("net_pct","exit_day")):
                raise ValueError("R25_R20_R22_EXIT_REPLAY_DRIFT")
        entry=float(g.open.iloc[t+1])
        full_mfe=float(original["expost_not_tradable"]["max_favorable_60_pct"])
        actual_high=float(g.high.iloc[t+1:t+r20.HORIZON+1].max())
        if abs(full_mfe-round(100*(actual_high/entry-1),6))>1e-5:
            raise ValueError("R25_FUTURE_HIGH_PROVENANCE_MISMATCH")
        big30=full_mfe>=MFE_THRESHOLD_PCT
        first=first_high30_day(g,t,entry)
        if big30!=(first is not None):
            raise ValueError("R25_OPPORTUNITY_LABEL_INCONSISTENT")
        level=float(g.high.iloc[t-55:t].max())
        result.append({
          "code":code,"date":date,"year":int(date[:4]),
          "d0_features_only":r24.vector(original),
          "outcomes":outcomes,
          "landmarks_DIAGNOSTIC_ONLY":{
             "D1":landmark_asof(g,t,1,entry,level),
             "D3":landmark_asof(g,t,3,entry,level),
             "D5":landmark_asof(g,t,5,entry,level)},
          "labels_EXPOST_NOT_FEATURES":{
             "big30_full60":big30,
             "full60_mfe_pct":full_mfe,
             "first_intraday_high30_day":first,
          }})
    return result


def train_opportunity_model(train:list[dict]):
    """A fixed capacity model and threshold; no review target used for fitting."""
    x=np.array([r["d0_features_only"] for r in train],dtype=float)
    y=np.array([int(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"])
                for r in train],dtype=int)
    if len(train)<30 or len(set(y))<2:
        raise ValueError("R25_TRAIN_TOO_SMALL_OR_SINGLE_CLASS")
    clf=make_pipeline(SimpleImputer(strategy="median",keep_empty_features=True),
                      StandardScaler(),
                      LogisticRegression(C=.25,solver="liblinear",
                                         max_iter=1000,random_state=SEED))
    clf.fit(x,y)
    score=clf.predict_proba(x)[:,1]
    threshold=float(np.quantile(score,PRESET_TRAIN_RETAIN_QUANTILE))
    if not np.isfinite(threshold):
        raise ValueError("R25_INVALID_TRAIN_ONLY_SELECTION_THRESHOLD")
    return clf,threshold


def score_review(clf,threshold:float,review:list[dict])->list[dict]:
    if not review:
        return []
    prob=clf.predict_proba(np.array([r["d0_features_only"] for r in review],
                                   dtype=float))[:,1]
    return [{**r,"d0_opportunity_score_TRAINONLY":round(float(p),8),
             "d0_selected_TRAINONLY":bool(p>=threshold)}
            for r,p in zip(review,prob)]


def descriptive(rows:list[dict],method:str)->dict:
    if not rows:
        return {"n":0,"mean_net_pct":None,"median_net_pct":None,
                "p10_net_pct":None,"win_rate_pct":None,
                "mean_excluding_best_pct":None,"worst_net_pct":None,
                "realized_net30_n":0,"realized_net15_n":0}
    vals=sorted(float(r["outcomes"][method]["net_pct"]) for r in rows)
    return {"n":len(vals),"unique_tickers":len({r["code"] for r in rows}),
            "mean_net_pct":round(statistics.mean(vals),6),
            "median_net_pct":round(statistics.median(vals),6),
            "p10_net_pct":round(float(np.quantile(vals,.10)),6),
            "worst_net_pct":round(vals[0],6),
            "win_rate_pct":round(100*sum(v>0 for v in vals)/len(vals),3),
            "mean_excluding_best_pct":round(statistics.mean(vals[:-1]),6)
                                      if len(vals)>1 else None,
            "realized_net30_n":sum(v>=30 for v in vals),
            "realized_net15_n":sum(v>=15 for v in vals)}


def recall_stats(rows:list[dict],selection:str|None=None)->dict:
    selected=[r for r in rows if selection is None or r[selection]]
    n=sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in rows)
    tp=sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in selected)
    return {"review_events_n":len(rows),"selected_n":len(selected),
            "coverage_pct":round(100*len(selected)/len(rows),3) if rows else None,
            "opportunities_EXPOST_n":n,"captured_opportunities_EXPOST_n":tp,
            "missed_opportunities_EXPOST_n":n-tp,
            "recall_pct":round(100*tp/n,3) if n else None,
            "precision_pct":round(100*tp/len(selected),3) if selected else None,
            "selected_exit_method_statistics":{
              m:descriptive(selected,m) for m in METHODS}}


def opportunity_exit_capture(rows:list[dict])->dict:
    opportunities=[r for r in rows if
                   r["labels_EXPOST_NOT_FEATURES"]["big30_full60"]]
    stats={}
    for method in METHODS:
        returns=[float(r["outcomes"][method]["net_pct"]) for r in opportunities]
        portions=[max(0.,float(r["outcomes"][method]["net_pct"]))/
                  r["labels_EXPOST_NOT_FEATURES"]["full60_mfe_pct"]*100
                  for r in opportunities]
        stats[method]={
          "opportunity_events_n":len(opportunities),
          "realized_net30_n":sum(v>=30 for v in returns),
          "realized_net15_n":sum(v>=15 for v in returns),
          "realized_positive_n":sum(v>0 for v in returns),
          "median_realized_net_pct":round(statistics.median(returns),6)
                                     if returns else None,
          "median_positive_return_over_full60_intraday_mfe_pct_EXPOST":
              round(statistics.median(portions),6) if portions else None,
          "exit_before_first_intraday30_n_EXPOST":sum(
              r["outcomes"][method]["exit_day"]<
              r["labels_EXPOST_NOT_FEATURES"]["first_intraday_high30_day"]
              for r in opportunities),
          "not_a_limit_fill_simulation":True,
        }
    return stats


def landmark_groups(rows:list[dict])->dict:
    """Descriptive; at D+1/3/5 CLOSE classification cannot authorize earlier trades."""
    result={}
    for k in (1,3,5):
        day="D%d"%k
        lookup="D%d"%k
        result[day]={}
        for field in ("asof_close_above_entry","asof_close_above_frozen55",
                      "asof_close_above_ma5"):
            sub={}
            for flag in (True,False):
                cohort=[r for r in rows if
                        r["landmarks_DIAGNOSTIC_ONLY"][lookup][field] is flag]
                post=[r["landmarks_DIAGNOSTIC_ONLY"][lookup]
                      ["future30_from_next_open_EXPOST"] for r in cohort]
                sub["YES" if flag else "NO"]={
                    "n":len(cohort),
                    "post_landmark30_from_next_open_n_EXPOST":sum(post),
                    "post_landmark30_from_next_open_rate_EXPOST":
                       round(100*sum(post)/len(cohort),3) if cohort else None}
            result[day][field]=sub
    return result


def evaluate(rows:list[dict],r24_summary:dict,
             r24_scored:list[dict])->tuple[dict,list[dict]]:
    train,gap,review=r24.fixed_split([{
       "code":r["code"],"date":r["date"],"year":r["year"],
       "features_d0":r["d0_features_only"],
       "labels_EXPOST_NOT_FEATURES":{
         "ma5_is_loss":int(r["outcomes"][MA5]["net_pct"]<=0),
         "big30_opportunity":r["labels_EXPOST_NOT_FEATURES"]["big30_full60"],
       },
       "outcomes":r["outcomes"],
    } for r in rows])
    # Restore original full records with same IDs; r24 split only uses dates.
    keys=lambda a:{(r["code"],r["date"]) for r in a}
    tr_keys,gap_keys,rev_keys=keys(train),keys(gap),keys(review)
    train=[r for r in rows if (r["code"],r["date"]) in tr_keys]
    review=[r for r in rows if (r["code"],r["date"]) in rev_keys]
    if (len(rows)!=149 or len(train)!=84 or len(gap_keys)!=6
        or len(review)!=59 or
        r24_summary.get("frozen_r19_r20_events")!=149 or
        r24_summary.get("review",{}).get("review_n")!=59):
        raise ValueError("R25_FROZEN_R24_TRAIN_REVIEW_COHORT_DRIFT")
    clf,threshold=train_opportunity_model(train)
    scored=score_review(clf,threshold,review)
    score_by={(r["code"],r["date"]):r for r in r24_scored}
    if keys(r24_scored)!=rev_keys or len(r24_scored)!=59:
        raise ValueError("R25_R24_SCORE_COHORT_DRIFT")
    missed_r24=sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"]
       and score_by[(r["code"],r["date"])]["research_review_group"]=="HIGH_RISK"
       for r in review)
    if missed_r24!=r24_summary["review"]["big30_60day_opportunities_rejected_EXPOST"]:
        raise ValueError("R25_R24_REJECTED_30PCT_PROVENANCE_DRIFT")
    truth=np.array([int(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"])
                    for r in scored],dtype=int)
    probs=np.array([r["d0_opportunity_score_TRAINONLY"] for r in scored])
    report={
       "revision":"R25_GENUINE55_BIG30_D0_RECALL_LANDMARK_EXIT_CAPTURE",
       "research_only":True,"source_kis_run_id":r20.KIS_RUN,
       "frozen_r19_events":len(rows),
       "train_end":TRAIN_END,"purge_2023Q4_n":len(gap_keys),
       "train_n":len(train),"review_n":len(scored),
       "label":"EXPOST_DPLUS1_OPEN_TO_DPLUS60_INTRADAY_HIGH_GE_30PCT",
       "train_big30_n":sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"]
                           for r in train),
       "all149_big30_n":sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"]
                            for r in rows),
       "d0_selection_model":{
         "features":list(r24.FEATURES),
         "train_only_C":.25,"train_only_score_quantile":PRESET_TRAIN_RETAIN_QUANTILE,
         "train_only_threshold":round(threshold,8),
         "review_auc":round(float(roc_auc_score(truth,probs)),6)
                     if len(set(truth))==2 else None,
         "review_brier":round(float(brier_score_loss(truth,probs)),6),
         "all_select_baseline":recall_stats(scored,None),
         "training_cutoff_selection":recall_stats(scored,"d0_selected_TRAINONLY"),
         "review_by_year":{str(y):{
             "all":recall_stats([r for r in scored if r["year"]==y],None),
             "selected":recall_stats([r for r in scored if r["year"]==y],
                                     "d0_selected_TRAINONLY")}
             for y in (2024,2025)},
         "reference_r24_loss_warning_rejected_big30_EXPOST":missed_r24,
         "selected_not_operational_search":True,
       },
       "all149_exit_methods":{m:descriptive(rows,m) for m in METHODS},
       "all149_big30_opportunity_exit_capture":opportunity_exit_capture(rows),
       "review_big30_opportunity_exit_capture":opportunity_exit_capture(review),
       "causal_landmark_d1_d3_d5_DESCRIPTIVE_NOT_TRADE":landmark_groups(rows),
       "execution":"CLOSE_TRIGGER_NEXT_SESSION_OPEN_NOT_INTRADAY_HIGH_FILL",
       "partial_15_two_legs_each_cost_weighted_for_one_total_roundtrip":True,
       "big30_expost_high_is_NOT_realized_profits":True,
       "no_future_label_as_feature_or_fit_on_review":True,
       "repeated_26_ticker_sample_not_full_market":True,
       "review_2024_25_previously_inspected_not_pristine_oos":True,
       "corporate_actions_not_independently_certified":True,
       "can_publish_trade_edge":False,
       "live_search_watch_changed":False,"orders_placed":0,
    }
    return report,scored


def run(r16_out:Path,r19_out:Path,r20_out:Path,
        r22_out:Path,r24_out:Path,out:Path)->dict:
    prices,events=r20.read_sources(r16_out,r19_out)
    s20=json.loads((r20_out/"R20_MA_EXIT_SUMMARY.json").read_text(encoding="utf-8"))
    s22=json.loads((r22_out/"R22_LEVEL_EXIT_SUMMARY.json").read_text(encoding="utf-8"))
    s24=json.loads((r24_out/"R24_D0_ENTRY_RISK_SUMMARY.json").read_text(encoding="utf-8"))
    if (s20.get("same_149_r19_entries")!=149 or
        s22.get("same_frozen_r19_events")!=149 or
        s24.get("source_kis_run_id")!=r20.KIS_RUN):
        raise ValueError("R25_SOURCE_MISSING_OR_UNPINNED")
    rows=build(prices,events,
      json.loads((r20_out/"R20_MA_PAIRED_EVENTS.json").read_text(encoding="utf-8")),
      json.loads((r22_out/"R22_LEVEL_PAIRED_EVENTS.json").read_text(encoding="utf-8")))
    report,scored=evaluate(rows,s24,json.loads(
       (r24_out/"R24_REVIEW_SCORES_RESEARCH_ONLY.json").read_text(encoding="utf-8")))
    out.mkdir(parents=True,exist_ok=True)
    for name,data in (
       ("R25_BIG30_RESEARCH_SUMMARY.json",report),
       ("R25_149_PAIRED_EXIT_AND_LANDMARK_EVENTS.json",rows),
       ("R25_2024_2025_D0_REVIEW_SCORES.json",scored)):
        (out/name).write_text(json.dumps(data,ensure_ascii=False,indent=2,
                                         allow_nan=False)+"\n",encoding="utf-8")
    return report


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    for name in ("r16-out","r19-out","r20-out","r22-out","r24-out","out"):
        p.add_argument("--"+name,type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(run(a.r16_out,a.r19_out,a.r20_out,
                         a.r22_out,a.r24_out,a.out),
                     ensure_ascii=False,indent=2,allow_nan=False))
