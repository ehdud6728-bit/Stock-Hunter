#!/usr/bin/env python3
"""R24 true 55-day breakout entry loss-warning validation; RESEARCH_ONLY.

The risk model is FIT ONLY on pre-2023Q4 D0 facts and past MA5 results.
The 2023Q4 embargo separates the 60-bar train outcomes from 2024 review.
2024-25 review previously appeared in earlier research: NOT pristine OOS.
No live order, filter, scheduler, rank, secret, or production change.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics
import sys

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, brier_score_loss
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"trendline_true55_r20_ma_exit_20261123"))
import r20_ma_exit as r20

TRAIN_END="2023-09-29"
REVIEW_START="2024-01-01"
COST_PCT=r20.COST_PCT
BASE="MA5_CLOSE_BELOW_NEXT_OPEN"
SECONDARY="MA20_CLOSE_BELOW_NEXT_OPEN"
# All five predictors exist at D0's completed close. No price at D+1 or later.
FEATURES=("close_location","amount_vs_prev20","volume_vs_prev20",
          "return_prev20_pct","breakout_vs_prior55_high_pct")
RISK_TRAIN_QUANTILE=.75
LOSS_THRESHOLD_PCT=0.0


def vector(event:dict)->list[float]:
    """Frozen R19 as-of-D0 features; never accept R19 post-entry outcomes."""
    raw=event["features"]
    result=[]
    for key in FEATURES:
        val=raw.get(key)
        try:
            number=float(val) if val is not None else np.nan
        except (ValueError,TypeError):
            number=np.nan
        result.append(number if np.isfinite(number) else np.nan)
    return result


def by_ids(events:list[dict],ma_rows:list[dict])->list[dict]:
    ids=lambda rows:[(str(r["code"]),str(r["date"])) for r in rows]
    if len(events)!=149 or len(ma_rows)!=149 or ids(events)!=ids(ma_rows):
        raise ValueError("R24_R19_R20_FROZEN149_IDENTITY_MISMATCH")
    out=[]
    for original,ma in zip(events,ma_rows):
        for old in ("D0_LOW_NEXT_OPEN","PREV5_LOW_NEXT_OPEN","PREV10_LOW_NEXT_OPEN"):
            if original["outcomes"][old]!=ma["outcomes"][old]:
                raise ValueError("R24_R19_R20_OLD_EXIT_EVENT_CHANGED")
        v=vector(original)
        if not any(np.isfinite(x) for x in v):
            raise ValueError("R24_ALL_D0_FEATURES_UNKNOWN")
        out.append({
          "code":str(original["code"]),"date":str(original["date"]),
          "year":int(original["year"]),"features_d0":v,
          "outcomes":{m:ma["outcomes"][m] for m in (BASE,SECONDARY)},
          "labels_EXPOST_NOT_FEATURES":{
             "ma5_is_loss":int(float(ma["outcomes"][BASE]["net_pct"]) <= LOSS_THRESHOLD_PCT),
             "max_favorable_60_pct":float(original["expost_not_tradable"]["max_favorable_60_pct"]),
             "big30_opportunity":float(original["expost_not_tradable"]["max_favorable_60_pct"])>=30,
          },
        })
    return out


def fixed_split(rows:list[dict])->tuple[list[dict],list[dict],list[dict]]:
    """Do not fit on Q4 2023 (60-trading-bar training label overlap guard)."""
    train=[r for r in rows if r["date"]<=TRAIN_END]
    gap=[r for r in rows if TRAIN_END<r["date"]<REVIEW_START]
    review=[r for r in rows if r["date"]>=REVIEW_START]
    if len(train)<30 or len(review)<20 or len(train)+len(gap)+len(review)!=len(rows):
        raise ValueError("R24_INSUFFICIENT_FROZEN_TEMPORAL_COHORT")
    return train,gap,review


def fit_train_only(train:list[dict]):
    """No hyperparameter or cutoff selection from later review labels."""
    x=np.array([r["features_d0"] for r in train],dtype=float)
    y=np.array([r["labels_EXPOST_NOT_FEATURES"]["ma5_is_loss"] for r in train],dtype=int)
    if len(set(y))!=2:
        raise ValueError("R24_TRAIN_HAS_ONE_OUTCOME_CLASS")
    # fixed pipeline fitted on TRAIN ONLY, imputation and standardization included
    clf=make_pipeline(SimpleImputer(strategy="median",keep_empty_features=True),
                      StandardScaler(),
                      LogisticRegression(C=.25,solver="liblinear",
                                         max_iter=1000,random_state=20261123))
    clf.fit(x,y)
    train_risk=clf.predict_proba(x)[:,1]
    threshold=float(np.quantile(train_risk,RISK_TRAIN_QUANTILE))
    if not np.isfinite(threshold):
        raise ValueError("R24_INVALID_TRAIN_QUANTILE")
    return clf,threshold


def group_stats(rows:list[dict],method:str)->dict:
    if not rows:
        return {"n":0,"mean_net_pct":None,"median_net_pct":None,
                "win_rate_pct":None,"p10_net_pct":None,
                "mean_excluding_best_pct":None,"worst_net_pct":None,
                "big30_opportunity_n_EXPOST":0}
    vals=sorted(float(r["outcomes"][method]["net_pct"]) for r in rows)
    return {"n":len(vals),"unique_tickers":len({r["code"] for r in rows}),
       "mean_net_pct":round(statistics.mean(vals),6),
       "median_net_pct":round(statistics.median(vals),6),
       "win_rate_pct":round(100*sum(v>0 for v in vals)/len(vals),3),
       "p10_net_pct":round(float(np.quantile(vals,.1)),6),
       "mean_excluding_best_pct":round(statistics.mean(vals[:-1]),6) if len(vals)>1 else None,
       "worst_net_pct":round(vals[0],6),
       "big30_opportunity_n_EXPOST":sum(r["labels_EXPOST_NOT_FEATURES"]["big30_opportunity"] for r in rows)}


def score_review(clf,threshold:float,review:list[dict]):
    """Only D0 facts enter predict_proba; labels used afterward for metrics."""
    x=np.array([r["features_d0"] for r in review],dtype=float)
    risks=clf.predict_proba(x)[:,1]
    scored=[]
    for r,risk in zip(review,risks):
        scored.append({**r,"loss_risk_score":round(float(risk),8),
                       "research_review_group":"HIGH_RISK" if risk>=threshold else "RETAINED"})
    return scored


def review_metrics(scored:list[dict],threshold:float)->dict:
    y=np.array([r["labels_EXPOST_NOT_FEATURES"]["ma5_is_loss"] for r in scored],dtype=int)
    risks=np.array([r["loss_risk_score"] for r in scored],dtype=float)
    retained=[r for r in scored if r["research_review_group"]=="RETAINED"]
    high=[r for r in scored if r["research_review_group"]=="HIGH_RISK"]
    result={
       "risk_score_cutoff_train_only":round(threshold,8),
       "review_n":len(scored),
       "review_actual_loss_rate_pct":round(100*y.mean(),3),
       "review_auc_for_ma5_loss":round(float(roc_auc_score(y,risks)),6)
                                      if len(set(y))==2 else None,
       "review_brier_for_ma5_loss":round(float(brier_score_loss(y,risks)),6),
       "all_review":{m:group_stats(scored,m) for m in (BASE,SECONDARY)},
       "retained":{m:group_stats(retained,m) for m in (BASE,SECONDARY)},
       "high_risk_rejected":{m:group_stats(high,m) for m in (BASE,SECONDARY)},
       "retained_fraction_pct":round(100*len(retained)/len(scored),3),
       "loss_events_avoided_EXPOST":sum(r["labels_EXPOST_NOT_FEATURES"]["ma5_is_loss"] for r in high),
       "positive_ma5_events_rejected_EXPOST":sum(r["outcomes"][BASE]["net_pct"]>0 for r in high),
       "big30_60day_opportunities_rejected_EXPOST":sum(
         r["labels_EXPOST_NOT_FEATURES"]["big30_opportunity"] for r in high),
       "review_by_year":{
          str(y):{
            name:{m:group_stats([r for r in scored if r["year"]==y and
                     (name=="ALL" or r["research_review_group"]==name)],m)
                    for m in (BASE,SECONDARY)}
            for name in ("ALL","RETAINED","HIGH_RISK")
          } for y in (2024,2025)
       },
       "warning_not_trade_decision":True,
    }
    return result


def run(r16_out:Path,r19_out:Path,r20_ma_out:Path,out:Path)->dict:
    prices,events=r20.read_sources(r16_out,r19_out)
    ma_summary=json.loads((r20_ma_out/"R20_MA_EXIT_SUMMARY.json").read_text(encoding="utf-8"))
    ma_rows=json.loads((r20_ma_out/"R20_MA_PAIRED_EVENTS.json").read_text(encoding="utf-8"))
    if ma_summary.get("same_149_r19_entries")!=149 or ma_summary.get("source_kis_run_id")!=r20.KIS_RUN:
        raise ValueError("R24_UNPINNED_R20_BASELINE")
    rows=by_ids(events,ma_rows)
    train,gap,review=fixed_split(rows)
    clf,threshold=fit_train_only(train)
    scored=score_review(clf,threshold,review)
    report={
      "revision":"R24_GENUINE55_D0_ONLY_TRAINONLY_LOSS_WARNING_REVIEW_AUDIT",
      "research_only":True,"source_kis_run_id":r20.KIS_RUN,
      "raw_archive_rows":len(prices),"raw_tickers":int(prices.code.nunique()),
      "frozen_r19_r20_events":len(rows),
      "train_end":TRAIN_END,"review_start":REVIEW_START,
      "train_n":len(train),"purged_2023q4_n":len(gap),"review_n":len(review),
      "predictors_d0_only":list(FEATURES),
      "loss_definition":"MA5_NET_PCT_LE_ZERO",
      "label_based_only_on_train_MA5_realized_returns":True,
      "regularization_C_fixed":.25,"high_risk_train_quantile_fixed":RISK_TRAIN_QUANTILE,
      "training_all":{m:group_stats(train,m) for m in (BASE,SECONDARY)},
      "gap_not_trained_or_evaluated_n":len(gap),
      "review":review_metrics(scored,threshold),
      "never_fit_imputer_scaler_classifier_on_review":True,
      "not_random_full_market_or_pristine_oos":True,
      "2024_2025_previously_examined":True,
      "train_purged_q4_to_reduce_60bar_overlap":True,
      "no_new_selected_strategy_or_trade_edge_claim":True,
      "corporate_actions_not_independently_certified":True,
      "live_search_watch_changed":False,"orders_placed":0,
      "can_publish_trade_edge":False,
    }
    out.mkdir(parents=True,exist_ok=True)
    (out/"R24_D0_ENTRY_RISK_SUMMARY.json").write_text(
        json.dumps(report,indent=2,ensure_ascii=False,allow_nan=False)+"\n",encoding="utf-8")
    # No individual score broadcast to operational channels; research artifact only.
    (out/"R24_REVIEW_SCORES_RESEARCH_ONLY.json").write_text(
        json.dumps(scored,indent=2,ensure_ascii=False,allow_nan=False)+"\n",encoding="utf-8")
    return report


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--r16-out",type=Path,required=True)
    p.add_argument("--r19-out",type=Path,required=True)
    p.add_argument("--r20-ma-out",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(run(a.r16_out,a.r19_out,a.r20_ma_out,a.out),
                     indent=2,ensure_ascii=False,allow_nan=False))
