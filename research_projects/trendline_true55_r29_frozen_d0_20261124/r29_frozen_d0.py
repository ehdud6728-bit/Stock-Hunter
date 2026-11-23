#!/usr/bin/env python3
"""R29 fixed R25 D0 TRAIN-only score transfer onto disjoint R28 2026 events.

RESEARCH_ONLY. Same R25 model, imputation, scaling, C=.25 and cutoff
from original 84 pre-Q4-2023 train events. No refit/threshold optimization
using 2024-25 or 2026. Same 36 R28 true55 entry/event identities and two
unaltered R25/R27 exit outcomes. Source JSON and BLUE raw OHLCV hashes pinned.
2026 has already been inspected by R28: NOT a pristine blind future OOS.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from pathlib import Path

import numpy as np
from sklearn.metrics import brier_score_loss,roc_auc_score

REPO=Path(__file__).resolve().parents[2]
ROOT=REPO/"research_projects"
sys.path.insert(0,str(ROOT/"trendline_true55_r25_big30_20261123"))
sys.path.insert(0,str(ROOT/"trendline_true55_r28_2026_oos_20261124"))
import r25_big30 as r25
import r28_2026_validation as r28

R25_RUN="45160999953"
R28_RUN="45229111748"
BLUE_RUN=r28.SOURCE_RUN
R25_PATHS={
    "events":("r25_big30/R25_149_PAIRED_EXIT_AND_LANDMARK_EVENTS.json",
              "00c27656f3ba4b7665038fa98b877f17709bcf95f47107ae084b3ce5c1d025cf"),
    "review":("r25_big30/R25_2024_2025_D0_REVIEW_SCORES.json",
              "17ef022830071454a6fcc5b506817fc85674efb28f554645d13c3d34782b92b6"),
    "summary":("r25_big30/R25_BIG30_RESEARCH_SUMMARY.json",
              "7585756a0f7bd60f7da9b2bac8393844378d285df0085ebf9df41753000a9fe0"),
}
R28_PATHS={
    "events":("R28_TRUE55_DISJOINT_2026_EVENTS.json",
              "edb1c0e6206124ee3cf1266d495a7e7e851e5ec373967d3d2a97aa3a6228f144"),
    "summary":("R28_TRUE55_DISJOINT_2026_SUMMARY.json",
               "7d75f632cd9f849bf8ca2f4cc9d6a17e0a48b9cc20cf781ff90d0c01981a8a71"),
}
BASE="BASE_R25_D0LOW_CLOSE30"
D3="FIXED_R27_D3_CONFIRM"
TRAIN_END=r25.TRAIN_END
REVIEW_START=r25.REVIEW_START
RETAIN_QUANTILE=r25.PRESET_TRAIN_RETAIN_QUANTILE
EXPECTED_TRAIN=84
EXPECTED_GAP=6
EXPECTED_REVIEW=59
EXPECTED_NEW_EVENTS=36
EXPECTED_2026_BIG30=20
EXPECTED_ORIGINAL_SELECTED44=44


def read_locked_json(root:Path,entry:tuple[str,str])->object:
    rel,expected=entry
    payload=(root/rel).read_bytes()
    actual=hashlib.sha256(payload).hexdigest()
    if actual!=expected:
        raise ValueError("R29_PINNED_JSON_SHA_MISMATCH:"+rel)
    return json.loads(payload)


def idof(r:dict)->tuple[str,str]:
    return str(r["code"]),str(r["date"])


def train_and_replay_original(r25_dir:Path):
    events=read_locked_json(r25_dir,R25_PATHS["events"])
    old_review=read_locked_json(r25_dir,R25_PATHS["review"])
    old_summary=read_locked_json(r25_dir,R25_PATHS["summary"])
    if (len(events)!=149 or len(old_review)!=EXPECTED_REVIEW or
        old_summary.get("source_kis_run_id")!=r25.r20.KIS_RUN or
        old_summary.get("train_n")!=EXPECTED_TRAIN or
        old_summary.get("purge_2023Q4_n")!=EXPECTED_GAP or
        old_summary.get("review_n")!=EXPECTED_REVIEW or
        old_summary.get("all149_big30_n")!=40):
        raise ValueError("R29_ORIGINAL_R25_COHORT_OR_PROVENANCE_DRIFT")
    if len({idof(x) for x in events})!=149 or len({idof(x) for x in old_review})!=59:
        raise ValueError("R29_DUPLICATE_R25_FROZEN_EVENT")
    train=[x for x in events if x["date"]<=TRAIN_END]
    purged=[x for x in events if TRAIN_END<x["date"]<REVIEW_START]
    review=[x for x in events if x["date"]>=REVIEW_START]
    if (len(train),len(purged),len(review))!=(84,6,59):
        raise ValueError("R29_ORIGINAL_TRAIN_2023Q4_REVIEW_SPLIT_DRIFT")
    if [idof(x) for x in review]!=[idof(x) for x in old_review]:
        raise ValueError("R29_ORIGINAL_R25_REVIEW_ORDER_OR_EVENT_DRIFT")
    model,cut=r25.train_opportunity_model(train)
    frozen_cut=float(old_summary["d0_selection_model"]["train_only_threshold"])
    if round(cut,8)!=frozen_cut:
        raise ValueError("R29_R25_TRAIN_ONLY_THRESHOLD_NOT_REPRODUCED")
    replay=r25.score_review(model,cut,review)
    for x,original in zip(replay,old_review):
        if (round(x["d0_opportunity_score_TRAINONLY"],8)!=
            original["d0_opportunity_score_TRAINONLY"] or
            x["d0_selected_TRAINONLY"] is not
            original["d0_selected_TRAINONLY"] or
            x["d0_features_only"]!=original["d0_features_only"]):
            raise ValueError("R29_ORIGINAL_2024_25_SCORE_AND_SELECTION_REPLAY_DRIFT")
    if (sum(x["d0_selected_TRAINONLY"] for x in replay)!=44 or
        sum(x["d0_selected_TRAINONLY"] and
            x["labels_EXPOST_NOT_FEATURES"]["big30_full60"]
            for x in replay)!=17):
        raise ValueError("R29_ORIGINAL_SELECTION44_BIG30_17_DRIFT")
    if tuple(old_summary["d0_selection_model"]["features"])!=tuple(r25.r24.FEATURES):
        raise ValueError("R29_FROZEN_R25_D0_FEATURE_ORDER_CHANGED")
    return model,cut,{"training_n":84,"purged_2023Q4_n":6,
                      "original_2024_25_review_n":59,
                      "replayed_original_R25_review_scores_identical":True,
                      "original_review_selected_n":44,
                      "original_review_selected_big30_n":17,
                      "cutoff":round(cut,8),
                      "cutoff_train_quantile":RETAIN_QUANTILE,
                      "input_features_in_exact_R25_order":list(r25.r24.FEATURES)}


def load_and_replay_R28(blue_dir:Path,r28_dir:Path):
    original=read_locked_json(r28_dir,R28_PATHS["events"])
    previous_summary=read_locked_json(r28_dir,R28_PATHS["summary"])
    prices,manifest=r28.load_pinned_source(blue_dir)
    reconstructed,audit=r28.frozen_true55_events(prices)
    if original!=reconstructed:
        raise ValueError("R29_UNCHANGED_R28_2026_ENTRY_AND_EXIT_REPLAY_DRIFT")
    if (len(original)!=EXPECTED_NEW_EVENTS or
        sum(x["expost_LABELS_NEVER_DECISION"]["full_D1_to_D60_high30"]
            for x in original)!=EXPECTED_2026_BIG30 or
        len(manifest["valid_codes"])!=29 or len(prices)!=13311 or
        previous_summary.get("source_run_id")!=BLUE_RUN or
        previous_summary.get("source_csv_sha256")!=r28.SOURCE_CSV_SHA256 or
        previous_summary.get("completed_R19_true55_2026_event_n")!=36 or
        previous_summary.get("full_D1_to_D60_30pct_high_opportunities_EXPOST_n")!=20 or
        previous_summary.get("source_audit")!=audit):
        raise ValueError("R29_2026_R28_COHORT_OR_SOURCE_REPLAY_DRIFT")
    if len({idof(x) for x in original})!=EXPECTED_NEW_EVENTS:
        raise ValueError("R29_2026_DUPLICATE_EVENT")
    return original,prices,manifest


def predict_asof_D0(model,cut:float,events:list[dict],prices)->list[dict]:
    """Reconstruct the exact original R17/R24 5 D0 features from OHLCV.

    Not D+1 or D+3. Only D0 and preceding bars. Selection is independent
    of the future big30 labels and realized exit outcomes.
    """
    r17=r25.r20.r19.load_r17(REPO) if hasattr(r25.r20,"r19") else None
    if r17 is None:
        # Same R19 source import used in R28; no duplicated feature formulas.
        r17=r28.r19.load_r17(REPO)
    frames={str(code):g.sort_values("date").reset_index(drop=True)
            for code,g in prices.groupby("code")}
    locs={code:{date:i for i,date in enumerate(g.date)}
          for code,g in frames.items()}
    vectors=[]
    for e in events:
        code,date=idof(e)
        if code not in locs or date not in locs[code]:
            raise ValueError("R29_R28_EVENT_MISSING_PRICE_AT_D0")
        g,t=frames[code],locs[code][date]
        if t<55 or t+r28.D60>=len(g):
            raise ValueError("R29_INCOMPLETE_OR_NONCAUSAL_SIGNAL")
        # The exact original make_features from R17, and R24.vector from R25.
        raw=r17.make_features(g,t)
        vector=r25.r24.vector({"features":raw})
        if len(vector)!=len(r25.r24.FEATURES) or not all(np.isfinite(vector)):
            raise ValueError("R29_ASOF_D0_VECTOR_INCOMPLETE")
        if abs(float(g.high.iloc[t-55:t].max())-e["frozen_prior55_high"])>1e-6:
            raise ValueError("R29_PRIOR55_FEATURE_AND_ENTRY_IDENTITY_MISMATCH")
        vectors.append(vector)
    from numpy import array
    probabilities=model.predict_proba(array(vectors,dtype=float))[:,1]
    return [
      {"code":e["code"],"date":e["date"],"year":e["year"],
       "R25_D0_features_only":vec,
       "R25_D0_score_TRAINONLY":round(float(p),8),
       "R25_frozen_cutoff":round(cut,8),
       "R25_D0_selected_TRAINONLY":bool(p>=cut),
       "expost_original_full60_intraday_high30":bool(
           e["expost_LABELS_NEVER_DECISION"]["full_D1_to_D60_high30"]),
       "outcomes":e["outcomes"]}
      for e,vec,p in zip(events,vectors,probabilities)
    ]


def score_metrics(scored:list[dict])->dict:
    if not scored:
        return {"n":0,"AUC":None,"Brier":None,"n_big30_EXPOST":0}
    truth=np.array([int(r["expost_original_full60_intraday_high30"]) for r in scored])
    pred=np.array([r["R25_D0_score_TRAINONLY"] for r in scored],dtype=float)
    return {"n":len(scored),
            "n_big30_EXPOST":int(truth.sum()),
            "AUC_EXPOST":round(float(roc_auc_score(truth,pred)),6)
                            if len(set(truth))>1 else None,
            "Brier_EXPOST":round(float(brier_score_loss(truth,pred)),6),
            "score_min":round(float(min(pred)),8),
            "score_median":round(float(np.median(pred)),8),
            "score_max":round(float(max(pred)),8)}


def selection_metrics(rows:list[dict])->dict:
    selected=[r for r in rows if r["R25_D0_selected_TRAINONLY"]]
    rejected=[r for r in rows if not r["R25_D0_selected_TRAINONLY"]]
    opportunities=sum(r["expost_original_full60_intraday_high30"] for r in rows)
    selected_big=sum(r["expost_original_full60_intraday_high30"] for r in selected)
    rejected_big=opportunities-selected_big
    return {
      "total_n":len(rows),"selected_n":len(selected),
      "rejected_n":len(rejected),
      "original_full60_intraday_big30_EXPOST_n":opportunities,
      "selected_original_big30_EXPOST_n":selected_big,
      "rejected_big30_opportunities_EXPOST_n":rejected_big,
      "big30_recall_pct_EXPOST":round(100*selected_big/opportunities,3)
                                   if opportunities else None,
      "big30_precision_pct_EXPOST":round(100*selected_big/len(selected),3)
                                      if selected else None,
      "missed_R25_actual_realized_net30_due_to_D0_rejection_EXPOST":sum(
          r["outcomes"][BASE]["net_pct"]>=30 for r in rejected),
      "rejected_R25_actual_loss_le15_n_EXPOST":sum(
          r["outcomes"][BASE]["net_pct"]<=-15 for r in rejected),
    }


def compact(rows:list[dict],score_only:bool=False)->dict:
    """Existing R28 paired methods, NO recalculation of trading outcomes."""
    as_r28=[{"code":r["code"],"date":r["date"],"year":r["year"],
             "outcomes":r["outcomes"],
             "expost_LABELS_NEVER_DECISION":{
                "full_D1_to_D60_high30":r["expost_original_full60_intraday_high30"]}}
             for r in rows]
    return {"n":len(rows),
            "big30_opportunities_EXPOST_n":sum(
                r["expost_original_full60_intraday_high30"] for r in rows),
            "by_method":{m:r28.describe(as_r28,m) for m in (BASE,D3)},
            "paired_R27_minus_R25":r28.paired(as_r28),
            "ticker_cluster_bootstrap":r28.ticker_cluster_bootstrap_ci(as_r28)}


def run(blue_source:Path,old25_dir:Path,old28_dir:Path,out:Path)->dict:
    model,cut,train_audit=train_and_replay_original(old25_dir)
    new_events,prices,manifest=load_and_replay_R28(blue_source,old28_dir)
    scored=predict_asof_D0(model,cut,new_events,prices)
    chosen=[r for r in scored if r["R25_D0_selected_TRAINONLY"]]
    rejected=[r for r in scored if not r["R25_D0_selected_TRAINONLY"]]
    summary={
      "revision":"R29_FROZEN_R25_D0_CLASSIFIER_TRANSFER_TO_R28_2026",
      "research_only":True,
      "BLUE_fixed_capture_run":BLUE_RUN,
      "R25_original_training_run":R25_RUN,
      "R28_2026_original_evaluation_run":R28_RUN,
      "BLUE_raw_CSV_sha256":r28.SOURCE_CSV_SHA256,
      "R25_original_json_sha256":{key:v[1] for key,v in R25_PATHS.items()},
      "R28_original_json_sha256":{key:v[1] for key,v in R28_PATHS.items()},
      "training_replay":train_audit,
      "new_2026_valid_tickers":len(manifest["valid_codes"]),
      "new_2026_locked_event_count":len(scored),
      "score_classification_performance_2026":score_metrics(scored),
      "selection_metrics":selection_metrics(scored),
      "cohorts":{
        "ALL_36":compact(scored),
        "FROZEN_D0_SELECTED":compact(chosen),
        "FROZEN_D0_REJECTED":compact(rejected)},
      "No_2026_label_or_exit_outcome_used_in_model_fit_or_cutoff":True,
      "five_original_R25_D0_features_asof_only":True,
      "R25_original_59_review_scores_exactly_replayed":True,
      "R28_original_event_ids_and_outcomes_exactly_replayed":True,
      "2026_already_inspected_in_R28_NOT_BLIND_OOS":True,
      "new_2026_29_tickers_handpicked_and_survivorship_unresolved":True,
      "unadjusted_raw_prices_no_independent_corporate_action_certification":True,
      "market_impact_slippage_and_PIT_unverified":True,
      "no_live_search_watch_changes":True,
      "orders_placed":0,
      "can_publish_trade_edge":False,
    }
    out.mkdir(parents=True,exist_ok=True)
    (out/"R29_FROZEN_D0_TRANSFER_SUMMARY.json").write_text(
       json.dumps(summary,ensure_ascii=False,indent=2,allow_nan=False)+"\n",
       encoding="utf-8")
    (out/"R29_2026_D0_SCORES_AND_PAIRED_EXITS.json").write_text(
       json.dumps(scored,ensure_ascii=False,indent=2,allow_nan=False)+"\n",
       encoding="utf-8")
    return summary


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    for name in ("blue-source","r25-original","r28-original","out"):
        p.add_argument("--"+name,type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(run(a.blue_source,a.r25_original,a.r28_original,a.out),
                     ensure_ascii=False,indent=2,allow_nan=False))
