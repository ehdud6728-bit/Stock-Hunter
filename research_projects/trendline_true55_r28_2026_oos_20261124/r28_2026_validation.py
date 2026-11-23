#!/usr/bin/env python3
"""R28 strictly source-pinned historical 2026, non-R19 30-ticker holdout.

Compare only the FIXED R25 original exit against FIXED R27 D+3 confirmation.
All entries reuse R19 exact previous55-high signal and 60-bar embargo;
signals, D0 features and 3-day confirmation use only then-known prices.
No model training, thresholds, future-label features, trade orders, DB writes,
or LIVE/SEARCH/WATCH changes. 2026 cohort is retrospective ticker-disjoint
convenience holdout, NOT representative market, not pristine prospective OOS.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"trendline_true55_r19_20261122"))
sys.path.insert(0,str(ROOT/"trendline_true55_r27_early_phase_20261123"))
import r19_true55_transfer as r19
import r27_early_phase as r27

R25=r27.BASE
R27_D3=r27.CONFIRM_D3
SOURCE_REPO="ehdud6728-bit/BLUE-Signal-Lab"
SOURCE_RUN="45221059025"
SOURCE_CSV_SHA256="PENDING_VERIFIED_CAPTURE_SHA256"
SOURCE_CSV="R28_UNSEEN30_2025_2026_UNADJUSTED.csv"
SOURCE_MANIFEST="R28_SOURCE_MANIFEST.json"
EXPECTED_CODES=(
 '005930','000660','035420','035720','051910','028260',
 '005380','000270','012330','105560','055550','086790',
 '003550','015760','034020','009150','006800','010130',
 '017670','033780','000100','078930','096770','032830',
 '047810','207940','066570','003490','011200','086280')
PREVIOUS_R19=(
 '093320','332290','125210','062970','068760','250000',
 '004380','042370','015710','057540','377480','054050',
 '012610','044450','100120','002450','177830','039240',
 '353200','226320','129260','053350','127120','001720',
 '052020','072870')
REVIEW_START="2026-01-02"
SOURCE_START="2025-01-02"
SOURCE_END="2026-11-20"
MIN_VALID_TICKERS=20
MIN_COMPLETE_EVENTS=20
ROUNDTRIP_COST=.2
D60=60
NO_INDEPENDENT_COA_AUDIT=True


def sha256(data:bytes)->str:
    return hashlib.sha256(data).hexdigest()


def load_pinned_source(folder:Path)->tuple[pd.DataFrame,dict]:
    if (len(EXPECTED_CODES)!=30 or len(set(EXPECTED_CODES))!=30 or
        len(PREVIOUS_R19)!=26 or len(set(PREVIOUS_R19))!=26 or
        set(EXPECTED_CODES)&set(PREVIOUS_R19)):
        raise ValueError("R28_COHORT_OVERLAP_OR_PREDECLARED_CODES_DRIFT")
    if not (len(SOURCE_CSV_SHA256)==64 and
            all(c in "0123456789abcdef" for c in SOURCE_CSV_SHA256)):
        raise ValueError("R28_REAL_SOURCE_SHA_NOT_PINNED_NO_EVALUATION")
    manifest=json.loads((folder/SOURCE_MANIFEST).read_text(encoding="utf-8"))
    if (manifest.get("status")!="SOURCE_CAPTURE_COMPLETE_NOT_EVALUATED" or
        str(manifest.get("capture_run_id"))!=SOURCE_RUN or
        manifest.get("source_repo")!=SOURCE_REPO or
        manifest.get("codes")!=list(EXPECTED_CODES) or
        manifest.get("warmup_start")!=SOURCE_START.replace("-","") or
        manifest.get("fixed_end")!=SOURCE_END.replace("-","") or
        manifest.get("analysis_start")!=REVIEW_START.replace("-","") or
        not manifest.get("GET_only") or manifest.get("adjusted_prices") is not False or
        manifest.get("PIT_corporate_action_certified") is not False or
        manifest.get("orders_placed")!=0 or manifest.get("turso_writes")!=0 or
        manifest.get("D1_writes")!=0):
        raise ValueError("R28_CAPTURE_MANIFEST_SCOPE_SOURCE_OR_SAFETY_CHANGED")
    if (manifest.get("raw_csv_sha256")!=SOURCE_CSV_SHA256 or
        len(manifest.get("valid_codes",[]))<MIN_VALID_TICKERS or
        not set(manifest["valid_codes"]).issubset(EXPECTED_CODES)):
        raise ValueError("R28_PINNED_SOURCE_HASH_OR_VALID_SET_CHANGED")
    raw=(folder/SOURCE_CSV).read_bytes()
    if sha256(raw)!=SOURCE_CSV_SHA256:
        raise ValueError("R28_SOURCE_CSV_SHA_MISMATCH")
    d=pd.read_csv(folder/SOURCE_CSV,dtype={"code":str,"date":str})
    cols=("date","code","open","high","low","close","volume","amount")
    if (list(d.columns)!=list(cols) or len(d)!=manifest.get("daily_rows") or
        d.duplicated(["code","date"]).any() or
        set(d.code)!=set(manifest["valid_codes"])):
        raise ValueError("R28_SOURCE_PRICE_SCHEMA_OR_ROWS_INVALID")
    if d.empty or d[["open","high","low","close","volume","amount"]].isna().any().any():
        raise ValueError("R28_SOURCE_MISSING_PRICE_FIELDS")
    nums=["open","high","low","close","volume","amount"]
    for name in nums:
        d[name]=pd.to_numeric(d[name],errors="coerce")
    if (d[nums].isna().any().any() or
        not np.isfinite(d[nums].to_numpy(dtype=float)).all() or
        (d[["open","high","low","close"]]<=0).any().any() or
        (d[["volume","amount"]]<0).any().any()):
        raise ValueError("R28_INVALID_REAL_PRICE_OR_AMOUNT")
    if ((d.high<d[["open","low","close"]].max(axis=1)).any()
        or (d.low>d[["open","high","close"]].min(axis=1)).any()):
        raise ValueError("R28_INCONSISTENT_OHLC")
    parsed=pd.to_datetime(d.date,errors="coerce",format="%Y-%m-%d")
    if parsed.isna().any() or (parsed.dt.strftime("%Y-%m-%d")!=d.date).any():
        raise ValueError("R28_DATE_NOT_REAL")
    if (d.date.min()<SOURCE_START or d.date.max()>SOURCE_END or
        d.code.str.len().ne(6).any()):
        raise ValueError("R28_SOURCE_DATE_OR_TICKER_RANGE")
    for code,g in d.groupby("code"):
        g=g.sort_values("date")
        if (len(g)<350 or g.date.iloc[0]>"2025-02-01" or
            g.date.iloc[-1]<"2026-11-02"):
            raise ValueError("R28_INVALID_WARMUP_OR_OOS_COVERAGE")
        if ((g.open.to_numpy()[1:] / g.close.to_numpy()[:-1]-1).__abs__()>.35).any():
            raise ValueError("R28_UNCERTIFIED_CORPORATE_ACTION_OR_EXTREME_GAP")
    return d,manifest


def frozen_true55_events(prices:pd.DataFrame)->tuple[list[dict],dict]:
    audit={"raw_true55_candidates":0,"eligible_after_60bar_embargo":0,
           "not_2026_eval_after_embargo":0,"incomplete_d60":0,
           "invalid_next_open_below_prior55":0,"evaluated_events":0}
    result=[]
    for code,raw in prices.groupby("code",sort=True):
        g=raw.sort_values("date").reset_index(drop=True)
        if len(g)<55:
            raise ValueError("R28_INSUFFICIENT_FIRST55")
        candidates,prior=r19.prior55_candidates(g)
        audit["raw_true55_candidates"]+=len(candidates)
        last=-D60
        for t in candidates:
            if t-last<D60:
                continue
            # R19 embargo is applied BEFORE day-window and next-open checks.
            last=t
            audit["eligible_after_60bar_embargo"]+=1
            if str(g.date.iloc[t])<REVIEW_START:
                audit["not_2026_eval_after_embargo"]+=1
                continue
            if t+D60>=len(g):
                audit["incomplete_d60"]+=1
                continue
            entry=float(g.open.iloc[t+1])
            if entry<=float(prior.iloc[t]):
                audit["invalid_next_open_below_prior55"]+=1
                continue
            baseline=r27.r25.simulate(g,t,R25)
            confirm=r27.simulate(g,t,R27_D3)
            full_high=float(g.high.iloc[t+1:t+D60+1].max())
            high30=(full_high>=entry*1.3)
            e={"code":str(code),"date":str(g.date.iloc[t]),
               "year":int(str(g.date.iloc[t])[:4]),
               "entry_day":str(g.date.iloc[t+1]),
               "entry_price":round(entry,6),
               "frozen_prior55_high":round(float(prior.iloc[t]),6),
               "frozen_d0_low":round(float(g.low.iloc[t]),6),
               "outcomes":{
                  "BASE_R25_D0LOW_CLOSE30":baseline,
                  "FIXED_R27_D3_CONFIRM":confirm},
               "expost_LABELS_NEVER_DECISION":{
                   "full_D1_to_D60_high30":bool(high30),
                   "full_D1_to_D60_high_pct":round(
                        100*(full_high/entry-1),6),
                   "full_D1_to_D60_low_pct":round(
                        100*(float(g.low.iloc[t+1:t+D60+1].min())/entry-1),6),
                }}
            result.append(e)
            audit["evaluated_events"]+=1
    if len({(r["code"],r["date"]) for r in result})!=len(result):
        raise ValueError("R28_DUPLICATE_EVENT_ID")
    return result,audit


def describe(events:list[dict],method:str)->dict:
    vals=sorted(float(e["outcomes"][method]["net_pct"]) for e in events)
    big=[e for e in events if e["expost_LABELS_NEVER_DECISION"]["full_D1_to_D60_high30"]]
    if not vals:
        return {"n":0,"mean_net_pct":None,"median_net_pct":None,
                "p10_net_pct":None,"loss_le15_n":0,
                "loss_le20_n":0,"realized_net30_n":0}
    return {"n":len(vals),"n_unique_tickers":len({e["code"] for e in events}),
       "mean_net_pct":round(statistics.mean(vals),6),
       "median_net_pct":round(statistics.median(vals),6),
       "p10_net_pct":round(float(np.quantile(vals,.10)),6),
       "worst_net_pct":round(vals[0],6),
       "win_rate_pct":round(100*sum(v>0 for v in vals)/len(vals),3),
       "loss_le10_n":sum(v<=-10 for v in vals),
       "loss_le15_n":sum(v<=-15 for v in vals),
       "loss_le20_n":sum(v<=-20 for v in vals),
       "realized_net30_n":sum(v>=30 for v in vals),
       "mean_excluding_one_best_net_pct":round(statistics.mean(vals[:-1]),6)
                                           if len(vals)>1 else None,
       "big30_opportunities_EXPOST_n":len(big),
       "big30_realized_net30_n":sum(
           e["outcomes"][method]["net_pct"]>=30 for e in big)}


def paired(events:list[dict])->dict:
    base=[e["outcomes"]["BASE_R25_D0LOW_CLOSE30"]["net_pct"] for e in events]
    test=[e["outcomes"]["FIXED_R27_D3_CONFIRM"]["net_pct"] for e in events]
    delta=[b-a for a,b in zip(base,test)]
    big=[e for e in events if e["expost_LABELS_NEVER_DECISION"]["full_D1_to_D60_high30"]]
    result={
      "paired_n":len(events),
      "mean_R27_minus_R25_net_percentage_points":round(
          statistics.mean(delta),6) if delta else None,
      "median_R27_minus_R25_net_percentage_points":round(
          statistics.median(delta),6) if delta else None,
      "R25_loss15_becomes_R27_above_minus15_n":sum(
          a<=-15 and b>-15 for a,b in zip(base,test)),
      "R25_above_minus15_becomes_R27_loss15_n":sum(
          a>-15 and b<=-15 for a,b in zip(base,test)),
      "R25_realized30_kept_by_R27_n":sum(
          e["outcomes"]["BASE_R25_D0LOW_CLOSE30"]["net_pct"]>=30 and
          e["outcomes"]["FIXED_R27_D3_CONFIRM"]["net_pct"]>=30 for e in big),
      "R25_realized30_lost_by_R27_n":sum(
          e["outcomes"]["BASE_R25_D0LOW_CLOSE30"]["net_pct"]>=30 and
          e["outcomes"]["FIXED_R27_D3_CONFIRM"]["net_pct"]<30 for e in big),
      "R27_new_realized30_n":sum(
          e["outcomes"]["BASE_R25_D0LOW_CLOSE30"]["net_pct"]<30 and
          e["outcomes"]["FIXED_R27_D3_CONFIRM"]["net_pct"]>=30 for e in big),
    }
    return result


def ticker_cluster_bootstrap_ci(events:list[dict],n:int=4000,seed:int=20261124):
    """Descriptive ticker-cluster CI; does not cure convenience sampling."""
    if not events or len({e["code"] for e in events})<5:
        return {"mean_delta_95pct_ci":None,"warning":"TOO_FEW_TICKER_CLUSTERS"}
    groups={}
    for e in events:
        groups.setdefault(e["code"],[]).append(
          e["outcomes"]["FIXED_R27_D3_CONFIRM"]["net_pct"]-
          e["outcomes"]["BASE_R25_D0LOW_CLOSE30"]["net_pct"])
    arr=list(groups.values())
    rng=np.random.default_rng(seed)
    means=[]
    for _ in range(n):
        indices=rng.integers(0,len(arr),size=len(arr))
        v=[item for i in indices for item in arr[i]]
        means.append(float(np.mean(v)))
    return {"mean_delta_95pct_ci":[
      round(float(x),6) for x in np.quantile(means,[.025,.975])],
      "n_cluster_tickers":len(groups),"method":"bootstrap_ticker_cluster_with_replacement",
      "uncertainty_does_not_correct_selection_bias":True}


def run(folder:Path,out:Path)->dict:
    data,manifest=load_pinned_source(folder)
    events,audit=frozen_true55_events(data)
    methods=("BASE_R25_D0LOW_CLOSE30","FIXED_R27_D3_CONFIRM")
    n_big=sum(e["expost_LABELS_NEVER_DECISION"]["full_D1_to_D60_high30"]
              for e in events)
    report={
      "revision":"R28_NEW_TICKERS_2026_LOCKED_TRUE55_R25_VS_R27_D3",
      "research_only":True,
      "source_run_id":SOURCE_RUN,"source_csv_sha256":SOURCE_CSV_SHA256,
      "source_repo":SOURCE_REPO,"valid_tickers":len(manifest["valid_codes"]),
      "source_price_rows":len(data),
      "frozen_new_ticker_candidate_codes":list(EXPECTED_CODES),
      "source_provider":"KIS_2025_2026_UNADJUSTED_UNCERTIFIED_COA",
      "2026_evaluation_only":True,"no_2025_result_scoring":True,
      "2025_warmup_embargo_carried_through":True,
      "max_holding_sessions":D60,"roundtrip_cost_pct":ROUNDTRIP_COST,
      "R19_entry_rule_reused":True,"R27_D3_exit_unmodified":True,
      "R25_baseline_exit_unmodified":True,
      "source_audit":audit,"completed_R19_true55_2026_event_n":len(events),
      "full_D1_to_D60_30pct_high_opportunities_EXPOST_n":n_big,
      "methods":{m:describe(events,m) for m in methods},
      "paired":paired(events),
      "uncertainty":ticker_cluster_bootstrap_ci(events),
      "evaluation_readiness":(
           "RETROSPECTIVE_TICKER_DISJOINT_HOLDOUT_DESCRIPTIVE_ONLY"
           if len(events)>=MIN_COMPLETE_EVENTS
           else "INSUFFICIENT_COMPLETED_EVENTS_NOT_EVALUABLE"),
      "not_pristine_prospective_OOS":True,
      "handpicked_liquid_stock_convenience_cohort":True,
      "market_wide_representative":False,
      "PIT_corporate_actions_and_listing_unverified":True,
      "suspensions_and_delisting_selection_bias_unresolved":True,
      "fills_are_next_open_without_impact_slippage":True,
      "no_model_or_threshold_tuning_on_2026":True,
      "no_search_live_watch_changes":True,
      "orders_placed":0,"eligible_for_live_promotion":False,
    }
    out.mkdir(parents=True,exist_ok=True)
    (out/"R28_TRUE55_DISJOINT_2026_SUMMARY.json").write_text(
      json.dumps(report,indent=2,ensure_ascii=False,allow_nan=False)+"\n",encoding="utf-8")
    (out/"R28_TRUE55_DISJOINT_2026_EVENTS.json").write_text(
      json.dumps(events,indent=2,ensure_ascii=False,allow_nan=False)+"\n",encoding="utf-8")
    return report


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source-dir",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(run(a.source_dir,a.out),ensure_ascii=False,
                     indent=2,allow_nan=False))
