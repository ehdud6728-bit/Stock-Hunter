#!/usr/bin/env python3
"""R30 descriptive anatomy of 30%-high winners versus other true55 entries.

RESEARCH_ONLY / exploratory. Reuse EXACT frozen R23 as-of-D0 feature
definitions on old149 AND new2026-36 ticker-disjoint events, plus original
R25 five D0 fields and exits. Show continuous ranks, baseline case rates,
factor sparsity, winner/loser overlap and D+3 fakeouts. The R30 hypotheses
were conceived AFTER seeing both cohorts; NEITHER is pristine validation.
Never manufacture new trade gates, retrain R25, tune thresholds, use future
labels as feature, place orders, or alter LIVE/SEARCH/WATCH.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

ROOT=Path(__file__).resolve().parents[1]
for name in ("trendline_true55_r23_entry_quality_20261123",
             "trendline_true55_r29_frozen_d0_20261124",
             "trendline_true55_r28_2026_oos_20261124",
             "trendline_true55_r25_big30_20261123"):
    sys.path.insert(0,str(ROOT/name))
import r23_entry_quality as r23
import r29_frozen_d0 as r29
import r28_2026_validation as r28
import r25_big30 as r25

OLD23={
  "events":("r23_quality/R23_149_ASOF_D0_FEATURE_AUDIT.json",
    "fb0d811258c7e685f2d48f67d589f28c9c13e4273509d474077ad31a4ccff3a2"),
  "summary":("r23_quality/R23_ENTRY_QUALITY_SUMMARY.json",
    "3351d2d20be466776a8a3dad5e3f41533640aa0445f9aa037166342d3d124685"),
}
OLD25=r29.R25_PATHS["events"]
NEW28=r29.R28_PATHS["events"]
NEW29={
  "events":("R29_2026_D0_SCORES_AND_PAIRED_EXITS.json",
   "9d25e84769b9e7503f325465cdfc80ba8025d7eb5ae4b7fbf8d4e84f76590b12"),
  "summary":("R29_FROZEN_D0_TRANSFER_SUMMARY.json",
   "c210ec5efb2bfd0114fa50a74fbb986f0d8d8e7681130396786d640d20757342"),
}
R25_EXIT=r25.FLOOR_30
NEW_BASE=r29.BASE
NEW_D3=r29.D3  # Exact stored R28/R29 outcome JSON keys, NOT policy IDs.
# 11 as-of-D0 hypotheses fixed prior to this automated review (after prior
# human inspection of the 2026 set; future samples mandatory for confirmation).
NUMERIC=(
  "return_prev20_pct",
  "first_wave_amplitude_pct",
  "d0_close_vs_ma224_pct",
  "pre20_amount_vs_previous60_ratio",
  "pre20_volume_vs_previous60_ratio",
  "d0_amount_vs_prior20_ratio",
  "d0_volume_vs_prior20_ratio",
  "pre_d0_pullback_depth_pct",
  "pre_d0_pullback_volume_ratio",
  "close_location",
  "breakout_vs_prior55_high_pct",
)
FACTORS=r23.FACTORS
EXAMPLE_N=15
FIXED_SEED=20261124
BOOTSTRAP_N=500


def read_pin(root:Path,pin:tuple[str,str]):
    relative,expected=pin
    buf=(root/relative).read_bytes()
    if hashlib.sha256(buf).hexdigest()!=expected:
        raise ValueError("R30_SOURCE_HASH_DRIFT:"+relative)
    return json.loads(buf)


def identity(r:dict):
    return str(r["code"]),str(r["date"])


def normalize_old(frozen23:list[dict],frozen25:list[dict])->list[dict]:
    if len(frozen23)!=149 or len(frozen25)!=149 or (
        [identity(r) for r in frozen23]!=[identity(r) for r in frozen25]):
        raise ValueError("R30_R23_R25_149_IDS_NOT_IDENTICAL")
    rows=[]
    for a,b in zip(frozen23,frozen25):
        f=dict(a["features_d0"])
        old5=b["d0_features_only"]
        if len(old5)!=5 or any(not np.isfinite(v) for v in old5):
            raise ValueError("R30_OLD_D0_FIVE_NOT_VALID")
        for key,x,y in (
           ("d0_amount_vs_prior20_ratio",f["d0_amount_vs_prior20_ratio"],old5[1]),
           ("d0_volume_vs_prior20_ratio",f["d0_volume_vs_prior20_ratio"],old5[2])):
            if x is None or abs(float(x)-float(y))>1e-5:
                raise ValueError("R30_R23_R25_D0_NUMERIC_DRIFT:"+key)
        if bool(a["labels_EXPOST"]["mfe30_full60"]) != bool(
                b["labels_EXPOST_NOT_FEATURES"]["big30_full60"]):
            raise ValueError("R30_R23_R25_EXPOST_HIGH30_DRIFT")
        f.update({"close_location":old5[0],
                  "return_prev20_pct":old5[3],
                  "breakout_vs_prior55_high_pct":old5[4]})
        rows.append({
           "code":str(a["code"]),"date":str(a["date"]),
           "year":int(a["year"]),"cohort":"ORIGINAL_2021_25_149",
           "features_ASOF_D0_ONLY":f,
           "expost_high30":bool(b["labels_EXPOST_NOT_FEATURES"]["big30_full60"]),
           "expost_full60_mfe_pct":float(b["labels_EXPOST_NOT_FEATURES"]["full60_mfe_pct"]),
           "R25_net_pct":float(b["outcomes"][R25_EXIT]["net_pct"]),
           "R25_exit_day":int(b["outcomes"][R25_EXIT]["exit_day"]),
           "R29_selected_frozen_D0":None,
        })
    if sum(r["expost_high30"] for r in rows)!=40:
        raise ValueError("R30_OLD149_HIGH30_40_DRIFT")
    return rows


def normalize_new(raw:pd.DataFrame,frozen28:list[dict],
                  frozen29:list[dict])->list[dict]:
    if (len(frozen28)!=36 or len(frozen29)!=36 or
        [identity(x) for x in frozen28]!=[identity(x) for x in frozen29]):
        raise ValueError("R30_NEW28_NEW29_36_IDS_DRIFT")
    frames={str(code):g.sort_values("date").reset_index(drop=True)
            for code,g in raw.groupby("code")}
    ids={code:{str(day):t for t,day in enumerate(g.date)}
         for code,g in frames.items()}
    out=[]
    for a,b in zip(frozen28,frozen29):
        # An algorithm's policy ID differs from the immutable R28 outcome
        # alias. Check both source JSONs before extraction; do not substitute.
        if (set(a.get("outcomes",{}))!={NEW_BASE,NEW_D3} or
            set(b.get("outcomes",{}))!={NEW_BASE,NEW_D3}):
            raise ValueError("R30_R28_R29_STORED_EXIT_ALIAS_DRIFT")
        code,date=identity(a)
        if code not in ids or date not in ids[code]:
            raise ValueError("R30_MISSING_FROZEN_2026_EVENT")
        g,t=frames[code],ids[code][date]
        f=r23.feature_asof_d0(g,t)  # Exact old R23 algorithm; NO variant.
        old5=b["R25_D0_features_only"]
        if len(old5)!=5 or len(r25.r24.FEATURES)!=5:
            raise ValueError("R30_NEW_FIVE_D0_MODEL_VECTOR_DRIFT")
        for label,actual,original in (
            ("amount",f["d0_amount_vs_prior20_ratio"],old5[1]),
            ("volume",f["d0_volume_vs_prior20_ratio"],old5[2])):
            if actual is None or abs(float(actual)-float(original))>1e-5:
                raise ValueError("R30_REUSED_R23_VS_R29_D0_FEATURE_MISMATCH_"+label)
        if (abs(float(g.high.iloc[t-55:t].max())-
                float(a["frozen_prior55_high"]))>1e-6 or
            abs(float(g.open.iloc[t+1])-float(a["entry_price"]))>1e-6 or
            b["expost_original_full60_intraday_high30"] !=
              a["expost_LABELS_NEVER_DECISION"]["full_D1_to_D60_high30"] or
            a["outcomes"]!=b["outcomes"]):
            raise ValueError("R30_LOCKED_2026_ENTRY_OR_OUTCOME_DRIFT")
        f.update({"close_location":old5[0],
                  "return_prev20_pct":old5[3],
                  "breakout_vs_prior55_high_pct":old5[4]})
        out.append({
          "code":code,"date":date,"year":int(date[:4]),
          "cohort":"NEW_2026_36",
          "features_ASOF_D0_ONLY":f,
          "expost_high30":bool(b["expost_original_full60_intraday_high30"]),
          "expost_full60_mfe_pct":float(
              a["expost_LABELS_NEVER_DECISION"]["full_D1_to_D60_high_pct"]),
          "R25_net_pct":float(b["outcomes"][NEW_BASE]["net_pct"]),
          "R25_exit_day":int(b["outcomes"][NEW_BASE]["exit_day"]),
          "R27_net_pct":float(b["outcomes"][NEW_D3]["net_pct"]),
          "R27_exit_day":int(b["outcomes"][NEW_D3]["exit_day"]),
          "R27_reason":str(b["outcomes"][NEW_D3]["exit_reason"]),
          "R29_selected_frozen_D0":bool(b["R25_D0_selected_TRAINONLY"]),
          "R25_frozen_D0_opportunity_score":float(b["R25_D0_score_TRAINONLY"]),
          "R25_frozen_D0_cutoff":float(b["R25_frozen_cutoff"]),
        })
    if sum(x["expost_high30"] for x in out)!=20 or sum(x["R29_selected_frozen_D0"] for x in out)!=28:
        raise ValueError("R30_NEW36_BIG30_20_SELECTED28_DRIFT")
    return out


def valid_float(v):
    try:
        x=float(v)
        return x if np.isfinite(x) else None
    except (ValueError,TypeError):
        return None


def auc_ticker_bootstrap(rows:list[dict],feature:str,n:int=BOOTSTRAP_N)->dict:
    valid=[r for r in rows if valid_float(r["features_ASOF_D0_ONLY"].get(feature)) is not None]
    label=np.array([r["expost_high30"] for r in valid],dtype=bool)
    val=np.array([r["features_ASOF_D0_ONLY"][feature] for r in valid],dtype=float)
    if len(valid)<15 or len(set(label))<2:
        return {"auc":None,"ticker_cluster_CI95":None,"n":len(valid)}
    observed=float(roc_auc_score(label,val))
    by_code={}
    for idx,r in enumerate(valid):
        by_code.setdefault(r["code"],[]).append(idx)
    codes=list(by_code)
    rng=np.random.default_rng(FIXED_SEED)
    boot=[]
    if len(codes)>=5:
        for _ in range(n):
            selected=rng.integers(0,len(codes),len(codes))
            ix=[j for code_index in selected for j in by_code[codes[int(code_index)]]]
            if len(set(label[ix]))<2:
                continue
            boot.append(float(roc_auc_score(label[ix],val[ix])))
    return {
      "n":len(valid),
      "auc":round(observed,4),
      "ticker_cluster_CI95":[round(float(q),4) for q in np.quantile(boot,[.025,.975])]
                             if len(boot)>=n*.75 else None,
      "bootstrap_valid_replicates":len(boot),
      "interpretation":"AUC_0.5_IS_NO_SEPARATION; AUC_LT_0.5_IS_INVERSE",
    }


def numeric_group(rows:list[dict],feature:str)->dict:
    valid=[r for r in rows if valid_float(r["features_ASOF_D0_ONLY"].get(feature)) is not None]
    big=[float(r["features_ASOF_D0_ONLY"][feature]) for r in valid if r["expost_high30"]]
    other=[float(r["features_ASOF_D0_ONLY"][feature]) for r in valid if not r["expost_high30"]]
    out=auc_ticker_bootstrap(rows,feature)
    out.update({
       "big30_n":len(big),"not_big30_n":len(other),
       "median_big30":round(statistics.median(big),6) if big else None,
       "median_not_big30":round(statistics.median(other),6) if other else None,
       "median_delta":round(statistics.median(big)-statistics.median(other),6)
                      if big and other else None,
       "analysis_only_predefined_original_R23_or_R25_feature":True,
    })
    return out


def categorical_group(rows:list[dict],factor:str)->dict:
    groups={}
    for value in ("YES","NO","UNKNOWN"):
        subset=[r for r in rows if r["features_ASOF_D0_ONLY"]["factors"][factor]==value]
        n=len(subset)
        big=sum(r["expost_high30"] for r in subset)
        actual=sum(r["R25_net_pct"]>=30 for r in subset)
        losses=sum(r["R25_net_pct"]<=-15 for r in subset)
        groups[value]={
          "n":n,"n_unique_tickers":len({r["code"] for r in subset}),
          "expost_intraday_high30_n":big,
          "expost_high30_pct":round(100*big/n,3) if n else None,
          "R25_actual_net30_n":actual,"R25_actual_loss15_n":losses,
          "mean_R25_net_pct":round(statistics.mean(
               r["R25_net_pct"] for r in subset),6) if n else None,
        }
    return groups


def compare_cohorts(old:list[dict],new:list[dict])->dict:
    cohorts={
      "ORIGINAL_149":old,
      "ORIGINAL_TRAIN_TO_2023Q3":[r for r in old if r["date"]<=r25.TRAIN_END],
      "ORIGINAL_REVIEW_2024_25":[r for r in old if r["date"]>=r25.REVIEW_START],
      "NEW_2026_36":new,
      "NEW_2026_R25_SELECTED_28":[r for r in new if r["R29_selected_frozen_D0"]],
      "NEW_2026_R25_REJECTED_8":[r for r in new if not r["R29_selected_frozen_D0"]],
    }
    out={}
    for label,subset in cohorts.items():
        out[label]={
          "n":len(subset),"n_tickers":len({r["code"] for r in subset}),
          "expost_full60_intraday_high30_n":sum(r["expost_high30"] for r in subset),
          "actual_R25_net30_n":sum(r["R25_net_pct"]>=30 for r in subset),
          "actual_R25_loss10_n":sum(r["R25_net_pct"]<=-10 for r in subset),
          "actual_R25_loss15_n":sum(r["R25_net_pct"]<=-15 for r in subset),
          "numeric":{f:numeric_group(subset,f) for f in NUMERIC},
          "categorical":{f:categorical_group(subset,f) for f in FACTORS},
        }
    consistent={}
    a=out["ORIGINAL_149"]["numeric"];b=out["NEW_2026_36"]["numeric"]
    for feature in NUMERIC:
        x,y=a[feature]["auc"],b[feature]["auc"]
        if x is not None and y is not None:
            same=(x>.5 and y>.5) or (x<.5 and y<.5)
            consistent[feature]={
              "old149_auc":x,"new36_auc":y,
              "same_direction":same,
              "both_above_random":x>.5 and y>.5,
              "both_auc_ge_0_6":x>=.6 and y>=.6,
              "min_observed_auc":round(min(x,y),4),
              "NOT_a_confirmed_screening_rule":True,
            }
    return {"cohorts":out,"directional_consistency_exploratory":consistent}


def d3_false_negative_audit(raw:pd.DataFrame,events28:list[dict],
                            events29:list[dict])->dict:
    """Known at D3 close versus future D4+ high. Never a D0 feature."""
    frames={str(c):g.sort_values("date").reset_index(drop=True)
            for c,g in raw.groupby("code")}
    positions={code:{day:i for i,day in enumerate(g.date)}
               for code,g in frames.items()}
    entries=[]
    for a,b in zip(events28,events29):
        if identity(a)!=identity(b):
            raise ValueError("R30_D3_EVENT_ORDER_DRIFT")
        code,date=identity(a);g=frames[code];t=positions[code][date]
        entry=float(a["entry_price"]);level=float(a["frozen_prior55_high"])
        d3close=float(g.close.iloc[t+3])
        d3high=float(g.high.iloc[t+3])
        d3low=float(g.low.iloc[t+3])
        r25out=a["outcomes"][NEW_BASE]
        r27out=a["outcomes"][NEW_D3]
        # No ex-post return labels enter event selection or method checks.
        still_held_baseline=(r25out["exit_reason"]=="WINDOW_END_CLOSE" or
                             int(r25out["exit_day"])>4)
        # D3 end decision; only bars D4 through D60 are post-D3.
        forward=g.iloc[t+4:t+61]
        if len(forward)!=57:
            raise ValueError("R30_D3_FORWARD_WINDOW_NOT_57")
        first30next=None
        for day in range(4,61):
            if float(g.high.iloc[t+day])>=entry*1.30:
                first30next=day;break
        d4open=float(g.open.iloc[t+4])
        entries.append({
          "code":code,"date":date,
          "R25_selected_D0":bool(b["R25_D0_selected_TRAINONLY"]),
          "D3_close_above_original_D1_entry":d3close>entry,
          "D3_close_above_prior55":d3close>level,
          "D3_close_vs_entry_pct_ASOF":round(100*(d3close/entry-1),6),
          "D3_close_vs_prior55_pct_ASOF":round(100*(d3close/level-1),6),
          "D3_candle_upper_wick_pct_ASOF":round(
            100*(d3high-d3close)/(d3high-d3low),6) if d3high>d3low else None,
          "baseline_R25_still_held_without_pending_next_open_exit_at_D3":
              still_held_baseline,
          "R27_D3_confirmation_failed_and_sold_D4":
              r27out["exit_reason"]=="D3_CONFIRMATION_FAILED" and
              int(r27out["exit_day"])==4,
          "expost_D4_to_D60_high30_vs_original_entry":
              bool(first30next is not None),
          "expost_first_D4plus_high30_day_from_D0":first30next,
          "expost_D4_to_D60_high_pct_vs_D4open":
              round(100*(float(forward.high.max())/d4open-1),6),
          "expost_R25_realized_net30":bool(r25out["net_pct"]>=30),
          "expost_R25_realized_loss15":bool(r25out["net_pct"]<=-15),
          "R25_actual_net_pct":float(r25out["net_pct"]),
          "R27_actual_net_pct":float(r27out["net_pct"]),
        })
    survivors=[r for r in entries if r["baseline_R25_still_held_without_pending_next_open_exit_at_D3"]]
    failure=[r for r in survivors if r["R27_D3_confirmation_failed_and_sold_D4"]]
    passers=[r for r in survivors if not r["R27_D3_confirmation_failed_and_sold_D4"]]
    # At D+3 close the original entry versus 55-day breakout level are
    # different known price facts. This subgroup was identified AFTER seeing
    # the 2026 results: explanatory ONLY, never a newly tuned exit policy.
    failure_level_held=[r for r in failure if r["D3_close_above_prior55"]]
    failure_level_lost=[r for r in failure if not r["D3_close_above_prior55"]]
    def describe(rows):
        return {
          "n":len(rows),
          "R25_actual_realized_net30_EXPOST":sum(x["expost_R25_realized_net30"] for x in rows),
          "R25_actual_loss15_EXPOST":sum(x["expost_R25_realized_loss15"] for x in rows),
          "D4_to_D60_future_high30_vs_original_entry_EXPOST":sum(
             x["expost_D4_to_D60_high30_vs_original_entry"] for x in rows),
          "D3_close_vs_entry_pct_median_ASOF":round(statistics.median(
              x["D3_close_vs_entry_pct_ASOF"] for x in rows),6) if rows else None,
        }
    return {
      "D3_asof_survivor_only":True,
      "forward_window_strictly_D4_to_D60":True,
      "not_a_new_D3_exit_or_entry_policy":True,
      "entire_new_36_D3_case_records":entries,
      "R25_still_held_without_pending_D4_sell":describe(survivors),
      "R27_D3_failed_among_R25_survivors":describe(failure),
      "R27_D3_failed_but_original55_level_held_EXPLORATORY":describe(failure_level_held),
      "R27_D3_failed_and_original55_level_lost_EXPLORATORY":describe(failure_level_lost),
      "R27_D3_passed_among_R25_survivors":describe(passers),
      "D3_level_subgroup_created_after_looking_at_2026_outcomes_not_a_rule":True,
      "R27_D3_failed_but_R25_actual_net30_case_ids":[
          {"code":r["code"],"date":r["date"],
           "R25_net_pct":r["R25_actual_net_pct"],
           "R27_net_pct":r["R27_actual_net_pct"],
           "D3_close_vs_entry_pct_ASOF":r["D3_close_vs_entry_pct_ASOF"],
           "first_future30_high_day_EXPOST":r["expost_first_D4plus_high30_day_from_D0"]}
          for r in failure if r["expost_R25_realized_net30"]],
      "D3_survivorship_bias_warning":True,
    }


def run(r23_dir:Path,r25_dir:Path,r28_dir:Path,r29_dir:Path,
        blue_dir:Path,out:Path)->dict:
    old23=read_pin(r23_dir,OLD23["events"])
    meta23=read_pin(r23_dir,OLD23["summary"])
    old25=read_pin(r25_dir,OLD25)
    old28=read_pin(r28_dir,NEW28)
    new29=read_pin(r29_dir,NEW29["events"])
    meta29=read_pin(r29_dir,NEW29["summary"])
    if (meta23.get("frozen_r19_after_embargo_dplus1_entry_events")!=149 or
        meta23.get("source_kis_run_id")!=r25.r20.KIS_RUN or
        meta23.get("predeclared_descriptive_thresholds",{}).get(
          "wave_min_amplitude_pct")!=r23.WAVE_MIN_PCT or
        len(old28)!=36 or len(new29)!=36 or
        meta29["new_2026_locked_event_count"]!=36 or
        meta29["training_replay"]["replayed_original_R25_review_scores_identical"] is not True or
        meta29["BLUE_raw_CSV_sha256"]!=r28.SOURCE_CSV_SHA256 or
        meta29["cohorts"]["FROZEN_D0_SELECTED"]["n"]!=28):
        raise ValueError("R30_ORIGINAL_R23_R25_R29_SOURCE_AUDIT_GATE_FAIL")
    raw,manifest=r28.load_pinned_source(blue_dir)
    old=normalize_old(old23,old25)
    new=normalize_new(raw,old28,new29)
    if len(raw)!=13311 or len(manifest["valid_codes"])!=29:
        raise ValueError("R30_NEW_BLUE_SOURCE_ROW_COUNT_DRIFT")
    feature_audit=compare_cohorts(old,new)
    d3=d3_false_negative_audit(raw,old28,new29)
    summary={
      "revision":"R30_BIG30_WINNER_VS_OTHER_TRUE55_D0_PATTERN_RESEARCH",
      "research_only":True,
      "all_hypotheses_exploratory_after_inspecting_2021_26":True,
      "R23_ASOF_D0_features_reused_unchanged":True,
      "R25_five_D0_features_from_original_train_model_reused":True,
      "R25_exits_as_written_no_replay_policy_change":True,
      "R28_R29_2026_entry_and_D0_score_unchanged":True,
      "old_149_source_KIS_run":r25.r20.KIS_RUN,
      "new_source_BLUE_capture_run":r28.SOURCE_RUN,
      "new_source_CSV_sha256":r28.SOURCE_CSV_SHA256,
      "original_R23_149_JSON_sha256":OLD23["events"][1],
      "original_R25_149_JSON_sha256":OLD25[1],
      "original_R28_36_JSON_sha256":NEW28[1],
      "original_R29_36_JSON_sha256":NEW29["events"][1],
      "numeric_features_tested":list(NUMERIC),
      "categorical_R23_original_factors":list(FACTORS),
      "cohort_feature_audits":feature_audit,
      "D3_confirm_false_negative_case_audit":d3,
      "opportunity_label":"MAX_INTRADAY_HIGH_D1_TO_D60_GTE_ENTRY_OPEN_X1_30_EXPOST_ONLY",
      "realized_profit_label":"FIXED_R25_EXIT_NET_GTE30_PCT",
      "loss_tail_label":"FIXED_R25_EXIT_NET_LTE_MINUS15_PCT",
      "treat_AUC_as_description_not_model_validation":True,
      "feature_multiplicity_and_correlated_ticker_risk":True,
      "no_cutoff_optimization_or_rule_promotion":True,
      "old_149_and_new_36_not_pristine_OOS":True,
      "new_2026_handpicked_29_ticker_universe":True,
      "corporate_action_undercertainty":True,
      "live_search_watch_changed":False,"orders_placed":0,
      "can_publish_trade_edge":False,
    }
    out.mkdir(parents=True,exist_ok=True)
    (out/"R30_WINNER_PATTERN_SUMMARY.json").write_text(
       json.dumps(summary,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    (out/"R30_185_EVENT_D0_FEATURES_AND_OUTCOMES.json").write_text(
       json.dumps(old+new,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    (out/"R30_2026_D3_FIRST30_CASES.json").write_text(
       json.dumps(d3["entire_new_36_D3_case_records"],ensure_ascii=False,
                  indent=2,allow_nan=False)+"\n",encoding="utf-8")
    return summary


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    for field in ("r23-dir","r25-dir","r28-dir","r29-dir","blue-dir","out"):
        p.add_argument("--"+field,type=Path,required=True)
    a=p.parse_args()
    summary=run(a.r23_dir,a.r25_dir,a.r28_dir,a.r29_dir,a.blue_dir,a.out)
    compact={
       "revision":summary["revision"],
       "source_sha":summary["new_source_CSV_sha256"],
       "directional_consistency":summary["cohort_feature_audits"]["directional_consistency_exploratory"],
       "cohort_counts":{k:{"n":v["n"],
                           "big30":v["expost_full60_intraday_high30_n"],
                           "realized30":v["actual_R25_net30_n"]}
                        for k,v in summary["cohort_feature_audits"]["cohorts"].items()},
       "D3_false_negative_cases":summary["D3_confirm_false_negative_case_audit"][
                                     "R27_D3_failed_but_R25_actual_net30_case_ids"],
    }
    print(json.dumps(compact,ensure_ascii=False,indent=2,allow_nan=False))
