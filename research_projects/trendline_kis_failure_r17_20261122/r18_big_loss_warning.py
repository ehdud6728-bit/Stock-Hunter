#!/usr/bin/env python3
"""R18: pre-entry big-loss warning, with ticker-disjoint temporal stress test.

RESEARCH_ONLY; no real-order code, no percent stops/targets, no winner promotion.
The threshold labels the worst tenth of TRAINING R14 outcomes. It is NOT a stop.
Models consume only D0 closing information and optionally D+1 opening prices.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

FEATURES_D0 = ("close_location", "amount_vs_prev20", "volume_vs_prev20",
               "close_vs_ma224_pct", "return_prev20_pct", "breakout_vs_prior55_high_pct")
FEATURES_DPLUS1_OPEN = ("open_gap_vs_D0_close_pct", "open_position_vs_D0_low_ATR")
TRAIN_END = "2023-12-31"
REVIEW_START = "2024-01-01"
N_FOLDS = 5


def gap_at_open(frame: pd.DataFrame, t: int) -> dict:
    """Only D0 and D+1 OPEN are ever consulted here."""
    d0 = frame.iloc[t]
    opening = float(frame.iloc[t+1].open)
    prev = frame.close.shift(1)
    tr = pd.concat((frame.high-frame.low, (frame.high-prev).abs(),
                    (frame.low-prev).abs()),axis=1)
    atr = float(tr.max(axis=1).iloc[max(0,t-13):t+1].mean())
    return {
        "open_gap_vs_D0_close_pct": 100*(opening/float(d0.close)-1),
        "open_position_vs_D0_low_ATR": (opening-float(d0.low))/atr if atr>0 else None,
    }


def features_array(rows: list[dict], features: tuple[str,...]) -> np.ndarray:
    return np.array([[np.nan if r["features"].get(k) is None else float(r["features"][k])
                      for k in features] for r in rows],dtype=float)


def classifier():
    return make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),
                         LogisticRegression(C=.25,solver="liblinear",max_iter=1000))


def net(row: dict) -> float:
    return float(row["outcomes"]["R14_INTRADAY"]["net_pct"])


def train_tail_threshold(rows: list[dict]) -> float:
    return float(np.quantile([net(r) for r in rows],.10))


def fold_of_code(code: str) -> int:
    return int(hashlib.sha256(("R18_UNSEEN_CODE_20261122|"+code).encode()).hexdigest()[:8],16)%N_FOLDS


def describe_review(pred: np.ndarray, labels: np.ndarray, cutoff: float, nets: np.ndarray) -> dict:
    high=pred>=cutoff
    def group(mask):
        vals=nets[mask]
        return {"n":len(vals),"tail_n":int(labels[mask].sum()),
                "mean_net_pct":round(float(np.mean(vals)),6),
                "median_net_pct":round(float(np.median(vals)),6),
                "p10_net_pct":round(float(np.quantile(vals,.10)),6),
                "win_rate_pct":round(100*float(np.mean(vals>0)),3)}
    return {"n":len(labels),"tail_n":int(labels.sum()),
            "auc":round(float(roc_auc_score(labels,pred)),6),
            "average_precision":round(float(average_precision_score(labels,pred)),6),
            "high_risk":group(high),"retained":group(~high),"all":group(np.ones(len(labels),dtype=bool))}


def analyze(r16_out: Path, r17_out: Path, out: Path) -> dict:
    manifest=json.loads((r16_out/"R16_SOURCE_MANIFEST.json").read_text())
    r17=json.loads((r17_out/"R17_SUMMARY.json").read_text())
    if manifest.get("kis_run_id",manifest.get("kis_source_run_id"))!="44988188271" or r17["r17_events"]!=425:
        raise ValueError("MISMATCHED_OR_INCOMPLETE_FROZEN_R16_R17_PROVENANCE")
    source=r16_out/"R16_FROZEN_HOLDOUT_UNADJUSTED_OHLCV.csv.gz"
    if hashlib.sha256(source.read_bytes()).hexdigest()!=manifest["raw_archive_sha256"]:
        raise ValueError("PRICE_INPUT_SHA_DRIFT")
    price=pd.read_csv(source,dtype={"code":str,"date":str})
    by_code={c:g.sort_values("date").reset_index(drop=True) for c,g in price.groupby("code")}
    by_date={c:{date:i for i,date in enumerate(g.date)} for c,g in by_code.items()}
    rows=json.loads((r17_out/"R17_EVENTS.json").read_text())
    if len(rows)!=425 or len({(r["code"],r["date"]) for r in rows})!=425:
        raise ValueError("UNIQUE_R17_SIGNALS_CHANGED")
    for r in rows:
        t=by_date[r["code"]][r["date"]]
        if t+1>=len(by_code[r["code"]]):
            raise ValueError("MISSING_ENTRY_OPEN")
        r["features"]={**r["features"],**gap_at_open(by_code[r["code"]],t)}

    train=[r for r in rows if r["date"]<=TRAIN_END]
    review=[r for r in rows if r["date"]>=REVIEW_START]
    if len(train)!=275 or len(review)!=150:
        raise ValueError("FROZEN_R16_TEMPORAL_COHORT_DRIFT")
    cutoff=train_tail_threshold(train)
    original_review_labels=np.array([int(net(r)<=cutoff) for r in review])
    if int(original_review_labels.sum())!=12:
        raise ValueError("FROZEN_LOSS_TAIL_COHORT_DRIFT")
    diagnostics={}
    for name,features in (("D0_SIX",FEATURES_D0),
                          ("D0_PLUS_NEXT_OPEN",FEATURES_D0+FEATURES_DPLUS1_OPEN)):
        yt=np.array([int(net(r)<=cutoff) for r in train])
        fitted=classifier().fit(features_array(train,features),yt)
        train_scores=fitted.predict_proba(features_array(train,features))[:,1]
        holdout_scores=fitted.predict_proba(features_array(review,features))[:,1]
        risk_cut=float(np.quantile(train_scores,.75))
        diagnostics[name]={"features":features,"train_n":len(train),
                           "train_tail_n":int(yt.sum()),
                           "train_tail_threshold_net_pct":round(cutoff,6),
                           "review":describe_review(holdout_scores,original_review_labels,risk_cut,
                                                    np.array([net(r) for r in review]))}

    # Every review ticker is absent from the entire training fit in its fold.
    # The high-risk quartile and tail threshold are calculated inside TRAINING.
    per_fold=[];pooled=[]
    for fold in range(N_FOLDS):
        tr=[r for r in train if fold_of_code(r["code"])!=fold]
        te=[r for r in review if fold_of_code(r["code"])==fold]
        if not tr or not te or set(r["code"] for r in tr)&set(r["code"] for r in te):
            raise AssertionError("NONDISJOINT_OR_EMPTY_CODE_FOLD")
        q10=train_tail_threshold(tr)
        yt=np.array([int(net(r)<=q10) for r in tr])
        clf=classifier().fit(features_array(tr,FEATURES_D0),yt)
        train_pred=clf.predict_proba(features_array(tr,FEATURES_D0))[:,1]
        test_pred=clf.predict_proba(features_array(te,FEATURES_D0))[:,1]
        top_cut=float(np.quantile(train_pred,.75))
        pooled.extend({"code":r["code"],"date":r["date"],"score":float(p),
                       "is_historical_tail":int(net(r)<=q10),"high_risk":bool(p>=top_cut),
                       "net_pct":net(r)} for p,r in zip(test_pred,te))
        per_fold.append({"fold":fold,"n_train":len(tr),"n_unseen_ticker_review":len(te),
                         "train_tail_threshold_net_pct":round(q10,6),
                         "tail_review_n":sum(net(r)<=q10 for r in te)})
    assert len(pooled)==len(review)
    yp=np.array([r["is_historical_tail"] for r in pooled]);p=np.array([r["score"] for r in pooled])
    selected=np.array([r["high_risk"] for r in pooled]);netvals=np.array([r["net_pct"] for r in pooled])
    # Different folds have different predeclared train-only quantiles; the
    # pooled within-fold risk group is used for summary, not a global cutoff.
    def values(mask):
        v=netvals[mask]
        return {"n":len(v),"tail_n":int(yp[mask].sum()),
                "mean_net_pct":round(float(np.mean(v)),6),
                "median_net_pct":round(float(np.median(v)),6),
                "p10_net_pct":round(float(np.quantile(v,.10)),6),
                "win_rate_pct":round(100*float(np.mean(v>0)),3)}
    stress={"n":len(pooled),"auc":round(float(roc_auc_score(yp,p)),6),
            "high_risk":values(selected),"retained":values(~selected),
            "all":values(np.ones(len(yp),dtype=bool)),"folds":per_fold}
    report={"revision":"R18_KIS_D0_BIG_LOSS_TAIL_WARNING_RESEARCH_ONLY",
            "source_kis_run_id":"44988188271","source_r16_price_sha256":manifest["raw_archive_sha256"],
            "source_r17_event_count":len(rows),"train_period":"2021-2023",
            "review_period":"2024-2025","review_not_pristine_out_of_sample":True,
            "tail_definition":"TRAIN_2021_2023_LOWER_TENTH_OF_FROZEN_R14_NET_RETURN_NOT_A_STOP",
            "unseen_ticker_temporal_5fold":stress,"secondary_comparisons":diagnostics,
            "source_corporate_actions_independently_certified":False,
            "representative_full_market_sample":False,"only_12_future_period_tail_events":True,
            "no_hyperparameter_search":True,"no_trade_policy_promotion":True,
            "live_search_watch_unchanged":True,"orders_placed":0,"can_publish_edge":False}
    out.mkdir(parents=True,exist_ok=True)
    (out/"R18_SUMMARY.json").write_text(json.dumps(report,indent=2,ensure_ascii=False,allow_nan=False)+"\n")
    (out/"R18_UNSEEN_TICKER_TEMPORAL_SCORES.json").write_text(
        json.dumps(pooled,indent=2,ensure_ascii=False,allow_nan=False)+"\n")
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--r16-out",required=True)
    p.add_argument("--r17-out",required=True)
    p.add_argument("--out",required=True)
    args=p.parse_args()
    report=analyze(Path(args.r16_out),Path(args.r17_out),Path(args.out))
    print("R18_RESULT",json.dumps({"n":report["unseen_ticker_temporal_5fold"]["n"],
                                   "auc":report["unseen_ticker_temporal_5fold"]["auc"],
                                   "high_risk":report["unseen_ticker_temporal_5fold"]["high_risk"],
                                   "retained":report["unseen_ticker_temporal_5fold"]["retained"]},
                                  ensure_ascii=False))


if __name__=="__main__":main()
