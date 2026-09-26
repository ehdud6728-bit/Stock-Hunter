#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_11_ACCUMULATION_CANDLE_ANATOMY_20260926"

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs: raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def load_marcap(root):
    p=Path(root)/"data"/"marcap-2026.parquet"
    if not p.exists(): raise SystemExit(f"MISSING_MARCAP:{p}")
    q=pd.read_parquet(p)
    if "Date" not in q.columns: q=q.reset_index()
    q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
    q["Code"]=q["Code"].map(norm_code)
    q=q[q["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])].copy()
    return q

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r210-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    # Use the repository's existing/frozen helper directly.
    sys.path.insert(0,str(Path.cwd()))
    from scanner.accumulation_wave_complete import calc_accum_candle_score

    ev=pd.read_csv(find_one(a.r210_root,"r210_event_anatomy.csv"),dtype={"code":str},low_memory=False)
    ev["code"]=ev["code"].map(norm_code)
    ev["signal_date"]=pd.to_datetime(ev["signal_date"],errors="coerce").dt.normalize()
    ev["success_d5_touch10"]=ev["success_d5_touch10"].astype(str).str.lower().isin(["true","1"])

    mar=load_marcap(a.marcap_root)
    bycode={c:g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
            for c,g in mar.groupby("Code",sort=False)}

    rows=[]
    long=[]
    for _,r in ev.iterrows():
        g=bycode.get(r["code"])
        rec={
            "signal_date":r["signal_date"],"code":r["code"],"name":r.get("name",""),
            "success_d5_touch10":bool(r["success_d5_touch10"]),
            "outcome10":"SUCCESS_10P_D5" if r["success_d5_touch10"] else "FAIL_NO_10P_D5",
        }
        if g is None:
            rec["accum_data_status"]="NO_CODE_HISTORY"; rows.append(rec); continue

        hits=g.index[g["Date"].eq(r["signal_date"])]
        if len(hits)!=1:
            rec["accum_data_status"]="NO_SIGNAL_DATE"; rows.append(rec); continue
        i=int(hits[0])

        # End history at signal day. Existing helper therefore cannot see D+1 onward.
        hist=g.iloc[:i+1].copy().reset_index(drop=True)
        if len(hist)<10:
            rec["accum_data_status"]="INSUFFICIENT"; rows.append(rec); continue

        start=max(1,len(hist)-21)  # candidate candles approx D-20..D-1
        best=None
        count20=0
        count40=0
        count70=0
        for idx in range(start,len(hist)-1):
            info=calc_accum_candle_score(hist,idx)
            d=int((len(hist)-1)-idx)
            item={
                "signal_date":r["signal_date"],"code":r["code"],"name":r.get("name",""),
                "success_d5_touch10":bool(r["success_d5_touch10"]),
                "candidate_days_before_signal":d,
                "candidate_date":hist.iloc[idx]["Date"],
                **info
            }
            long.append(item)
            sc=float(info.get("score",0) or 0)
            if sc>=20: count20+=1
            if sc>=40: count40+=1
            if sc>=70: count70+=1
            if best is None or sc>float(best.get("score",0) or 0):
                best=item

        rec["accum_data_status"]="OK"
        rec["accum_candidate20_count"]=count20
        rec["accum_true40_count"]=count40
        rec["accum_strong70_count"]=count70
        if best:
            rec["best_accum_score"]=best.get("score")
            rec["best_accum_grade"]=best.get("grade")
            rec["best_accum_label"]=best.get("label")
            rec["best_accum_is_true40"]=bool(float(best.get("score",0) or 0)>=40)
            rec["best_accum_is_strong70"]=bool(float(best.get("score",0) or 0)>=70)
            rec["best_accum_days_before_signal"]=best.get("candidate_days_before_signal")
            rec["best_accum_date"]=best.get("candidate_date")
            rec["best_accum_vol_ratio"]=best.get("vol_ratio")
            rec["best_accum_close_pos"]=best.get("close_pos")
            rec["best_accum_lower_wick_ratio"]=best.get("lower_wick_ratio")
            rec["best_accum_upper_wick_ratio"]=best.get("upper_wick_ratio")
            rec["best_accum_desc"]=best.get("desc")
            rec["best_accum_high"]=best.get("high")
            rec["best_accum_low"]=best.get("low")
        rows.append(rec)

    z=pd.DataFrame(rows)
    l=pd.DataFrame(long)
    z.to_csv(out/"r211_accumulation_event_audit.csv",index=False,encoding="utf-8-sig")
    l.to_csv(out/"r211_all_presignal_candle_scores.csv",index=False,encoding="utf-8-sig")

    # Success vs failure comparison.
    summary=[]
    for label,g in [
        ("SUCCESS_10P_D5",z[z["success_d5_touch10"]]),
        ("FAIL_NO_10P_D5",z[~z["success_d5_touch10"]]),
        ("ALL",z),
    ]:
        n=len(g)
        summary.append({
            "group":label,
            "n":n,
            "any_candidate20_n":int(pd.to_numeric(g["accum_candidate20_count"],errors="coerce").fillna(0).gt(0).sum()),
            "any_candidate20_rate_pct":float(pd.to_numeric(g["accum_candidate20_count"],errors="coerce").fillna(0).gt(0).mean()*100) if n else np.nan,
            "any_true40_n":int(pd.to_numeric(g["accum_true40_count"],errors="coerce").fillna(0).gt(0).sum()),
            "any_true40_rate_pct":float(pd.to_numeric(g["accum_true40_count"],errors="coerce").fillna(0).gt(0).mean()*100) if n else np.nan,
            "any_strong70_n":int(pd.to_numeric(g["accum_strong70_count"],errors="coerce").fillna(0).gt(0).sum()),
            "any_strong70_rate_pct":float(pd.to_numeric(g["accum_strong70_count"],errors="coerce").fillna(0).gt(0).mean()*100) if n else np.nan,
            "best_score_median":float(pd.to_numeric(g["best_accum_score"],errors="coerce").median()),
            "true40_count_median":float(pd.to_numeric(g["accum_true40_count"],errors="coerce").median()),
            "best_days_before_signal_median":float(pd.to_numeric(g["best_accum_days_before_signal"],errors="coerce").median()),
            "best_vol_ratio_median":float(pd.to_numeric(g["best_accum_vol_ratio"],errors="coerce").median()),
            "best_lower_wick_median":float(pd.to_numeric(g["best_accum_lower_wick_ratio"],errors="coerce").median()),
            "best_close_pos_median":float(pd.to_numeric(g["best_accum_close_pos"],errors="coerce").median()),
        })
    s=pd.DataFrame(summary)
    s.to_csv(out/"r211_accumulation_success_failure_summary.csv",index=False,encoding="utf-8-sig")

    # Cross with R2.10 objective characteristics for inspection, not gating.
    cols=[c for c in ["signal_date","code","name","success_d5_touch10","Market","cap_bucket",
        "industry_group","theme_tags","amount_to_marcap_pct","d5_mfe_pct","d5_mae_pct",
        "best_accum_score","best_accum_grade","best_accum_days_before_signal",
        "accum_true40_count","accum_strong70_count","best_accum_desc"] if c in ev.columns or c in z.columns]
    merged=ev.merge(z.drop(columns=["name","success_d5_touch10"],errors="ignore"),
                    on=["signal_date","code"],how="left")
    audit_cols=[c for c in [
        "signal_date","code","name","outcome10","Market","cap_bucket","industry_group","theme_tags",
        "amount_to_marcap_pct","d5_mfe_pct","d5_mae_pct","d5_close_ret_pct",
        "best_accum_score","best_accum_grade","best_accum_days_before_signal",
        "accum_candidate20_count","accum_true40_count","accum_strong70_count",
        "best_accum_vol_ratio","best_accum_lower_wick_ratio","best_accum_close_pos","best_accum_desc"
    ] if c in merged.columns]
    merged[audit_cols].to_csv(out/"r211_accumulation_x_characteristics.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REV,
        "research_only":True,
        "production_logic_changed":False,
        "new_gate_created":False,
        "same_sample_tuning":False,
        "source_accumulation_logic":"scanner/accumulation_wave_complete.py::calc_accum_candle_score",
        "signal_day_or_earlier_only":True,
        "post_signal_data_used":False,
        "candidate_threshold":20,
        "true_accum_threshold":40,
        "strong_accum_threshold":70,
        "lookback_trading_days":20,
        "events":len(z),
        "success_n":int(z["success_d5_touch10"].sum()),
        "failure_n":int((~z["success_d5_touch10"]).sum()),
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    report=[
        "# ODOLI R2.11 — Accumulation Candle Anatomy","",
        "- Uses existing Stock-Hunter accumulation-candle scoring logic unchanged.",
        "- Candidate candle search: ~20 trading days before the signal.",
        "- History is truncated at signal day; no D+1 or later information is visible to the candle scorer.",
        "- Existing score interpretation: >=20 weak candidate, >=40 accumulation candle, >=70 strong.",
        "- Descriptive comparison only; no gate/promotion.","",
        "## Success vs failure",
        "```",s.to_string(index=False),"```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

if __name__=="__main__":
    main()
