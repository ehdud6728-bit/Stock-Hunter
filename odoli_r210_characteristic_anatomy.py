#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_10_CHARACTERISTIC_ANATOMY_20260926"

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs: raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def cap_bucket(x):
    if pd.isna(x): return "UNKNOWN"
    eok=x/1e8
    if eok < 2000: return "<2000억"
    if eok < 5000: return "2000~5000억"
    if eok < 10000: return "5000억~1조"
    if eok < 30000: return "1~3조"
    return "3조+"

def med(x):
    x=pd.to_numeric(x,errors="coerce").dropna()
    return float(x.median()) if len(x) else np.nan

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r29-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--tags",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    z=pd.read_csv(find_one(a.r29_root,"prediscovery_core_odoli_events.csv"),dtype={"code":str},low_memory=False)
    z["code"]=z["code"].map(norm_code)
    z["signal_date"]=pd.to_datetime(z["signal_date"],errors="coerce").dt.normalize()
    z["success_d5_touch10"]=z["d5_touch_10"].astype(str).str.lower().isin(["true","1"])
    z["outcome10"]=np.where(z["success_d5_touch10"],"SUCCESS_10P_D5","FAIL_NO_10P_D5")

    p=Path(a.marcap_root)/"data"/"marcap-2026.parquet"
    m=pd.read_parquet(p)
    if "Date" not in m.columns: m=m.reset_index()
    m["Date"]=pd.to_datetime(m["Date"],errors="coerce").dt.normalize()
    m["Code"]=m["Code"].map(norm_code)
    keep=[c for c in ["Date","Code","Market","Marcap","Amount","Volume","Close","Name"] if c in m.columns]
    m=m[keep].copy()
    m=m.sort_values(["Date","Code"]).drop_duplicates(["Date","Code"],keep="last")
    z=z.merge(m,left_on=["signal_date","code"],right_on=["Date","Code"],how="left",validate="one_to_one")

    z["cap_bucket"]=pd.to_numeric(z["Marcap"],errors="coerce").map(cap_bucket)
    z["amount_to_marcap_pct"]=pd.to_numeric(z["Amount"],errors="coerce") / pd.to_numeric(z["Marcap"],errors="coerce") * 100

    tags=pd.read_csv(a.tags,dtype={"code":str},low_memory=False)
    tags["code"]=tags["code"].map(norm_code)
    z=z.merge(tags,on=["code"],how="left",validate="many_to_one")
    for c in ["industry_group","theme_tags","tag_confidence","tag_note"]:
        if c not in z.columns: z[c]=""
        z[c]=z[c].fillna("")

    z.to_csv(out/"r210_event_anatomy.csv",index=False,encoding="utf-8-sig")

    # Market summary
    rows=[]
    for col in ["Market","cap_bucket","industry_group"]:
        for val,g in z.groupby(col,dropna=False):
            rows.append({
                "dimension":col,"label":str(val),
                "n":len(g),
                "success_n":int(g["success_d5_touch10"].sum()),
                "success_rate_pct":float(g["success_d5_touch10"].mean()*100),
                "d5_mfe_median_pct":med(g["d5_mfe_pct"]),
                "d5_mae_median_pct":med(g["d5_mae_pct"]),
                "d5_close_median_pct":med(g["d5_close_ret_pct"]),
                "marcap_median_eok":med(pd.to_numeric(g["Marcap"],errors="coerce")/1e8),
                "amount_median_eok":med(pd.to_numeric(g["Amount"],errors="coerce")/1e8),
                "amount_to_marcap_median_pct":med(g["amount_to_marcap_pct"]),
            })
    pd.DataFrame(rows).to_csv(out/"categorical_anatomy_summary.csv",index=False,encoding="utf-8-sig")

    # Multi-label theme summary
    theme_rows=[]
    exploded=[]
    for _,r in z.iterrows():
        ts=[x.strip() for x in str(r["theme_tags"]).split("|") if x.strip()]
        if not ts: ts=["UNTAGGED"]
        for t in ts:
            exploded.append({
                "signal_date":r["signal_date"],"code":r["code"],"name":r.get("name",""),
                "theme":t,"success_d5_touch10":r["success_d5_touch10"],
                "d5_mfe_pct":r.get("d5_mfe_pct"),"d5_mae_pct":r.get("d5_mae_pct"),
                "d5_close_ret_pct":r.get("d5_close_ret_pct"),
                "Marcap":r.get("Marcap"),"Amount":r.get("Amount"),
                "amount_to_marcap_pct":r.get("amount_to_marcap_pct"),
            })
    ex=pd.DataFrame(exploded)
    ex.to_csv(out/"theme_event_long.csv",index=False,encoding="utf-8-sig")
    for t,g in ex.groupby("theme"):
        theme_rows.append({
            "theme":t,"n":len(g),
            "success_n":int(g["success_d5_touch10"].sum()),
            "success_rate_pct":float(g["success_d5_touch10"].mean()*100),
            "d5_mfe_median_pct":med(g["d5_mfe_pct"]),
            "d5_mae_median_pct":med(g["d5_mae_pct"]),
            "d5_close_median_pct":med(g["d5_close_ret_pct"]),
            "marcap_median_eok":med(pd.to_numeric(g["Marcap"],errors="coerce")/1e8),
            "amount_to_marcap_median_pct":med(g["amount_to_marcap_pct"]),
        })
    pd.DataFrame(theme_rows).sort_values(["n","success_rate_pct"],ascending=[False,False]).to_csv(
        out/"theme_anatomy_summary.csv",index=False,encoding="utf-8-sig")

    # Success vs failure continuous variables
    cont=["Marcap","Amount","amount_to_marcap_pct","ma5_ma10_gap_pct_r23",
          "down_up_amount_ratio_10d","down_up_volume_ratio_10d",
          "d5_mfe_pct","d5_mae_pct","d5_close_ret_pct"]
    rr=[]
    for c in cont:
        s=z[z["success_d5_touch10"]]
        f=z[~z["success_d5_touch10"]]
        sm=med(s[c]); fm=med(f[c])
        if c in ["Marcap","Amount"]:
            sm/=1e8 if pd.notna(sm) else 1
            fm/=1e8 if pd.notna(fm) else 1
        rr.append({
            "feature":c,
            "success_n":len(s),"failure_n":len(f),
            "success_median":sm,"failure_median":fm,
            "median_diff_success_minus_failure":sm-fm if pd.notna(sm) and pd.notna(fm) else np.nan
        })
    pd.DataFrame(rr).to_csv(out/"success_failure_continuous_comparison.csv",index=False,encoding="utf-8-sig")

    # Name-level audit table for human review
    audit_cols=[c for c in [
        "signal_date","code","name","outcome10","Market","Marcap","Amount","amount_to_marcap_pct",
        "cap_bucket","industry_group","theme_tags","tag_confidence","tag_note",
        "d5_mfe_pct","d5_mae_pct","d5_close_ret_pct"
    ] if c in z.columns]
    z[audit_cols].sort_values(["outcome10","signal_date"]).to_csv(
        out/"success_failure_name_audit.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REV,"research_only":True,"production_logic_changed":False,
        "same_sample_tuning":False,"new_gate_created":False,
        "success_definition":"D5_HIGH_TOUCH_GE_10P",
        "n":len(z),"success_n":int(z["success_d5_touch10"].sum()),
        "failure_n":int((~z["success_d5_touch10"]).sum()),
        "theme_tags_role":"MANUAL_RESEARCH_MULTI_LABEL_DESCRIPTIVE_ONLY"
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    (out/"REPORT.md").write_text(
        "# ODOLI R2.10 — Characteristic Anatomy\n\n"
        f"- CORE+ODOLI events: {len(z)}\n"
        f"- D+5 +10% touch success: {int(z['success_d5_touch10'].sum())}\n"
        f"- failure: {int((~z['success_d5_touch10']).sum())}\n"
        "- Market/cap/liquidity are signal-date PIT values.\n"
        "- Theme tags are manual multi-label research descriptors, not authoritative exchange classifications.\n"
        "- No feature is promoted into a gate in this run.\n",
        encoding="utf-8"
    )

if __name__=="__main__":
    main()
