#!/usr/bin/env python3
from pathlib import Path
import argparse, json
import numpy as np
import pandas as pd

REV="ODOLI_A_OVERLAP_R2_AUDIT_20260925"

def find(root,name):
    xs=list(Path(root).rglob(name))
    if not xs: raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(s):
    s=str(s).replace(".0","").strip()
    return s.zfill(6)

def complete_rate(s,cut):
    x=pd.to_numeric(s,errors="coerce").dropna()
    return float((x>=cut).mean()*100) if len(x) else np.nan

def pos_rate(s):
    x=pd.to_numeric(s,errors="coerce").dropna()
    return float((x>0).mean()*100) if len(x) else np.nan

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--odoli-root",required=True)
    ap.add_argument("--r13-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    od=pd.read_csv(find(a.odoli_root,"odoli_all_market_events.csv"),dtype={"code":str},low_memory=False)
    r13=pd.read_csv(find(a.r13_root,"shadow_outcome_refined_events.csv"),dtype={"code":str},low_memory=False)
    od["code"]=od["code"].map(norm_code)
    r13["code"]=r13["code"].map(norm_code)
    od["signal_date"]=pd.to_datetime(od["signal_date"]).dt.strftime("%Y-%m-%d")
    r13["signal_date"]=pd.to_datetime(r13["signal_date"]).dt.strftime("%Y-%m-%d")

    A=r13[r13["pattern"].eq("A")].copy()
    okeys=set(zip(od.loc[od["r13_pattern"].eq("A"),"signal_date"],od.loc[od["r13_pattern"].eq("A"),"code"]))
    A["odoli_overlap"]=list(zip(A["signal_date"],A["code"]))
    A["odoli_overlap"]=A["odoli_overlap"].isin(okeys)

    def summ(q,label):
        r5=pd.to_numeric(q["ret_max_high_5d"],errors="coerce")
        r10=pd.to_numeric(q["ret_max_high_10d"],errors="coerce")
        c5=pd.to_numeric(q["ret_close_5d"],errors="coerce")
        c10=pd.to_numeric(q["ret_close_10d"],errors="coerce")
        return {
            "group":label,"n":len(q),"d5_complete_n":int(r5.notna().sum()),"d10_complete_n":int(r10.notna().sum()),
            "d5_plus5_rate_complete_pct":complete_rate(r5,5),"d10_plus5_rate_complete_pct":complete_rate(r10,5),
            "d5_positive_rate_complete_pct":pos_rate(c5),"d10_positive_rate_complete_pct":pos_rate(c10),
            "d5_close_median_pct":float(c5.median()),"d10_close_median_pct":float(c10.median()),
            "d5_mfe_median_pct":float(r5.median()),"d10_mfe_median_pct":float(r10.median())
        }

    comp=pd.DataFrame([
        summ(A,"A_ALL"),
        summ(A[A.odoli_overlap],"A_AND_ODOLI"),
        summ(A[~A.odoli_overlap],"A_WITHOUT_ODOLI"),
    ])
    comp.to_csv(out/"a_vs_a_odoli_complete_denominator.csv",index=False,encoding="utf-8-sig")

    ao=od[od["r13_pattern"].eq("A")].copy()
    ao.to_csv(out/"a_odoli_21_events.csv",index=False,encoding="utf-8-sig")

    feats=["ma5_slope_1d_pct","ma5_ma10_gap_pct","vol20_ratio","amount20_ratio",
           "close_loc_pct","upper_wick_pct","gap_pct","ret5_pct"]
    rows=[]
    for fam,q in ao.groupby("odoli_path_r1"):
        row={"path":fam,"n":len(q)}
        for c in feats:
            row[c+"_median"]=float(pd.to_numeric(q[c],errors="coerce").median())
        rows.append(row)
    pd.DataFrame(rows).to_csv(out/"a_odoli_path_feature_medians.csv",index=False,encoding="utf-8-sig")

    # Pre-existing whole-ODOLI quintiles only; no thresholds learned from the 21 A events.
    for c in ["ma5_slope_1d_pct","amount20_ratio","vol20_ratio","ma5_ma10_gap_pct","close_loc_pct"]:
        x=pd.to_numeric(od[c],errors="coerce")
        try:
            cats,bins=pd.qcut(x,5,retbins=True,duplicates="drop")
        except Exception:
            continue
        ao[c+"_global_q"]=pd.cut(pd.to_numeric(ao[c],errors="coerce"),bins=bins,include_lowest=True)
    ao.to_csv(out/"a_odoli_21_with_global_quintiles.csv",index=False,encoding="utf-8-sig")

    notes=[
        "# A × ODOLI R2 Audit",
        "",
        "- Source ODOLI authority: frozen successful PIT run 36129293163.",
        "- Source R1.3 authority: frozen run 35443265652.",
        "- A population is 97 R1.3 A-events; exact signal_date+code overlap with ODOLI is 21.",
        "- Rates use only completed R1.3 horizons in their denominator.",
        "- No threshold is optimized from the 21-event overlap.",
        "- Whole-ODOLI quintile boundaries are used only as pre-existing descriptive buckets.",
        "- Intraday GAP_FAIL_BOX_RESTART cannot be validated from daily marcap bars; it needs a separate intraday lane.",
        "",
        comp.to_string(index=False)
    ]
    (out/"REPORT.md").write_text("\n".join(notes),encoding="utf-8")
    meta={"revision":REV,"research_only":True,"production_logic_changed":False,
          "same_sample_threshold_tuning":False,"a_n":len(A),"a_odoli_n":int(A.odoli_overlap.sum()),
          "intraday_box_restart_tested":False}
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

if __name__=="__main__":
    main()
