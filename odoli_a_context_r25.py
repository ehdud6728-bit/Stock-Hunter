#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_A_CONTEXT_R2_5_A97_CORE_COHORT_20260925"

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs:
        raise SystemExit(f"MISSING:{name}")
    return xs[0]

def num(s):
    return pd.to_numeric(s, errors="coerce")

def summ_r13(q,label):
    # R1.3 outcome columns available for all A97.
    r5=num(q["ret_max_high_5d"])
    r10=num(q["ret_max_high_10d"])
    c5=num(q["ret_close_5d"])
    c10=num(q["ret_close_10d"])
    return {
        "group":label,
        "n":len(q),
        "d5_complete_n":int(r5.notna().sum()),
        "d10_complete_n":int(r10.notna().sum()),
        "d5_plus5_rate_complete_pct":float((r5.dropna()>=5).mean()*100) if r5.notna().any() else np.nan,
        "d10_plus5_rate_complete_pct":float((r10.dropna()>=5).mean()*100) if r10.notna().any() else np.nan,
        "d5_positive_close_rate_complete_pct":float((c5.dropna()>0).mean()*100) if c5.notna().any() else np.nan,
        "d10_positive_close_rate_complete_pct":float((c10.dropna()>0).mean()*100) if c10.notna().any() else np.nan,
        "d5_close_median_pct":float(c5.median()) if c5.notna().any() else np.nan,
        "d10_close_median_pct":float(c10.median()) if c10.notna().any() else np.nan,
        "d5_mfe_median_pct":float(r5.median()) if r5.notna().any() else np.nan,
        "d10_mfe_median_pct":float(r10.median()) if r10.notna().any() else np.nan,
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r23-root",required=True)
    ap.add_argument("--r24-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    A97=pd.read_csv(find_one(a.r23_root,"a97_good_pullback_descriptors.csv"),low_memory=False)
    A21=pd.read_csv(find_one(a.r24_root,"a21_combo_trace.csv"),low_memory=False)
    bins=json.loads(find_one(a.r23_root,"a_context_bins.json").read_text(encoding="utf-8"))

    # Rebuild frozen A97 bins exactly as R2.4.
    for col,b in bins.items():
        if not b or len(b)<2 or col not in A97.columns:
            continue
        A97[col+"_q"]=pd.cut(num(A97[col]), bins=np.array(b,dtype=float),
                            labels=False, include_lowest=True)+1

    A97["AX_MA5_GT_MA10"]=num(A97["ma5_ma10_gap_pct_r23"]).gt(0)
    A97["AX_DOWN_AMOUNT_WEAK"]=num(A97["down_up_amount_ratio_10d_q"]).le(2)
    A97["AX_DOWN_VOLUME_WEAK"]=num(A97["down_up_volume_ratio_10d_q"]).le(2)
    A97["AX_PARTICIPATION_WEAK"]=A97["AX_DOWN_AMOUNT_WEAK"] | A97["AX_DOWN_VOLUME_WEAK"]
    A97["CORE21"]=A97["AX_MA5_GT_MA10"] & A97["AX_PARTICIPATION_WEAK"]

    # Exact A×ODOLI membership from frozen R2.3.
    if "odoli_overlap" not in A97.columns:
        raise SystemExit("R23_MISSING_ODOLI_OVERLAP")
    A97["odoli_overlap"]=A97["odoli_overlap"].astype(str).str.lower().isin(["true","1"])

    core=A97[A97["CORE21"]].copy()
    core_od=core[core["odoli_overlap"]].copy()
    core_only=core[~core["odoli_overlap"]].copy()

    # Primary comparison: same A-context/core condition; does ODOLI add a restart trigger?
    comp=pd.DataFrame([
        summ_r13(A97,"A97_ALL"),
        summ_r13(A97[A97["odoli_overlap"]],"A97_AND_ODOLI"),
        summ_r13(A97[~A97["odoli_overlap"]],"A97_WITHOUT_ODOLI"),
        summ_r13(core,"CORE_MA5GTMA10_AND_WEAK_PARTICIPATION_ALL"),
        summ_r13(core_od,"CORE_AND_ODOLI"),
        summ_r13(core_only,"CORE_WITHOUT_ODOLI"),
    ])
    comp.to_csv(out/"core21_odoli_incremental_comparison.csv",index=False,encoding="utf-8-sig")

    # Event-level trace for all 21 core cases.
    keep=[
        c for c in [
            "signal_date","code","name","pattern","odoli_overlap",
            "ma5_ma10_gap_pct_r23","down_up_amount_ratio_10d","down_up_volume_ratio_10d",
            "range_contract_3v10","body_contract_3v10",
            "ret_max_high_5d","ret_max_high_10d","ret_close_5d","ret_close_10d",
            "outcome_family","refined_outcome","shadow_outcome","family"
        ] if c in core.columns
    ]
    trace=core[keep].copy()
    trace.to_csv(out/"core21_event_trace.csv",index=False,encoding="utf-8-sig")

    # Matched descriptive comparison: core+ODOLI 4 vs core-only 17.
    desc_cols=[
        "ma5_ma10_gap_pct_r23","down_up_amount_ratio_10d","down_up_volume_ratio_10d",
        "range_contract_3v10","body_contract_3v10","pullback_depth_pct",
        "recovery_ratio_to_pre10_high","signal_reclaim_pre3_high_pct",
        "signal_vs_pre20_high_pct","signal_vs_pre20_low_pct"
    ]
    rows=[]
    for label,g in [("CORE_AND_ODOLI",core_od),("CORE_WITHOUT_ODOLI",core_only)]:
        r={"group":label,"n":len(g)}
        for c in desc_cols:
            if c in g.columns:
                x=num(g[c])
                r[c+"_median"]=float(x.median()) if x.notna().any() else np.nan
        rows.append(r)
    pd.DataFrame(rows).to_csv(out/"core21_structure_medians.csv",index=False,encoding="utf-8-sig")

    # Leave-one-out stability for the tiny ODOLI n=4 cohort:
    # report how the D5 metrics move if any one event is removed.
    loo=[]
    if len(core_od)>=2:
        for i in core_od.index:
            g=core_od.drop(index=i)
            r=summ_r13(g,f"DROP_{i}")
            r["dropped_signal_date"]=str(core_od.loc[i,"signal_date"]) if "signal_date" in core_od.columns else ""
            r["dropped_code"]=str(core_od.loc[i,"code"]) if "code" in core_od.columns else ""
            loo.append(r)
    pd.DataFrame(loo).to_csv(out/"core_odoli_leave_one_out.csv",index=False,encoding="utf-8-sig")

    # A97 support details for nearby definitions, outcome-blind support only.
    support_defs={
        "MA5_GT_MA10": A97["AX_MA5_GT_MA10"],
        "DOWN_AMOUNT_WEAK": A97["AX_DOWN_AMOUNT_WEAK"],
        "DOWN_VOLUME_WEAK": A97["AX_DOWN_VOLUME_WEAK"],
        "PARTICIPATION_WEAK": A97["AX_PARTICIPATION_WEAK"],
        "CORE21": A97["CORE21"],
    }
    sup=[]
    for name,m in support_defs.items():
        sup.append({
            "definition":name,
            "a97_n":int(m.sum()),
            "a97_pct":float(m.mean()*100),
            "odoli_overlap_n":int((m & A97["odoli_overlap"]).sum()),
            "odoli_overlap_pct_within_definition":float((A97.loc[m,"odoli_overlap"].mean()*100) if m.any() else np.nan)
        })
    pd.DataFrame(sup).to_csv(out/"a97_core_support.csv",index=False,encoding="utf-8-sig")

    report=[
        "# ODOLI R2.5 — A97 core cohort incremental audit",
        "",
        "- Frozen A population: 97 R1.3 A events.",
        "- Frozen core definition: MA5>MA10 AND weak down participation (Amount OR Volume in A97 Q1-Q2).",
        "- Core support in A97 is expected to be 21 events.",
        "- Primary question: inside the same core A-context, does ODOLI add incremental restart information?",
        "- Comparison is CORE+ODOLI vs CORE without ODOLI.",
        "- R1.3 complete-horizon denominators are used for all A97 outcomes.",
        "- Leave-one-out sensitivity is emitted for the tiny CORE+ODOLI subgroup.",
        "- Discovery only; no new threshold fitting; not OOS.",
        "",
        "## Incremental comparison",
        "```",
        comp.to_string(index=False),
        "```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

    meta={
        "revision":REV,
        "research_only":True,
        "production_logic_changed":False,
        "same_sample_threshold_tuning":False,
        "a_population_n":len(A97),
        "core_n":len(core),
        "core_odoli_n":len(core_od),
        "core_without_odoli_n":len(core_only),
        "true_oos_validation":False
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

if __name__=="__main__":
    main()
