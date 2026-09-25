#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_A_CONTEXT_R2_4_THREE_AXIS_COMBO_20260925"

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs: raise SystemExit(f"MISSING:{name}")
    return xs[0]

def complete_bool(s):
    return s.astype(str).str.lower().isin(["true","1"])

def summ(q,label):
    d=q[complete_bool(q["d5_complete"])].copy()
    if d.empty:
        return {"group":label,"n":len(q),"d5_complete_n":0}
    mfe=pd.to_numeric(d["d5_mfe_pct"],errors="coerce")
    mae=pd.to_numeric(d["d5_mae_pct"],errors="coerce")
    close=pd.to_numeric(d["d5_close_ret_pct"],errors="coerce")
    held=d["odoli_path_r1"].eq("EARLY_WIN_HELD")
    give=d["odoli_path_r1"].eq("EARLY_WIN_GIVEBACK")
    fail=d["odoli_path_r1"].eq("NO_RECOVERY_BY_D5")
    return {
        "group":label,
        "n":len(q),
        "d5_complete_n":len(d),
        "d5_plus5_rate_pct":float(mfe.ge(5).mean()*100),
        "d5_positive_close_rate_pct":float(close.gt(0).mean()*100),
        "d5_close_median_pct":float(close.median()),
        "d5_mfe_median_pct":float(mfe.median()),
        "d5_mae_median_pct":float(mae.median()),
        "held_rate_pct":float(held.mean()*100),
        "giveback_rate_pct":float(give.mean()*100),
        "no_recovery_rate_pct":float(fail.mean()*100),
        "held_minus_giveback_pp":float((held.mean()-give.mean())*100),
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r23-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    A97=pd.read_csv(find_one(a.r23_root,"a97_good_pullback_descriptors.csv"),low_memory=False)
    A21=pd.read_csv(find_one(a.r23_root,"a21_odoli_good_pullback_descriptors.csv"),low_memory=False)
    bins=json.loads(find_one(a.r23_root,"a_context_bins.json").read_text(encoding="utf-8"))

    # Build A97 outcome-blind quintiles using frozen R2.3 bins.
    for col,b in bins.items():
        if not b or len(b)<2 or col not in A97.columns: continue
        A97[col+"_q"]=pd.cut(pd.to_numeric(A97[col],errors="coerce"),
                            bins=np.array(b,dtype=float),labels=False,include_lowest=True)+1
        if col in A21.columns:
            A21[col+"_q"]=pd.cut(pd.to_numeric(A21[col],errors="coerce"),
                                bins=np.array(b,dtype=float),labels=False,include_lowest=True)+1

    # Axis definitions: all predeclared, no fitting on A21 outcomes.
    def add_axes(df):
        df=df.copy()
        df["AX_MA5_GT_MA10"]=pd.to_numeric(df["ma5_ma10_gap_pct_r23"],errors="coerce").gt(0)
        # "weak down participation" = bottom 40% of A97 distribution
        df["AX_DOWN_AMOUNT_WEAK"]=pd.to_numeric(df["down_up_amount_ratio_10d_q"],errors="coerce").le(2)
        df["AX_DOWN_VOLUME_WEAK"]=pd.to_numeric(df["down_up_volume_ratio_10d_q"],errors="coerce").le(2)
        # compression = bottom 40% range/body ratios
        df["AX_RANGE_CONTRACT"]=pd.to_numeric(df["range_contract_3v10_q"],errors="coerce").le(2)
        df["AX_BODY_CONTRACT"]=pd.to_numeric(df["body_contract_3v10_q"],errors="coerce").le(2)
        df["AX_PARTICIPATION_WEAK"]=df["AX_DOWN_AMOUNT_WEAK"] | df["AX_DOWN_VOLUME_WEAK"]
        df["AX_COMPRESSION"]=df["AX_RANGE_CONTRACT"] | df["AX_BODY_CONTRACT"]
        df["AX_BOTH_PARTICIPATION_WEAK"]=df["AX_DOWN_AMOUNT_WEAK"] & df["AX_DOWN_VOLUME_WEAK"]
        df["AX_BOTH_COMPRESSION"]=df["AX_RANGE_CONTRACT"] & df["AX_BODY_CONTRACT"]
        return df

    A97=add_axes(A97)
    A21=add_axes(A21)

    # Frequency / support in A97 regardless of outcomes.
    axis_cols=[
        "AX_MA5_GT_MA10","AX_DOWN_AMOUNT_WEAK","AX_DOWN_VOLUME_WEAK",
        "AX_RANGE_CONTRACT","AX_BODY_CONTRACT",
        "AX_PARTICIPATION_WEAK","AX_COMPRESSION",
        "AX_BOTH_PARTICIPATION_WEAK","AX_BOTH_COMPRESSION"
    ]
    freq=[]
    for c in axis_cols:
        freq.append({"axis":c,"a97_n":int(A97[c].sum()),"a97_rate_pct":float(A97[c].mean()*100),
                     "a21_n":int(A21[c].sum()),"a21_rate_pct":float(A21[c].mean()*100)})
    pd.DataFrame(freq).to_csv(out/"axis_support_counts.csv",index=False,encoding="utf-8-sig")

    # Explicit single / pair / triple screens on A21 outcome-bearing overlap.
    combos={
        "BASE_A_ODOLI": pd.Series(True,index=A21.index),

        "MA5_GT_MA10": A21["AX_MA5_GT_MA10"],
        "DOWN_AMOUNT_WEAK": A21["AX_DOWN_AMOUNT_WEAK"],
        "DOWN_VOLUME_WEAK": A21["AX_DOWN_VOLUME_WEAK"],
        "RANGE_CONTRACT": A21["AX_RANGE_CONTRACT"],
        "BODY_CONTRACT": A21["AX_BODY_CONTRACT"],
        "PARTICIPATION_WEAK": A21["AX_PARTICIPATION_WEAK"],
        "COMPRESSION": A21["AX_COMPRESSION"],

        "MA5_GT_MA10__DOWN_AMOUNT_WEAK":
            A21["AX_MA5_GT_MA10"] & A21["AX_DOWN_AMOUNT_WEAK"],
        "MA5_GT_MA10__DOWN_VOLUME_WEAK":
            A21["AX_MA5_GT_MA10"] & A21["AX_DOWN_VOLUME_WEAK"],
        "MA5_GT_MA10__PARTICIPATION_WEAK":
            A21["AX_MA5_GT_MA10"] & A21["AX_PARTICIPATION_WEAK"],
        "MA5_GT_MA10__RANGE_CONTRACT":
            A21["AX_MA5_GT_MA10"] & A21["AX_RANGE_CONTRACT"],
        "MA5_GT_MA10__BODY_CONTRACT":
            A21["AX_MA5_GT_MA10"] & A21["AX_BODY_CONTRACT"],
        "MA5_GT_MA10__COMPRESSION":
            A21["AX_MA5_GT_MA10"] & A21["AX_COMPRESSION"],
        "DOWN_AMOUNT_WEAK__RANGE_CONTRACT":
            A21["AX_DOWN_AMOUNT_WEAK"] & A21["AX_RANGE_CONTRACT"],
        "DOWN_AMOUNT_WEAK__BODY_CONTRACT":
            A21["AX_DOWN_AMOUNT_WEAK"] & A21["AX_BODY_CONTRACT"],
        "PARTICIPATION_WEAK__COMPRESSION":
            A21["AX_PARTICIPATION_WEAK"] & A21["AX_COMPRESSION"],

        "MA5_GT_MA10__DOWN_AMOUNT_WEAK__RANGE_CONTRACT":
            A21["AX_MA5_GT_MA10"] & A21["AX_DOWN_AMOUNT_WEAK"] & A21["AX_RANGE_CONTRACT"],
        "MA5_GT_MA10__DOWN_AMOUNT_WEAK__BODY_CONTRACT":
            A21["AX_MA5_GT_MA10"] & A21["AX_DOWN_AMOUNT_WEAK"] & A21["AX_BODY_CONTRACT"],
        "MA5_GT_MA10__PARTICIPATION_WEAK__COMPRESSION":
            A21["AX_MA5_GT_MA10"] & A21["AX_PARTICIPATION_WEAK"] & A21["AX_COMPRESSION"],
        "STRICT_3AXIS":
            A21["AX_MA5_GT_MA10"] & A21["AX_BOTH_PARTICIPATION_WEAK"] & A21["AX_BOTH_COMPRESSION"],
    }

    rows=[]
    for name,m in combos.items():
        r=summ(A21[m].copy(),name)
        # support in A97 with same logical expression reconstructed below
        rows.append(r)
    screen=pd.DataFrame(rows)
    screen.to_csv(out/"a21_three_axis_combo_screen.csv",index=False,encoding="utf-8-sig")

    # Match support of same combos in A97.
    combos97={
        "BASE_A_ODOLI": pd.Series(True,index=A97.index),
        "MA5_GT_MA10": A97["AX_MA5_GT_MA10"],
        "DOWN_AMOUNT_WEAK": A97["AX_DOWN_AMOUNT_WEAK"],
        "DOWN_VOLUME_WEAK": A97["AX_DOWN_VOLUME_WEAK"],
        "RANGE_CONTRACT": A97["AX_RANGE_CONTRACT"],
        "BODY_CONTRACT": A97["AX_BODY_CONTRACT"],
        "PARTICIPATION_WEAK": A97["AX_PARTICIPATION_WEAK"],
        "COMPRESSION": A97["AX_COMPRESSION"],
        "MA5_GT_MA10__DOWN_AMOUNT_WEAK": A97["AX_MA5_GT_MA10"] & A97["AX_DOWN_AMOUNT_WEAK"],
        "MA5_GT_MA10__DOWN_VOLUME_WEAK": A97["AX_MA5_GT_MA10"] & A97["AX_DOWN_VOLUME_WEAK"],
        "MA5_GT_MA10__PARTICIPATION_WEAK": A97["AX_MA5_GT_MA10"] & A97["AX_PARTICIPATION_WEAK"],
        "MA5_GT_MA10__RANGE_CONTRACT": A97["AX_MA5_GT_MA10"] & A97["AX_RANGE_CONTRACT"],
        "MA5_GT_MA10__BODY_CONTRACT": A97["AX_MA5_GT_MA10"] & A97["AX_BODY_CONTRACT"],
        "MA5_GT_MA10__COMPRESSION": A97["AX_MA5_GT_MA10"] & A97["AX_COMPRESSION"],
        "DOWN_AMOUNT_WEAK__RANGE_CONTRACT": A97["AX_DOWN_AMOUNT_WEAK"] & A97["AX_RANGE_CONTRACT"],
        "DOWN_AMOUNT_WEAK__BODY_CONTRACT": A97["AX_DOWN_AMOUNT_WEAK"] & A97["AX_BODY_CONTRACT"],
        "PARTICIPATION_WEAK__COMPRESSION": A97["AX_PARTICIPATION_WEAK"] & A97["AX_COMPRESSION"],
        "MA5_GT_MA10__DOWN_AMOUNT_WEAK__RANGE_CONTRACT":
            A97["AX_MA5_GT_MA10"] & A97["AX_DOWN_AMOUNT_WEAK"] & A97["AX_RANGE_CONTRACT"],
        "MA5_GT_MA10__DOWN_AMOUNT_WEAK__BODY_CONTRACT":
            A97["AX_MA5_GT_MA10"] & A97["AX_DOWN_AMOUNT_WEAK"] & A97["AX_BODY_CONTRACT"],
        "MA5_GT_MA10__PARTICIPATION_WEAK__COMPRESSION":
            A97["AX_MA5_GT_MA10"] & A97["AX_PARTICIPATION_WEAK"] & A97["AX_COMPRESSION"],
        "STRICT_3AXIS":
            A97["AX_MA5_GT_MA10"] & A97["AX_BOTH_PARTICIPATION_WEAK"] & A97["AX_BOTH_COMPRESSION"],
    }
    support=[]
    for name,m in combos97.items():
        support.append({
            "group":name,
            "a97_support_n":int(m.sum()),
            "a97_support_pct":float(m.mean()*100),
            "a21_support_n":int(combos[name].sum()),
            "a21_support_pct":float(combos[name].mean()*100)
        })
    sup=pd.DataFrame(support)
    sup.to_csv(out/"three_axis_combo_support.csv",index=False,encoding="utf-8-sig")

    merged=screen.merge(sup,on="group",how="left")
    merged.to_csv(out/"three_axis_combo_with_support_and_outcomes.csv",index=False,encoding="utf-8-sig")

    # Resolved-path prevalence for direct interpretability.
    resolved=A21[A21["odoli_path_r1"].isin(["EARLY_WIN_HELD","EARLY_WIN_GIVEBACK","NO_RECOVERY_BY_D5"])].copy()
    prev=[]
    for p,g in resolved.groupby("odoli_path_r1"):
        rr={"path":p,"n":len(g)}
        for c in axis_cols:
            rr[c+"_rate_pct"]=float(g[c].mean()*100)
        prev.append(rr)
    pd.DataFrame(prev).to_csv(out/"resolved_path_axis_prevalence.csv",index=False,encoding="utf-8-sig")

    # Trace table with combo flags.
    trace=A21.copy()
    for name,m in combos.items():
        trace["COMBO__"+name]=m
    trace.to_csv(out/"a21_combo_trace.csv",index=False,encoding="utf-8-sig")

    report=[
        "# ODOLI R2.4 — A-context three-axis combination audit",
        "",
        "- Frozen source: R2.3 run 36143654300.",
        "- A population = 97; exact A×ODOLI overlap = 21.",
        "- No cutpoint is fitted from the 21 outcomes.",
        "- Weak participation / compression use frozen A97 Q1-Q2 bins from R2.3.",
        "- MA5>MA10 is a structural zero-crossing.",
        "- Same-period discovery only; not OOS.",
        "",
        "## Combo screen",
        "```",
        merged.to_string(index=False),
        "```",
        "",
        "Interpretation rule:",
        "- Prefer combinations with non-trivial A97 support and improvement in HELD-GIVEBACK,",
        "  not merely high +5% touch rate.",
        "- Tiny n remains hypothesis generation only.",
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

    meta={
        "revision":REV,
        "research_only":True,
        "production_logic_changed":False,
        "same_sample_threshold_tuning":False,
        "a_population_n":len(A97),
        "a_odoli_n":len(A21),
        "true_oos_validation":False
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

if __name__=="__main__":
    main()
