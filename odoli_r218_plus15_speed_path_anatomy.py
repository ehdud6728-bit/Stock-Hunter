#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_18_PLUS15_SPEED_PATH_ANATOMY_20260928"

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs:
        raise SystemExit(f"MISSING:{name}")
    return xs[0]

def bool_rate(g,c):
    if c not in g.columns: return np.nan
    x=g[c].dropna().astype(bool)
    return float(x.mean()*100) if len(x) else np.nan

def num_median(g,c):
    if c not in g.columns: return np.nan
    x=pd.to_numeric(g[c],errors="coerce").dropna()
    return float(x.median()) if len(x) else np.nan

def num_mean(g,c):
    if c not in g.columns: return np.nan
    x=pd.to_numeric(g[c],errors="coerce").dropna()
    return float(x.mean()) if len(x) else np.nan

def classify_speed(r):
    if not bool(r.get("d60_complete",False)):
        return "PENDING_D60"
    d10=bool(r.get("d10_touch_15",False))
    d20=bool(r.get("d20_touch_15",False))
    d60=bool(r.get("d60_touch_15",False))
    if d10:
        return "FAST_D1_10"
    if d20:
        return "MID_D11_20"
    if d60:
        return "SLOW_D21_60"
    return "NO_PLUS15_D60"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r217-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()

    out=Path(a.output_dir)
    out.mkdir(parents=True,exist_ok=True)

    z=pd.read_csv(find_one(a.r217_root,"r217_event_redefinition.csv"),dtype={"code":str},low_memory=False)
    if len(z)!=100:
        raise SystemExit(f"COHORT_MISMATCH:{len(z)}")

    z["plus15_speed_class"]=z.apply(classify_speed,axis=1)

    # Earliest +15 touch day based on frozen High/MFE outcomes.
    def first15(r):
        for h in [10,20,40,60]:
            c=f"d{h}_first_touch_day_15"
            v=pd.to_numeric(pd.Series([r.get(c)]),errors="coerce").iloc[0]
            if pd.notna(v):
                return float(v)
        return np.nan
    z["plus15_first_touch_day"]=z.apply(first15,axis=1)

    z.to_csv(out/"r218_event_speed_classes.csv",index=False,encoding="utf-8-sig")

    groups=["FAST_D1_10","MID_D11_20","SLOW_D21_60","NO_PLUS15_D60","PENDING_D60"]
    num_features=[
        "candle_only_score",
        "accum_volume_ratio20","accum_amount_ratio20",
        "accum_close_pos","accum_lower_wick_ratio","accum_upper_wick_ratio",
        "accum_days_before_signal",
        "pullback_volume_vs_accum","pullback_amount_vs_accum",
        "range_contract_3v10",
        "signal_volume_ratio20","signal_amount_ratio20",
        "sequence_component_count",
        "d5_mfe_pct","d5_mae_pct",
        "d10_mfe_pct","d10_mae_pct",
        "d20_mfe_pct","d20_mae_pct",
        "d60_mfe_pct","d60_mae_pct",
        "d60_giveback_pp"
    ]
    bool_features=[
        "pre_signal_holds_accum_low",
        "pre_signal_close_holds_accum_mid",
        "signal_close_above_ma5",
        "signal_ma5_slope_up"
    ]

    rows=[]
    for label in groups:
        g=z[z["plus15_speed_class"].eq(label)].copy()
        row={"group":label,"n":len(g),
             "rate_pct":float(len(g)/len(z)*100),
             "plus15_first_touch_day_median":num_median(g,"plus15_first_touch_day")}
        for c in num_features:
            row[c+"_median"]=num_median(g,c)
            row[c+"_mean"]=num_mean(g,c)
        for c in bool_features:
            row[c+"_rate_pct"]=bool_rate(g,c)
        rows.append(row)
    summary=pd.DataFrame(rows)
    summary.to_csv(out/"r218_speed_group_anatomy.csv",index=False,encoding="utf-8-sig")

    # Pairwise deltas: descriptive only.
    pairs=[
        ("FAST_D1_10","NO_PLUS15_D60"),
        ("MID_D11_20","NO_PLUS15_D60"),
        ("SLOW_D21_60","NO_PLUS15_D60"),
        ("FAST_D1_10","SLOW_D21_60")
    ]
    deltas=[]
    by={r["group"]:r for r in rows}
    focus=[
        "accum_volume_ratio20_median","accum_amount_ratio20_median",
        "pullback_volume_vs_accum_median","pullback_amount_vs_accum_median",
        "range_contract_3v10_median",
        "signal_volume_ratio20_median","signal_amount_ratio20_median",
        "sequence_component_count_median",
        "signal_ma5_slope_up_rate_pct",
        "pre_signal_holds_accum_low_rate_pct",
        "d5_mae_pct_median","d10_mae_pct_median"
    ]
    for a1,b1 in pairs:
        if a1 not in by or b1 not in by: continue
        rec={"group_a":a1,"group_b":b1}
        for c in focus:
            va=by[a1].get(c,np.nan); vb=by[b1].get(c,np.nan)
            rec[c+"_delta_a_minus_b"]=float(va-vb) if pd.notna(va) and pd.notna(vb) else np.nan
        deltas.append(rec)
    pd.DataFrame(deltas).to_csv(out/"r218_pairwise_deltas.csv",index=False,encoding="utf-8-sig")

    # Distribution by half-year to expose regime concentration without inferring causality.
    if "period" in z.columns:
        ct=pd.crosstab(z["period"],z["plus15_speed_class"],dropna=False)
        ct.to_csv(out/"r218_speed_by_period_counts.csv",encoding="utf-8-sig")
        rt=ct.div(ct.sum(axis=1),axis=0)*100
        rt.to_csv(out/"r218_speed_by_period_rates.csv",encoding="utf-8-sig")

    # Event lists for manual chart review.
    cols=[
        "signal_date","code","name","period","plus15_speed_class","plus15_first_touch_day",
        "candle_only_score","accum_volume_ratio20","accum_amount_ratio20",
        "pullback_volume_vs_accum","pullback_amount_vs_accum","range_contract_3v10",
        "signal_volume_ratio20","signal_amount_ratio20","sequence_component_count",
        "d5_mfe_pct","d5_mae_pct","d10_mfe_pct","d10_mae_pct",
        "d20_mfe_pct","d20_mae_pct","d60_mfe_pct","d60_mae_pct","d60_giveback_pp"
    ]
    cols=[c for c in cols if c in z.columns]
    z[cols].sort_values(["plus15_speed_class","plus15_first_touch_day","signal_date"]).to_csv(
        out/"r218_manual_review_ledger.csv",index=False,encoding="utf-8-sig"
    )

    meta={
        "revision":REV,
        "research_only":True,
        "production_logic_changed":False,
        "same_sample_tuning":False,
        "new_gate_created":False,
        "cohort_n":len(z),
        "profit_authority":"INTRAPERIOD_HIGH_TOUCH_MFE",
        "target_pct":15,
        "speed_classes":{
            "FAST_D1_10":"first +15 High touch by D10",
            "MID_D11_20":"first +15 High touch D11-D20",
            "SLOW_D21_60":"first +15 High touch D21-D60",
            "NO_PLUS15_D60":"no +15 High touch through completed D60",
            "PENDING_D60":"D60 incomplete"
        },
        "descriptive_only":True
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    report=[
        "# ODOLI R2.18 — +15 Speed Path Anatomy","",
        "- Frozen source: R2.17 100-event CORE+ODOLI cohort.",
        "- Profit authority: intraperiod High / MFE.",
        "- +15 speed classes are timing labels only; no threshold optimization.",
        "- Compare accumulation inflow, pullback dry-up, compression, restart participation, MA5 restart, and adverse path.",
        "- No production/search/rank/order changes.","",
        "## Speed group summary","```",summary.to_string(index=False),"```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

if __name__=="__main__":
    main()
