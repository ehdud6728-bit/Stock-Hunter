#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_21_SLOW_CONFIRMATION_COMBOS_20260928"

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs:
        raise SystemExit(f"MISSING:{name}")
    return xs[0]

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r220-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()

    out=Path(a.output_dir)
    out.mkdir(parents=True,exist_ok=True)

    z=pd.read_csv(find_one(a.r220_root,"r220_entry_simulation_events.csv"),
                  dtype={"code":str}, low_memory=False)
    if len(z)!=100:
        raise SystemExit(f"COHORT_MISMATCH:{len(z)}")

    q=z[z["plus15_speed_class"].isin(["SLOW_D21_60","NO_PLUS15_D60"])].copy()
    q["is_slow"]=q["plus15_speed_class"].eq("SLOW_D21_60")

    q["C1_LOW_SURVIVE_MA5"] = (
        q["MA5_RECLAIM_confirmed"].fillna(False).astype(bool) &
        q["MA5_RECLAIM_signal_low_survived_to_confirm"].fillna(False).astype(bool)
    )
    q["C2_LOW_SURVIVE_HL_MA5"] = (
        q["HL_MA5_confirmed"].fillna(False).astype(bool) &
        q["HL_MA5_signal_low_survived_to_confirm"].fillna(False).astype(bool)
    )
    q["C3_LOW_SURVIVE_AMOUNT_MA5"] = (
        q["AMOUNT_MA5_confirmed"].fillna(False).astype(bool) &
        q["AMOUNT_MA5_signal_low_survived_to_confirm"].fillna(False).astype(bool)
    )
    q["C4_HL_AND_AMOUNT_MA5"] = (
        q["HL_MA5_confirmed"].fillna(False).astype(bool) &
        q["AMOUNT_MA5_confirmed"].fillna(False).astype(bool) &
        q["HL_MA5_signal_low_survived_to_confirm"].fillna(False).astype(bool)
    )

    combos=[
        ("C1_LOW_SURVIVE_MA5","MA5 reclaim + signal-low survival"),
        ("C2_LOW_SURVIVE_HL_MA5","higher-low + MA5 + signal-low survival"),
        ("C3_LOW_SURVIVE_AMOUNT_MA5","amount >= 20d mean + MA5 + signal-low survival"),
        ("C4_HL_AND_AMOUNT_MA5","HL+MA5 AND amount+MA5 + signal-low survival"),
    ]

    slow_n=int(q["is_slow"].sum())
    no_n=int((~q["is_slow"]).sum())
    rows=[]
    for c,label in combos:
        flag=q[c].fillna(False).astype(bool)
        slow_pass=int((flag & q["is_slow"]).sum())
        no_pass=int((flag & ~q["is_slow"]).sum())
        pass_n=int(flag.sum())
        rows.append({
            "combo":c,
            "definition":label,
            "slow_n":slow_n,
            "no_n":no_n,
            "slow_pass_n":slow_pass,
            "no_pass_n":no_pass,
            "pass_n":pass_n,
            "slow_recall_pct":slow_pass/slow_n*100 if slow_n else np.nan,
            "slow_precision_among_pass_pct":slow_pass/pass_n*100 if pass_n else np.nan,
            "no_pass_rate_pct":no_pass/no_n*100 if no_n else np.nan,
            "no_reject_rate_pct":(no_n-no_pass)/no_n*100 if no_n else np.nan,
            "slow_minus_no_pass_rate_pp":(
                slow_pass/slow_n*100 - no_pass/no_n*100
                if slow_n and no_n else np.nan
            )
        })
    summary=pd.DataFrame(rows)
    summary.to_csv(out/"r221_combo_discrimination_summary.csv",index=False,encoding="utf-8-sig")

    mapping={
        "C1_LOW_SURVIVE_MA5":"MA5_RECLAIM",
        "C2_LOW_SURVIVE_HL_MA5":"HL_MA5",
        "C3_LOW_SURVIVE_AMOUNT_MA5":"AMOUNT_MA5",
    }
    erows=[]
    for c,_ in combos:
        for cls in ["SLOW_D21_60","NO_PLUS15_D60"]:
            g=q[q[c] & q["plus15_speed_class"].eq(cls)].copy()
            rec={"combo":c,"class":cls,"n":len(g)}
            if c in mapping:
                p=mapping[c]
                for mode in ["DELAY","SPLIT"]:
                    for metric in ["mfe_pct","mae_pct","last_close_ret_pct"]:
                        col=f"{p}_{mode}_{metric}"
                        rec[f"{mode.lower()}_{metric}_median"]=float(pd.to_numeric(g[col],errors="coerce").median()) if len(g) and col in g.columns else np.nan
                    for t in [5,10,15]:
                        col=f"{p}_{mode}_touch_{t}"
                        x=g[col].dropna().astype(bool) if len(g) and col in g.columns else pd.Series(dtype=bool)
                        rec[f"{mode.lower()}_touch{t}_rate_pct"]=float(x.mean()*100) if len(x) else np.nan
            erows.append(rec)
    pd.DataFrame(erows).to_csv(out/"r221_combo_entry_outcomes.csv",index=False,encoding="utf-8-sig")

    keep=[
        "signal_date","code","name","period","plus15_speed_class",
        "MA5_RECLAIM_confirm_day","HL_MA5_confirm_day","AMOUNT_MA5_confirm_day",
        "MA5_RECLAIM_signal_low_survived_to_confirm",
        "HL_MA5_signal_low_survived_to_confirm",
        "AMOUNT_MA5_signal_low_survived_to_confirm",
        "MA5_RECLAIM_confirm_amount_ratio20",
        "HL_MA5_confirm_amount_ratio20",
        "AMOUNT_MA5_confirm_amount_ratio20",
    ]+[c for c,_ in combos]
    keep=[c for c in keep if c in q.columns]
    q[keep].to_csv(out/"r221_combo_event_ledger.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REV,
        "research_only":True,
        "production_logic_changed":False,
        "same_sample_tuning":False,
        "new_gate_created":False,
        "population":"SLOW_D21_60 vs NO_PLUS15_D60 from frozen R2.20",
        "slow_n":slow_n,
        "no_n":no_n,
        "combos":[{"id":c,"definition":d} for c,d in combos],
        "warning":"Do not select a same-sample winner as a production gate.",
        "next_required_step":"freeze candidate observers and validate prospectively or on untouched data"
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    report=[
        "# ODOLI R2.21 — SLOW Confirmation Combination Anatomy","",
        "- Population: frozen SLOW vs NO +15 from R2.20.",
        "- Four predeclared combinations compared in parallel.",
        "- No numeric threshold search and no production promotion.","",
        "## Discrimination summary","```",summary.to_string(index=False),"```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

if __name__=="__main__":
    main()
