#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math
from pathlib import Path
import numpy as np
import pandas as pd

VERSION="CLOSEBET_SHADOW_OUTCOME_REFINEMENT_R13_20260919"

def read_csv(p):
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,encoding=enc,low_memory=False)
        except Exception:pass
    return pd.DataFrame()

def refined_outcome(r):
    """Refine outcome using ONLY thresholds/horizons already present in R1.2:
    - existing stop event
    - existing +5% event
    - established D5 / D10 horizons
    - D5 close relative to the same +5% threshold
    No new optimized threshold is introduced.
    """
    stop=pd.to_numeric(pd.Series([r.get("path_first_stop_day")]),errors="coerce").iloc[0]
    p5=pd.to_numeric(pd.Series([r.get("path_first_plus5_day")]),errors="coerce").iloc[0]
    c5=pd.to_numeric(pd.Series([r.get("ret_close_5d")]),errors="coerce").iloc[0]
    avail=pd.to_numeric(pd.Series([r.get("eval_available_days")]),errors="coerce").iloc[0]

    if pd.isna(stop) or pd.isna(p5):
        if pd.notna(avail) and avail < 10:
            return "MATURE_PENDING"
        return "PATH_EVENT_MISSING"

    if stop == p5:
        return "AMBIGUOUS_SAME_DAY"

    if p5 < stop:
        if p5 <= 5:
            if pd.notna(c5) and c5 >= 5:
                return "PLUS5_FIRST_HELD_TO_D5"
            return "PLUS5_FIRST_GIVEBACK_BY_D5"
        return "PLUS5_FIRST_AFTER_D5"

    # stop < p5
    if p5 <= 5:
        return "STOP_FIRST_THEN_PLUS5_BY_D5"
    if p5 <= 10:
        return "STOP_FIRST_THEN_PLUS5_D6_10"
    return "STOP_FIRST_NO_PLUS5_BY_D10"

def family(label):
    if label=="PLUS5_FIRST_HELD_TO_D5": return "EARLY_WIN_HELD"
    if label=="PLUS5_FIRST_GIVEBACK_BY_D5": return "EARLY_WIN_GIVEBACK"
    if label in ("STOP_FIRST_THEN_PLUS5_BY_D5","STOP_FIRST_THEN_PLUS5_D6_10"): return "SHAKEOUT_THEN_RECOVERY"
    if label=="STOP_FIRST_NO_PLUS5_BY_D10": return "EARLY_STOP_SLOW_OR_NO_RECOVERY"
    return "AMBIGUOUS_OR_PENDING"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source-root",default="source_replay")
    ap.add_argument("--output-dir",default="reports/shadow_outcome_refinement_r13")
    ap.add_argument("--source-replay-run-id",default="")
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    xs=list(Path(a.source_root).rglob("historical_shadow_replay_events.csv"))
    if not xs: raise SystemExit("R12_EVENTS_NOT_FOUND")
    src=xs[0]
    df=read_csv(src)
    if df.empty: raise SystemExit("R12_EVENTS_EMPTY")

    df["refined_outcome_r13"]=df.apply(refined_outcome,axis=1)
    df["refined_family_r13"]=df["refined_outcome_r13"].map(family)
    df.to_csv(out/"shadow_outcome_refined_events.csv",index=False,encoding="utf-8-sig")

    # Pattern x refined class
    psummary=[]
    for (p,c),g in df.groupby(["pattern","refined_outcome_r13"],dropna=False):
        rec={
            "pattern":p,"refined_outcome_r13":c,"n":len(g),
            "independent_signal_dates":g["signal_date"].nunique(),
        }
        for col in [
            "winner_like_feature_n","risk_like_feature_n","mixed_feature_n",
            "entry_close_loc_pct","entry_vol20_ratio","entry_amount20_ratio",
            "entry_atr_pct","entry_ma20_dist_pct","entry_ma60_dist_pct",
            "entry_ma224_dist_pct","entry_upper_wick_pct","entry_ret5_pct",
            "ret_max_high_5d","ret_close_5d","ret_max_high_10d","ret_close_10d",
            "giveback_hd_pctpt",
        ]:
            if col in g:
                x=pd.to_numeric(g[col],errors="coerce").dropna()
                rec[col+"_median"]=float(x.median()) if len(x) else np.nan
                rec[col+"_mean"]=float(x.mean()) if len(x) else np.nan
        psummary.append(rec)
    ps=pd.DataFrame(psummary)
    ps.to_csv(out/"shadow_outcome_pattern_summary.csv",index=False,encoding="utf-8-sig")

    # Family summary
    fsummary=[]
    for (p,f),g in df.groupby(["pattern","refined_family_r13"],dropna=False):
        fsummary.append({
            "pattern":p,"refined_family_r13":f,"n":len(g),
            "independent_signal_dates":g["signal_date"].nunique(),
            "winner_like_features_median":pd.to_numeric(g.get("winner_like_feature_n"),errors="coerce").median(),
            "risk_like_features_median":pd.to_numeric(g.get("risk_like_feature_n"),errors="coerce").median(),
            "ret_max_high_10d_median":pd.to_numeric(g.get("ret_max_high_10d"),errors="coerce").median(),
            "ret_close_10d_median":pd.to_numeric(g.get("ret_close_10d"),errors="coerce").median(),
        })
    pd.DataFrame(fsummary).to_csv(out/"shadow_outcome_family_summary.csv",index=False,encoding="utf-8-sig")

    # Join R1.2 trait alignment to refined outcome.
    align_files=list(Path(a.source_root).rglob("historical_shadow_trait_alignment_long.csv"))
    if align_files:
        al=read_csv(align_files[0])
        key=df[["event_key","refined_outcome_r13","refined_family_r13"]].drop_duplicates("event_key")
        al=al.merge(key,on="event_key",how="left",validate="many_to_one")
        al.to_csv(out/"shadow_trait_alignment_refined_long.csv",index=False,encoding="utf-8-sig")
        cross=(al.groupby(["pattern","feature","frozen_r11_alignment","refined_outcome_r13"])
                 .size().reset_index(name="n"))
        cross.to_csv(out/"shadow_trait_alignment_vs_refined_outcome.csv",index=False,encoding="utf-8-sig")
    else:
        pd.DataFrame().to_csv(out/"shadow_trait_alignment_refined_long.csv",index=False,encoding="utf-8-sig")
        pd.DataFrame().to_csv(out/"shadow_trait_alignment_vs_refined_outcome.csv",index=False,encoding="utf-8-sig")

    # Counts for report
    counts=(df.groupby(["pattern","refined_outcome_r13"]).size()
              .reset_index(name="n").sort_values(["pattern","refined_outcome_r13"]))
    fam=(df.groupby(["pattern","refined_family_r13"]).size()
           .reset_index(name="n").sort_values(["pattern","refined_family_r13"]))

    lines=[
        "# Closing Bet SHADOW Outcome Refinement R1.3",
        "",
        f"- source replay run: `{a.source_replay_run_id}`",
        f"- events: {len(df)}",
        f"- independent signal dates: {df['signal_date'].nunique()}",
        "",
        "## Outcome definitions",
        "- PLUS5_FIRST_HELD_TO_D5: +5% event occurred before stop and D5 close remained >= +5%.",
        "- PLUS5_FIRST_GIVEBACK_BY_D5: +5% event occurred before stop but D5 close fell below +5%.",
        "- STOP_FIRST_THEN_PLUS5_BY_D5: stop event occurred first, then +5% was reached by D5.",
        "- STOP_FIRST_THEN_PLUS5_D6_10: stop event occurred first, then +5% was reached during D6-D10.",
        "- STOP_FIRST_NO_PLUS5_BY_D10: stop event occurred first and +5% was not reached by D10.",
        "- AMBIGUOUS_SAME_DAY: stop and +5% first-event day are equal; no intraday ordering is inferred.",
        "",
        "## Guardrails",
        "- No new threshold optimization.",
        "- +5%, stop-event semantics, D5 and D10 are inherited from existing research.",
        "- R1.1 Winner/Risk reference values are unchanged.",
        "- Future outcome is used only for retrospective grouping.",
        "- No production score, ranking, filter or order logic is created.",
        "- Same-day ambiguous events are kept separate because daily data cannot establish event order.",
        "",
        "## Pattern x refined outcome counts",
    ]
    for _,r in counts.iterrows():
        lines.append(f"- {r['pattern']} / {r['refined_outcome_r13']}: n={int(r['n'])}")
    lines += ["","## Pattern x response-family counts"]
    for _,r in fam.iterrows():
        lines.append(f"- {r['pattern']} / {r['refined_family_r13']}: n={int(r['n'])}")
    lines += [
        "",
        "## Intended use",
        "Use these groups to improve SHADOW response/watchlist language, not to tune the scanner.",
        "Example: distinguish an early +5% move that held through D5 from one that gave back, and distinguish a stop-first shakeout that recovered within D5/D10 from one that did not.",
    ]
    (out/"REPORT.md").write_text("\n".join(lines),encoding="utf-8")

    meta={
        "version":VERSION,"status":"PASS","research_only":True,
        "source_replay_run_id":a.source_replay_run_id,
        "events":len(df),"independent_signal_dates":int(df["signal_date"].nunique()),
        "new_threshold_optimization":False,
        "same_sample_tuning":False,
        "inherited_thresholds":["existing stop event","+5%","D5","D10"],
        "candidate_membership_changed":False,
        "candidate_order_changed":False,
        "score_rank_changed":False,
        "production_logic_changed":False,
        "intraday_order_inferred_for_same_day":False,
        "refined_classes":counts["refined_outcome_r13"].drop_duplicates().tolist(),
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False,indent=2))
    print(counts.to_string(index=False))

if __name__=="__main__":
    main()
