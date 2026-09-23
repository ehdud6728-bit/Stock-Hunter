#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, hashlib
from pathlib import Path
import numpy as np
import pandas as pd

RESEARCH_ID = "REAL_FULL_WATERMELON_LONGMA_PHASE_INTERACTION_R1"
REVISION = "R1_0_2_HOLDOUT_VARIABLE_FIX_20260923"
DISCOVERY_END = pd.Timestamp("2026-08-28")
HOLDOUT_START = pd.Timestamp("2026-09-01")

def sha_obj(x):
    return hashlib.sha256(json.dumps(x, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()

def mature_discovery(df):
    z=df[pd.to_datetime(df["origin_date"],errors="coerce").le(DISCOVERY_END)].copy()
    return z[pd.to_numeric(z.get("d20_complete"),errors="coerce").eq(1)].copy()

def outcome_rows(df, group_cols, axis_name):
    rows=[]
    if df.empty:
        return pd.DataFrame()
    for key,g in df.groupby(group_cols,dropna=False):
        if not isinstance(key, tuple):
            key=(key,)
        d={c:str(v) for c,v in zip(group_cols,key)}
        d.update({
            "axis":axis_name,
            "events":len(g),
            "success_events":int(g["forensic_group"].eq("SUCCESS").sum()),
            "failure_events":int(g["forensic_group"].eq("FAILURE").sum()),
            "fast_success_events":int(g["path_class"].eq("FAST_SUCCESS").sum()),
            "shakeout_then_go_events":int(g["path_class"].eq("SHAKEOUT_THEN_GO").sum()),
            "early_spike_giveback_events":int(g["path_class"].eq("EARLY_SPIKE_GIVEBACK").sum()),
            "time_failure_events":int(g["path_class"].eq("TIME_FAILURE").sum()),
            "success_rate":float(g["forensic_group"].eq("SUCCESS").mean()),
            "failure_rate":float(g["forensic_group"].eq("FAILURE").mean()),
            "giveback_rate":float(g["path_class"].eq("EARLY_SPIKE_GIVEBACK").mean()),
            "d20_close_median":pd.to_numeric(g["d20_close_ret_pct"],errors="coerce").median(),
            "d20_mfe_median":pd.to_numeric(g["d20_mfe_pct"],errors="coerce").median(),
            "d20_mae_median":pd.to_numeric(g["d20_mae_pct"],errors="coerce").median(),
            "sample_flag":"REPEATED_5PLUS" if len(g)>=5 else ("SMALL_3_4" if len(g)>=3 else "TINY_1_2"),
            "research_only":1,
            "production_gate_allowed":0,
        })
        rows.append(d)
    return pd.DataFrame(rows)

def interaction_candidate_table(tbl):
    if tbl.empty:
        return tbl.copy()
    z=tbl.copy()
    # Candidate means only "worth future observation". It is NOT a winning rule.
    z["candidate_status"]=np.where(
        (z["events"]>=3) & (z["success_events"]>=2),
        "DISCOVERY_REPEAT_OBSERVE_OOS",
        "DESCRIPTIVE_ONLY"
    )
    z["do_not_promote_to_production"]=1
    return z

def run(a):
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    src=pd.read_csv(a.source_master)
    src["origin_date"]=pd.to_datetime(src["origin_date"],errors="coerce").dt.normalize()

    discovery=src[src["origin_date"].le(DISCOVERY_END)].copy()
    holdout=src[src["origin_date"].ge(HOLDOUT_START)].copy()
    mature=mature_discovery(src)

    tables=[]

    # 1) Modern Watermelon state x each long-MA phase
    for ma in ["ma112_phase","ma224_phase","ma448_phase"]:
        t=outcome_rows(mature,["wm_modern_state",ma],f"MODERN_STATE_X_{ma.upper()}")
        tables.append(t)

    # 2) Modern Watermelon state x 224/448 and full 112/224/448 transition matrix
    tables.append(outcome_rows(mature,["wm_modern_state","ma224_phase","ma448_phase"],"MODERN_STATE_X_MA224_X_MA448"))
    tables.append(outcome_rows(mature,["wm_modern_state","ma112_phase","ma224_phase","ma448_phase"],"MODERN_STATE_X_ALL_LONGMA_PHASES"))

    # 3) Legacy/modern binary signals x long-MA phases
    signal_cols=[
        "wm_legacy12","wm_legacy15","wm_bb40_green","wm_bb40_red",
        "wm_old_real448","wm_modern_green","wm_modern_red","wm_modern_blue"
    ]
    for sig in signal_cols:
        if sig not in mature.columns:
            continue
        for ma in ["ma112_phase","ma224_phase","ma448_phase"]:
            t=outcome_rows(mature,[sig,ma],f"{sig.upper()}_X_{ma.upper()}")
            tables.append(t)

    master=pd.concat([t for t in tables if t is not None and not t.empty],ignore_index=True)
    master.to_csv(out/"watermelon_longma_interaction_master.csv",index=False,encoding="utf-8-sig")

    # Focus view: groups with actual watermelon state/signals and repeated observations.
    focus=master.copy()
    focus["is_active_watermelon"]=1
    if "wm_modern_state" in focus.columns:
        bad=focus.get("wm_modern_state",pd.Series(index=focus.index,dtype=object)).astype(str).isin(["없음","nan",""])
        focus.loc[bad,"is_active_watermelon"]=0
    signal_cols_in=[c for c in signal_cols if c in focus.columns]
    for c in signal_cols_in:
        mask=focus[c].astype(str).isin(["0","0.0","False","nan"])
        focus.loc[mask,"is_active_watermelon"]=0
    focus=focus[focus["events"]>=3].copy()
    focus=interaction_candidate_table(focus)
    focus.sort_values(["events","d20_close_median"],ascending=[False,False]).to_csv(
        out/"repeated_interactions_discovery_only.csv",index=False,encoding="utf-8-sig"
    )

    # Dedicated long-MA phase matrix, regardless of Watermelon, for control/reference.
    control=outcome_rows(mature,["ma112_phase","ma224_phase","ma448_phase"],"LONGMA_PHASE_CONTROL")
    control.to_csv(out/"longma_phase_control_summary.csv",index=False,encoding="utf-8-sig")

    # Holdout visibility only: NO D20-based ranking/selection.
    hold_cols=[
        "origin_date","code","name","origin_search_pattern",
        "wm_legacy12","wm_legacy15","wm_bb40_green","wm_bb40_red","wm_old_real448",
        "wm_modern_state","wm_modern_green","wm_modern_red","wm_modern_blue",
        "wm_modern_eval_status",
        "ma112_phase","ma224_phase","ma448_phase","longma_phase_code",
        "longma_position_code","close_vs_ma112_pct","close_vs_ma224_pct","close_vs_ma448_pct"
    ]
    holdout[[c for c in hold_cols if c in holdout.columns]].to_csv(
        out/"holdout_watermelon_longma_phase_MONITOR_ONLY.csv",index=False,encoding="utf-8-sig"
    )

    # Integrity / freeze.
    design={
        "source":"Watermelon Long-MA Audit R1.0.1",
        "source_run_default":"35818983451",
        "discovery_end":"2026-08-28",
        "holdout_start":"2026-09-01",
        "phase_definitions":["BELOW","RECLAIM_20","ABOVE_STABLE"],
        "interaction_axes":[
            "modern_state x MA112/224/448 phase",
            "modern_state x MA224 x MA448",
            "modern_state x all long-MA phases",
            "legacy/modern binary Watermelon signals x long-MA phases"
        ],
        "candidate_min_n":3,
        "candidate_meaning":"observation target only; not optimized rule",
        "research_only":True,
        "production_eligible":False,
        "same_sample_retuning":False,
        "thresholds_newly_optimized":False
    }
    meta={
        "research_id":RESEARCH_ID,
        "revision":REVISION,
        "status":"PASS",
        "events":len(src),
        "discovery_events":len(discovery),
        "mature_discovery_events":len(mature),
        "holdout_events":len(holdout),
        "interaction_rows":len(master),
        "repeated_rows_n3plus":int((master["events"]>=3).sum()) if len(master) else 0,
        "research_only":True,
        "production_eligible":False,
        "selection_logic_changed":False,
        "score_rank_changed":False,
        "order_logic_changed":False,
        "same_sample_retuning":False,
        "holdout_outcomes_used_for_tuning":False,
        "design_hash":sha_obj(design),
        "design":design
    }
    (out/"watermelon_longma_interaction_meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    report=[
        f"🧪 [{RESEARCH_ID}]",
        f"status=PASS revision={REVISION}",
        f"events={len(src)} discovery={len(discovery)} mature_discovery={len(mature)} holdout={len(holdout)}",
        f"interaction_rows={len(master)} repeated_n3plus={meta['repeated_rows_n3plus']}",
        "No new Watermelon threshold and no outcome-based retuning.",
        "Candidate status means future OOS observation target only; never a production gate.",
        "Holdout is monitor-only and excluded from Discovery ranking.",
        "research_only=1 production changes=0"
    ]
    (out/"watermelon_longma_interaction_report.txt").write_text("\n".join(report)+"\n",encoding="utf-8")
    print("\n".join(report))

def self_test():
    d=pd.DataFrame({
        "origin_date":["2026-01-01"]*4,
        "code":["1","2","3","4"],
        "d20_complete":[1,1,1,1],
        "forensic_group":["SUCCESS","FAILURE","SUCCESS","FAILURE"],
        "path_class":["FAST_SUCCESS","EARLY_SPIKE_GIVEBACK","SHAKEOUT_THEN_GO","FAILURE"],
        "d20_close_ret_pct":[10,-10,5,-5],
        "d20_mfe_pct":[15,4,9,2],
        "d20_mae_pct":[-2,-15,-5,-10],
        "wm_modern_state":["후행수박"]*4,
        "ma224_phase":["BELOW"]*4,
        "ma448_phase":["RECLAIM_20"]*4
    })
    t=outcome_rows(d,["wm_modern_state","ma224_phase","ma448_phase"],"TEST")
    assert len(t)==1 and int(t.iloc[0]["events"])==4
    assert t.iloc[0]["sample_flag"]=="SMALL_3_4"
    print("REAL_FULL_WATERMELON_LONGMA_PHASE_INTERACTION_R1_SELF_TEST PASS")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source-master",default="source_wm/reports/real_full_watermelon_longma_audit_r1/watermelon_event_master.csv")
    ap.add_argument("--output-dir",default="reports/real_full_watermelon_longma_phase_interaction_r1")
    ap.add_argument("--self-test",action="store_true")
    a=ap.parse_args()
    return self_test() if a.self_test else run(a)

if __name__=="__main__":
    main()
