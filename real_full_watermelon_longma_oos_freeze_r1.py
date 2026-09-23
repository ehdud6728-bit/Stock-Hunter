#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import numpy as np
import pandas as pd

RESEARCH_ID="REAL_FULL_WATERMELON_LONGMA_OOS_FREEZE_R1"
REVISION="R1_FROZEN_INTERACTION_SIGNATURES_20260923"
DISCOVERY_END=pd.Timestamp("2026-08-28")
HOLDOUT_START=pd.Timestamp("2026-09-01")
FROZEN_AXES={
    "MODERN_STATE_X_MA224_X_MA448":["wm_modern_state","ma224_phase","ma448_phase"],
    "MODERN_STATE_X_ALL_LONGMA_PHASES":["wm_modern_state","ma112_phase","ma224_phase","ma448_phase"],
}
MIN_DISCOVERY_N=3

def sha_bytes(b: bytes)->str:
    return hashlib.sha256(b).hexdigest()

def canonical_registry(df: pd.DataFrame)->pd.DataFrame:
    cols=[
        "axis","wm_modern_state","ma112_phase","ma224_phase","ma448_phase",
        "events","success_events","failure_events","fast_success_events",
        "shakeout_then_go_events","early_spike_giveback_events","time_failure_events",
        "success_rate","failure_rate","giveback_rate","d20_close_median",
        "d20_mfe_median","d20_mae_median","sample_flag"
    ]
    z=df[df["axis"].isin(FROZEN_AXES)].copy()
    z=z[pd.to_numeric(z["events"],errors="coerce").ge(MIN_DISCOVERY_N)].copy()
    z=z[[c for c in cols if c in z.columns]]
    for c in ["wm_modern_state","ma112_phase","ma224_phase","ma448_phase"]:
        if c not in z: z[c]=""
        z[c]=z[c].fillna("").astype(str)
    z["signature_id"]=z.apply(
        lambda r: "WMF_"+hashlib.sha256(
            ("|".join([str(r.get("axis","")),str(r.get("wm_modern_state","")),
                       str(r.get("ma112_phase","")),str(r.get("ma224_phase","")),
                       str(r.get("ma448_phase",""))])).encode("utf-8")
        ).hexdigest()[:12].upper(),
        axis=1
    )
    z["freeze_role"]="ALL_REPEATED_DISCOVERY_SIGNATURES_N3PLUS"
    z["research_only"]=1
    z["production_gate_allowed"]=0
    z=z.sort_values(["axis","wm_modern_state","ma112_phase","ma224_phase","ma448_phase"]).reset_index(drop=True)
    return z

def row_match(row, sig):
    axis=sig["axis"]
    for col in FROZEN_AXES[axis]:
        if str(row.get(col,"") if pd.notna(row.get(col,"")) else "") != str(sig.get(col,"") if pd.notna(sig.get(col,"")) else ""):
            return False
    return True

def summarize_oos(matched: pd.DataFrame)->pd.DataFrame:
    rows=[]
    if matched.empty:return pd.DataFrame()
    for sid,g in matched.groupby("signature_id"):
        mature=g[pd.to_numeric(g.get("d20_complete"),errors="coerce").eq(1)].copy()
        d={
            "signature_id":sid,
            "oos_events_total":len(g),
            "oos_d20_mature_events":len(mature),
            "oos_d20_immature_events":len(g)-len(mature),
        }
        if len(mature):
            d.update({
                "oos_success_events":int(mature["forensic_group"].eq("SUCCESS").sum()),
                "oos_failure_events":int(mature["forensic_group"].eq("FAILURE").sum()),
                "oos_fast_success_events":int(mature["path_class"].eq("FAST_SUCCESS").sum()),
                "oos_shakeout_then_go_events":int(mature["path_class"].eq("SHAKEOUT_THEN_GO").sum()),
                "oos_early_spike_giveback_events":int(mature["path_class"].eq("EARLY_SPIKE_GIVEBACK").sum()),
                "oos_success_rate":float(mature["forensic_group"].eq("SUCCESS").mean()),
                "oos_failure_rate":float(mature["forensic_group"].eq("FAILURE").mean()),
                "oos_d20_close_median":pd.to_numeric(mature["d20_close_ret_pct"],errors="coerce").median(),
            })
        rows.append(d)
    return pd.DataFrame(rows)

def run(a):
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    inter=pd.read_csv(a.interaction_master)
    registry=canonical_registry(inter)
    if registry.empty:
        raise SystemExit("NO_FROZEN_SIGNATURES")
    registry.to_csv(out/"FROZEN_watermelon_longma_signature_registry.csv",index=False,encoding="utf-8-sig")
    registry_hash=sha_bytes((out/"FROZEN_watermelon_longma_signature_registry.csv").read_bytes())

    src=pd.read_csv(a.watermelon_master)
    src["origin_date"]=pd.to_datetime(src["origin_date"],errors="coerce").dt.normalize()
    oos=src[src["origin_date"].ge(HOLDOUT_START)].copy()

    matches=[]
    for _,r in oos.iterrows():
        for _,sig in registry.iterrows():
            if row_match(r,sig):
                d=r.to_dict()
                d.update({
                    "signature_id":sig["signature_id"],
                    "signature_axis":sig["axis"],
                    "discovery_events_frozen":sig["events"],
                    "discovery_success_rate_frozen":sig.get("success_rate",np.nan),
                    "discovery_failure_rate_frozen":sig.get("failure_rate",np.nan),
                    "discovery_d20_close_median_frozen":sig.get("d20_close_median",np.nan),
                    "registry_hash":registry_hash,
                })
                matches.append(d)

    matched=pd.DataFrame(matches)
    monitor_cols=[
        "origin_date","code","name","origin_search_pattern","signature_id","signature_axis",
        "wm_modern_state","ma112_phase","ma224_phase","ma448_phase","longma_phase_code",
        "discovery_events_frozen","registry_hash"
    ]
    if matched.empty:
        pd.DataFrame(columns=monitor_cols).to_csv(out/"oos_signature_matches_MONITOR_ONLY.csv",index=False,encoding="utf-8-sig")
    else:
        matched[[c for c in monitor_cols if c in matched.columns]].to_csv(
            out/"oos_signature_matches_MONITOR_ONLY.csv",index=False,encoding="utf-8-sig"
        )

    summary=summarize_oos(matched)
    summary.to_csv(out/"oos_signature_outcome_summary_MATURE_ONLY.csv",index=False,encoding="utf-8-sig")

    meta={
        "research_id":RESEARCH_ID,
        "revision":REVISION,
        "status":"PASS",
        "freeze_source_interaction_run_default":"35827339397",
        "watermelon_source_run_default":"35818983451",
        "discovery_end":"2026-08-28",
        "holdout_start":"2026-09-01",
        "frozen_axes":FROZEN_AXES,
        "min_discovery_n":MIN_DISCOVERY_N,
        "frozen_signature_count":len(registry),
        "registry_sha256":registry_hash,
        "oos_events_available":len(oos),
        "oos_match_rows":len(matched),
        "research_only":True,
        "production_eligible":False,
        "selection_logic_changed":False,
        "score_rank_changed":False,
        "order_logic_changed":False,
        "same_sample_retuning":False,
        "registry_retuning_allowed":False,
        "immature_d20_treated_as_failure":False,
    }
    (out/"watermelon_longma_oos_freeze_meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    (out/"watermelon_longma_oos_freeze_report.txt").write_text(
        "\n".join([
            f"🧊 [{RESEARCH_ID}]",
            f"status=PASS revision={REVISION}",
            f"frozen_signatures={len(registry)} registry_sha256={registry_hash}",
            f"oos_events_available={len(oos)} match_rows={len(matched)}",
            "All Discovery repeated modern-state/long-MA signatures with n>=3 are frozen; no cherry-pick.",
            "D20 immature rows remain immature and are never scored as failure.",
            "research_only=1 production_gate=0 retuning=0",
        ])+"\n",encoding="utf-8"
    )
    print((out/"watermelon_longma_oos_freeze_report.txt").read_text())

def self_test():
    inter=pd.DataFrame([
        dict(axis="MODERN_STATE_X_MA224_X_MA448",wm_modern_state="후행수박",ma224_phase="BELOW",ma448_phase="RECLAIM_20",events=3,success_events=2,failure_events=1),
        dict(axis="MODERN_STATE_X_MA224_X_MA448",wm_modern_state="후행수박",ma224_phase="ABOVE_STABLE",ma448_phase="ABOVE_STABLE",events=2,success_events=1,failure_events=1),
    ])
    reg=canonical_registry(inter)
    assert len(reg)==1
    row={"wm_modern_state":"후행수박","ma224_phase":"BELOW","ma448_phase":"RECLAIM_20"}
    assert row_match(row,reg.iloc[0])
    print("REAL_FULL_WATERMELON_LONGMA_OOS_FREEZE_R1_SELF_TEST PASS")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--interaction-master",default="source_interaction/reports/real_full_watermelon_longma_phase_interaction_r1/watermelon_longma_interaction_master.csv")
    ap.add_argument("--watermelon-master",default="source_wm/reports/real_full_watermelon_longma_audit_r1/watermelon_event_master.csv")
    ap.add_argument("--output-dir",default="reports/real_full_watermelon_longma_oos_freeze_r1")
    ap.add_argument("--self-test",action="store_true")
    a=ap.parse_args()
    return self_test() if a.self_test else run(a)

if __name__=="__main__":
    main()
