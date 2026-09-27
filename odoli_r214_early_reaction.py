#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_14_EARLY_REACTION_ANATOMY_20260927"

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs: raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def load_marcap(root):
    p=Path(root)/"data"/"marcap-2026.parquet"
    if not p.exists(): raise SystemExit(f"MISSING:{p}")
    q=pd.read_parquet(p)
    if "Date" not in q.columns: q=q.reset_index()
    q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
    q["Code"]=q["Code"].map(norm_code)
    q=q[q["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])].copy()
    return q

def pct(a,b):
    if pd.isna(a) or pd.isna(b) or b==0: return np.nan
    return (a/b-1)*100

def median(x):
    s=pd.to_numeric(x,errors="coerce").dropna()
    return float(s.median()) if len(s) else np.nan

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r210-root",required=True)
    ap.add_argument("--r211-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    base=pd.read_csv(find_one(a.r210_root,"r210_event_anatomy.csv"),dtype={"code":str},low_memory=False)
    base["code"]=base["code"].map(norm_code)
    base["signal_date"]=pd.to_datetime(base["signal_date"],errors="coerce").dt.normalize()
    base["success_d5_touch10"]=base["success_d5_touch10"].astype(str).str.lower().isin(["true","1"])

    acc=pd.read_csv(find_one(a.r211_root,"r211_accumulation_event_audit.csv"),dtype={"code":str},low_memory=False)
    acc["code"]=acc["code"].map(norm_code)
    acc["signal_date"]=pd.to_datetime(acc["signal_date"],errors="coerce").dt.normalize()
    keep=[c for c in ["signal_date","code","best_accum_low","best_accum_high",
                      "best_accum_score","best_accum_days_before_signal"] if c in acc.columns]
    base=base.merge(acc[keep],on=["signal_date","code"],how="left",validate="one_to_one")

    if len(base)!=19 or base["success_d5_touch10"].sum()!=10:
        raise SystemExit(f"COHORT_MISMATCH n={len(base)} success={base['success_d5_touch10'].sum()}")

    mar=load_marcap(a.marcap_root)
    bycode={c:g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
            for c,g in mar.groupby("Code",sort=False)}

    rows=[]
    for _,r in base.iterrows():
        g=bycode.get(r["code"])
        if g is None: continue
        hit=g.index[g["Date"].eq(r["signal_date"])]
        if len(hit)!=1: continue
        i=int(hit[0])
        if i+3>=len(g) or i<20: continue

        sig=g.loc[i]
        sig_close=float(sig["Close"]); sig_low=float(sig["Low"]); sig_high=float(sig["High"])
        prev20=g.iloc[i-20:i]
        vol20=float(pd.to_numeric(prev20["Volume"],errors="coerce").mean())
        amt20=float(pd.to_numeric(prev20["Amount"],errors="coerce").mean()) if "Amount" in prev20 else np.nan

        rec=r.to_dict()
        rec["signal_close"]=sig_close
        rec["signal_low"]=sig_low
        rec["signal_high"]=sig_high

        closes=[]
        lows=[]
        highs=[]
        for d in [1,2,3]:
            rr=g.loc[i+d]
            o=float(rr["Open"]); h=float(rr["High"]); l=float(rr["Low"]); c=float(rr["Close"])
            v=float(rr["Volume"]); amt=float(rr["Amount"]) if "Amount" in rr else np.nan
            rng=max(h-l,1e-9)
            close_loc=(c-l)/rng
            body=(c-o)/o*100 if o else np.nan

            rec[f"d{d}_open_gap_pct"]=pct(o,sig_close)
            rec[f"d{d}_high_ret_pct"]=pct(h,sig_close)
            rec[f"d{d}_low_ret_pct"]=pct(l,sig_close)
            rec[f"d{d}_close_ret_pct"]=pct(c,sig_close)
            rec[f"d{d}_close_loc"]=close_loc
            rec[f"d{d}_body_pct"]=body
            rec[f"d{d}_volume_vs20"]=v/vol20 if vol20>0 else np.nan
            rec[f"d{d}_amount_vs20"]=amt/amt20 if pd.notna(amt20) and amt20>0 else np.nan
            rec[f"d{d}_holds_signal_low"]=bool(l>=sig_low)
            rec[f"d{d}_close_above_signal_close"]=bool(c>sig_close)
            rec[f"d{d}_high_breaks_signal_high"]=bool(h>sig_high)

            closes.append(c); lows.append(l); highs.append(h)

        # Early path structure
        rec["d1_3_mfe_pct"]=pct(max(highs),sig_close)
        rec["d1_3_mae_pct"]=pct(min(lows),sig_close)
        rec["d1_3_all_hold_signal_low"]=all(rec[f"d{d}_holds_signal_low"] for d in [1,2,3])
        rec["d1_3_any_close_above_signal"]=any(rec[f"d{d}_close_above_signal_close"] for d in [1,2,3])
        rec["d1_3_any_high_break_signal"]=any(rec[f"d{d}_high_breaks_signal_high"] for d in [1,2,3])

        # Higher-low approximation across D1-D3.
        rec["d1_2_higher_low"]=bool(lows[1]>lows[0])
        rec["d2_3_higher_low"]=bool(lows[2]>lows[1])
        rec["two_step_higher_low"]=bool(lows[1]>lows[0] and lows[2]>lows[1])

        # Signal-to-MA5 early persistence.
        hist_close=pd.to_numeric(g["Close"],errors="coerce")
        ma5=hist_close.rolling(5).mean()
        for d in [1,2,3]:
            rec[f"d{d}_close_above_ma5"]=bool(float(g.loc[i+d,"Close"])>float(ma5.iloc[i+d]))
        rec["d1_3_all_above_ma5"]=all(rec[f"d{d}_close_above_ma5"] for d in [1,2,3])

        # Accumulation support persistence.
        al=pd.to_numeric(pd.Series([r.get("best_accum_low")]),errors="coerce").iloc[0]
        ah=pd.to_numeric(pd.Series([r.get("best_accum_high")]),errors="coerce").iloc[0]
        if np.isfinite(al) and np.isfinite(ah):
            amid=(al+ah)/2
            rec["d1_3_all_close_above_accum_mid"]=all(c>=amid for c in closes)
            rec["d1_3_any_break_accum_low"]=any(l<al for l in lows)
            rec["d1_3_any_close_above_accum_high"]=any(c>ah for c in closes)
        else:
            rec["d1_3_all_close_above_accum_mid"]=np.nan
            rec["d1_3_any_break_accum_low"]=np.nan
            rec["d1_3_any_close_above_accum_high"]=np.nan

        rows.append(rec)

    z=pd.DataFrame(rows)
    z.to_csv(out/"r214_early_reaction_events.csv",index=False,encoding="utf-8-sig")

    # Continuous feature comparison.
    cont=[
        "d1_open_gap_pct","d1_high_ret_pct","d1_low_ret_pct","d1_close_ret_pct","d1_close_loc",
        "d1_volume_vs20","d1_amount_vs20",
        "d2_high_ret_pct","d2_low_ret_pct","d2_close_ret_pct","d2_close_loc",
        "d3_high_ret_pct","d3_low_ret_pct","d3_close_ret_pct","d3_close_loc",
        "d1_3_mfe_pct","d1_3_mae_pct"
    ]
    crows=[]
    s=z[z["success_d5_touch10"]]; f=z[~z["success_d5_touch10"]]
    for c in cont:
        if c not in z.columns: continue
        sm=median(s[c]); fm=median(f[c])
        crows.append({
            "feature":c,
            "success_n":len(s),"failure_n":len(f),
            "success_median":sm,"failure_median":fm,
            "median_diff_success_minus_failure":sm-fm if pd.notna(sm) and pd.notna(fm) else np.nan
        })
    cdf=pd.DataFrame(crows)
    cdf.to_csv(out/"r214_continuous_success_failure.csv",index=False,encoding="utf-8-sig")

    # Boolean feature rates.
    bools=[
        "d1_holds_signal_low","d2_holds_signal_low","d3_holds_signal_low",
        "d1_close_above_signal_close","d2_close_above_signal_close","d3_close_above_signal_close",
        "d1_high_breaks_signal_high","d2_high_breaks_signal_high","d3_high_breaks_signal_high",
        "d1_3_all_hold_signal_low","d1_3_any_close_above_signal","d1_3_any_high_break_signal",
        "d1_2_higher_low","d2_3_higher_low","two_step_higher_low",
        "d1_close_above_ma5","d2_close_above_ma5","d3_close_above_ma5","d1_3_all_above_ma5",
        "d1_3_all_close_above_accum_mid","d1_3_any_break_accum_low","d1_3_any_close_above_accum_high"
    ]
    brows=[]
    for c in bools:
        if c not in z.columns: continue
        for label,g in [("SUCCESS_10P_D5",s),("FAIL_NO_10P_D5",f)]:
            vals=g[c].dropna()
            if len(vals)==0: continue
            vals=vals.astype(bool)
            brows.append({
                "feature":c,"group":label,"n":len(vals),
                "true_n":int(vals.sum()),"true_rate_pct":float(vals.mean()*100)
            })
    bdf=pd.DataFrame(brows)
    bdf.to_csv(out/"r214_boolean_success_failure.csv",index=False,encoding="utf-8-sig")

    # Name audit.
    name_cols=[c for c in [
        "signal_date","code","name","success_d5_touch10",
        "d1_close_ret_pct","d1_low_ret_pct","d1_volume_vs20","d1_close_loc",
        "d2_close_ret_pct","d2_low_ret_pct","d2_volume_vs20","d2_close_loc",
        "d3_close_ret_pct","d3_low_ret_pct","d3_volume_vs20","d3_close_loc",
        "d1_3_mfe_pct","d1_3_mae_pct","d1_3_all_hold_signal_low",
        "d1_3_any_high_break_signal","two_step_higher_low","d1_3_all_above_ma5",
        "d1_3_all_close_above_accum_mid","d1_3_any_break_accum_low"
    ] if c in z.columns]
    z[name_cols].sort_values(["success_d5_touch10","signal_date"],ascending=[False,True]).to_csv(
        out/"r214_name_audit.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REV,"research_only":True,"production_logic_changed":False,
        "new_gate_created":False,"same_sample_tuning":False,
        "cohort_n":len(z),"success_n":int(z["success_d5_touch10"].sum()),
        "failure_n":int((~z["success_d5_touch10"]).sum()),
        "outcome":"D5_HIGH_TOUCH_GE_10P",
        "exposure_window":"D1_TO_D3_ONLY",
        "purpose":"DESCRIPTIVE_EARLY_REACTION_ANATOMY"
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    report=[
        "# ODOLI R2.14 — Early Reaction Anatomy","",
        "- Frozen cohort: 19 CORE+ODOLI events (10 D+5 +10% successes / 9 misses).",
        "- Compares only D+1 to D+3 behavior.",
        "- No feature is promoted into a gate.","",
        "## Continuous medians","```",cdf.to_string(index=False),"```","",
        "## Boolean rates","```",bdf.to_string(index=False),"```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

if __name__=="__main__":
    main()
