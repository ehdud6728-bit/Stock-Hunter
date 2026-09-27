#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_15_EARLY_REACTION_PROSPECTIVE_20260927"
REQ=["signal_date","code","name","is_A","is_CORE","is_ODOLI"]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def load_signals(path):
    p=Path(path)
    if not p.exists(): raise SystemExit(f"MISSING_SIGNALS_FILE:{p}")
    q=pd.read_csv(p,dtype={"code":str},low_memory=False)
    miss=[c for c in REQ if c not in q.columns]
    if miss: raise SystemExit(f"MISSING_SIGNAL_COLUMNS:{miss}")
    if len(q)==0: return q
    q["code"]=q["code"].map(norm_code)
    q["signal_date"]=pd.to_datetime(q["signal_date"],errors="coerce").dt.normalize()
    return q

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
    if b==0 or pd.isna(a) or pd.isna(b): return np.nan
    return (a/b-1)*100

def boolify(s):
    return s.astype(str).str.lower().isin(["true","1"])

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--signals",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    sig=load_signals(a.signals)
    mar=load_marcap(a.marcap_root)

    if len(sig)==0:
        pd.DataFrame(columns=REQ).to_csv(out/"r215_prospective_early_reaction_state.csv",index=False,encoding="utf-8-sig")
        pd.DataFrame(columns=[
            "signal_date","code","name","d1_complete","d2_complete","d3_complete",
            "d1_2_higher_low","d2_holds_signal_low","d2_close_above_ma5",
            "d1_3_all_hold_signal_low","d1_3_all_above_ma5"
        ]).to_csv(out/"r215_prospective_early_reaction_observations.csv",index=False,encoding="utf-8-sig")
        meta={
            "revision":REV,"research_only":True,"production_logic_changed":False,
            "automatic_ordering":False,"new_gate_created":False,"same_sample_tuning":False,
            "prospective_observation_only":True,"r214_features_frozen_as_descriptors":True,
            "bootstrap_empty_signal_file":True,"rows":0,
            "max_market_date":str(mar["Date"].max().date()) if len(mar) else ""
        }
        (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        (out/"REPORT.md").write_text(
            "# ODOLI R2.15 Early Reaction Prospective Observer\n\n"
            "- Initialized successfully.\n"
            "- Prospective signal file is currently empty.\n"
            "- No historical backfill is performed.\n"
            "- R2.14 early-reaction findings are frozen as observation columns only, not gates.\n",
            encoding="utf-8")
        return

    sig["is_A"]=boolify(sig["is_A"])
    sig["is_CORE"]=boolify(sig["is_CORE"])
    sig["is_ODOLI"]=boolify(sig["is_ODOLI"])

    bycode={c:g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
            for c,g in mar.groupby("Code",sort=False)}

    rows=[]
    for _,r in sig.iterrows():
        rec=r.to_dict()
        g=bycode.get(r["code"])
        if g is None:
            rec["status"]="NO_CODE_HISTORY"; rows.append(rec); continue

        hit=g.index[g["Date"].eq(r["signal_date"])]
        if len(hit)!=1:
            rec["status"]="NO_SIGNAL_DATE"; rows.append(rec); continue
        i=int(hit[0])
        if i<20:
            rec["status"]="INSUFFICIENT_PREHISTORY"; rows.append(rec); continue

        sigrow=g.loc[i]
        sig_close=float(sigrow["Close"])
        sig_low=float(sigrow["Low"])
        sig_high=float(sigrow["High"])
        ma5=pd.to_numeric(g["Close"],errors="coerce").rolling(5).mean()

        prev20=g.iloc[i-20:i]
        vol20=float(pd.to_numeric(prev20["Volume"],errors="coerce").mean())
        amt20=float(pd.to_numeric(prev20["Amount"],errors="coerce").mean()) if "Amount" in prev20 else np.nan

        rec["status"]="OK"
        rec["signal_close"]=sig_close
        rec["signal_low"]=sig_low
        rec["signal_high"]=sig_high

        lows=[]
        closes=[]
        complete={}
        for d in [1,2,3]:
            if i+d>=len(g):
                complete[d]=False
                rec[f"d{d}_complete"]=False
                continue
            complete[d]=True
            rec[f"d{d}_complete"]=True
            rr=g.loc[i+d]
            o=float(rr["Open"]); h=float(rr["High"]); l=float(rr["Low"]); c=float(rr["Close"])
            v=float(rr["Volume"]); amt=float(rr["Amount"]) if "Amount" in rr else np.nan
            rng=max(h-l,1e-9)

            rec[f"d{d}_open_gap_pct"]=pct(o,sig_close)
            rec[f"d{d}_high_ret_pct"]=pct(h,sig_close)
            rec[f"d{d}_low_ret_pct"]=pct(l,sig_close)
            rec[f"d{d}_close_ret_pct"]=pct(c,sig_close)
            rec[f"d{d}_close_loc"]=(c-l)/rng
            rec[f"d{d}_volume_vs20"]=v/vol20 if vol20>0 else np.nan
            rec[f"d{d}_amount_vs20"]=amt/amt20 if pd.notna(amt20) and amt20>0 else np.nan
            rec[f"d{d}_holds_signal_low"]=bool(l>=sig_low)
            rec[f"d{d}_close_above_signal_close"]=bool(c>sig_close)
            rec[f"d{d}_high_breaks_signal_high"]=bool(h>sig_high)
            rec[f"d{d}_close_above_ma5"]=bool(c>float(ma5.iloc[i+d]))
            lows.append((d,l))
            closes.append((d,c))

        # Freeze R2.14 descriptors exactly as observations.
        if complete.get(2):
            d1low=dict(lows).get(1,np.nan); d2low=dict(lows).get(2,np.nan)
            rec["d1_2_higher_low"]=bool(d2low>d1low) if np.isfinite(d1low) and np.isfinite(d2low) else np.nan
            rec["r214_flag_d2_holds_signal_low"]=rec.get("d2_holds_signal_low")
            rec["r214_flag_d2_close_above_ma5"]=rec.get("d2_close_above_ma5")
        else:
            rec["d1_2_higher_low"]=np.nan
            rec["r214_flag_d2_holds_signal_low"]=np.nan
            rec["r214_flag_d2_close_above_ma5"]=np.nan

        if all(complete.get(d,False) for d in [1,2,3]):
            rec["d1_3_all_hold_signal_low"]=all(bool(rec[f"d{d}_holds_signal_low"]) for d in [1,2,3])
            rec["d1_3_all_above_ma5"]=all(bool(rec[f"d{d}_close_above_ma5"]) for d in [1,2,3])
            rec["d1_3_any_close_above_signal"]=any(bool(rec[f"d{d}_close_above_signal_close"]) for d in [1,2,3])
            rec["d1_3_any_high_break_signal"]=any(bool(rec[f"d{d}_high_breaks_signal_high"]) for d in [1,2,3])
            rec["d1_3_mfe_pct"]=max(float(rec[f"d{d}_high_ret_pct"]) for d in [1,2,3])
            rec["d1_3_mae_pct"]=min(float(rec[f"d{d}_low_ret_pct"]) for d in [1,2,3])
        else:
            for c in ["d1_3_all_hold_signal_low","d1_3_all_above_ma5",
                      "d1_3_any_close_above_signal","d1_3_any_high_break_signal",
                      "d1_3_mfe_pct","d1_3_mae_pct"]:
                rec[c]=np.nan

        # Outcome is observed only after D+5, never used to alter inclusion.
        if i+5<len(g):
            q=g.iloc[i+1:i+6]
            rec["d5_complete"]=True
            rec["d5_touch10_outcome"]=bool((pd.to_numeric(q["High"],errors="coerce")/sig_close-1).mul(100).ge(10).any())
            rec["d5_mfe_pct_outcome"]=float(((pd.to_numeric(q["High"],errors="coerce")/sig_close-1)*100).max())
            rec["d5_mae_pct_outcome"]=float(((pd.to_numeric(q["Low"],errors="coerce")/sig_close-1)*100).min())
            rec["d5_close_ret_pct_outcome"]=float((float(q.iloc[-1]["Close"])/sig_close-1)*100)
        else:
            rec["d5_complete"]=False
            rec["d5_touch10_outcome"]=np.nan

        rows.append(rec)

    z=pd.DataFrame(rows).sort_values(["signal_date","code"]).drop_duplicates(["signal_date","code"],keep="last")
    z.to_csv(out/"r215_prospective_early_reaction_state.csv",index=False,encoding="utf-8-sig")

    obs_cols=[c for c in [
        "signal_date","code","name","is_A","is_CORE","is_ODOLI","status",
        "d1_complete","d2_complete","d3_complete",
        "d1_open_gap_pct","d1_high_ret_pct","d1_low_ret_pct","d1_close_ret_pct","d1_volume_vs20","d1_amount_vs20",
        "d2_high_ret_pct","d2_low_ret_pct","d2_close_ret_pct","d2_volume_vs20","d2_amount_vs20",
        "d3_high_ret_pct","d3_low_ret_pct","d3_close_ret_pct","d3_volume_vs20","d3_amount_vs20",
        "d1_2_higher_low","d2_holds_signal_low","d2_close_above_ma5",
        "d1_3_all_hold_signal_low","d1_3_all_above_ma5",
        "d1_3_any_close_above_signal","d1_3_any_high_break_signal",
        "d1_3_mfe_pct","d1_3_mae_pct",
        "d5_complete","d5_touch10_outcome","d5_mfe_pct_outcome","d5_mae_pct_outcome","d5_close_ret_pct_outcome"
    ] if c in z.columns]
    z[obs_cols].to_csv(out/"r215_prospective_early_reaction_observations.csv",index=False,encoding="utf-8-sig")

    # Prospective summary only after outcomes mature.
    mature=z[z.get("d5_complete",False)==True].copy() if "d5_complete" in z.columns else pd.DataFrame()
    summary=[]
    if len(mature):
        for feat in ["d1_2_higher_low","d2_holds_signal_low","d2_close_above_ma5",
                     "d1_3_all_hold_signal_low","d1_3_all_above_ma5"]:
            if feat not in mature.columns: continue
            for val,g in mature.groupby(feat,dropna=True):
                if len(g)==0: continue
                summary.append({
                    "feature":feat,"feature_value":bool(val),"n":len(g),
                    "d5_touch10_n":int(g["d5_touch10_outcome"].fillna(False).astype(bool).sum()),
                    "d5_touch10_rate_pct":float(g["d5_touch10_outcome"].fillna(False).astype(bool).mean()*100)
                })
    pd.DataFrame(summary).to_csv(out/"r215_prospective_feature_outcome_summary.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REV,"research_only":True,"production_logic_changed":False,
        "automatic_ordering":False,"new_gate_created":False,"same_sample_tuning":False,
        "prospective_observation_only":True,"r214_features_frozen_as_descriptors":True,
        "bootstrap_empty_signal_file":False,"rows":len(z),
        "mature_d5_rows":int(z["d5_complete"].fillna(False).astype(bool).sum()) if "d5_complete" in z.columns else 0,
        "max_market_date":str(mar["Date"].max().date()) if len(mar) else ""
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    (out/"REPORT.md").write_text(
        "# ODOLI R2.15 Early Reaction Prospective Observer\n\n"
        f"- Prospective rows: {len(z)}\n"
        f"- Mature D+5 outcomes: {meta['mature_d5_rows']}\n"
        "- R2.14 early-reaction findings are recorded unchanged as descriptors only.\n"
        "- No feature changes candidate inclusion, ranking, or ordering.\n",
        encoding="utf-8"
    )

if __name__=="__main__":
    main()
