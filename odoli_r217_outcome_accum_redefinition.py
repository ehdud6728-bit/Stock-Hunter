#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_17_OUTCOME_ACCUMULATION_REDEFINITION_20260928"
HORIZONS=[5,10,20,40,60]
TARGETS=[3,5,7,10,15,20,30]

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs: raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def load_marcap(root):
    frames=[]
    for y in [2024,2025,2026]:
        p=Path(root)/"data"/f"marcap-{y}.parquet"
        if not p.exists(): raise SystemExit(f"MISSING_MARCAP:{p}")
        q=pd.read_parquet(p)
        if "Date" not in q.columns: q=q.reset_index()
        q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
        q["Code"]=q["Code"].map(norm_code)
        q=q[q["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])].copy()
        frames.append(q)
    return pd.concat(frames,ignore_index=True)

def pct(a,b):
    if pd.isna(a) or pd.isna(b) or b==0:return np.nan
    return (a/b-1)*100

def candle_only_score(g,idx):
    if idx<=0 or idx>=len(g): return {}
    r=g.iloc[idx]
    prev=g.iloc[max(0,idx-20):idx]
    o,h,l,c=[float(r[x]) for x in ["Open","High","Low","Close"]]
    v=float(r["Volume"])
    amt=float(r["Amount"]) if "Amount" in r.index and pd.notna(r["Amount"]) else np.nan
    rng=max(h-l,1e-9); body=abs(c-o)
    lw=(min(o,c)-l)/rng
    uw=(h-max(o,c))/rng
    close_pos=(c-l)/rng
    body_ratio=body/rng
    vma=float(pd.to_numeric(prev["Volume"],errors="coerce").mean()) if len(prev) else np.nan
    ama=float(pd.to_numeric(prev["Amount"],errors="coerce").mean()) if "Amount" in prev and len(prev) else np.nan
    vr=v/vma if pd.notna(vma) and vma>0 else np.nan
    ar=amt/ama if pd.notna(amt) and pd.notna(ama) and ama>0 else np.nan

    score=0
    if lw>=.35: score+=25
    elif lw>=.20: score+=15
    elif lw>=.10: score+=5
    if close_pos>=.75: score+=20
    elif close_pos>=.60: score+=12
    elif close_pos>=.50: score+=5
    if body_ratio>=.35: score+=12
    elif body_ratio>=.20: score+=6
    if uw>=.35: score-=10
    elif uw>=.25: score-=5
    if pd.notna(vr):
        if vr>=2: score+=22
        elif vr>=1.4: score+=14
        elif vr>=1.1: score+=6

    return {
        "candle_only_score":max(0,min(82,int(round(score)))),
        "accum_open":o,"accum_high":h,"accum_low":l,"accum_close":c,
        "accum_volume_ratio20":vr,"accum_amount_ratio20":ar,
        "accum_close_pos":close_pos,"accum_lower_wick_ratio":lw,
        "accum_upper_wick_ratio":uw,"accum_body_ratio":body_ratio
    }

def sequence_metrics(g,sig_i,lookback=20):
    # Pick strongest pre-signal candle using candle-only information only.
    start=max(1,sig_i-lookback)
    best=None; best_i=None
    for j in range(start,sig_i):
        x=candle_only_score(g,j)
        if not x: continue
        if best is None or x["candle_only_score"]>best["candle_only_score"]:
            best=x; best_i=j
    if best is None:return {}

    al,ah=best["accum_low"],best["accum_high"]
    amid=(al+ah)/2
    between=g.iloc[best_i+1:sig_i+1].copy()
    post_before_signal=g.iloc[best_i+1:sig_i].copy()

    best["accum_days_before_signal"]=sig_i-best_i
    best["accum_mid"]=amid

    if len(post_before_signal):
        lows=pd.to_numeric(post_before_signal["Low"],errors="coerce")
        closes=pd.to_numeric(post_before_signal["Close"],errors="coerce")
        best["pre_signal_holds_accum_low"]=bool(lows.min()>=al)
        best["pre_signal_close_holds_accum_mid"]=bool(closes.min()>=amid)

        # Dry-up is continuous, not a tuned binary gate.
        cand_v=float(g.iloc[best_i]["Volume"])
        cand_a=float(g.iloc[best_i]["Amount"]) if "Amount" in g.columns else np.nan
        best["pullback_volume_vs_accum"]=float(pd.to_numeric(post_before_signal["Volume"],errors="coerce").mean()/cand_v) if cand_v>0 else np.nan
        best["pullback_amount_vs_accum"]=float(pd.to_numeric(post_before_signal["Amount"],errors="coerce").mean()/cand_a) if "Amount" in post_before_signal and pd.notna(cand_a) and cand_a>0 else np.nan

        # Compression: last 3 pre-signal average range versus preceding up-to-10 days.
        pre3=post_before_signal.tail(3)
        prev10=post_before_signal.iloc[max(0,len(post_before_signal)-13):max(0,len(post_before_signal)-3)]
        pre3_rng=((pd.to_numeric(pre3["High"],errors="coerce")-pd.to_numeric(pre3["Low"],errors="coerce"))/
                  pd.to_numeric(pre3["Close"],errors="coerce").replace(0,np.nan)*100).mean() if len(pre3) else np.nan
        prev_rng=((pd.to_numeric(prev10["High"],errors="coerce")-pd.to_numeric(prev10["Low"],errors="coerce"))/
                  pd.to_numeric(prev10["Close"],errors="coerce").replace(0,np.nan)*100).mean() if len(prev10) else np.nan
        best["range_contract_3v10"]=pre3_rng/prev_rng if pd.notna(prev_rng) and prev_rng>0 else np.nan
    else:
        best["pre_signal_holds_accum_low"]=np.nan
        best["pre_signal_close_holds_accum_mid"]=np.nan
        best["pullback_volume_vs_accum"]=np.nan
        best["pullback_amount_vs_accum"]=np.nan
        best["range_contract_3v10"]=np.nan

    # Signal restart descriptors.
    sig=g.iloc[sig_i]
    close=pd.to_numeric(g["Close"],errors="coerce")
    ma5=close.rolling(5).mean()
    best["signal_close_above_ma5"]=bool(float(sig["Close"])>float(ma5.iloc[sig_i])) if pd.notna(ma5.iloc[sig_i]) else np.nan
    best["signal_ma5_slope_up"]=bool(float(ma5.iloc[sig_i])>float(ma5.iloc[sig_i-1])) if sig_i>=1 and pd.notna(ma5.iloc[sig_i-1]) else np.nan

    pre20=g.iloc[max(0,sig_i-20):sig_i]
    v20=float(pd.to_numeric(pre20["Volume"],errors="coerce").mean()) if len(pre20) else np.nan
    a20=float(pd.to_numeric(pre20["Amount"],errors="coerce").mean()) if "Amount" in pre20 and len(pre20) else np.nan
    sv=float(sig["Volume"]); sa=float(sig["Amount"]) if "Amount" in sig.index else np.nan
    best["signal_volume_ratio20"]=sv/v20 if pd.notna(v20) and v20>0 else np.nan
    best["signal_amount_ratio20"]=sa/a20 if pd.notna(sa) and pd.notna(a20) and a20>0 else np.nan

    # Non-authoritative sequence component count. Existing thresholds only; descriptive.
    comp=[]
    comp.append(bool(pd.notna(best["accum_volume_ratio20"]) and best["accum_volume_ratio20"]>=1.4))
    comp.append(bool(best.get("pre_signal_holds_accum_low") is True))
    comp.append(bool(pd.notna(best.get("pullback_volume_vs_accum")) and best["pullback_volume_vs_accum"]<1.0))
    comp.append(bool(pd.notna(best.get("range_contract_3v10")) and best["range_contract_3v10"]<1.0))
    comp.append(bool(best.get("signal_ma5_slope_up") is True))
    best["sequence_component_count"]=sum(comp)
    return best

def path_metrics(g,sig_i):
    sig=float(g.iloc[sig_i]["Close"])
    fut=g.iloc[sig_i+1:sig_i+61].copy()
    out={}
    for h in HORIZONS:
        q=fut.iloc[:h]
        out[f"d{h}_complete"]=len(q)>=h
        if len(q)>=h:
            high_ret=(pd.to_numeric(q["High"],errors="coerce")/sig-1)*100
            low_ret=(pd.to_numeric(q["Low"],errors="coerce")/sig-1)*100
            close_ret=(pd.to_numeric(q["Close"],errors="coerce")/sig-1)*100
            out[f"d{h}_mfe_pct"]=float(high_ret.max())
            out[f"d{h}_mae_pct"]=float(low_ret.min())
            out[f"d{h}_close_ret_pct"]=float(close_ret.iloc[-1])
            out[f"d{h}_giveback_pp"]=out[f"d{h}_mfe_pct"]-out[f"d{h}_close_ret_pct"]
            for t in TARGETS:
                hit=high_ret.ge(t)
                out[f"d{h}_touch_{t}"]=bool(hit.any())
                out[f"d{h}_first_touch_day_{t}"]=int(np.argmax(hit.to_numpy())+1) if hit.any() else np.nan
        else:
            for c in ["mfe_pct","mae_pct","close_ret_pct","giveback_pp"]:
                out[f"d{h}_{c}"]=np.nan
            for t in TARGETS:
                out[f"d{h}_touch_{t}"]=np.nan
                out[f"d{h}_first_touch_day_{t}"]=np.nan

    # Outcome taxonomy: descriptive, not a gate.
    if out.get("d5_complete") and bool(out.get("d5_touch_10",False)):
        out["outcome_class"]="FAST_BIG_WAVE"
    elif out.get("d5_complete") and bool(out.get("d5_touch_5",False)):
        out["outcome_class"]="FAST_WIN"
    elif out.get("d60_complete") and bool(out.get("d60_touch_20",False)):
        out["outcome_class"]="DELAYED_BIG_WAVE"
    elif out.get("d60_complete") and bool(out.get("d60_touch_10",False)):
        out["outcome_class"]="DELAYED_WIN"
    elif out.get("d60_complete"):
        out["outcome_class"]="NO_10_BY_D60"
    else:
        out["outcome_class"]="PENDING_D60"
    return out

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r216-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)

    src=pd.read_csv(find_one(a.r216_root,"r216_core_odoli_events.csv"),dtype={"code":str},low_memory=False)
    src["code"]=src["code"].map(norm_code)
    src["signal_date"]=pd.to_datetime(src["signal_date"],errors="coerce").dt.normalize()
    if len(src)!=100: raise SystemExit(f"CORE_ODOLI_COUNT_MISMATCH:{len(src)}")

    mar=load_marcap(a.marcap_root)
    bycode={c:g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
            for c,g in mar.groupby("Code",sort=False)}

    rows=[]
    for _,r in src.iterrows():
        rec={k:r[k] for k in ["signal_date","code","name","period"] if k in r.index}
        g=bycode.get(r["code"])
        if g is None:
            rec["status"]="NO_HISTORY"; rows.append(rec); continue
        hit=g.index[g["Date"].eq(r["signal_date"])]
        if len(hit)!=1:
            rec["status"]="NO_SIGNAL_DATE"; rows.append(rec); continue
        i=int(hit[0])
        rec["status"]="OK"
        rec.update(sequence_metrics(g,i,20))
        rec.update(path_metrics(g,i))
        rows.append(rec)

    z=pd.DataFrame(rows)
    z.to_csv(out/"r217_event_redefinition.csv",index=False,encoding="utf-8-sig")

    # Multi-target / multi-horizon table.
    rr=[]
    for h in HORIZONS:
        d=z[z[f"d{h}_complete"].fillna(False).astype(bool)].copy()
        for t in TARGETS:
            rr.append({
                "horizon":h,"target_pct":t,"complete_n":len(d),
                "touch_n":int(d[f"d{h}_touch_{t}"].fillna(False).astype(bool).sum()),
                "touch_rate_pct":float(d[f"d{h}_touch_{t}"].fillna(False).astype(bool).mean()*100) if len(d) else np.nan,
                "median_first_touch_day":float(pd.to_numeric(
                    d.loc[d[f"d{h}_touch_{t}"].fillna(False).astype(bool),f"d{h}_first_touch_day_{t}"],
                    errors="coerce").median()) if len(d) else np.nan
            })
    pd.DataFrame(rr).to_csv(out/"r217_target_horizon_matrix.csv",index=False,encoding="utf-8-sig")

    # Outcome taxonomy.
    oc=(z.groupby("outcome_class",dropna=False).size().rename("n").reset_index())
    oc["rate_pct"]=oc["n"]/len(z)*100
    oc.to_csv(out/"r217_outcome_classes.csv",index=False,encoding="utf-8-sig")

    # Compare accumulation descriptors by outcome class and by D60 +10/+20.
    desc=[
        "candle_only_score","accum_volume_ratio20","accum_amount_ratio20",
        "pullback_volume_vs_accum","pullback_amount_vs_accum","range_contract_3v10",
        "signal_volume_ratio20","signal_amount_ratio20","sequence_component_count"
    ]
    comps=[]
    for label,mask in [
        ("D5_TOUCH5",z["d5_touch_5"].fillna(False).astype(bool)),
        ("D5_NO_TOUCH5",~z["d5_touch_5"].fillna(False).astype(bool)),
        ("D5_TOUCH10",z["d5_touch_10"].fillna(False).astype(bool)),
        ("D5_NO_TOUCH10",~z["d5_touch_10"].fillna(False).astype(bool)),
        ("D60_TOUCH10",z["d60_touch_10"].fillna(False).astype(bool)),
        ("D60_NO_TOUCH10",~z["d60_touch_10"].fillna(False).astype(bool)),
        ("D60_TOUCH20",z["d60_touch_20"].fillna(False).astype(bool)),
        ("D60_NO_TOUCH20",~z["d60_touch_20"].fillna(False).astype(bool)),
    ]:
        g=z[mask].copy()
        row={"group":label,"n":len(g)}
        for c in desc:
            row[c+"_median"]=float(pd.to_numeric(g[c],errors="coerce").median()) if c in g else np.nan
        for c in ["pre_signal_holds_accum_low","pre_signal_close_holds_accum_mid",
                  "signal_close_above_ma5","signal_ma5_slope_up"]:
            if c in g:
                x=g[c].dropna().astype(bool)
                row[c+"_rate_pct"]=float(x.mean()*100) if len(x) else np.nan
        comps.append(row)
    pd.DataFrame(comps).to_csv(out/"r217_accum_sequence_outcome_compare.csv",index=False,encoding="utf-8-sig")

    # Long-horizon path summary by initial D5 state.
    ls=[]
    for label,mask in [
        ("D5_TOUCH10",z["d5_touch_10"].fillna(False).astype(bool)),
        ("D5_MISS10",~z["d5_touch_10"].fillna(False).astype(bool)),
        ("D5_TOUCH5",z["d5_touch_5"].fillna(False).astype(bool)),
        ("D5_MISS5",~z["d5_touch_5"].fillna(False).astype(bool)),
    ]:
        g=z[mask & z["d60_complete"].fillna(False).astype(bool)]
        ls.append({
            "group":label,"d60_complete_n":len(g),
            "d60_touch10_rate_pct":float(g["d60_touch_10"].fillna(False).astype(bool).mean()*100) if len(g) else np.nan,
            "d60_touch20_rate_pct":float(g["d60_touch_20"].fillna(False).astype(bool).mean()*100) if len(g) else np.nan,
            "d60_close_positive_rate_pct":float(pd.to_numeric(g["d60_close_ret_pct"],errors="coerce").gt(0).mean()*100) if len(g) else np.nan,
            "d60_mfe_median_pct":float(pd.to_numeric(g["d60_mfe_pct"],errors="coerce").median()) if len(g) else np.nan,
            "d60_mae_median_pct":float(pd.to_numeric(g["d60_mae_pct"],errors="coerce").median()) if len(g) else np.nan,
            "d60_close_median_pct":float(pd.to_numeric(g["d60_close_ret_pct"],errors="coerce").median()) if len(g) else np.nan,
        })
    pd.DataFrame(ls).to_csv(out/"r217_long_horizon_reclassification.csv",index=False,encoding="utf-8-sig")

    # +10 / +15 HIGH-touch phenotype anatomy.
    phenotype_features=[
        "candle_only_score","accum_volume_ratio20","accum_amount_ratio20",
        "accum_close_pos","accum_lower_wick_ratio","accum_upper_wick_ratio",
        "accum_days_before_signal","pullback_volume_vs_accum","pullback_amount_vs_accum",
        "range_contract_3v10","signal_volume_ratio20","signal_amount_ratio20",
        "sequence_component_count"
    ]
    phenotype_bools=[
        "pre_signal_holds_accum_low","pre_signal_close_holds_accum_mid",
        "signal_close_above_ma5","signal_ma5_slope_up"
    ]
    phen=[]
    for h in [5,10,20,40,60]:
        complete=z[z[f"d{h}_complete"].fillna(False).astype(bool)].copy()
        for target in [10,15]:
            hitcol=f"d{h}_touch_{target}"
            if hitcol not in complete.columns:
                continue
            for label,mask in [
                (f"D{h}_HIGH_TOUCH_{target}", complete[hitcol].fillna(False).astype(bool)),
                (f"D{h}_NO_HIGH_TOUCH_{target}", ~complete[hitcol].fillna(False).astype(bool))
            ]:
                g=complete[mask].copy()
                row={"horizon":h,"target_pct":target,"group":label,"n":len(g)}
                for c in phenotype_features:
                    if c in g.columns:
                        x=pd.to_numeric(g[c],errors="coerce")
                        row[c+"_median"]=float(x.median()) if len(x.dropna()) else np.nan
                        row[c+"_mean"]=float(x.mean()) if len(x.dropna()) else np.nan
                for c in phenotype_bools:
                    if c in g.columns:
                        x=g[c].dropna().astype(bool)
                        row[c+"_rate_pct"]=float(x.mean()*100) if len(x) else np.nan
                for c in [f"d{h}_mfe_pct",f"d{h}_mae_pct",f"d{h}_close_ret_pct",f"d{h}_giveback_pp"]:
                    if c in g.columns:
                        x=pd.to_numeric(g[c],errors="coerce")
                        row[c+"_median"]=float(x.median()) if len(x.dropna()) else np.nan
                phen.append(row)
    pd.DataFrame(phen).to_csv(out/"r217_high_touch_10_15_phenotype.csv",index=False,encoding="utf-8-sig")

    ledger_cols=["signal_date","code","name","period","outcome_class"]
    for h in [5,10,20,40,60]:
        ledger_cols += [
            f"d{h}_complete",f"d{h}_mfe_pct",f"d{h}_mae_pct",
            f"d{h}_touch_10",f"d{h}_first_touch_day_10",
            f"d{h}_touch_15",f"d{h}_first_touch_day_15",
            f"d{h}_close_ret_pct",f"d{h}_giveback_pp"
        ]
    ledger_cols += phenotype_features + phenotype_bools
    ledger_cols=[c for c in ledger_cols if c in z.columns]
    z[ledger_cols].to_csv(out/"r217_high_touch_10_15_event_ledger.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REV,"research_only":True,"production_logic_changed":False,
        "same_sample_tuning":False,"new_gate_created":False,
        "cohort_n":len(z),
        "success_failure_binary_replaced_by_multiaxis_description":True,
        "primary_profit_outcome_authority":"INTRAPERIOD_HIGH_TOUCH_MFE",
        "close_return_role":"SECONDARY_GIVEBACK_DURABILITY_ONLY",
        "high_touch_10_15_phenotype_anatomy":True,
        "targets":TARGETS,"horizons":HORIZONS,
        "accumulation_candle_and_sequence_separated":True,
        "sequence_component_count_is_descriptive_only":True
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    mat=pd.DataFrame(rr)
    report=[
        "# ODOLI R2.17 — Outcome & Accumulation Redefinition","",
        "- Frozen cohort: 100 R2.16 CORE+ODOLI events.",
        "- Primary profit outcome is intraperiod HIGH touch / MFE; close is secondary for giveback and durability.",
        "- Binary `D5 +10 = success/fail` is no longer the sole evaluation frame.",
        "- +10% and +15% HIGH-touch cohorts are anatomized separately at D5/D10/D20/D40/D60.",
        "- Targets +3/+5/+7/+10/+15/+20/+30 are reported at D5/D10/D20/D40/D60.",
        "- Accumulation is split into candle-only morphology and pre-signal sequence behavior.",
        "- No new gate, no threshold promotion, no production change.","",
        "## Target × horizon matrix","```",mat.to_string(index=False),"```","",
        "## Outcome classes","```",oc.to_string(index=False),"```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

if __name__=="__main__":
    main()
