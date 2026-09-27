#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_13_LONG_DELAYED_WAVE_20260927"

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
    return q[q["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])].copy()

def ichimoku(df):
    h=pd.to_numeric(df["High"],errors="coerce")
    l=pd.to_numeric(df["Low"],errors="coerce")
    tenkan=(h.rolling(9).max()+l.rolling(9).min())/2
    kijun=(h.rolling(26).max()+l.rolling(26).min())/2
    span_a=((tenkan+kijun)/2).shift(26)
    span_b=((h.rolling(52).max()+l.rolling(52).min())/2).shift(26)
    out=df.copy()
    out["cloud_top"]=pd.concat([span_a,span_b],axis=1).max(axis=1)
    out["cloud_bottom"]=pd.concat([span_a,span_b],axis=1).min(axis=1)
    return out

def cloud_state(row):
    c=float(row["Close"]); top=row.get("cloud_top",np.nan); bot=row.get("cloud_bottom",np.nan)
    if not np.isfinite(top) or not np.isfinite(bot): return "NO_CLOUD"
    if c>top: return "ABOVE_CLOUD"
    if c>=bot: return "IN_CLOUD"
    return "BELOW_CLOUD"

def first_touch(path,target=10):
    q=path[path["high_ret_pct"]>=target]
    return int(q.iloc[0]["day"]) if len(q) else None

def first_day_true(path,col):
    q=path[path[col].fillna(False).astype(bool)]
    return int(q.iloc[0]["day"]) if len(q) else None

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r212-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    base=pd.read_csv(find_one(a.r212_root,"r212_delayed_resolution_events.csv"),dtype={"code":str},low_memory=False)
    base["code"]=base["code"].map(norm_code)
    base["signal_date"]=pd.to_datetime(base["signal_date"],errors="coerce").dt.normalize()
    base["success_d5_touch10"]=base["success_d5_touch10"].astype(str).str.lower().isin(["true","1"])
    cohort=base[~base["success_d5_touch10"]].copy()
    if len(cohort)!=9: raise SystemExit(f"EXPECTED_9_ORIGINAL_D5_MISSES_GOT_{len(cohort)}")

    mar=load_marcap(a.marcap_root)
    bycode={c:ichimoku(g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)) for c,g in mar.groupby("Code",sort=False)}
    rows=[]; paths=[]
    for _,r in cohort.iterrows():
        g=bycode.get(r["code"])
        if g is None: continue
        hits=g.index[g["Date"].eq(r["signal_date"])]
        if len(hits)!=1: continue
        i=int(hits[0]); sig=float(g.loc[i,"Close"])
        acc_low=pd.to_numeric(pd.Series([r.get("best_accum_low")]),errors="coerce").iloc[0]
        acc_high=pd.to_numeric(pd.Series([r.get("best_accum_high")]),errors="coerce").iloc[0]
        acc_mid=(acc_low+acc_high)/2 if np.isfinite(acc_low) and np.isfinite(acc_high) else np.nan
        pp=[]
        for d in range(1,61):
            if i+d>=len(g): break
            rr=g.loc[i+d]; close=float(rr["Close"]); high=float(rr["High"]); low=float(rr["Low"]); cs=cloud_state(rr)
            pp.append({
                "signal_date":r["signal_date"],"code":r["code"],"name":r.get("name",""),"day":d,"date":rr["Date"],
                "high_ret_pct":(high/sig-1)*100,"low_ret_pct":(low/sig-1)*100,"close_ret_pct":(close/sig-1)*100,
                "cloud_state":cs,"above_cloud":cs=="ABOVE_CLOUD",
                "holds_accum_low":bool(close>=acc_low) if np.isfinite(acc_low) else np.nan,
                "holds_accum_mid":bool(close>=acc_mid) if np.isfinite(acc_mid) else np.nan,
                "reclaims_accum_high":bool(close>=acc_high) if np.isfinite(acc_high) else np.nan,
            })
        p=pd.DataFrame(pp)
        if p.empty: continue
        rec=r.to_dict(); rec["available_forward_days"]=len(p); rec["d60_complete"]=len(p)>=60
        for h in [20,30,40,50,60]:
            q=p[p["day"]<=h]
            rec[f"d{h}_touch10"]=bool((q["high_ret_pct"]>=10).any())
            rec[f"d{h}_mfe_pct"]=float(q["high_ret_pct"].max()) if len(q) else np.nan
            rec[f"d{h}_mae_pct"]=float(q["low_ret_pct"].min()) if len(q) else np.nan
            rec[f"d{h}_close_ret_pct"]=float(q.iloc[-1]["close_ret_pct"]) if len(q)>=h else np.nan
        ft=first_touch(p,10)
        rec["first_plus10_day_60"]=ft if ft is not None else np.nan
        rec["first_above_cloud_day_60"]=first_day_true(p,"above_cloud")
        rec["first_reclaim_accum_high_day_60"]=first_day_true(p,"reclaims_accum_high") if "reclaims_accum_high" in p else np.nan
        if np.isfinite(acc_low):
            below_idx=p.index[p["holds_accum_low"].eq(False)].tolist()
            rec["ever_broke_accum_low_d60"]=bool(below_idx)
            if below_idx:
                first_break=int(p.loc[below_idx[0],"day"]); later=p[p["day"]>first_break]; re=later[later["holds_accum_low"].eq(True)]
                rec["first_accum_low_break_day"]=first_break
                rec["reclaimed_accum_low_after_break"]=bool(len(re))
                rec["first_accum_low_reclaim_day"]=int(re.iloc[0]["day"]) if len(re) else np.nan
            else:
                rec["first_accum_low_break_day"]=np.nan; rec["reclaimed_accum_low_after_break"]=False; rec["first_accum_low_reclaim_day"]=np.nan
        q1=p[(p["day"]>=6)&(p["day"]<=20)]; q2=p[(p["day"]>=21)&(p["day"]<=60)]
        rec["pullback1_low_ret_pct"]=float(q1["low_ret_pct"].min()) if len(q1) else np.nan
        rec["pullback2_low_ret_pct"]=float(q2["low_ret_pct"].min()) if len(q2) else np.nan
        rec["second_pullback_higher_low"]=bool(np.isfinite(rec["pullback1_low_ret_pct"]) and np.isfinite(rec["pullback2_low_ret_pct"]) and rec["pullback2_low_ret_pct"]>rec["pullback1_low_ret_pct"])
        if ft is not None and ft<=40: cls="LATE_SUCCESS_D21_40"
        elif ft is not None and ft<=60: cls="VERY_LATE_SUCCESS_D41_60"
        else:
            final=p.iloc[-1]; above=final["cloud_state"] in ("ABOVE_CLOUD","IN_CLOUD")
            holdlow=bool(final["holds_accum_low"]) if pd.notna(final["holds_accum_low"]) else False
            holdmid=bool(final["holds_accum_mid"]) if pd.notna(final["holds_accum_mid"]) else False
            reclaimed=bool(rec.get("reclaimed_accum_low_after_break",False)); higher=bool(rec.get("second_pullback_higher_low",False))
            if above and holdmid and higher: cls="STILL_BUILDING_SUPPORTED"
            elif holdlow and higher: cls="SECOND_PULLBACK_BUILDING"
            elif reclaimed and above: cls="DEEP_PULLBACK_RECLAIMED"
            elif not holdlow and not above: cls="STRUCTURAL_BREAKDOWN"
            else: cls="UNRESOLVED_MIXED"
        rec["long_resolution_class"]=cls
        rows.append(rec); paths.extend(pp)

    z=pd.DataFrame(rows); pth=pd.DataFrame(paths)
    z.to_csv(out/"r213_long_resolution_events.csv",index=False,encoding="utf-8-sig")
    pth.to_csv(out/"r213_d1_d60_paths.csv",index=False,encoding="utf-8-sig")
    summary=z.groupby("long_resolution_class").size().reset_index(name="n").sort_values("n",ascending=False)
    summary.to_csv(out/"r213_long_resolution_summary.csv",index=False,encoding="utf-8-sig")
    strong=z[pd.to_numeric(z["best_accum_score"],errors="coerce").ge(70)].copy()
    strong.to_csv(out/"r213_strong_accum_long_resolution.csv",index=False,encoding="utf-8-sig")
    horizon_rows=[]
    for h in [20,30,40,50,60]:
        comp=z[z["available_forward_days"]>=h]
        horizon_rows.append({"horizon":f"D+{h}","complete_n":len(comp),"plus10_touch_n":int(comp[f"d{h}_touch10"].sum()) if len(comp) else 0,"plus10_touch_rate_pct":float(comp[f"d{h}_touch10"].mean()*100) if len(comp) else np.nan,"mfe_median_pct":float(pd.to_numeric(comp[f"d{h}_mfe_pct"],errors="coerce").median()) if len(comp) else np.nan,"mae_median_pct":float(pd.to_numeric(comp[f"d{h}_mae_pct"],errors="coerce").median()) if len(comp) else np.nan,"close_median_pct":float(pd.to_numeric(comp[f"d{h}_close_ret_pct"],errors="coerce").median()) if len(comp) else np.nan})
    hs=pd.DataFrame(horizon_rows); hs.to_csv(out/"r213_horizon_summary.csv",index=False,encoding="utf-8-sig")
    report=["# ODOLI R2.13 — Long Delayed Wave Follow-up","","- Primary cohort: the same 9 original D+5 +10% misses.","- Extends observation through D+60 without changing the +10% target.","- Tracks cloud position, accumulation low/mid/high hold/reclaim, and second-pullback structure.","- No new production gate and no same-sample promotion.","","## Horizon summary","```",hs.to_string(index=False),"```","","## Long resolution classes","```",summary.to_string(index=False),"```","","## Strong accumulation subset","```",strong[[c for c in ["signal_date","code","name","best_accum_score","first_plus10_day_60","long_resolution_class","d40_mfe_pct","d60_mfe_pct","ever_broke_accum_low_d60","reclaimed_accum_low_after_break","second_pullback_higher_low"] if c in strong.columns]].to_string(index=False),"```"]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")
    meta={"revision":REV,"research_only":True,"production_logic_changed":False,"new_gate_created":False,"same_sample_tuning":False,"source_run_r212":36294223914,"primary_cohort":"ORIGINAL_D5_PLUS10_NON_TOUCH_9","target_unchanged_pct":10,"max_forward_days":60,"events":len(z),"d60_complete_n":int(z["d60_complete"].fillna(False).astype(bool).sum()),"resolution_classes":summary.to_dict(orient="records")}
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")

if __name__=="__main__": main()
