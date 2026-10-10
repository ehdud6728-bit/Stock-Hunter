#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, re
from pathlib import Path
import numpy as np
import pandas as pd
REVISION="EARLY_POWER_R8_OOS_MONITOR_20261010"
FROZEN_RULE_ID="R6_FROZEN_V1"
H=[1,3,5,10]
def norm_code(v):
    s=re.sub(r"\D","",str(v or "")); return s[-6:].zfill(6) if s else ""
def is_spac_name(v):
    s=str(v or "").upper().replace(" ",""); return ("스팩" in s) or ("SPAC" in s) or ("기업인수목적" in s)
def add_flags(x):
    q=x.copy(); q["code"]=q["code"].map(norm_code); q["name"]=q.get("name","").fillna("").astype(str); q["spac_flag"]=q["name"].map(is_spac_name); q["spac_reason"]=np.where(q.spac_flag,"NAME_MATCH_SPAC",""); return q
def mature_summary(x,view_name):
    rows=[]
    for h in H:
        rc=f"d{h}_ret_pct"; mc=f"mfe{h}_pct"; ac=f"mae{h}_pct"
        for lane,g in x.groupby("lane",sort=False):
            s=pd.to_numeric(g[rc],errors="coerce") if rc in g.columns else pd.Series(np.nan,index=g.index,dtype=float); ok=s.notna()
            r={"view":view_name,"horizon":f"D{h}","lane":lane,"n":int(ok.sum()),"signal_days":int(g.loc[ok,"signal_date"].astype(str).nunique()) if ok.any() else 0,"mean_ret_pct":float(s[ok].mean()) if ok.any() else np.nan,"median_ret_pct":float(s[ok].median()) if ok.any() else np.nan,"positive_pct":float((s[ok]>0).mean()*100) if ok.any() else np.nan}
            for col,key in [(mc,"mfe_median_pct"),(ac,"mae_median_pct")]:
                z=pd.to_numeric(g.loc[ok,col],errors="coerce") if col in g.columns and ok.any() else pd.Series(dtype=float); r[key]=float(z.median()) if z.notna().any() else np.nan
            z=pd.to_numeric(g.loc[ok,mc],errors="coerce") if mc in g.columns and ok.any() else pd.Series(dtype=float)
            r["mfe_ge5_pct"]=float((z.dropna()>=5).mean()*100) if z.notna().any() else np.nan; r["mfe_ge10_pct"]=float((z.dropna()>=10).mean()*100) if z.notna().any() else np.nan
            z=pd.to_numeric(g.loc[ok,ac],errors="coerce") if ac in g.columns and ok.any() else pd.Series(dtype=float); r["mae_le_minus5_pct"]=float((z.dropna()<=-5).mean()*100) if z.notna().any() else np.nan
            rows.append(r)
    return pd.DataFrame(rows)
def daily_compare(x):
    rows=[]
    for (sd,lane),g in x.groupby(["signal_date","lane"],sort=False):
        r={"signal_date":str(sd)[:10],"lane":lane,"candidates":len(g),"spac_n":int(g.spac_flag.sum()),"non_spac_n":int((~g.spac_flag).sum())}
        for h in H:
            rc=f"d{h}_ret_pct"; s=pd.to_numeric(g[rc],errors="coerce") if rc in g.columns else pd.Series(np.nan,index=g.index); ok=s.notna(); r[f"d{h}_n"]=int(ok.sum()); r[f"d{h}_mean"]=float(s[ok].mean()) if ok.any() else np.nan; r[f"d{h}_median"]=float(s[ok].median()) if ok.any() else np.nan
        rows.append(r)
    return pd.DataFrame(rows).sort_values(["signal_date","lane"],kind="stable")
def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--candidates",required=True); ap.add_argument("--cohorts",required=True); ap.add_argument("--out",default="reports/early_power_r8"); ap.add_argument("--target-independent-days",type=int,default=20); a=ap.parse_args(); out=Path(a.out); out.mkdir(parents=True,exist_ok=True)
    c=pd.read_csv(a.candidates,dtype={"code":str},low_memory=False); h=pd.read_csv(a.cohorts,low_memory=False)
    if c.empty: raise SystemExit("R8_EMPTY_CANDIDATE_LEDGER")
    c=add_flags(c); c["signal_date"]=c.signal_date.astype(str).str[:10]; h["signal_date"]=h.signal_date.astype(str).str[:10]
    c.to_csv(out/"r8_candidate_ledger_flagged.csv",index=False,encoding="utf-8-sig"); h.to_csv(out/"r8_cohort_ledger.csv",index=False,encoding="utf-8-sig")
    sm=pd.concat([mature_summary(c,"ALL"),mature_summary(c[~c.spac_flag].copy(),"NON_SPAC")],ignore_index=True); sm.to_csv(out/"r8_mature_summary.csv",index=False,encoding="utf-8-sig")
    day=daily_compare(c); day.to_csv(out/"r8_daily_lane_compare.csv",index=False,encoding="utf-8-sig")
    spac=c[c.spac_flag].copy(); spac.to_csv(out/"r8_spac_watch.csv",index=False,encoding="utf-8-sig")
    valid=h[~h.early_power_status.astype(str).eq("INVALID_NON_TRADING_DATE")].copy(); ready=valid[valid.early_power_status.astype(str).isin(["READY","BACKFILLED_READY"])]; ep_days=int(ready.loc[pd.to_numeric(ready.early_power_rows,errors="coerce").fillna(0).gt(0),"signal_date"].nunique()); all_ready_days=int(ready.signal_date.nunique()); target=int(a.target_independent_days)
    meta={"revision":REVISION,"research_only":True,"frozen_rule_id":FROZEN_RULE_ID,"production_changed":False,"production_search_changed":False,"production_score_changed":False,"production_rank_changed":False,"production_order_changed":False,"automatic_orders":False,"same_sample_retuning":False,"spac_filter_changed":False,"spac_policy":"FLAG_ONLY_DO_NOT_EXCLUDE_FROM_FROZEN_V1","candidate_rows":int(len(c)),"cohort_rows":int(len(h)),"latest_signal_date":str(h.signal_date.max()),"ready_trading_days":all_ready_days,"early_power_nonempty_days":ep_days,"target_independent_days":target,"readiness_pct":float(min(100,ep_days/target*100)) if target else 100.0,"readiness_status":"READY_FOR_PRIMARY_OOS_REVIEW" if ep_days>=target else "ACCUMULATING","spac_candidate_rows":int(c.spac_flag.sum()),"note":"SPAC is descriptive only. R6_FROZEN_V1 candidate eligibility is unchanged."}
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    pivot=[]
    for view in ["ALL","NON_SPAC"]:
      q=sm[sm.view.eq(view)]
      for hz in [f"D{x}" for x in H]:
        z=q[q.horizon.eq(hz)]; aa=z[z.lane.eq("ORIGINAL_TOP3")]; ee=z[z.lane.eq("EARLY_POWER")]; ar=aa.iloc[0] if len(aa) else None; er=ee.iloc[0] if len(ee) else None
        if ar is None and er is None: continue
        pivot.append({"view":view,"horizon":hz,"original_n":int(ar["n"]) if ar is not None else 0,"early_n":int(er["n"]) if er is not None else 0,"original_mean":float(ar["mean_ret_pct"]) if ar is not None else np.nan,"early_mean":float(er["mean_ret_pct"]) if er is not None else np.nan,"original_median":float(ar["median_ret_pct"]) if ar is not None else np.nan,"early_median":float(er["median_ret_pct"]) if er is not None else np.nan,"mean_diff_early_minus_original":float(er["mean_ret_pct"])-float(ar["mean_ret_pct"]) if ar is not None and er is not None and pd.notna(ar["mean_ret_pct"]) and pd.notna(er["mean_ret_pct"]) else np.nan})
    pv=pd.DataFrame(pivot); pv.to_csv(out/"r8_head_to_head.csv",index=False,encoding="utf-8-sig")
    report=["# EARLY POWER R8 Prospective OOS Monitor","",json.dumps(meta,ensure_ascii=False,indent=2),"","## Head-to-head",pv.to_string(index=False),"","## Mature summary",sm.to_string(index=False),"","## Daily comparison",day.tail(40).to_string(index=False),"","## SPAC watch",spac[["signal_date","lane","code","name","shadow_rank"]].to_string(index=False) if len(spac) else "NONE"]
    (out/"REPORT.txt").write_text("\n".join(report),encoding="utf-8"); print("EARLY_POWER_R8_OOS_MONITOR_PASS"); print(json.dumps(meta,ensure_ascii=False)); print(pv.to_string(index=False))
if __name__=="__main__": main()
