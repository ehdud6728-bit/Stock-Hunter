#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EARLY POWER R6 — Prospective OOS Shadow Board (RESEARCH_ONLY)

Freeze date: 2026-10-04
Purpose:
- Freeze one EARLY POWER definition after R1~R5 exploration.
- Prospectively compare ORIGINAL TOP3 (real_full_trust_source rank authority)
  vs EARLY POWER up-to-TOP3 on the SAME signal date.
- Append cohorts across runs and update D1/D3/D5/D10 outcomes.
- Never retune from the same prospective ledger.

R6_FROZEN_V1 eligibility:
  S3 transition context:
    MA5 > MA20
    MA20/60/112 convergence <= 8%
    inverse MA5<MA20<MA60<MA112 observed within prior 60 sessions
  BB40 width <= 10%
  controlled 5-session convergence change in [-0.40, +0.80] percentage points
  recent 5-session mean Volume / prior-15 mean Volume in [1.27, 1.80]
  first-wave low->high development in [11, 29] sessions

These R5-derived research ranges are now frozen ONLY for prospective shadow
validation. They are NOT production rules.

Priority within eligible candidates is deterministic and outcome-free:
  1 no-overheat first
  2 volume ratio closest to 1.50
  3 convergence change closest to +0.20
  4 longer upside room to 60-session high
  5 code ascending
"""
from __future__ import annotations
import argparse, json, math, re
from pathlib import Path
import numpy as np
import pandas as pd

REVISION="EARLY_POWER_R6_FROZEN_V1_OOS_20261004"
FROZEN_RULE_ID="R6_FROZEN_V1"
HORIZONS=[1,3,5,10]

def norm_code(v):
    s=re.sub(r"\D","",str(v or ""))
    return s[-6:].zfill(6) if s else ""

def first_col(df, names):
    for c in names:
        if c in df.columns:return c
    return None

def load_marcap(paths):
    xs=[]
    for p in paths:
        q=pd.read_parquet(p)
        if "Date" not in q.columns:q=q.reset_index()
        xs.append(q)
    x=pd.concat(xs,ignore_index=True,sort=False)
    x["Date"]=pd.to_datetime(x["Date"],errors="coerce").dt.normalize()
    x["Code"]=x["Code"].map(norm_code)
    for c in ["Open","High","Low","Close","Volume","Amount"]:
        if c in x.columns:x[c]=pd.to_numeric(x[c],errors="coerce")
    x=x.dropna(subset=["Date","Code","Open","High","Low","Close"])
    x=x[(x.Open>0)&(x.High>0)&(x.Low>0)&(x.Close>0)]
    return x.sort_values(["Code","Date"]).drop_duplicates(["Code","Date"],keep="last").reset_index(drop=True)

def calc_features(g):
    g=g.sort_values("Date").copy().reset_index(drop=True)
    c=g.Close.astype(float); v=g.Volume.fillna(0).astype(float)
    for n in [5,20,40,60,112,224]:
        g[f"ma{n}"]=c.rolling(n,min_periods=n).mean()
    g["bb40_width"]=c.rolling(40,min_periods=40).std()*4/g.ma40*100
    vals=pd.concat([g.ma20,g.ma60,g.ma112],axis=1)
    g["conv_pct"]=(vals.max(axis=1)-vals.min(axis=1))/vals.max(axis=1)*100
    g["conv_change_5d"]=g.conv_pct-g.conv_pct.shift(5)
    g["inverse4"]=(g.ma5<g.ma20)&(g.ma20<g.ma60)&(g.ma60<g.ma112)
    g["inverse_recent60"]=g.inverse4.astype(int).rolling(60,min_periods=1).max().astype(bool)
    g["volume_last5_mean"]=v.rolling(5,min_periods=5).mean()
    g["volume_prev15_mean"]=v.shift(5).rolling(15,min_periods=15).mean()
    g["volume_last5_vs_prev15"]=g.volume_last5_mean/g.volume_prev15_mean.replace(0,np.nan)
    g["ret5"]=(c/c.shift(5)-1)*100
    # frozen R1 overheat diagnostics
    d=c.diff()
    up=d.clip(lower=0).rolling(14,min_periods=14).mean()
    dn=(-d.clip(upper=0)).rolling(14,min_periods=14).mean()
    rs=up/dn.replace(0,np.nan)
    g["rsi14"]=(100-(100/(1+rs))).fillna(50)
    g["disp_ma20"]=c/g.ma20*100
    g["overheat_count"]=(g.rsi14.ge(68).astype(int)+g.disp_ma20.ge(112).astype(int)+g.ret5.ge(12).astype(int))
    g["high60"]=g.High.rolling(60,min_periods=20).max()
    g["space_high60_pct"]=(g.high60/c-1)*100
    return g

def wave_path_at(g, i):
    if i<29:return (np.nan,np.nan,np.nan)
    w=g.iloc[i-29:i+1].copy().reset_index(drop=True)
    li=int(w.Low.idxmin())
    hi=int(w.iloc[li:].High.idxmax())
    lo=float(w.loc[li,"Low"]); hh=float(w.loc[hi,"High"])
    return hi-li, (hh/lo-1)*100 if lo>0 else np.nan, (hh-float(w.iloc[-1].Close))/(hh-lo)*100 if hh>lo else np.nan

def build_early_power(px, signal_date):
    rows=[]
    for code,g0 in px.groupby("Code",sort=False):
        g=calc_features(g0)
        hit=g.index[g.Date.eq(signal_date)]
        if len(hit)==0:continue
        i=int(hit[0]); r=g.iloc[i]
        wave_days, impulse, retrace=wave_path_at(g,i)
        s3=bool(r.ma5>r.ma20 and r.conv_pct<=8 and r.inverse_recent60)
        bb=bool(pd.notna(r.bb40_width) and r.bb40_width<=10)
        cv=bool(pd.notna(r.conv_change_5d) and -0.40<=r.conv_change_5d<=0.80)
        vr=bool(pd.notna(r.volume_last5_vs_prev15) and 1.27<=r.volume_last5_vs_prev15<=1.80)
        wave=bool(pd.notna(wave_days) and 11<=wave_days<=29)
        if not (s3 and bb and cv and vr and wave):continue
        name=str(r.get("Name","")) if "Name" in g.columns else ""
        rows.append({
            "signal_date":signal_date.date().isoformat(),"lane":"EARLY_POWER","code":code,"name":name,
            "entry_price":float(r.Close),"source_rank":np.nan,
            "frozen_rule_id":FROZEN_RULE_ID,
            "conv_pct":float(r.conv_pct),"conv_change_5d":float(r.conv_change_5d),
            "bb40_width":float(r.bb40_width),"volume_last5_vs_prev15":float(r.volume_last5_vs_prev15),
            "wave_low_to_high_sessions":int(wave_days),"impulse_pct":float(impulse),"retrace_pct":float(retrace),
            "overheat_count":int(r.overheat_count),"space_high60_pct":float(r.space_high60_pct),
            "_priority_overheat":int(r.overheat_count),
            "_priority_vol":abs(float(r.volume_last5_vs_prev15)-1.50),
            "_priority_conv":abs(float(r.conv_change_5d)-0.20),
        })
    if not rows:return pd.DataFrame()
    q=pd.DataFrame(rows)
    q=q.sort_values(["_priority_overheat","_priority_vol","_priority_conv","space_high60_pct","code"],
                    ascending=[True,True,True,False,True],kind="stable").head(3).copy()
    q["shadow_rank"]=range(1,len(q)+1)
    return q.drop(columns=[c for c in q.columns if c.startswith("_priority_")])

def load_original_top3(path):
    s=pd.read_csv(path,dtype=str,low_memory=False)
    dcol=first_col(s,["signal_date","date","Date","기준일"])
    ccol=first_col(s,["code","Code","종목코드"])
    ncol=first_col(s,["name","Name","종목명"])
    rcol=first_col(s,["rank","origin_rank","TOP15순위","순위"])
    pcol=first_col(s,["snapshot_price","entry_price","현재가","종가","Close"])
    if not all([dcol,ccol,rcol,pcol]):
        raise SystemExit(f"R6_ORIGINAL_SCHEMA_MISSING d={dcol} c={ccol} r={rcol} p={pcol}")
    s["_date"]=pd.to_datetime(s[dcol],errors="coerce").dt.normalize()
    s["_rank"]=pd.to_numeric(s[rcol],errors="coerce")
    s["_price"]=pd.to_numeric(s[pcol],errors="coerce")
    s["_code"]=s[ccol].map(norm_code)
    s=s[s._date.notna() & s._rank.notna() & s._price.gt(0) & s._code.ne("")].copy()
    if s.empty:raise SystemExit("R6_ORIGINAL_EMPTY")
    sd=s._date.max()
    q=s[s._date.eq(sd)].sort_values("_rank").head(3)
    out=[]
    for _,r in q.iterrows():
        out.append({
            "signal_date":sd.date().isoformat(),"lane":"ORIGINAL_TOP3","code":r._code,
            "name":str(r[ncol]) if ncol else "","entry_price":float(r._price),
            "source_rank":int(r._rank),"shadow_rank":int(r._rank),
            "frozen_rule_id":"ORIGINAL_TRUST_SOURCE_RANK_AUTHORITY"
        })
    return sd,pd.DataFrame(out)

def update_outcomes(ledger, px):
    if ledger.empty:return ledger
    by={c:g.sort_values("Date").reset_index(drop=True) for c,g in px.groupby("Code",sort=False)}
    for idx,r in ledger.iterrows():
        code=norm_code(r.code); sd=pd.Timestamp(r.signal_date).normalize()
        g=by.get(code)
        if g is None:continue
        hit=g.index[g.Date.eq(sd)]
        if len(hit)==0:continue
        pos=int(hit[0]); entry=float(r.entry_price)
        ledger.loc[idx,"price_calendar_has_signal_date"]=True
        for h in HORIZONS:
            if pos+h<len(g):
                w=g.iloc[pos+1:pos+h+1]
                ledger.loc[idx,f"d{h}_date"]=g.iloc[pos+h].Date.date().isoformat()
                ledger.loc[idx,f"d{h}_ret_pct"]=(float(g.iloc[pos+h].Close)/entry-1)*100
                ledger.loc[idx,f"mfe{h}_pct"]=(float(w.High.max())/entry-1)*100
                ledger.loc[idx,f"mae{h}_pct"]=(float(w.Low.min())/entry-1)*100
                ledger.loc[idx,f"d{h}_mature"]=True
            else:
                ledger.loc[idx,f"d{h}_mature"]=False
    return ledger

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--marcap",nargs="+",required=True)
    ap.add_argument("--original-source",required=True)
    ap.add_argument("--previous-ledger",default="")
    ap.add_argument("--out",default="reports/early_power_r6")
    args=ap.parse_args()
    out=Path(args.out); out.mkdir(parents=True,exist_ok=True)

    px=load_marcap(args.marcap)
    source_date,orig=load_original_top3(args.original_source)
    price_dates=set(px.Date.unique())
    if np.datetime64(source_date) not in price_dates and pd.Timestamp(source_date) not in price_dates:
        # explicit fail-closed cohort preservation; never silently substitute a different date
        raise SystemExit(f"R6_SOURCE_DATE_NOT_IN_PRICE_CALENDAR:{source_date.date()}")

    ep=build_early_power(px,source_date)
    new=pd.concat([orig,ep],ignore_index=True,sort=False)
    new["revision"]=REVISION
    new["research_only"]=True
    new["cohort_key"]=new.signal_date.astype(str)+"|"+new.lane.astype(str)+"|"+new.code.astype(str)
    new["price_calendar_has_signal_date"]=True

    prior=pd.DataFrame()
    if args.previous_ledger and Path(args.previous_ledger).exists():
        prior=pd.read_csv(args.previous_ledger,dtype={"code":str},low_memory=False)
    ledger=pd.concat([prior,new],ignore_index=True,sort=False)
    ledger["code"]=ledger.code.map(norm_code)
    ledger=ledger.drop_duplicates(["signal_date","lane","code"],keep="first")
    ledger=update_outcomes(ledger,px)
    ledger=ledger.sort_values(["signal_date","lane","shadow_rank","code"],kind="stable").reset_index(drop=True)
    ledger.to_csv(out/"r6_oos_candidate_ledger.csv",index=False,encoding="utf-8-sig")

    today=ledger[ledger.signal_date.eq(source_date.date().isoformat())].copy()
    today.to_csv(out/"r6_today_shadow_board.csv",index=False,encoding="utf-8-sig")

    # Matured comparison summaries; descriptive only.
    sums=[]
    for h in HORIZONS:
        col=f"d{h}_ret_pct"
        for lane,g in ledger.groupby("lane"):
            x=pd.to_numeric(g.get(col),errors="coerce")
            ok=x.notna()
            sums.append({
                "horizon":f"D{h}","lane":lane,"n":int(ok.sum()),
                "signal_days":int(g.loc[ok,"signal_date"].nunique()) if ok.any() else 0,
                "mean_ret_pct":float(x[ok].mean()) if ok.any() else np.nan,
                "median_ret_pct":float(x[ok].median()) if ok.any() else np.nan,
                "positive_pct":float((x[ok]>0).mean()*100) if ok.any() else np.nan,
                "mfe_median_pct":float(pd.to_numeric(g.loc[ok,f"mfe{h}_pct"],errors="coerce").median()) if ok.any() else np.nan,
                "mae_median_pct":float(pd.to_numeric(g.loc[ok,f"mae{h}_pct"],errors="coerce").median()) if ok.any() else np.nan,
            })
    summary=pd.DataFrame(sums)
    summary.to_csv(out/"r6_oos_summary.csv",index=False,encoding="utf-8-sig")

    meta={
      "revision":REVISION,"research_only":True,
      "production_changed":False,"production_search_changed":False,
      "production_score_changed":False,"production_rank_changed":False,
      "production_order_changed":False,"automatic_orders":False,
      "same_sample_retuning":False,"prospective_only":True,
      "frozen_rule_id":FROZEN_RULE_ID,
      "comparison_date":source_date.date().isoformat(),
      "original_top3_rows":int(len(orig)),"early_power_rows":int(len(ep)),
      "ledger_rows":int(len(ledger)),"ledger_signal_days":int(ledger.signal_date.nunique()),
      "early_power_rule":{
        "S3":"MA5>MA20; MA20/60/112 convergence<=8%; inverse4 observed in prior60",
        "BB40":"width<=10%",
        "controlled_convergence_5d":"-0.40..+0.80 percentage points",
        "volume_last5_vs_prev15":"1.27..1.80",
        "wave_low_to_high_sessions":"11..29",
        "forced_top3":False
      },
      "note":"R5-derived exploratory ranges are frozen from 2026-10-04 forward. No future R6 outcome may retune this rule in-place."
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    rep=["# EARLY POWER R6 Prospective OOS","",json.dumps(meta,ensure_ascii=False,indent=2),
         "","## Today",today.to_string(index=False),"","## Matured summary",summary.to_string(index=False)]
    (out/"REPORT.txt").write_text("\n".join(rep),encoding="utf-8")
    print("EARLY_POWER_R6_OOS_PASS")
    print(json.dumps(meta,ensure_ascii=False))
    print(today.to_string(index=False))

if __name__=="__main__":
    main()
