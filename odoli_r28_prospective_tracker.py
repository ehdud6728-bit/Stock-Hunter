#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_8_PROSPECTIVE_TRACKER_20260926"
TARGETS=[3,5,7,10,15,20]
HORIZONS=[1,3,5,10]

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs:
        raise SystemExit(f"MISSING:{name}")
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
    return q

def event_path(g,sigdate):
    g=g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
    hit=g.index[g["Date"].eq(sigdate)]
    if len(hit)!=1:return None
    i=int(hit[0]); sig=float(g.loc[i,"Close"])
    rows=[]
    for step in range(1,11):
        if i+step>=len(g): break
        r=g.loc[i+step]
        rows.append({
            "day":step,"date":r["Date"],
            "high_ret_pct":(float(r["High"])/sig-1)*100,
            "low_ret_pct":(float(r["Low"])/sig-1)*100,
            "close_ret_pct":(float(r["Close"])/sig-1)*100
        })
    return sig,pd.DataFrame(rows)

def build_metrics(path):
    out={}
    for h in HORIZONS:
        q=path[path["day"]<=h].copy()
        out[f"d{h}_complete"]=len(q)>=h
        if len(q)>=h:
            out[f"d{h}_mfe_pct"]=float(q["high_ret_pct"].max())
            out[f"d{h}_mae_pct"]=float(q["low_ret_pct"].min())
            out[f"d{h}_close_ret_pct"]=float(q.iloc[-1]["close_ret_pct"])
            out[f"d{h}_giveback_pp"]=out[f"d{h}_mfe_pct"]-out[f"d{h}_close_ret_pct"]
            for t in TARGETS:
                touched=q["high_ret_pct"].ge(t)
                out[f"d{h}_touch_{t}"]=bool(touched.any())
                out[f"d{h}_first_touch_day_{t}"]=int(q.loc[touched,"day"].iloc[0]) if touched.any() else np.nan
    return out

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--signals",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--state",required=False)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()

    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    sig=pd.read_csv(a.signals,dtype={"code":str},low_memory=False)
    sig["code"]=sig["code"].map(norm_code)
    sig["signal_date"]=pd.to_datetime(sig["signal_date"]).dt.normalize()

    required=["signal_date","code","name","is_A","is_CORE","is_ODOLI"]
    miss=[c for c in required if c not in sig.columns]
    if miss: raise SystemExit(f"MISSING_SIGNAL_COLUMNS:{miss}")

    mar=load_marcap(a.marcap_root)
    bycode={c:g.copy() for c,g in mar.groupby("Code",sort=False)}

    rows=[]
    for _,r in sig.iterrows():
        rec=r.to_dict()
        g=bycode.get(r["code"])
        ep=event_path(g,r["signal_date"]) if g is not None else None
        if ep:
            sigclose,path=ep
            rec["signal_close"]=sigclose
            rec.update(build_metrics(path))
        rows.append(rec)
    cur=pd.DataFrame(rows)

    if a.state and Path(a.state).exists():
        old=pd.read_csv(a.state,dtype={"code":str},low_memory=False)
        old["code"]=old["code"].map(norm_code)
        old["signal_date"]=pd.to_datetime(old["signal_date"]).dt.normalize()
        key=["signal_date","code"]
        cur=old.merge(cur,on=key,how="outer",suffixes=("_old",""))
        # Prefer refreshed/current columns where present.
        for c in list(cur.columns):
            if c.endswith("_old"):
                base=c[:-4]
                if base in cur.columns:
                    cur[base]=cur[base].combine_first(cur[c])
                else:
                    cur.rename(columns={c:base},inplace=True)
        cur=cur[[c for c in cur.columns if not c.endswith("_old")]]

    cur=cur.sort_values(["signal_date","code"]).drop_duplicates(["signal_date","code"],keep="last")
    cur.to_csv(out/"prospective_tracker_state.csv",index=False,encoding="utf-8-sig")

    # Summary by frozen cohort labels
    cur["is_CORE"]=cur["is_CORE"].astype(str).str.lower().isin(["true","1"])
    cur["is_ODOLI"]=cur["is_ODOLI"].astype(str).str.lower().isin(["true","1"])
    cur["group"]=np.select(
        [
            cur["is_CORE"] & cur["is_ODOLI"],
            cur["is_CORE"] & ~cur["is_ODOLI"],
            ~cur["is_CORE"] & cur["is_ODOLI"]
        ],
        ["CORE_AND_ODOLI","CORE_ONLY","ODOLI_ONLY"],
        default="OTHER"
    )

    srows=[]
    for grp,g in cur.groupby("group"):
        for h in HORIZONS:
            comp=g[g.get(f"d{h}_complete",False)==True].copy()
            if len(comp)==0: continue
            row={"group":grp,"horizon":h,"complete_n":len(comp)}
            row["close_positive_rate_pct"]=float(pd.to_numeric(comp[f"d{h}_close_ret_pct"],errors="coerce").gt(0).mean()*100)
            row["close_median_pct"]=float(pd.to_numeric(comp[f"d{h}_close_ret_pct"],errors="coerce").median())
            row["mfe_median_pct"]=float(pd.to_numeric(comp[f"d{h}_mfe_pct"],errors="coerce").median())
            row["mae_median_pct"]=float(pd.to_numeric(comp[f"d{h}_mae_pct"],errors="coerce").median())
            row["giveback_median_pp"]=float(pd.to_numeric(comp[f"d{h}_giveback_pp"],errors="coerce").median())
            for t in TARGETS:
                row[f"touch_{t}_rate_pct"]=float(comp[f"d{h}_touch_{t}"].astype(bool).mean()*100)
            srows.append(row)
    summary=pd.DataFrame(srows)
    summary.to_csv(out/"prospective_tracker_summary.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REV,
        "research_only":True,
        "production_logic_changed":False,
        "automatic_ordering":False,
        "signal_and_core_definitions_frozen":True,
        "prospective_observation_only":True,
        "targets":TARGETS,
        "horizons":HORIZONS,
        "rows":len(cur),
        "max_market_date":str(mar["Date"].max().date()) if len(mar) else ""
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    report=[
        "# ODOLI R2.8 — Prospective Tracker","",
        "- Frozen signal/core definitions.",
        "- No threshold retuning.",
        "- No automatic orders.",
        "- Tracks future signals and refreshes D+1/D+3/D+5/D+10 outcomes.",
        "- Records Close, MFE/High Touch, MAE, Giveback, and +3/+5/+7/+10/+15/+20 target touches.","",
        "## Current summary","```",summary.to_string(index=False),"```"
    ]
    (out/"REPORT.md").write_text("\\n".join(report),encoding="utf-8")

if __name__=="__main__":
    main()
