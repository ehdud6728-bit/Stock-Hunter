#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_EXIT_R2_7_TARGET_TOUCH_20260926"
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
        high=float(r["High"]); low=float(r["Low"]); close=float(r["Close"])
        rows.append({
            "day":step,"date":r["Date"],"high_ret_pct":(high/sig-1)*100,
            "low_ret_pct":(low/sig-1)*100,"close_ret_pct":(close/sig-1)*100
        })
    return sig,pd.DataFrame(rows)

def target_metrics(path,target,h):
    q=path[path["day"]<=h].copy()
    if len(q)<h:
        return {"complete":False}
    touched=q["high_ret_pct"].ge(target)
    return {
        "complete":True,
        "touch":bool(touched.any()),
        "first_touch_day":int(q.loc[touched,"day"].iloc[0]) if touched.any() else np.nan,
        "mfe_pct":float(q["high_ret_pct"].max()),
        "mae_pct":float(q["low_ret_pct"].min()),
        "close_ret_pct":float(q.iloc[-1]["close_ret_pct"]),
        "giveback_from_mfe_to_close_pp":float(q["high_ret_pct"].max()-q.iloc[-1]["close_ret_pct"]),
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r26-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    core=pd.read_csv(find_one(a.r26_root,"core21_forward_refreshed.csv"),dtype={"code":str},low_memory=False)
    core["code"]=core["code"].map(norm_code)
    core["signal_date"]=pd.to_datetime(core["signal_date"]).dt.normalize()
    core["odoli_overlap"]=core["odoli_overlap"].astype(str).str.lower().isin(["true","1"])

    mar=load_marcap(a.marcap_root)
    bycode={c:g.copy() for c,g in mar.groupby("Code",sort=False)}

    rows=[]; pathrows=[]
    for _,r in core.iterrows():
        g=bycode.get(r["code"])
        ep=event_path(g,r["signal_date"]) if g is not None else None
        if ep is None: continue
        sig,path=ep
        for _,pr in path.iterrows():
            pathrows.append({
                "signal_date":r["signal_date"],"code":r["code"],"name":r.get("name",""),
                "odoli_overlap":r["odoli_overlap"],"signal_close":sig,**pr.to_dict()
            })
        for h in HORIZONS:
            for t in TARGETS:
                m=target_metrics(path,t,h)
                rows.append({
                    "signal_date":r["signal_date"],"code":r["code"],"name":r.get("name",""),
                    "odoli_overlap":r["odoli_overlap"],"signal_close":sig,
                    "horizon":h,"target_pct":t,**m
                })

    detail=pd.DataFrame(rows)
    paths=pd.DataFrame(pathrows)
    detail.to_csv(out/"target_touch_event_detail.csv",index=False,encoding="utf-8-sig")
    paths.to_csv(out/"daily_high_low_close_paths.csv",index=False,encoding="utf-8-sig")

    sums=[]
    for label,g0 in [("CORE_ALL",core),("CORE_AND_ODOLI",core[core.odoli_overlap]),("CORE_WITHOUT_ODOLI",core[~core.odoli_overlap])]:
        keys=set(zip(g0["signal_date"],g0["code"]))
        d=detail[[ (sd,cd) in keys for sd,cd in zip(detail["signal_date"],detail["code"]) ]].copy()
        for h in HORIZONS:
            for t in TARGETS:
                q=d[(d.horizon==h)&(d.target_pct==t)&(d.complete==True)].copy()
                if len(q):
                    touch=q["touch"].astype(bool)
                    ft=pd.to_numeric(q.loc[touch,"first_touch_day"],errors="coerce")
                    sums.append({
                        "group":label,"horizon":h,"target_pct":t,
                        "complete_n":len(q),"touch_n":int(touch.sum()),
                        "touch_rate_pct":float(touch.mean()*100),
                        "median_first_touch_day":float(ft.median()) if len(ft) else np.nan,
                        "mfe_median_pct":float(pd.to_numeric(q["mfe_pct"],errors="coerce").median()),
                        "mae_median_pct":float(pd.to_numeric(q["mae_pct"],errors="coerce").median()),
                        "close_ret_median_pct":float(pd.to_numeric(q["close_ret_pct"],errors="coerce").median()),
                        "giveback_median_pp":float(pd.to_numeric(q["giveback_from_mfe_to_close_pp"],errors="coerce").median()),
                    })
    summary=pd.DataFrame(sums)
    summary.to_csv(out/"target_touch_summary.csv",index=False,encoding="utf-8-sig")

    exec_rows=[]
    for _,r in detail[detail.complete==True].iterrows():
        exec_rows.append({
            "signal_date":r.signal_date,"code":r.code,"name":r["name"],"odoli_overlap":r.odoli_overlap,
            "horizon":r.horizon,"target_pct":r.target_pct,
            "standing_limit_sell_assumed_filled":bool(r.touch),
            "assumed_realized_return_pct":float(r.target_pct) if r.touch else np.nan,
            "first_touch_day":r.first_touch_day,
        })
    pd.DataFrame(exec_rows).to_csv(out/"standing_limit_sell_simulation.csv",index=False,encoding="utf-8-sig")

    report=[
        "# ODOLI R2.7 — Exit Research Sidecar","",
        "- Signal definition frozen.",
        "- Core definition frozen.",
        "- No automatic order execution; research-only simulation.",
        "- Entry assumption: signal-day close.",
        "- Exit assumption: standing limit sell at +3/+5/+7/+10/+15/+20%.",
        "- If daily High reaches the target, target exit is counted as filled.",
        "- No stop-loss/OCO simulation because daily OHLC cannot resolve intraday order sequence.","",
        "## Target-touch summary","```",summary.to_string(index=False),"```",
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")
    meta={
        "revision":REV,"research_only":True,"production_logic_changed":False,
        "automatic_ordering":False,"signal_definition_frozen":True,"core_definition_frozen":True,
        "targets":TARGETS,"horizons":HORIZONS,"core_n":len(core),
        "core_odoli_n":int(core.odoli_overlap.sum()),"core_without_odoli_n":int((~core.odoli_overlap).sum()),
        "intraday_sequence_resolved":False
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

if __name__=="__main__":
    main()
