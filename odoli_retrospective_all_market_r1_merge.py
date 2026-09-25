#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

VERSION="ODOLI_RETROSPECTIVE_ALL_MARKET_R1_MERGE_20260925"

def read_csv(p):
    try:return pd.read_csv(p,dtype={"code":str},low_memory=False)
    except Exception:return pd.DataFrame()

def rate(s):
    x=pd.to_numeric(s,errors="coerce").dropna()
    return float(x.mean()*100) if len(x) else np.nan

def summarize(q,label):
    if q.empty:return {"group":label,"n":0}
    return {
        "group":label,"n":len(q),
        "d5_complete_n":int(pd.to_numeric(q.get("d5_complete"),errors="coerce").fillna(0).astype(bool).sum()),
        "d5_plus5_rate_pct":rate(q.get("d5_hit_plus5",pd.Series(dtype=float))),
        "d10_plus5_rate_pct":rate(q.get("d10_hit_plus5",pd.Series(dtype=float))),
        "d5_positive_rate_pct":rate(pd.to_numeric(q.get("d5_close_ret_pct"),errors="coerce")>0),
        "d10_positive_rate_pct":rate(pd.to_numeric(q.get("d10_close_ret_pct"),errors="coerce")>0),
        "d5_ret_median_pct":float(pd.to_numeric(q.get("d5_close_ret_pct"),errors="coerce").median()),
        "d10_ret_median_pct":float(pd.to_numeric(q.get("d10_close_ret_pct"),errors="coerce").median()),
        "d5_mfe_median_pct":float(pd.to_numeric(q.get("d5_mfe_pct"),errors="coerce").median()),
        "d5_mae_median_pct":float(pd.to_numeric(q.get("d5_mae_pct"),errors="coerce").median()),
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--shards-root",required=True)
    ap.add_argument("--r13-root",default="")
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    xs=list(Path(a.shards_root).rglob("odoli_events.csv"))
    frames=[read_csv(p) for p in xs]
    frames=[x for x in frames if not x.empty]
    ev=pd.concat(frames,ignore_index=True) if frames else pd.DataFrame()
    if not ev.empty:
        ev["code"]=ev["code"].astype(str).str.replace(r"\.0$","",regex=True).str.zfill(6)
        ev["signal_date"]=pd.to_datetime(ev["signal_date"]).dt.strftime("%Y-%m-%d")
        ev=ev.sort_values(["signal_date","code"]).drop_duplicates(["signal_date","code"],keep="first")

    r13=pd.DataFrame()
    if a.r13_root:
        ps=list(Path(a.r13_root).rglob("shadow_outcome_refined_events.csv"))
        if ps:r13=read_csv(ps[0])
    if not ev.empty:
        ev["r13_overlap"]=False; ev["r13_pattern"]=""
        if not r13.empty:
            r13["code"]=r13["code"].astype(str).str.replace(r"\.0$","",regex=True).str.zfill(6)
            r13["signal_date"]=pd.to_datetime(r13["signal_date"]).dt.strftime("%Y-%m-%d")
            cols=["signal_date","code"]
            if "pattern" in r13.columns:cols.append("pattern")
            z=r13[cols].drop_duplicates(["signal_date","code"]).rename(columns={"pattern":"r13_pattern"})
            ev=ev.drop(columns=["r13_pattern"]).merge(z,on=["signal_date","code"],how="left")
            ev["r13_overlap"]=ev["r13_pattern"].notna()
            ev["r13_pattern"]=ev["r13_pattern"].fillna("")

    ev.to_csv(out/"odoli_all_market_events.csv",index=False,encoding="utf-8-sig")
    rows=[summarize(ev,"ALL_ODOLI")]
    if not ev.empty:
        rows.append(summarize(ev[ev["r13_overlap"]],"ODOLI_AND_R13_EXISTING_PATTERN"))
        rows.append(summarize(ev[~ev["r13_overlap"]],"ODOLI_NOT_IN_R13_523"))
        for pat,q in ev[ev["r13_overlap"]].groupby("r13_pattern"):
            rows.append(summarize(q,f"ODOLI_AND_R13_{pat}"))
    sm=pd.DataFrame(rows)
    sm.to_csv(out/"odoli_summary.csv",index=False,encoding="utf-8-sig")

    # Frozen descriptive quintiles only; they do not become gates.
    quant_rows=[]
    for col in ("ma5_slope_1d_pct","ma5_ma10_gap_pct","vol20_ratio","amount20_ratio","close_loc_pct"):
        if ev.empty or col not in ev:continue
        x=pd.to_numeric(ev[col],errors="coerce")
        try:
            bucket=pd.qcut(x,5,duplicates="drop")
        except Exception:continue
        for b,q in ev.assign(_bucket=bucket).dropna(subset=["_bucket"]).groupby("_bucket",observed=True):
            r=summarize(q,f"{col}:{b}")
            r["feature"]=col; r["bucket"]=str(b)
            quant_rows.append(r)
    pd.DataFrame(quant_rows).to_csv(out/"odoli_feature_quintiles.csv",index=False,encoding="utf-8-sig")

    path_counts=(ev["odoli_path_r1"].value_counts(dropna=False).rename_axis("path").reset_index(name="n") if not ev.empty else pd.DataFrame())
    path_counts.to_csv(out/"odoli_path_counts.csv",index=False,encoding="utf-8-sig")

    report=[
        "# ODOLI_RETROSPECTIVE_ALL_MARKET_R1",
        "",
        f"- events: {len(ev)}",
        f"- R1.3 523 overlap: {int(ev['r13_overlap'].sum()) if (not ev.empty and 'r13_overlap' in ev) else 0}",
        "- Main population: all historical KOSPI/KOSDAQ ODOLI events.",
        "- R1.3 overlap is secondary only; it is not the source population.",
        "- Feature quintiles are descriptive discovery outputs, not tuned production thresholds.",
        "- production/search/score/rank/order changes: 0",
        "",
        sm.to_string(index=False) if not sm.empty else "NO EVENTS",
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")
    meta={
        "version":VERSION,"research_only":True,"events":len(ev),
        "r13_overlap_n":int(ev["r13_overlap"].sum()) if (not ev.empty and "r13_overlap" in ev) else 0,
        "population_authority":"ALL_MARKET_ODOLI_EVENTS_NOT_R13_523",
        "feature_quintiles_descriptive_only":True,
        "production_logic_changed":False,
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(meta,ensure_ascii=False))

if __name__=="__main__":
    main()
