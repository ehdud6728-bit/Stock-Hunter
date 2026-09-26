#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd

REV="ODOLI_R2_9_PREDISCOVERY_HISTORICAL_VALIDATION_20260926"
START=pd.Timestamp("2026-03-18")
END=pd.Timestamp("2026-08-18")
TARGETS=[3,5,7,10,15,20]
HORIZONS=[1,3,5,10]

def find_one(root,name):
    xs=list(Path(root).rglob(name))
    if not xs: raise SystemExit(f"MISSING:{name}")
    return xs[0]

def norm_code(v):
    s=str(v or "").replace(".0","").strip()
    return s.zfill(6)

def safe_mean(x):
    x=pd.to_numeric(x,errors="coerce").dropna()
    return float(x.mean()) if len(x) else np.nan

def load_marcap(root):
    p=Path(root)/"data"/"marcap-2026.parquet"
    if not p.exists(): raise SystemExit(f"MISSING_MARCAP:{p}")
    q=pd.read_parquet(p)
    if "Date" not in q.columns: q=q.reset_index()
    q["Date"]=pd.to_datetime(q["Date"],errors="coerce").dt.normalize()
    q["Code"]=q["Code"].map(norm_code)
    q=q[q["Market"].astype(str).str.upper().isin(["KOSPI","KOSDAQ"])].copy()
    return q

def descriptors(g,sigdate,bins):
    g=g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
    hit=g.index[g["Date"].eq(sigdate)]
    if len(hit)!=1:return None
    i=int(hit[0])
    if i<25:return None

    pre10=g.iloc[i-10:i].copy()
    hist=g.iloc[:i+1].copy()
    close=pd.to_numeric(hist["Close"],errors="coerce")
    ma5=float(close.rolling(5).mean().iloc[-1])
    ma10=float(close.rolling(10).mean().iloc[-1])

    p10=pre10.copy()
    p10["ret"]=pd.to_numeric(p10["Close"],errors="coerce").pct_change()
    down=p10[p10["ret"]<0]; up=p10[p10["ret"]>0]
    da=safe_mean(down["Amount"]); ua=safe_mean(up["Amount"])
    dv=safe_mean(down["Volume"]); uv=safe_mean(up["Volume"])
    ar=da/ua if ua and ua>0 else np.nan
    vr=dv/uv if uv and uv>0 else np.nan

    def frozen_q(val,feature):
        b=np.array(bins.get(feature,[]),dtype=float)
        if len(b)<2 or not np.isfinite(val): return np.nan
        # pd.cut semantics used in R2.3/R2.4, labels 1..N
        q=pd.cut(pd.Series([val]),bins=b,labels=False,include_lowest=True).iloc[0]
        return float(q+1) if pd.notna(q) else np.nan

    aq=frozen_q(ar,"down_up_amount_ratio_10d")
    vq=frozen_q(vr,"down_up_volume_ratio_10d")
    return {
        "ma5_ma10_gap_pct_r23":(ma5/ma10-1)*100 if ma10>0 else np.nan,
        "down_up_amount_ratio_10d":ar,
        "down_up_volume_ratio_10d":vr,
        "down_up_amount_ratio_10d_q_frozen":aq,
        "down_up_volume_ratio_10d_q_frozen":vq,
        "AX_MA5_GT_MA10":bool(ma5>ma10),
        "AX_DOWN_AMOUNT_WEAK":bool(pd.notna(aq) and aq<=2),
        "AX_DOWN_VOLUME_WEAK":bool(pd.notna(vq) and vq<=2),
    }

def path_metrics(g,sigdate):
    g=g.sort_values("Date").drop_duplicates("Date",keep="last").reset_index(drop=True)
    hit=g.index[g["Date"].eq(sigdate)]
    if len(hit)!=1:return None
    i=int(hit[0]); sig=float(g.loc[i,"Close"])
    out={"signal_close":sig}
    future=[]
    for step in range(1,11):
        if i+step>=len(g): break
        r=g.loc[i+step]
        future.append({
            "day":step,
            "high_ret_pct":(float(r["High"])/sig-1)*100,
            "low_ret_pct":(float(r["Low"])/sig-1)*100,
            "close_ret_pct":(float(r["Close"])/sig-1)*100
        })
    p=pd.DataFrame(future)
    for h in HORIZONS:
        q=p[p["day"]<=h].copy()
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

def summary_row(g,label,h):
    c=f"d{h}_complete"
    d=g[g[c].fillna(False).astype(bool)].copy()
    r={"group":label,"horizon":h,"n":len(g),"complete_n":len(d)}
    if d.empty:return r
    close=pd.to_numeric(d[f"d{h}_close_ret_pct"],errors="coerce")
    mfe=pd.to_numeric(d[f"d{h}_mfe_pct"],errors="coerce")
    mae=pd.to_numeric(d[f"d{h}_mae_pct"],errors="coerce")
    r.update({
        "close_positive_rate_pct":float(close.gt(0).mean()*100),
        "close_median_pct":float(close.median()),
        "mfe_median_pct":float(mfe.median()),
        "mae_median_pct":float(mae.median()),
        "giveback_median_pp":float(pd.to_numeric(d[f"d{h}_giveback_pp"],errors="coerce").median()),
    })
    for t in TARGETS:
        r[f"touch_{t}_rate_pct"]=float(d[f"d{h}_touch_{t}"].fillna(False).astype(bool).mean()*100)
        ft=pd.to_numeric(d.loc[d[f"d{h}_touch_{t}"].fillna(False).astype(bool),f"d{h}_first_touch_day_{t}"],errors="coerce")
        r[f"touch_{t}_median_first_day"]=float(ft.median()) if len(ft) else np.nan
    return r

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--v4976-root",required=True)
    ap.add_argument("--odoli-root",required=True)
    ap.add_argument("--r23-root",required=True)
    ap.add_argument("--marcap-root",required=True)
    ap.add_argument("--output-dir",required=True)
    a=ap.parse_args()
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)

    src=pd.read_csv(find_one(a.v4976_root,"v49_76_selected_enriched_outcomes.csv"),dtype={"code":str},low_memory=False)
    src["code"]=src["code"].map(norm_code)
    src["signal_date"]=pd.to_datetime(src["signal_date"],errors="coerce").dt.normalize()
    src["primary_strategy"]=src["primary_strategy"].astype(str).str.upper().str.strip()

    # Frozen upstream label: do NOT infer/reclassify A.
    A=src[(src["signal_date"]>=START)&(src["signal_date"]<=END)&(src["primary_strategy"].eq("A"))].copy()
    A=A.sort_values(["signal_date","code"]).drop_duplicates(["signal_date","code"],keep="first")

    od=pd.read_csv(find_one(a.odoli_root,"odoli_all_market_events.csv"),dtype={"code":str},low_memory=False)
    od["code"]=od["code"].map(norm_code)
    od["signal_date"]=pd.to_datetime(od["signal_date"],errors="coerce").dt.normalize()
    odkeys=set(zip(od["signal_date"],od["code"]))
    A["odoli_overlap"]=[(d,c) in odkeys for d,c in zip(A["signal_date"],A["code"])]

    bins=json.loads(find_one(a.r23_root,"a_context_bins.json").read_text(encoding="utf-8"))
    mar=load_marcap(a.marcap_root)
    bycode={c:g.copy() for c,g in mar.groupby("Code",sort=False)}

    rows=[]
    for _,r in A.iterrows():
        rec={
            "signal_date":r["signal_date"],"code":r["code"],"name":r.get("name",""),
            "primary_strategy":"A","odoli_overlap":bool(r["odoli_overlap"])
        }
        g=bycode.get(r["code"])
        d=descriptors(g,r["signal_date"],bins) if g is not None else None
        p=path_metrics(g,r["signal_date"]) if g is not None else None
        if d: rec.update(d)
        if p: rec.update(p)
        # frozen source outcomes retained for cross-check only
        for c in ["ret_max_high_1d","ret_max_high_3d","ret_max_high_5d","ret_max_high_10d",
                  "ret_close_1d","ret_close_3d","ret_close_5d","ret_close_10d","eval_available_days"]:
            if c in r.index: rec["source_"+c]=r[c]
        rows.append(rec)
    z=pd.DataFrame(rows)

    z["AX_PARTICIPATION_WEAK"]=z["AX_DOWN_AMOUNT_WEAK"].fillna(False).astype(bool) | z["AX_DOWN_VOLUME_WEAK"].fillna(False).astype(bool)
    z["is_CORE"]=z["AX_MA5_GT_MA10"].fillna(False).astype(bool) & z["AX_PARTICIPATION_WEAK"]
    z["is_CORE_ODOLI"]=z["is_CORE"] & z["odoli_overlap"]

    z.to_csv(out/"prediscovery_a_events_with_frozen_core.csv",index=False,encoding="utf-8-sig")
    z[z["odoli_overlap"]].to_csv(out/"prediscovery_a_odoli_events.csv",index=False,encoding="utf-8-sig")
    z[z["is_CORE_ODOLI"]].to_csv(out/"prediscovery_core_odoli_events.csv",index=False,encoding="utf-8-sig")

    groups=[
        ("A_ALL",z),
        ("A_AND_ODOLI",z[z["odoli_overlap"]]),
        ("A_WITHOUT_ODOLI",z[~z["odoli_overlap"]]),
        ("CORE_ALL",z[z["is_CORE"]]),
        ("CORE_AND_ODOLI",z[z["is_CORE_ODOLI"]]),
        ("CORE_WITHOUT_ODOLI",z[z["is_CORE"] & ~z["odoli_overlap"]]),
    ]
    s=[]
    for label,g in groups:
        for h in HORIZONS:
            s.append(summary_row(g,label,h))
    summ=pd.DataFrame(s)
    summ.to_csv(out/"prediscovery_validation_summary.csv",index=False,encoding="utf-8-sig")

    # Cross-check our refreshed high/close against source outcomes for overlapping completed horizons.
    checks=[]
    for h in HORIZONS:
        for kind,ours,src_col in [
            ("mfe",f"d{h}_mfe_pct",f"source_ret_max_high_{h}d"),
            ("close",f"d{h}_close_ret_pct",f"source_ret_close_{h}d"),
        ]:
            if ours in z.columns and src_col in z.columns:
                x=pd.to_numeric(z[ours],errors="coerce")
                y=pd.to_numeric(z[src_col],errors="coerce")
                m=x.notna()&y.notna()
                diff=(x[m]-y[m]).abs()
                checks.append({
                    "horizon":h,"metric":kind,"matched_n":int(m.sum()),
                    "median_abs_diff_pp":float(diff.median()) if len(diff) else np.nan,
                    "max_abs_diff_pp":float(diff.max()) if len(diff) else np.nan,
                })
    pd.DataFrame(checks).to_csv(out/"source_outcome_crosscheck.csv",index=False,encoding="utf-8-sig")

    counts={
        "a_events":len(z),
        "a_signal_dates":int(z["signal_date"].nunique()),
        "a_odoli":int(z["odoli_overlap"].sum()),
        "core":int(z["is_CORE"].sum()),
        "core_odoli":int(z["is_CORE_ODOLI"].sum()),
        "core_without_odoli":int((z["is_CORE"] & ~z["odoli_overlap"]).sum()),
    }

    report=[
        "# ODOLI R2.9 — Pre-discovery historical validation","",
        f"- Validation window: {START.date()} ~ {END.date()}.",
        "- A membership authority: frozen upstream v49.76 `primary_strategy == A`; no A reclassification.",
        "- ODOLI authority: frozen PIT all-market R1.",
        "- CORE authority: R2.3 frozen A97 bins + MA5>MA10; no percentile recomputation on this historical period.",
        "- This period predates the 2026-08-19 discovery window.",
        "- Research-only. No production/search/rank/order changes.","",
        "## Counts",
        "```",json.dumps(counts,ensure_ascii=False,indent=2),"```","",
        "## Outcome / target-touch summary",
        "```",summ.to_string(index=False),"```"
    ]
    (out/"REPORT.md").write_text("\n".join(report),encoding="utf-8")

    meta={
        "revision":REV,"research_only":True,"production_logic_changed":False,
        "a_membership_reclassified":False,"core_thresholds_recomputed":False,
        "same_sample_threshold_tuning":False,
        "validation_start":str(START.date()),"validation_end":str(END.date()),
        "historical_validation_role":"PRE_DISCOVERY_OUT_OF_PERIOD",
        "true_live_oos":False,
        **counts
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

if __name__=="__main__":
    main()
